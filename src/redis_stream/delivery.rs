use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::{
    Delivery, DeliveryInspector, DeliveryState, EventBusError, Message, SubscriptionConfig,
    HEADER_DEAD_LETTER_REASON, HEADER_RETRY_ATTEMPT, HEADER_RETRY_REASON,
};

use super::{
    backend::{SharedBackend, StreamBackend},
    bus::PendingAckTracker,
};

pub(super) struct RedisStreamDelivery<B: StreamBackend> {
    backend: SharedBackend<B>,
    message_id: String,
    message: Message,
    state: DeliveryState,
    config: SubscriptionConfig,
    pending_ack_tracker: Arc<PendingAckTracker>,
    finalized: AtomicBool,
}

impl<B: StreamBackend> RedisStreamDelivery<B> {
    pub(super) fn new(
        backend: SharedBackend<B>,
        message_id: String,
        message: Message,
        state: DeliveryState,
        config: SubscriptionConfig,
        pending_ack_tracker: Arc<PendingAckTracker>,
    ) -> Self {
        Self {
            backend,
            message_id,
            message,
            state,
            config,
            pending_ack_tracker,
            finalized: AtomicBool::new(false),
        }
    }

    pub(super) fn is_finalized(&self) -> bool {
        self.finalized.load(Ordering::Acquire)
    }

    async fn mark_acked(&self) -> Result<(), EventBusError> {
        self.backend
            .ack(
                &self.config.topic,
                &self.config.consumer_group,
                &self.message_id,
            )
            .await
    }

    async fn publish_dead_letter(&self, reason: &str) -> Result<(), EventBusError> {
        if let Some(dead_letter_topic) = self.config.dead_letter_topic.as_deref() {
            let mut dead_letter = self.message.clone();
            dead_letter.topic = dead_letter_topic.to_string();
            dead_letter
                .headers
                .insert(HEADER_DEAD_LETTER_REASON.to_string(), reason.to_string());
            self.backend.publish(dead_letter_topic, dead_letter).await?;
        }

        Ok(())
    }

    fn begin_finalize(&self) -> bool {
        self.finalized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn reset_finalize(&self) {
        self.finalized.store(false, Ordering::Release);
    }
}

impl<B: StreamBackend> DeliveryInspector for RedisStreamDelivery<B> {
    async fn state(&self) -> Result<DeliveryState, EventBusError> {
        Ok(self.state.clone())
    }
}

impl<B: StreamBackend> Delivery for RedisStreamDelivery<B> {
    fn message(&self) -> &Message {
        &self.message
    }

    async fn ack(&self) -> Result<(), EventBusError> {
        if !self.begin_finalize() {
            return Ok(());
        }

        if let Err(err) = self.mark_acked().await {
            self.reset_finalize();
            return Err(err);
        }

        self.pending_ack_tracker.release(&self.message_id).await;
        Ok(())
    }

    async fn nack(
        &self,
        reason: &(dyn std::error::Error + Send + Sync),
    ) -> Result<(), EventBusError> {
        if !self.begin_finalize() {
            return Ok(());
        }

        if let Err(err) = self.publish_dead_letter(&reason.to_string()).await {
            self.reset_finalize();
            return Err(err);
        }

        if let Err(err) = self.mark_acked().await {
            self.reset_finalize();
            return Err(err);
        }

        self.pending_ack_tracker.release(&self.message_id).await;
        Ok(())
    }

    async fn retry(
        &self,
        reason: &(dyn std::error::Error + Send + Sync),
    ) -> Result<(), EventBusError> {
        if !self.begin_finalize() {
            return Ok(());
        }

        let retry_exhausted = self.state.attempt >= self.state.max_attempt;

        if retry_exhausted {
            if let Err(err) = self.publish_dead_letter(&reason.to_string()).await {
                self.reset_finalize();
                return Err(err);
            }
        } else {
            let mut retried = self.message.clone();
            retried.headers.insert(
                HEADER_RETRY_ATTEMPT.to_string(),
                self.state.attempt.to_string(),
            );
            retried
                .headers
                .insert(HEADER_RETRY_REASON.to_string(), reason.to_string());

            if let Err(err) = self.backend.publish(&self.config.topic, retried).await {
                self.reset_finalize();
                return Err(err);
            }
        }

        if let Err(err) = self.mark_acked().await {
            self.reset_finalize();
            return Err(err);
        }

        self.pending_ack_tracker.release(&self.message_id).await;
        Ok(())
    }
}
