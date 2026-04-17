use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};

use crate::{
    Delivery, DeliveryInspector, DeliveryState, EventBusError, Message, SubscriptionConfig,
    HEADER_DEAD_LETTER_REASON, HEADER_RETRY_ATTEMPT, HEADER_RETRY_REASON,
};

use super::ack_flusher::AckRequest;
use super::backend::{SharedBackend, StreamBackend};

/// Single message in flight.
///
/// The `_permit` field is an [`OwnedSemaphorePermit`] whose `Drop` returns the
/// slot to `RuntimeState::limiter`. This is the single source of truth for
/// in-flight accounting: the permit is held for the full handler + ack
/// round-trip, and auto-released on every termination path (ack, nack,
/// retry, handler panic, task cancel, Delivery dropped without finalize).
///
/// Acks are batched: `mark_acked` sends the message ID to a background
/// flusher via `ack_tx`, then awaits a oneshot for the result. This turns
/// N per-message XACK round-trips into ~(N / batch_size) batched commands.
pub(super) struct StreamDelivery<B: StreamBackend> {
    backend: SharedBackend<B>,
    ack_tx: mpsc::Sender<AckRequest>,
    message_id: String,
    message: Arc<Message>,
    state: DeliveryState,
    config: Arc<SubscriptionConfig>,
    finalized: AtomicBool,
    _permit: OwnedSemaphorePermit,
}

impl<B: StreamBackend> StreamDelivery<B> {
    pub(super) fn new(
        backend: SharedBackend<B>,
        ack_tx: mpsc::Sender<AckRequest>,
        message_id: String,
        message: Arc<Message>,
        state: DeliveryState,
        config: Arc<SubscriptionConfig>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            backend,
            ack_tx,
            message_id,
            message,
            state,
            config,
            finalized: AtomicBool::new(false),
            _permit: permit,
        }
    }

    pub(super) fn is_finalized(&self) -> bool {
        self.finalized.load(Ordering::Acquire)
    }

    async fn mark_acked(&self) -> Result<(), EventBusError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.ack_tx
            .send(AckRequest {
                id: self.message_id.clone(),
                done: done_tx,
            })
            .await
            .map_err(|_| EventBusError::Internal("ack flusher shut down".into()))?;
        done_rx
            .await
            .map_err(|_| EventBusError::Internal("ack flusher dropped response".into()))?
    }

    async fn publish_dead_letter(&self, reason: &str) -> Result<(), EventBusError> {
        if let Some(dead_letter_topic) = self.config.dead_letter_topic.as_deref() {
            let mut dead_letter = Message::clone(&self.message);
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

impl<B: StreamBackend> DeliveryInspector for StreamDelivery<B> {
    async fn state(&self) -> Result<DeliveryState, EventBusError> {
        Ok(self.state.clone())
    }
}

impl<B: StreamBackend> Delivery for StreamDelivery<B> {
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
            let mut retried = Message::clone(&self.message);
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

        Ok(())
    }
}
