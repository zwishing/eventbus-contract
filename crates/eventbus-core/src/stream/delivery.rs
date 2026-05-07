use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};

use crate::{
    BoxedError, Delivery, DeliveryControl, DeliveryInspector, DeliveryState, EventBusError,
    Message, SubscriptionConfig, HEADER_DEAD_LETTER_REASON, HEADER_RETRY_ATTEMPT,
    HEADER_RETRY_REASON,
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
///
/// Finalization is enforced by the type system: [`DeliveryControl`] methods
/// take `Box<Self>`, so calling `ack()` consumes the box and the compiler
/// rejects any subsequent call. No runtime guard is needed.
pub(super) struct StreamDelivery<B: StreamBackend> {
    backend: SharedBackend<B>,
    ack_tx: mpsc::Sender<AckRequest>,
    message_id: String,
    message: Arc<Message>,
    state: DeliveryState,
    config: Arc<SubscriptionConfig>,
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
            _permit: permit,
        }
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

    /// AutoOnReceive seam: ack the message *before* delivering to the handler.
    /// Internally just runs `mark_acked` without touching `Self`'s ownership.
    /// If the handler later calls `ack()` on its `Box<Self>`, the redundant
    /// XACK is treated as idempotent at the backend layer.
    pub(super) async fn pre_ack(&self) -> Result<(), EventBusError> {
        self.mark_acked().await
    }
}

impl<B: StreamBackend> DeliveryInspector for StreamDelivery<B> {
    fn state(&self) -> crate::BoxFuture<'_, Result<DeliveryState, EventBusError>> {
        Box::pin(async move { Ok(self.state.clone()) })
    }
}

impl<B: StreamBackend> Delivery for StreamDelivery<B> {
    fn message(&self) -> &Message {
        &self.message
    }
}

impl<B: StreamBackend> DeliveryControl for StreamDelivery<B> {
    fn ack(self: Box<Self>) -> crate::BoxFuture<'static, Result<(), EventBusError>> {
        Box::pin(async move { self.mark_acked().await })
    }

    fn nack(
        self: Box<Self>,
        reason: BoxedError,
    ) -> crate::BoxFuture<'static, Result<(), EventBusError>> {
        Box::pin(async move {
            let reason_str = reason.to_string();
            self.publish_dead_letter(&reason_str).await?;
            self.mark_acked().await
        })
    }

    fn retry(
        self: Box<Self>,
        reason: BoxedError,
    ) -> crate::BoxFuture<'static, Result<(), EventBusError>> {
        Box::pin(async move {
            let retry_exhausted = self.state.attempt >= self.state.max_attempt;
            let reason_str = reason.to_string();

            if retry_exhausted {
                if self.config.dead_letter_topic.is_none() {
                    return Err(EventBusError::Validation(format!(
                        "retry exhausted ({}/{}) but no dead_letter_topic configured for topic {}",
                        self.state.attempt, self.state.max_attempt, self.config.topic,
                    )));
                }
                self.publish_dead_letter(&reason_str).await?;
            } else {
                let mut retried = Message::clone(&self.message);
                retried.headers.insert(
                    HEADER_RETRY_ATTEMPT.to_string(),
                    self.state.attempt.to_string(),
                );
                retried
                    .headers
                    .insert(HEADER_RETRY_REASON.to_string(), reason_str);

                self.backend.publish(&self.config.topic, retried).await?;
            }
            self.mark_acked().await
        })
    }
}
