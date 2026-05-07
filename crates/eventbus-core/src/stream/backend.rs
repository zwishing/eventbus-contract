use std::{future::Future, sync::Arc, time::Duration};

use crate::{EventBusError, Message, PartialDeliveryState};

#[derive(Debug, Clone)]
pub struct ClaimedMessage {
    pub id: String,
    pub message: Arc<Message>,
    /// Backend-supplied half of the delivery state. The bus layer combines
    /// this with the subscription's retry budget to produce the full
    /// [`crate::DeliveryState`] handed to handlers.
    pub state: PartialDeliveryState,
}

/// One result per Redis Stream entry returned from `read_new` / `reclaim_idle`.
///
/// Backends report decode failures **per entry** instead of poisoning the
/// whole batch with a single `Result<Vec<_>, _>` short-circuit. Otherwise a
/// single corrupt PEL entry would loop forever:
///
/// - `XREADGROUP` already moved the entry into the consumer's PEL.
/// - The bus would receive `Err`, log/observe, back off, retry.
/// - On the next cycle the same entry would re-decode → fail again.
///
/// With per-entry results the bus can ack the bad entry (so it leaves the
/// PEL), publish a synthetic dead-letter envelope (when `dead_letter_topic`
/// is configured), and surface the error to the [`crate::ErrorObserver`].
#[derive(Debug)]
pub enum FetchedEntry {
    Decoded(ClaimedMessage),
    Malformed { id: String, error: EventBusError },
}

pub trait StreamBackend: Send + Sync + 'static {
    fn create_group(
        &self,
        stream: &str,
        group: &str,
        start_id: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send;

    fn publish(
        &self,
        stream: &str,
        message: Message,
    ) -> impl Future<Output = Result<String, EventBusError>> + Send;

    fn reclaim_idle(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        count: usize,
    ) -> impl Future<Output = Result<Vec<FetchedEntry>, EventBusError>> + Send;

    fn read_new(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<FetchedEntry>, EventBusError>> + Send;

    fn ack(
        &self,
        stream: &str,
        group: &str,
        message_id: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send;

    /// Batch-acknowledge multiple message IDs in a single round-trip.
    ///
    /// Redis Streams `XACK` accepts N IDs in one command, turning what would
    /// be N RTTs into one. Backends without native batch support can rely on
    /// the default implementation (a serial loop over [`Self::ack`]), but the point
    /// of this method is to let the Redis backend collapse the round-trips.
    ///
    /// # Contract
    /// - Returning `Ok(())` means the whole batch was accepted by the server.
    ///   It does *not* prove every ID existed in the PEL — Redis silently
    ///   ignores unknown IDs. Callers must treat this as at-least-once.
    /// - An `Err` signals the batch did not reach the server, so the caller
    ///   should surface the same error to every waiter; messages will be
    ///   re-claimed on the next cycle.
    fn ack_many(
        &self,
        stream: &str,
        group: &str,
        message_ids: &[String],
    ) -> impl Future<Output = Result<(), EventBusError>> + Send {
        async move {
            for id in message_ids {
                self.ack(stream, group, id).await?;
            }
            Ok(())
        }
    }

    /// Drop any per-(stream, group, consumer) state cached inside the backend
    /// (e.g., XAUTOCLAIM cursors). Called by [`crate::stream::StreamBus`] on subscription
    /// shutdown so backends do not accumulate cursor entries indefinitely
    /// under churn (auto-generated consumer names, restarting pods, etc.).
    ///
    /// The default impl is a no-op; backends without per-consumer state can
    /// ignore it.
    #[allow(unused_variables)]
    fn forget_consumer(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }
}

pub(crate) type SharedBackend<B> = Arc<B>;
