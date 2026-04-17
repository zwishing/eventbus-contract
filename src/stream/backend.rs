use std::{future::Future, sync::Arc, time::Duration};

use crate::{DeliveryState, EventBusError, Message};

#[derive(Debug, Clone)]
pub struct ClaimedMessage {
    pub id: String,
    pub message: Arc<Message>,
    pub state: DeliveryState,
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
    ) -> impl Future<Output = Result<Vec<ClaimedMessage>, EventBusError>> + Send;

    fn read_new(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<ClaimedMessage>, EventBusError>> + Send;

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
    /// the default implementation (a serial loop over [`ack`]), but the point
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
}

pub(crate) type SharedBackend<B> = Arc<B>;
