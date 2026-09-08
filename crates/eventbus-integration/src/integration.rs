use std::future::Future;

use eventbus_core::{EventBusError, Message};

pub trait IntegrationEvent: Send + Sync {
    fn event_topic(&self) -> &str;
    fn event_key(&self) -> &str;
    fn event_kind(&self) -> &str;
}

pub trait MessageFactory: Send + Sync {
    fn new_message<E>(
        &self,
        event: &E,
    ) -> impl Future<Output = Result<Message, EventBusError>> + Send
    where
        E: IntegrationEvent + Sync;

    fn new_messages<E>(
        &self,
        events: &[E],
    ) -> impl Future<Output = Result<Vec<Message>, EventBusError>> + Send
    where
        E: IntegrationEvent + Sync;
}

pub trait EventPublisher: Send + Sync {
    fn publish_event<E>(&self, event: &E) -> impl Future<Output = Result<(), EventBusError>> + Send
    where
        E: IntegrationEvent + Sync;

    fn publish_events<E>(
        &self,
        events: &[E],
    ) -> impl Future<Output = Result<(), EventBusError>> + Send
    where
        E: IntegrationEvent + Sync;
}
