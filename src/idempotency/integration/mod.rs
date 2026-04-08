use crate::error::EventBusError;
use crate::eventbus::{Message, Topic};

pub trait IntegrationEvent: Send + Sync {
    fn event_topic(&self) -> &Topic;
    fn event_key(&self) -> &str;
    fn event_kind(&self) -> &str;
}

pub trait MessageFactory: Send + Sync {
    async fn new_message<E>(&self, event: &E) -> Result<Message, EventBusError>
    where
        E: IntegrationEvent + Sync;

    async fn new_messages<E>(&self, events: &[E]) -> Result<Vec<Message>, EventBusError>
    where
        E: IntegrationEvent + Sync;
}

pub trait EventPublisher: Send + Sync {
    async fn publish_event<E>(&self, event: &E) -> Result<(), EventBusError>
    where
        E: IntegrationEvent + Sync;

    async fn publish_events<E>(&self, events: &[E]) -> Result<(), EventBusError>
    where
        E: IntegrationEvent + Sync;
}
