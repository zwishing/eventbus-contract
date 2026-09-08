use eventbus_integration::{EventPublisher, IntegrationEvent, MessageFactory};

fn require_send(_: impl Send) {}

#[allow(dead_code)]
fn integration_contract<F: MessageFactory, P: EventPublisher, E: IntegrationEvent>(
    factory: &F,
    publisher: &P,
    event: &E,
    events: &[E],
) {
    require_send(factory.new_message(event));
    require_send(factory.new_messages(events));
    require_send(publisher.publish_event(event));
    require_send(publisher.publish_events(events));
}
