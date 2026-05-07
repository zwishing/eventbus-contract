//! Compile-time witnesses that public traits remain object-safe.
//! If any of these stop compiling, the public API is regressing.

use std::sync::Arc;

use eventbus_core::{Bus, Handler, Publisher, Subscriber, Subscription};

#[allow(dead_code)]
fn assert_publisher_dyn(_: &dyn Publisher) {}

#[allow(dead_code)]
fn assert_subscriber_dyn(_: &dyn Subscriber) {}

#[allow(dead_code)]
fn assert_handler_dyn(_: Arc<dyn Handler>) {}

#[allow(dead_code)]
fn assert_subscription_dyn(_: Arc<dyn Subscription>) {}

#[allow(dead_code)]
fn assert_bus_dyn(_: Arc<dyn Bus>) {}

#[test]
fn dyn_safety_witness_compiles() {
    // The presence of the assert_* fns at compile time IS the test.
}
