//! Observability hooks for the [`StreamBus`](super::StreamBus) runtime.
//!
//! Without an observer the bus silently retries / backs off on transient
//! errors so steady-state traffic isn't poisoned by occasional failures.
//! Production deployments usually want those errors surfaced to metrics,
//! tracing, or alerting — that's what [`ErrorObserver`] is for.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
//! use eventbus_core::{EventBusError, ErrorScope, ErrorObserver};
//! use eventbus_core::stream::StreamBusOptions;
//!
//! struct Counter(AtomicU64);
//!
//! impl ErrorObserver for Counter {
//!     fn on_error(&self, scope: ErrorScope, err: &EventBusError) {
//!         eprintln!("[bus:{scope:?}] {err}");
//!         self.0.fetch_add(1, Ordering::Relaxed);
//!     }
//! }
//!
//! let opts = StreamBusOptions::default()
//!     .with_error_observer(Arc::new(Counter(AtomicU64::new(0))));
//! ```

use crate::EventBusError;

/// Where in the bus runtime an error was raised.
///
/// `#[non_exhaustive]` lets us add new sources without breaking observers.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorScope {
    /// `XREADGROUP` (or backend equivalent) failed; the consume loop will
    /// back off and retry.
    Read,
    /// The reclaim task failed to fetch idle pending entries; the task will
    /// back off and retry.
    Reclaim,
    /// A batched ack flush to the backend failed. The waiters got the error
    /// via their oneshot channels; this hook fires once for the whole batch.
    AckFlush,
    /// Subscription was dropped without `close()` having been called. Fired
    /// at most once from the [`Drop`] impl on `StreamSubscription`.
    Drop,
    /// A delivery task panicked. The panic message is delivered to
    /// [`ErrorObserver::on_panic`]; this scope tags any associated
    /// `on_error` invocation.
    HandlerPanic,
}

/// Receives bus-level transient errors so they can be surfaced to metrics
/// or tracing.
///
/// Implementations **must not block** — the hook is called from inside the
/// consume / reclaim / ack loops. Push the event onto a queue or counter
/// and return.
pub trait ErrorObserver: Send + Sync {
    fn on_error(&self, scope: ErrorScope, err: &EventBusError);

    /// Called when a delivery task panics. Default empty for backwards
    /// compatibility — implementors can override to route to crash metrics.
    fn on_panic(&self, _scope: ErrorScope, _payload: &str) {}
}
