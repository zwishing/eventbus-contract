//! In-process [`StreamBackend`](eventbus_core::stream::StreamBackend) for the
//! [`eventbus`](https://docs.rs/eventbus) facade.
//!
//! - Re-exports [`MemoryStreamBackend`] from `eventbus-core` behind the
//!   `test-utils` feature (default-on).
//! - Intended for tests, examples, and local-dev workflows — disable in
//!   production builds that should not link an in-memory queue.
//! - Pair with [`StreamBus`](eventbus_core::stream::StreamBus) to get a fully
//!   functional `Publisher + Subscriber` without external services.
//!
//! See the facade crate [`eventbus`](https://docs.rs/eventbus) for the
//! recommended consumption surface.

#[cfg(feature = "test-utils")]
pub use eventbus_core::stream::MemoryStreamBackend;
