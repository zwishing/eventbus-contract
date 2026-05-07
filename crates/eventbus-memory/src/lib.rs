//! In-process [`StreamBackend`](eventbus_core::stream::StreamBackend) for the
//! [`eventbus`](https://docs.rs/eventbus) facade.
//!
//! Behind the `test-utils` feature (default-on) for parity with backend
//! crates that ship test fixtures. Disable in production consumers that
//! should not link the in-memory backend.
//!
//! Pair with [`StreamBus`](eventbus_core::stream::StreamBus) to get a fully
//! functional `Publisher + Subscriber` without external services.

#[cfg(feature = "test-utils")]
pub mod memory;
#[cfg(feature = "test-utils")]
pub use memory::MemoryStreamBackend;
