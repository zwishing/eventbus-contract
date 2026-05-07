//! In-process [`StreamBackend`](eventbus_core::stream::StreamBackend) implementation.
//!
//! Behind the `test-utils` feature (default-on). Disable in production
//! consumers that should not link the in-memory backend.

#[cfg(feature = "test-utils")]
pub use eventbus_core::stream::MemoryStreamBackend;
