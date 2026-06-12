//! Redis Streams [`StreamBackend`](eventbus_core::stream::StreamBackend) for
//! the [`eventbus`](https://docs.rs/eventbus) facade.
//!
//! - [`RedisBackend`] - production backend over `XADD` / `XREADGROUP` /
//!   `XACK` / `XAUTOCLAIM`, suitable for at-least-once delivery with consumer
//!   groups.
//! - [`JsonCodec`] - default JSON [`Codec`](eventbus_core::Codec) for envelope
//!   payloads.
//! - [`RedisStreamCodec`] - Redis field-map codec boundary used for
//!   per-stream, per-group, and per-subscription wire-format selection.
//! - `WatermillStreamCodec` - optional `watermill` feature codec for Go
//!   Watermill canonical entries (`_watermill_message_uuid`, `metadata`,
//!   `payload`).
//! - [`AutoDetectRedisStreamCodec`] - read-only codec that tries registered
//!   stream codecs in order, useful for opt-in mixed-format streams.
//!
//! Enable via `eventbus-contract = { version = "0.2", features = ["redis"] }`.
//! Pair with [`StreamBus`](eventbus_core::stream::StreamBus) to obtain a
//! `Publisher + Subscriber`. The `tls` feature wires `redis` over native-TLS.
//! For Watermill compatibility through the facade, enable
//! `eventbus-contract = { version = "0.2", features = ["redis-watermill"] }`
//! and register the Watermill read codec on the stream that consumes Go
//! Watermill entries.

pub mod codec;
pub mod redis;

#[cfg(feature = "watermill")]
pub use codec::WatermillStreamCodec;
pub use codec::{
    AutoDetectRedisStreamCodec, DecodeContext, EncodeContext, EventbusJsonStreamCodec, JsonCodec,
    RedisStreamCodec,
};
pub use redis::{stream_bus_from_connection, RedisBackend};
