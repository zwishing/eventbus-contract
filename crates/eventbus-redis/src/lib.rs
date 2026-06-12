//! Redis Streams [`StreamBackend`](eventbus_core::stream::StreamBackend) for
//! the [`eventbus`](https://docs.rs/eventbus) facade.
//!
//! - [`RedisBackend`] — production backend over `XADD` / `XREADGROUP` / `XACK`
//!   / `XAUTOCLAIM`, suitable for at-least-once delivery with consumer groups.
//! - [`JsonCodec`] — default JSON [`Codec`](eventbus_core::Codec) for envelope
//!   payloads.
//!
//! Enable via `eventbus-contract = { version = "0.2", features = ["redis"] }`. Pair
//! with [`StreamBus`](eventbus_core::stream::StreamBus) to obtain a
//! `Publisher + Subscriber`. The `tls` feature wires `redis` over native-TLS.

pub mod codec;
pub mod redis;

#[cfg(feature = "watermill")]
pub use codec::WatermillStreamCodec;
pub use codec::{
    AutoDetectRedisStreamCodec, DecodeContext, EncodeContext, EventbusJsonStreamCodec, JsonCodec,
    RedisStreamCodec,
};
pub use redis::{stream_bus_from_connection, RedisBackend};
