# eventbus-redis

[![Crates.io](https://img.shields.io/crates/v/eventbus-redis.svg)](https://crates.io/crates/eventbus-redis)
[![Docs.rs](https://docs.rs/eventbus-redis/badge.svg)](https://docs.rs/eventbus-redis)

Redis Streams `StreamBackend` for the [eventbus-contract](https://crates.io/crates/eventbus-contract) workspace, plus `JsonCodec` (Go-wire-compatible envelope).

## Use via the facade (recommended)

```toml
[dependencies]
eventbus-contract = { version = "0.2", features = ["redis"] }
```

```rust
use std::sync::Arc;
use eventbus_contract::core::stream::StreamBusOptions;
use eventbus_contract::redis::{stream_bus_from_connection, RedisBackend, JsonCodec};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = redis::Client::open("redis://127.0.0.1/")?;
let conn = client.get_multiplexed_async_connection().await?;
let bus = stream_bus_from_connection(conn, StreamBusOptions::default())?;
# Ok(()) }
```

## Features

- `default = []` — base Redis support over `redis-rs` `tokio-comp`.
- `tls` — enable `rediss://` connections via `redis/tls-native-tls` + `redis/tokio-native-tls-comp`. Use `rediss://` URLs in production and ensure CA validation.

## Wire format

`JsonCodec` is the default. It encodes each `Message` inside a `{"message":{...}}` envelope stored in the `"message"` field of each Stream entry — wire-compatible with the Go `StreamBus`. Swap with `RedisBackend::with_codec(conn, codec)` when wire-compat is not required and a binary codec is preferred.

## Connection / TLS / auth

`RedisBackend::new` takes an already-connected `MultiplexedConnection`; the caller chooses the URL and any TLS / auth settings. The crate does not require, default to, or downgrade TLS.

## License

MIT
