# eventbus-redis

[![Crates.io](https://img.shields.io/crates/v/eventbus-redis.svg)](https://crates.io/crates/eventbus-redis)
[![Docs.rs](https://docs.rs/eventbus-redis/badge.svg)](https://docs.rs/eventbus-redis)

Redis Streams `StreamBackend` for the [eventbus-contract](https://crates.io/crates/eventbus-contract) workspace, plus JSON and Redis field-level stream codecs.

## Use via the facade (recommended)

```toml
[dependencies]
eventbus-contract = { version = "0.2", features = ["redis"] }
```

```rust
use eventbus_contract::core::stream::StreamBusOptions;
use eventbus_contract::redis::stream_bus_from_connection;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = redis::Client::open("redis://127.0.0.1/")?;
let conn = client.get_multiplexed_async_connection().await?;
let bus = stream_bus_from_connection(conn, StreamBusOptions::default())?;
# Ok(()) }
```

## Features

- `default = []` - base Redis support over `redis-rs` `tokio-comp`.
- `tls` - enable `rediss://` connections via `redis/tls-native-tls` + `redis/tokio-native-tls-comp`. Use `rediss://` URLs in production and ensure CA validation.
- `watermill` - add `WatermillStreamCodec` and `AutoDetectRedisStreamCodec::with_watermill()` for reading Go Watermill canonical Redis Stream entries.

## Wire format

`JsonCodec` is the default. It encodes each `Message` inside a `{"message":{...}}` envelope stored in the `"message"` field of each Stream entry. Existing Redis users keep this behavior unless they register another stream codec.

For Watermill-produced streams, enable the feature and register the codec per stream:

```toml
[dependencies]
eventbus-redis = { version = "0.2", features = ["watermill"] }
```

```rust
use std::sync::Arc;
use eventbus_redis::{RedisBackend, WatermillStreamCodec};

# fn configure(backend: &RedisBackend) {
backend.set_stream_read_codec("mapset.mosaic", Arc::new(WatermillStreamCodec));
# }
```

Watermill canonical entries are decoded from the whole Redis field map:

```text
_watermill_message_uuid = raw UTF-8 string
metadata                 = msgpack map<string,string>
payload                  = raw bytes
```

`_watermill_message_uuid` becomes `Message.uid` and the fallback idempotency key, `metadata` becomes `Message.headers`, `payload` becomes `Message.payload` without base64 or JSON decoding, and the subscribed Redis stream name becomes `Message.topic`.

For mixed streams where the read side should accept either eventbus JSON entries or Watermill entries, opt in per stream:

```rust
# use eventbus_redis::RedisBackend;
# fn configure(backend: &RedisBackend) {
backend.set_auto_detect_read_stream("mapset.mixed");
# }
```

Auto-detect is read-only. Writes still use the stream write codec, defaulting to the eventbus JSON `"message"` field.

## Connection / TLS / auth

`RedisBackend::new` takes an already-connected `MultiplexedConnection`; the caller chooses the URL and any TLS / auth settings. The crate does not require, default to, or downgrade TLS.

## License

MIT
