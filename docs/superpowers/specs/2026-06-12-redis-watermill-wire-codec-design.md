# Redis Watermill Wire Codec Design

## Purpose

`eventbus-redis` must read Redis Stream entries written by Go Watermill producers without changing those producers. The immediate production use case is a Rust consumer, such as `maptile`, subscribing to Redis Streams already written by Go services such as `api-gateway` or `mapset`.

Those Go services use Watermill's canonical Redis Stream entry shape:

```text
_watermill_message_uuid = raw string
metadata                = vmihailenco/msgpack map<string,string>
payload                 = raw bytes
```

The current Rust backend reads only one Redis field named `message` and passes that field's bytes to `eventbus_core::Codec`. That works for the existing eventbus JSON envelope, but it cannot read Watermill entries because the codec never sees the full Redis field map.

## Goals

- Let `eventbus-redis::RedisBackend` decode Watermill canonical entries.
- Preserve existing eventbus JSON envelope behavior for current users.
- Support one process consuming different streams with different wire formats.
- Keep Redis-specific field-map logic inside `eventbus-redis`, not `eventbus-core`.
- Keep write behavior explicit and backward-compatible.
- Provide a small extension surface for future Redis Stream formats.

## Non-Goals

- Do not modify Go Watermill producers.
- Do not change the `eventbus-core::StreamBackend` trait.
- Do not generalize this into Kafka, NATS, or non-Redis transport codecs.
- Do not implement broad msgpack coercion for arbitrary metadata values in the first version.
- Do not infer event type or source from payload JSON.

## Current Limitation

`RedisBackend::publish` writes:

```text
XADD <stream> * message <JsonCodec bytes>
```

`decode_entry` reads:

```text
fields["message"] -> Codec::decode(bytes)
```

This makes field selection part of `RedisBackend`, while the codec can only operate on a single byte slice. Replacing `JsonCodec` cannot fix Watermill support because Watermill requires reading `_watermill_message_uuid`, `metadata`, and `payload` together.

## Recommended Architecture

Add a Redis-specific field-level codec trait in `eventbus-redis`.

```rust
pub trait RedisStreamCodec: Send + Sync {
    fn name(&self) -> &str;

    fn encode_fields(
        &self,
        ctx: EncodeContext<'_>,
        msg: &Message,
    ) -> Result<Vec<(String, Vec<u8>)>, EventBusError>;

    fn decode_fields(
        &self,
        ctx: DecodeContext<'_>,
        fields: &RedisStreamFields,
    ) -> Result<Message, EventBusError>;

    fn can_decode(&self, fields: &RedisStreamFields) -> bool;
}
```

Keep the boundary:

```text
eventbus-core
  Message, StreamBus, StreamBackend
  No Redis field-map concepts.

eventbus-redis
  Redis entry fields <-> Message
  Codec selection, Watermill compatibility, auto-detection.
```

Recommended supporting types:

```rust
pub struct EncodeContext<'a> {
    pub stream: &'a str,
}

pub struct DecodeContext<'a> {
    pub stream: &'a str,
    pub redis_id: &'a str,
}

pub type RedisStreamFields = std::collections::HashMap<String, Vec<u8>>;
```

`DecodeContext::stream` is required because Watermill entries do not carry a topic. The subscribed Redis stream becomes `Message.topic`.

## Built-In Codecs

### EventbusJsonStreamCodec

This codec preserves the existing wire format.

```text
encode_fields:
  message = JsonCodec.encode(Message)

decode_fields:
  JsonCodec.decode(fields["message"])
```

It should wrap the existing `eventbus_core::Codec` path so `RedisBackend::with_codec(conn, codec)` remains compatible.

### WatermillStreamCodec

This codec reads and optionally writes Watermill canonical Redis Stream fields.

Required decode mapping:

```text
_watermill_message_uuid -> Message.uid
payload                 -> Message.payload as raw bytes
metadata                -> Message.headers after msgpack decode
ctx.stream              -> Message.topic
```

Recommended field completion:

```text
Message.key:
  metadata["key"] or metadata["partition_key"] or ""

Message.kind:
  metadata["event_type"] or metadata["type"] or metadata["kind"] or "watermill.message"

Message.source:
  metadata["producer"] or metadata["source"] or "watermill"

Message.occurred_at:
  metadata["occurred_at"] or metadata["timestamp"] if parseable, otherwise Utc::now()

Message.content_type:
  metadata["content-type"] or metadata["content_type"]

Message.event_version:
  metadata["event-version"] or metadata["event_version"]
```

Idempotency handling:

```text
Message.uid = _watermill_message_uuid
Message.idempotency_key = metadata["idempotency-key"] if present
Message.idempotency_key = _watermill_message_uuid if metadata does not provide one
headers["idempotency-key"] = _watermill_message_uuid if metadata does not provide one
```

The raw Watermill UUID should also be preserved in headers:

```text
headers["_watermill_message_uuid"] = _watermill_message_uuid
```

`payload` must remain raw bytes. It must not be base64-decoded or JSON-decoded. This is the key compatibility requirement for maptile mosaic registration.

### AutoDetectRedisStreamCodec

This codec composes other codecs and chooses the first codec whose `can_decode` matches.

Recommended matching order:

```text
1. EventbusJsonStreamCodec if field "message" exists.
2. WatermillStreamCodec if fields "_watermill_message_uuid" and "payload" exist.
3. Malformed if no codec matches.
```

Auto-detect is useful for mixed consumers, but it should not be the implicit default unless the library intentionally wants broader read behavior by default.

## Codec Selection

`RedisBackend` should hold separate read and write codec selection.

Read priority:

```text
1. subscription override: (stream, group, consumer)
2. group override:        (stream, group)
3. stream override:       stream
4. default read codec
```

Write priority:

```text
1. stream override
2. default write codec
```

Publish has no subscription context, so write selection should not use group or consumer.

Recommended API:

```rust
let backend = RedisBackend::new(conn)
    .with_default_read_codec(Arc::new(EventbusJsonStreamCodec::default()))
    .with_default_write_codec(Arc::new(EventbusJsonStreamCodec::default()))
    .with_stream_read_codec("mapset.events", Arc::new(WatermillStreamCodec::default()));
```

Runtime registration methods are also useful when the backend is already wrapped in `Arc`:

```rust
backend.set_stream_read_codec("mapset.events", Arc::new(WatermillStreamCodec::default()));
backend.set_group_read_codec("mapset.events", "maptile", Arc::new(WatermillStreamCodec::default()));
backend.set_subscription_read_codec(
    "mapset.events",
    "maptile",
    "consumer-1",
    Arc::new(WatermillStreamCodec::default()),
);
backend.set_stream_write_codec("native.events", Arc::new(EventbusJsonStreamCodec::default()));
```

Convenience helpers can reduce boilerplate:

```rust
backend.set_watermill_read_stream("mapset.events");
backend.set_auto_detect_read_stream("mixed.events");
```

## Default Behavior

Use conservative defaults:

```text
default read codec  = EventbusJsonStreamCodec
default write codec = EventbusJsonStreamCodec
```

This preserves current behavior and avoids silently broadening accepted data. Applications that need Watermill support opt in per stream:

```rust
backend.set_stream_read_codec("mapset.events", Arc::new(WatermillStreamCodec::default()));
```

Applications that deliberately want mixed-format reading can opt into auto-detection:

```rust
backend.set_stream_read_codec("mixed.events", Arc::new(AutoDetectRedisStreamCodec::default()));
```

## Error Handling

All decode failures remain per-entry failures and should return `FetchedEntry::Malformed`. Existing poison-pill behavior stays intact: malformed entries can be observed, optionally sent to DLQ, and acked out of the pending list.

Watermill decode failures:

```text
missing _watermill_message_uuid -> Malformed
missing payload                 -> Malformed
metadata missing                -> allowed, headers empty
metadata msgpack decode failed  -> Malformed
metadata not map<string,string> -> Malformed in the first version
invalid injected topic          -> Malformed
```

Auto-detect failures should name the visible field set and explain that no registered codec matched:

```text
no RedisStreamCodec could decode entry fields: [_watermill_message_uuid, payload]
```

Codec-specific errors should include the codec name:

```text
watermill metadata msgpack decode failed: <source>
eventbus-json field 'message' decode failed: <source>
```

## Dependencies

Add `rmp-serde` to `eventbus-redis` for Watermill metadata decoding. This dependency should not be added to `eventbus-core`.

Watermill support should be feature-gated so existing Redis users do not take an extra msgpack dependency unless they need this compatibility mode.

```toml
[features]
default = []
watermill = ["dep:rmp-serde"]
```

The facade crate should expose a matching feature so application users can enable it from `eventbus-contract`:

```toml
eventbus-contract = { version = "0.2", features = ["redis", "redis-watermill"] }
```

## Testing Matrix

### EventbusJsonStreamCodec

- Encodes a single `message` field.
- Decodes the existing JSON envelope.
- Preserves existing `JsonCodec` negative-path behavior.
- Reports missing `message` as a codec miss or malformed error depending on call path.

### WatermillStreamCodec

- Decodes canonical Watermill fields.
- Preserves raw `payload` bytes exactly.
- Maps `_watermill_message_uuid` into `Message.uid`.
- Sets `Message.idempotency_key` from metadata or falls back to the Watermill UUID.
- Decodes msgpack metadata into headers.
- Injects `Message.topic` from `DecodeContext::stream`.
- Maps `event_type`, `producer`, content type, event version, and timestamp fields when present.
- Allows missing metadata.
- Rejects missing UUID.
- Rejects missing payload.
- Rejects damaged msgpack metadata.

### RedisBackend

- `publish` uses the selected write codec and sends all returned fields to `XADD`.
- `read_new` decodes the full Redis field map with the selected read codec.
- `reclaim_idle` uses the same selected read codec path as `read_new`.
- Stream read override lets one backend read eventbus JSON on one stream and Watermill on another.
- Group or subscription override takes precedence over stream override.
- Existing malformed-entry ack and DLQ behavior still works.

## Migration Notes

Existing callers keep using:

```rust
let backend = RedisBackend::new(conn);
let bus = StreamBus::new(Arc::new(backend), options)?;
```

Existing `RedisBackend::with_codec(conn, codec)` remains supported by wrapping `codec` in `EventbusJsonStreamCodec`.

Watermill consumers opt in:

```rust
let backend = RedisBackend::new(conn)
    .with_stream_read_codec("mapset.events", Arc::new(WatermillStreamCodec::default()));
```

Mixed-format consumers opt in:

```rust
let backend = RedisBackend::new(conn)
    .with_stream_read_codec("mixed.events", Arc::new(AutoDetectRedisStreamCodec::default()));
```

## Future Extensions

This design can add new Redis codecs without changing `eventbus-core`:

- `CloudEventsRedisStreamCodec`
- `LegacyJsonStreamCodec`
- `RawPayloadStreamCodec`
- lenient Watermill metadata mode
- codec-level observability, such as decode codec name and malformed reason tags
- stream write codecs for Rust producers that need to emit Watermill-compatible entries

## Implementation Decisions

- Watermill support is behind an `eventbus-redis/watermill` feature and a facade `eventbus-contract/redis-watermill` feature.
- Default read stays conservative: `EventbusJsonStreamCodec`.
- Default write stays conservative: `EventbusJsonStreamCodec`.
- Runtime registry mutation uses `DashMap`, matching the existing RedisBackend reclaim cursor state.
- Auto-detect is opt-in per stream or as an explicit default read codec chosen by the application.
