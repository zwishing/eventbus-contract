# Redis Watermill Wire Codec Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Redis-specific field-level wire codecs so `eventbus-redis::RedisBackend` can read Go Watermill canonical Redis Stream entries while preserving existing eventbus JSON behavior.

**Architecture:** Keep `eventbus-core` unchanged. Add `RedisStreamCodec` and codec selection inside `eventbus-redis`, wrap the current `JsonCodec` as the default field-level codec, and add a feature-gated `WatermillStreamCodec` for canonical Watermill entries. `RedisBackend` selects read codecs by subscription/group/stream/default and write codecs by stream/default.

**Tech Stack:** Rust 1.82, `redis-rs`, `dashmap`, `bytes`, `chrono`, existing `eventbus_core::Codec`, optional `rmp-serde` behind `eventbus-redis/watermill`.

---

## File Structure

- Modify `Cargo.toml`: add workspace dependency `rmp-serde`.
- Modify `crates/eventbus-redis/Cargo.toml`: add `watermill` feature and optional `rmp-serde`.
- Modify `crates/eventbus-contract/Cargo.toml`: add facade feature `redis-watermill`.
- Modify `crates/eventbus-redis/src/codec.rs`: add Redis field-level codec trait, JSON field codec, optional Watermill codec, auto-detect codec, and tests.
- Modify `crates/eventbus-redis/src/redis.rs`: replace single-field `Codec` storage with read/write `RedisStreamCodec` registry and update publish/read/reclaim helpers.
- Modify `crates/eventbus-redis/src/lib.rs`: re-export new codec types.
- Modify `crates/eventbus-redis/README.md`: document Watermill opt-in and mixed stream configuration.
- Modify `crates/eventbus-contract/src/lib.rs`: re-export feature remains through `eventbus_redis`; no code change unless rustdoc feature notes are added.

## Task 1: Add Redis Field Codec Types and Preserve JSON Behavior

**Files:**
- Modify: `crates/eventbus-redis/src/codec.rs`
- Test: `crates/eventbus-redis/src/codec.rs`

- [ ] **Step 1: Write the failing JSON field codec tests**

Add these tests inside `crates/eventbus-redis/src/codec.rs` under the existing `#[cfg(test)]` area. If the existing test module remains nested in `mod json`, add a second crate-level `#[cfg(test)] mod stream_codec_tests` below `mod json`.

```rust
#[cfg(test)]
mod stream_codec_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::Utc;
    use eventbus_core::{Codec, Message, Topic};

    use super::{DecodeContext, EncodeContext, EventbusJsonStreamCodec, RedisStreamCodec};
    use crate::codec::JsonCodec;

    fn sample_message() -> Message {
        Message {
            uid: "msg-json".into(),
            topic: Topic::new("native.events").expect("topic"),
            key: "key-1".into(),
            kind: "native.kind".into(),
            source: "test".into(),
            occurred_at: Utc::now(),
            headers: HashMap::new(),
            payload: bytes::Bytes::from_static(b"hello-json"),
            content_type: Some("application/json".into()),
            event_version: Some("v1".into()),
            idempotency_key: Some("idem-json".into()),
            expires_at: None,
            trace_uid: None,
            correlation_uid: None,
        }
    }

    #[test]
    fn eventbus_json_stream_codec_writes_single_message_field() {
        let codec = EventbusJsonStreamCodec::default();
        let fields = codec
            .encode_fields(EncodeContext { stream: "native.events" }, &sample_message())
            .expect("encode fields");

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "message");
        let json = std::str::from_utf8(&fields[0].1).expect("utf8 json");
        assert!(json.starts_with(r#"{"message":"#), "json was {json}");
    }

    #[test]
    fn eventbus_json_stream_codec_decodes_existing_message_field() {
        let message = sample_message();
        let bytes = JsonCodec.encode(&message).expect("json encode");
        let fields = HashMap::from([("message".to_string(), bytes)]);
        let codec = EventbusJsonStreamCodec::default();

        let decoded = codec
            .decode_fields(
                DecodeContext {
                    stream: "native.events",
                    redis_id: "1-0",
                },
                &fields,
            )
            .expect("decode fields");

        assert_eq!(decoded.uid, "msg-json");
        assert_eq!(decoded.topic.as_str(), "native.events");
        assert_eq!(decoded.payload, bytes::Bytes::from_static(b"hello-json"));
    }

    #[test]
    fn eventbus_json_stream_codec_wraps_custom_core_codec() {
        #[derive(Debug)]
        struct ConstantCodec;

        impl Codec for ConstantCodec {
            fn name(&self) -> &str {
                "constant"
            }

            fn encode(&self, _msg: &Message) -> Result<Vec<u8>, eventbus_core::EventBusError> {
                Ok(b"constant-wire".to_vec())
            }

            fn decode(&self, _bytes: &[u8]) -> Result<Message, eventbus_core::EventBusError> {
                Ok(sample_message())
            }
        }

        let codec = EventbusJsonStreamCodec::from_core_codec(Arc::new(ConstantCodec));
        let fields = codec
            .encode_fields(EncodeContext { stream: "native.events" }, &sample_message())
            .expect("encode fields");

        assert_eq!(fields, vec![("message".to_string(), b"constant-wire".to_vec())]);
    }
}
```

- [ ] **Step 2: Run the tests to verify RED**

Run:

```powershell
cargo test -p eventbus-redis stream_codec_tests -- --nocapture
```

Expected: compile failure because `DecodeContext`, `EncodeContext`, `EventbusJsonStreamCodec`, and `RedisStreamCodec` do not exist.

- [ ] **Step 3: Add the minimal field codec API and JSON wrapper**

In `crates/eventbus-redis/src/codec.rs`, add these public items after `pub use json::JsonCodec;`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use eventbus_core::{Codec, EventBusError, Message};

pub type RedisStreamFields = HashMap<String, Vec<u8>>;

#[derive(Debug, Clone, Copy)]
pub struct EncodeContext<'a> {
    pub stream: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct DecodeContext<'a> {
    pub stream: &'a str,
    pub redis_id: &'a str,
}

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

#[derive(Clone)]
pub struct EventbusJsonStreamCodec {
    inner: Arc<dyn Codec>,
}

impl Default for EventbusJsonStreamCodec {
    fn default() -> Self {
        Self::from_core_codec(Arc::new(JsonCodec))
    }
}

impl EventbusJsonStreamCodec {
    pub fn from_core_codec(inner: Arc<dyn Codec>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for EventbusJsonStreamCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventbusJsonStreamCodec")
            .field("inner", &self.inner.name())
            .finish()
    }
}

impl RedisStreamCodec for EventbusJsonStreamCodec {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn encode_fields(
        &self,
        _ctx: EncodeContext<'_>,
        msg: &Message,
    ) -> Result<Vec<(String, Vec<u8>)>, EventBusError> {
        Ok(vec![("message".to_string(), self.inner.encode(msg)?)])
    }

    fn decode_fields(
        &self,
        _ctx: DecodeContext<'_>,
        fields: &RedisStreamFields,
    ) -> Result<Message, EventBusError> {
        let bytes = fields.get("message").ok_or_else(|| {
            EventBusError::Serialization("eventbus-json entry missing 'message' field".into())
        })?;
        self.inner.decode(bytes)
    }

    fn can_decode(&self, fields: &RedisStreamFields) -> bool {
        fields.contains_key("message")
    }
}
```

- [ ] **Step 4: Run the tests to verify GREEN**

Run:

```powershell
cargo test -p eventbus-redis stream_codec_tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

Run:

```powershell
git add crates/eventbus-redis/src/codec.rs
git commit -m "Introduce Redis field-level JSON codec" -m "Redis Stream compatibility now needs codecs that operate on full entry fields, while preserving the existing single message-field JSON envelope through a wrapper around eventbus_core::Codec." -m "Constraint: eventbus-core remains transport-neutral" -m "Rejected: Replace eventbus_core::Codec | existing users still rely on the byte-level codec API" -m "Confidence: high" -m "Scope-risk: narrow" -m "Tested: cargo test -p eventbus-redis stream_codec_tests -- --nocapture" -m "Not-tested: Watermill codec and backend registry are outside this task"
```

## Task 2: Add Codec Registry and Switch RedisBackend to Field Codecs

**Files:**
- Modify: `crates/eventbus-redis/src/redis.rs`
- Modify: `crates/eventbus-redis/src/lib.rs`
- Test: `crates/eventbus-redis/src/redis.rs`

- [ ] **Step 1: Write failing registry and decode-entry tests**

Add these tests to `#[cfg(test)] mod tests` in `crates/eventbus-redis/src/redis.rs`:

```rust
use crate::codec::{EventbusJsonStreamCodec, RedisStreamCodec};

#[test]
fn decode_entry_uses_full_field_codec_instead_of_hardcoded_message_field() {
    #[derive(Debug)]
    struct PayloadOnlyCodec;

    impl RedisStreamCodec for PayloadOnlyCodec {
        fn name(&self) -> &str {
            "payload-only"
        }

        fn encode_fields(
            &self,
            _ctx: crate::codec::EncodeContext<'_>,
            _msg: &Message,
        ) -> Result<Vec<(String, Vec<u8>)>, EventBusError> {
            unreachable!("decode-only test")
        }

        fn decode_fields(
            &self,
            ctx: crate::codec::DecodeContext<'_>,
            fields: &crate::codec::RedisStreamFields,
        ) -> Result<Message, EventBusError> {
            let payload = fields.get("payload").expect("payload field").clone();
            Ok(Message {
                uid: "payload-only-id".into(),
                topic: eventbus_core::Topic::new(ctx.stream).expect("topic"),
                key: String::new(),
                kind: "payload.only".into(),
                source: "test".into(),
                occurred_at: Utc::now(),
                headers: HashMap::new(),
                payload: bytes::Bytes::from(payload),
                content_type: None,
                event_version: None,
                idempotency_key: None,
                expires_at: None,
                trace_uid: None,
                correlation_uid: None,
            })
        }

        fn can_decode(&self, fields: &crate::codec::RedisStreamFields) -> bool {
            fields.contains_key("payload")
        }
    }

    let entry = StreamId {
        id: "1-0".into(),
        map: HashMap::from([(
            "payload".into(),
            Value::BulkString(b"raw-payload".to_vec()),
        )]),
        milliseconds_elapsed_from_delivery: None,
        delivered_count: None,
    };

    let decoded = decode_entry(
        "custom.stream",
        &entry,
        false,
        Arc::new(PayloadOnlyCodec),
    );

    let claimed = match decoded {
        FetchedEntry::Decoded(c) => c,
        FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
    };
    assert_eq!(claimed.message.topic.as_str(), "custom.stream");
    assert_eq!(claimed.message.payload, bytes::Bytes::from_static(b"raw-payload"));
}

#[test]
fn registry_prefers_subscription_then_group_then_stream_then_default_read_codec() {
    let registry = CodecRegistry::new(Arc::new(EventbusJsonStreamCodec::default()));
    let stream_codec = Arc::new(NamedTestCodec("stream"));
    let group_codec = Arc::new(NamedTestCodec("group"));
    let subscription_codec = Arc::new(NamedTestCodec("subscription"));

    registry.set_stream_read_codec("s", stream_codec);
    registry.set_group_read_codec("s", "g", group_codec);
    registry.set_subscription_read_codec("s", "g", "c", subscription_codec);

    assert_eq!(registry.read_codec("s", "g", "c").name(), "subscription");
    assert_eq!(registry.read_codec("s", "g", "other").name(), "group");
    assert_eq!(registry.read_codec("s", "other", "other").name(), "stream");
    assert_eq!(registry.read_codec("other", "other", "other").name(), "json");
}

#[derive(Debug)]
struct NamedTestCodec(&'static str);

impl RedisStreamCodec for NamedTestCodec {
    fn name(&self) -> &str {
        self.0
    }

    fn encode_fields(
        &self,
        _ctx: crate::codec::EncodeContext<'_>,
        _msg: &Message,
    ) -> Result<Vec<(String, Vec<u8>)>, EventBusError> {
        Ok(vec![("message".to_string(), self.0.as_bytes().to_vec())])
    }

    fn decode_fields(
        &self,
        _ctx: crate::codec::DecodeContext<'_>,
        _fields: &crate::codec::RedisStreamFields,
    ) -> Result<Message, EventBusError> {
        unreachable!("registry test only checks selection")
    }

    fn can_decode(&self, _fields: &crate::codec::RedisStreamFields) -> bool {
        true
    }
}
```

- [ ] **Step 2: Run the tests to verify RED**

Run:

```powershell
cargo test -p eventbus-redis registry_prefers_subscription decode_entry_uses_full_field_codec -- --nocapture
```

Expected: compile failure because `CodecRegistry` and the new `decode_entry` signature do not exist.

- [ ] **Step 3: Implement registry and field-map conversion**

In `crates/eventbus-redis/src/redis.rs`:

1. Replace the `codec: Arc<dyn Codec>` field with:

```rust
registry: CodecRegistry,
```

2. Add this internal registry near `ReclaimCursorKey`:

```rust
type StreamGroupKey = (String, String);
type StreamGroupConsumerKey = (String, String, String);

struct CodecRegistry {
    default_read: Arc<dyn RedisStreamCodec>,
    default_write: Arc<dyn RedisStreamCodec>,
    stream_read: DashMap<String, Arc<dyn RedisStreamCodec>>,
    group_read: DashMap<StreamGroupKey, Arc<dyn RedisStreamCodec>>,
    subscription_read: DashMap<StreamGroupConsumerKey, Arc<dyn RedisStreamCodec>>,
    stream_write: DashMap<String, Arc<dyn RedisStreamCodec>>,
}

impl CodecRegistry {
    fn new(default_codec: Arc<dyn RedisStreamCodec>) -> Self {
        Self {
            default_read: Arc::clone(&default_codec),
            default_write: default_codec,
            stream_read: DashMap::new(),
            group_read: DashMap::new(),
            subscription_read: DashMap::new(),
            stream_write: DashMap::new(),
        }
    }

    fn read_codec(&self, stream: &str, group: &str, consumer: &str) -> Arc<dyn RedisStreamCodec> {
        let subscription_key = (stream.to_string(), group.to_string(), consumer.to_string());
        if let Some(codec) = self.subscription_read.get(&subscription_key) {
            return Arc::clone(codec.value());
        }
        let group_key = (stream.to_string(), group.to_string());
        if let Some(codec) = self.group_read.get(&group_key) {
            return Arc::clone(codec.value());
        }
        if let Some(codec) = self.stream_read.get(stream) {
            return Arc::clone(codec.value());
        }
        Arc::clone(&self.default_read)
    }

    fn write_codec(&self, stream: &str) -> Arc<dyn RedisStreamCodec> {
        self.stream_write
            .get(stream)
            .map(|codec| Arc::clone(codec.value()))
            .unwrap_or_else(|| Arc::clone(&self.default_write))
    }

    fn set_stream_read_codec(&self, stream: impl Into<String>, codec: Arc<dyn RedisStreamCodec>) {
        self.stream_read.insert(stream.into(), codec);
    }

    fn set_group_read_codec(
        &self,
        stream: impl Into<String>,
        group: impl Into<String>,
        codec: Arc<dyn RedisStreamCodec>,
    ) {
        self.group_read.insert((stream.into(), group.into()), codec);
    }

    fn set_subscription_read_codec(
        &self,
        stream: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
        codec: Arc<dyn RedisStreamCodec>,
    ) {
        self.subscription_read
            .insert((stream.into(), group.into(), consumer.into()), codec);
    }

    fn set_stream_write_codec(&self, stream: impl Into<String>, codec: Arc<dyn RedisStreamCodec>) {
        self.stream_write.insert(stream.into(), codec);
    }
}
```

3. Add public `RedisBackend` registration methods that delegate to `self.registry`.

4. Convert Redis values into a full field map:

```rust
fn entry_fields(entry: &StreamId) -> Result<RedisStreamFields, EventBusError> {
    let mut fields = RedisStreamFields::with_capacity(entry.map.len());
    for (key, val) in &entry.map {
        let bytes = redis_value_to_bytes(val.clone())
            .map_err(|err| EventBusError::source(format!("read stream field {key}"), err))?;
        fields.insert(key.clone(), bytes);
    }
    Ok(fields)
}

fn redis_value_to_bytes(value: Value) -> Result<Vec<u8>, redis::RedisError> {
    match value {
        Value::BulkString(b) => Ok(b),
        Value::SimpleString(s) => Ok(s.into_bytes()),
        other => {
            let s: String = FromRedisValue::from_redis_value(other)?;
            Ok(s.into_bytes())
        }
    }
}
```

5. Change `decode_entry` to accept `stream` and `Arc<dyn RedisStreamCodec>`.

6. Update `read_new` and `parse_autoclaim` to resolve and pass read codecs.

7. Update `publish` to call `write_codec(stream).encode_fields(...)` and append every returned field to `XADD`.

- [ ] **Step 4: Run the focused tests to verify GREEN**

Run:

```powershell
cargo test -p eventbus-redis registry_prefers_subscription decode_entry_uses_full_field_codec -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run existing Redis tests**

Run:

```powershell
cargo test -p eventbus-redis -- --nocapture
```

Expected: PASS. Existing JSON codec and malformed-entry tests must still pass.

- [ ] **Step 6: Commit Task 2**

Run:

```powershell
git add crates/eventbus-redis/src/redis.rs crates/eventbus-redis/src/lib.rs
git commit -m "Route Redis streams through field-level codec registry" -m "RedisBackend now chooses codecs per stream, group, or subscription for reads and per stream for writes, while preserving the existing JSON envelope as the default codec." -m "Constraint: maptile must consume mixed native and Watermill streams in one process" -m "Rejected: Global RedisBackend codec only | cannot satisfy mixed stream consumption" -m "Confidence: high" -m "Scope-risk: moderate" -m "Tested: cargo test -p eventbus-redis -- --nocapture" -m "Not-tested: Watermill msgpack decoding is added in the next task"
```

## Task 3: Add Feature-Gated WatermillStreamCodec

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/eventbus-redis/Cargo.toml`
- Modify: `crates/eventbus-contract/Cargo.toml`
- Modify: `crates/eventbus-redis/src/codec.rs`
- Test: `crates/eventbus-redis/src/codec.rs`

- [ ] **Step 1: Write failing Watermill codec tests**

Add this test module to `crates/eventbus-redis/src/codec.rs`:

```rust
#[cfg(all(test, feature = "watermill"))]
mod watermill_codec_tests {
    use std::collections::HashMap;

    use super::{DecodeContext, RedisStreamCodec, WatermillStreamCodec};

    fn metadata(fields: &[(&str, &str)]) -> Vec<u8> {
        let map: HashMap<String, String> = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        rmp_serde::to_vec_named(&map).expect("msgpack metadata")
    }

    #[test]
    fn watermill_stream_codec_decodes_canonical_entry_with_raw_payload() {
        let fields = HashMap::from([
            (
                "_watermill_message_uuid".to_string(),
                b"wm-uuid-1".to_vec(),
            ),
            (
                "metadata".to_string(),
                metadata(&[
                    ("event_type", "mosaic.registered"),
                    ("producer", "mapset"),
                    ("content-type", "application/json"),
                    ("event-version", "v7"),
                    ("traceparent", "00-abc-def-01"),
                ]),
            ),
            ("payload".to_string(), b"{\"mosaic\":\"ok\"}".to_vec()),
        ]);

        let codec = WatermillStreamCodec::default();
        let decoded = codec
            .decode_fields(
                DecodeContext {
                    stream: "mapset.mosaic",
                    redis_id: "1-0",
                },
                &fields,
            )
            .expect("decode watermill");

        assert_eq!(decoded.uid, "wm-uuid-1");
        assert_eq!(decoded.idempotency_key.as_deref(), Some("wm-uuid-1"));
        assert_eq!(decoded.topic.as_str(), "mapset.mosaic");
        assert_eq!(decoded.kind, "mosaic.registered");
        assert_eq!(decoded.source, "mapset");
        assert_eq!(decoded.content_type.as_deref(), Some("application/json"));
        assert_eq!(decoded.event_version.as_deref(), Some("v7"));
        assert_eq!(decoded.payload, bytes::Bytes::from_static(b"{\"mosaic\":\"ok\"}"));
        assert_eq!(
            decoded.headers.get("_watermill_message_uuid").map(String::as_str),
            Some("wm-uuid-1")
        );
        assert_eq!(
            decoded.headers.get("traceparent").map(String::as_str),
            Some("00-abc-def-01")
        );
    }

    #[test]
    fn watermill_stream_codec_allows_missing_metadata() {
        let fields = HashMap::from([
            ("_watermill_message_uuid".to_string(), b"wm-uuid-2".to_vec()),
            ("payload".to_string(), b"raw".to_vec()),
        ]);

        let decoded = WatermillStreamCodec::default()
            .decode_fields(
                DecodeContext {
                    stream: "mapset.raw",
                    redis_id: "2-0",
                },
                &fields,
            )
            .expect("decode watermill without metadata");

        assert_eq!(decoded.uid, "wm-uuid-2");
        assert_eq!(decoded.kind, "watermill.message");
        assert_eq!(decoded.source, "watermill");
        assert_eq!(decoded.payload, bytes::Bytes::from_static(b"raw"));
    }

    #[test]
    fn watermill_stream_codec_rejects_missing_uuid() {
        let fields = HashMap::from([("payload".to_string(), b"raw".to_vec())]);

        let err = WatermillStreamCodec::default()
            .decode_fields(
                DecodeContext {
                    stream: "mapset.raw",
                    redis_id: "3-0",
                },
                &fields,
            )
            .expect_err("missing uuid should fail");

        assert!(err.to_string().contains("_watermill_message_uuid"));
    }

    #[test]
    fn watermill_stream_codec_rejects_damaged_metadata() {
        let fields = HashMap::from([
            ("_watermill_message_uuid".to_string(), b"wm-uuid-3".to_vec()),
            ("metadata".to_string(), b"not-msgpack".to_vec()),
            ("payload".to_string(), b"raw".to_vec()),
        ]);

        let err = WatermillStreamCodec::default()
            .decode_fields(
                DecodeContext {
                    stream: "mapset.raw",
                    redis_id: "4-0",
                },
                &fields,
            )
            .expect_err("damaged metadata should fail");

        assert!(err.to_string().contains("metadata"));
    }
}
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```powershell
cargo test -p eventbus-redis --features watermill watermill_stream_codec -- --nocapture
```

Expected: compile failure because the `watermill` feature and `WatermillStreamCodec` do not exist.

- [ ] **Step 3: Add feature dependencies**

Modify root `Cargo.toml`:

```toml
[workspace.dependencies]
rmp-serde = "1"
```

Modify `crates/eventbus-redis/Cargo.toml`:

```toml
[features]
default = []
tls = ["redis/tls-native-tls", "redis/tokio-native-tls-comp"]
watermill = ["dep:rmp-serde"]

[dependencies]
rmp-serde = { workspace = true, optional = true }
```

Modify `crates/eventbus-contract/Cargo.toml`:

```toml
[features]
redis-watermill = ["redis", "eventbus-redis/watermill"]
```

- [ ] **Step 4: Implement WatermillStreamCodec**

In `crates/eventbus-redis/src/codec.rs`, add:

```rust
#[cfg(feature = "watermill")]
#[derive(Debug, Default, Clone, Copy)]
pub struct WatermillStreamCodec;

#[cfg(feature = "watermill")]
impl RedisStreamCodec for WatermillStreamCodec {
    fn name(&self) -> &str {
        "watermill"
    }

    fn encode_fields(
        &self,
        _ctx: EncodeContext<'_>,
        msg: &Message,
    ) -> Result<Vec<(String, Vec<u8>)>, EventBusError> {
        let uuid = msg.uid.as_bytes().to_vec();
        let metadata = rmp_serde::to_vec_named(&msg.headers)
            .map_err(|err| EventBusError::source("watermill metadata encode", err))?;
        Ok(vec![
            ("_watermill_message_uuid".to_string(), uuid),
            ("metadata".to_string(), metadata),
            ("payload".to_string(), msg.payload.to_vec()),
        ])
    }

    fn decode_fields(
        &self,
        ctx: DecodeContext<'_>,
        fields: &RedisStreamFields,
    ) -> Result<Message, EventBusError> {
        let uid = read_utf8_field(fields, "_watermill_message_uuid")?;
        let payload = fields
            .get("payload")
            .ok_or_else(|| EventBusError::Serialization("watermill entry missing 'payload' field".into()))?
            .clone();

        let mut headers = match fields.get("metadata") {
            Some(bytes) => rmp_serde::from_slice::<HashMap<String, String>>(bytes)
                .map_err(|err| EventBusError::source("watermill metadata decode", err))?,
            None => HashMap::new(),
        };

        headers.insert("_watermill_message_uuid".into(), uid.clone());
        if !headers.contains_key("idempotency-key") {
            headers.insert("idempotency-key".into(), uid.clone());
        }

        let topic = eventbus_core::Topic::new(ctx.stream)?;
        let key = first_header(&headers, &["key", "partition_key"]).unwrap_or_default();
        let kind = first_header(&headers, &["event_type", "type", "kind"])
            .unwrap_or_else(|| "watermill.message".to_string());
        let source = first_header(&headers, &["producer", "source"])
            .unwrap_or_else(|| "watermill".to_string());
        let content_type = first_header(&headers, &["content-type", "content_type"]);
        let event_version = first_header(&headers, &["event-version", "event_version"]);
        let idempotency_key = headers.get("idempotency-key").cloned();

        Ok(Message {
            uid,
            topic,
            key,
            kind,
            source,
            occurred_at: chrono::Utc::now(),
            headers,
            payload: bytes::Bytes::from(payload),
            content_type,
            event_version,
            idempotency_key,
            expires_at: None,
            trace_uid: None,
            correlation_uid: None,
        })
    }

    fn can_decode(&self, fields: &RedisStreamFields) -> bool {
        fields.contains_key("_watermill_message_uuid") && fields.contains_key("payload")
    }
}

#[cfg(feature = "watermill")]
fn read_utf8_field(fields: &RedisStreamFields, name: &str) -> Result<String, EventBusError> {
    let bytes = fields
        .get(name)
        .ok_or_else(|| EventBusError::Serialization(format!("watermill entry missing '{name}' field")))?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|err| EventBusError::source(format!("watermill field '{name}' is not utf-8"), err))
}

#[cfg(feature = "watermill")]
fn first_header(headers: &HashMap<String, String>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| headers.get(*key).cloned())
}
```

- [ ] **Step 5: Run Watermill tests to verify GREEN**

Run:

```powershell
cargo test -p eventbus-redis --features watermill watermill_stream_codec -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit Task 3**

Run:

```powershell
git add Cargo.toml Cargo.lock crates/eventbus-redis/Cargo.toml crates/eventbus-contract/Cargo.toml crates/eventbus-redis/src/codec.rs
git commit -m "Add Watermill Redis Stream codec" -m "Watermill canonical entries store UUID, msgpack metadata, and raw payload as separate Redis Stream fields, so the Redis field-level codec decodes those fields directly into Message." -m "Constraint: Go producers cannot change their Watermill wire format" -m "Rejected: Decode payload as JSON | maptile requires raw bytes for mosaic registration" -m "Confidence: high" -m "Scope-risk: moderate" -m "Directive: Do not move Watermill msgpack handling into eventbus-core" -m "Tested: cargo test -p eventbus-redis --features watermill watermill_stream_codec -- --nocapture" -m "Not-tested: Real Redis integration against Go-produced entries"
```

## Task 4: Add Auto-Detect Codec and Stream Convenience Helpers

**Files:**
- Modify: `crates/eventbus-redis/src/codec.rs`
- Modify: `crates/eventbus-redis/src/redis.rs`
- Modify: `crates/eventbus-redis/src/lib.rs`
- Test: `crates/eventbus-redis/src/codec.rs`

- [ ] **Step 1: Write failing auto-detect tests**

Add to `crates/eventbus-redis/src/codec.rs`:

```rust
#[cfg(all(test, feature = "watermill"))]
mod auto_detect_tests {
    use std::collections::HashMap;

    use chrono::Utc;
    use eventbus_core::{Codec, Message, Topic};

    use super::{
        AutoDetectRedisStreamCodec, DecodeContext, EventbusJsonStreamCodec, JsonCodec,
        RedisStreamCodec, WatermillStreamCodec,
    };

    fn sample_message() -> Message {
        Message {
            uid: "json-id".into(),
            topic: Topic::new("native.events").expect("topic"),
            key: String::new(),
            kind: "native.kind".into(),
            source: "test".into(),
            occurred_at: Utc::now(),
            headers: HashMap::new(),
            payload: bytes::Bytes::from_static(b"json-payload"),
            content_type: None,
            event_version: None,
            idempotency_key: None,
            expires_at: None,
            trace_uid: None,
            correlation_uid: None,
        }
    }

    #[test]
    fn auto_detect_decodes_eventbus_json_before_watermill() {
        let fields = HashMap::from([(
            "message".to_string(),
            JsonCodec.encode(&sample_message()).expect("json encode"),
        )]);
        let codec = AutoDetectRedisStreamCodec::new(vec![
            std::sync::Arc::new(EventbusJsonStreamCodec::default()),
            std::sync::Arc::new(WatermillStreamCodec::default()),
        ]);

        let decoded = codec
            .decode_fields(
                DecodeContext {
                    stream: "native.events",
                    redis_id: "1-0",
                },
                &fields,
            )
            .expect("auto decode json");

        assert_eq!(decoded.uid, "json-id");
    }

    #[test]
    fn auto_detect_decodes_watermill_when_watermill_fields_exist() {
        let fields = HashMap::from([
            ("_watermill_message_uuid".to_string(), b"wm-auto".to_vec()),
            ("payload".to_string(), b"raw".to_vec()),
        ]);
        let codec = AutoDetectRedisStreamCodec::new(vec![
            std::sync::Arc::new(EventbusJsonStreamCodec::default()),
            std::sync::Arc::new(WatermillStreamCodec::default()),
        ]);

        let decoded = codec
            .decode_fields(
                DecodeContext {
                    stream: "mapset.auto",
                    redis_id: "2-0",
                },
                &fields,
            )
            .expect("auto decode watermill");

        assert_eq!(decoded.uid, "wm-auto");
        assert_eq!(decoded.payload, bytes::Bytes::from_static(b"raw"));
    }
}
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```powershell
cargo test -p eventbus-redis --features watermill auto_detect -- --nocapture
```

Expected: compile failure because `AutoDetectRedisStreamCodec` does not exist.

- [ ] **Step 3: Implement auto-detect codec**

Add to `crates/eventbus-redis/src/codec.rs`:

```rust
#[derive(Clone)]
pub struct AutoDetectRedisStreamCodec {
    codecs: Vec<Arc<dyn RedisStreamCodec>>,
}

impl AutoDetectRedisStreamCodec {
    pub fn new(codecs: Vec<Arc<dyn RedisStreamCodec>>) -> Self {
        Self { codecs }
    }
}

#[cfg(feature = "watermill")]
impl Default for AutoDetectRedisStreamCodec {
    fn default() -> Self {
        Self::new(vec![
            Arc::new(EventbusJsonStreamCodec::default()),
            Arc::new(WatermillStreamCodec::default()),
        ])
    }
}

impl std::fmt::Debug for AutoDetectRedisStreamCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.codecs.iter().map(|codec| codec.name()).collect();
        f.debug_struct("AutoDetectRedisStreamCodec")
            .field("codecs", &names)
            .finish()
    }
}

impl RedisStreamCodec for AutoDetectRedisStreamCodec {
    fn name(&self) -> &str {
        "auto-detect"
    }

    fn encode_fields(
        &self,
        _ctx: EncodeContext<'_>,
        _msg: &Message,
    ) -> Result<Vec<(String, Vec<u8>)>, EventBusError> {
        Err(EventBusError::Validation(
            "auto-detect codec cannot be used for writes".into(),
        ))
    }

    fn decode_fields(
        &self,
        ctx: DecodeContext<'_>,
        fields: &RedisStreamFields,
    ) -> Result<Message, EventBusError> {
        for codec in &self.codecs {
            if codec.can_decode(fields) {
                return codec.decode_fields(ctx, fields);
            }
        }
        let mut names: Vec<&str> = fields.keys().map(String::as_str).collect();
        names.sort_unstable();
        Err(EventBusError::Serialization(format!(
            "no RedisStreamCodec could decode entry fields: [{}]",
            names.join(", ")
        )))
    }

    fn can_decode(&self, fields: &RedisStreamFields) -> bool {
        self.codecs.iter().any(|codec| codec.can_decode(fields))
    }
}
```

Add RedisBackend convenience helpers:

```rust
#[cfg(feature = "watermill")]
pub fn set_watermill_read_stream(&self, stream: impl Into<String>) {
    self.set_stream_read_codec(stream, Arc::new(crate::codec::WatermillStreamCodec::default()));
}

#[cfg(feature = "watermill")]
pub fn set_auto_detect_read_stream(&self, stream: impl Into<String>) {
    self.set_stream_read_codec(stream, Arc::new(crate::codec::AutoDetectRedisStreamCodec::default()));
}
```

- [ ] **Step 4: Run tests to verify GREEN**

Run:

```powershell
cargo test -p eventbus-redis --features watermill auto_detect -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit Task 4**

Run:

```powershell
git add crates/eventbus-redis/src/codec.rs crates/eventbus-redis/src/redis.rs crates/eventbus-redis/src/lib.rs
git commit -m "Add opt-in Redis stream codec auto-detection" -m "Mixed-format consumers can opt into auto-detection per stream while default reads and writes remain on the existing JSON envelope." -m "Constraint: Auto-detect is read-only because writes require an explicit target format" -m "Rejected: Make auto-detect the default read codec | silently broadens library behavior" -m "Confidence: high" -m "Scope-risk: narrow" -m "Tested: cargo test -p eventbus-redis --features watermill auto_detect -- --nocapture" -m "Not-tested: Live mixed Redis streams"
```

## Task 5: Backend Integration Tests for Mixed Formats

**Files:**
- Modify: `crates/eventbus-redis/src/redis.rs`
- Test: `crates/eventbus-redis/src/redis.rs`

- [ ] **Step 1: Write failing mixed-format backend decode tests**

Add to `#[cfg(test)] mod tests` in `crates/eventbus-redis/src/redis.rs`:

```rust
#[cfg(feature = "watermill")]
#[test]
fn decode_entry_uses_stream_specific_codec_for_watermill_without_message_field() {
    let metadata = std::collections::HashMap::<String, String>::new();
    let entry = StreamId {
        id: "10-0".into(),
        map: HashMap::from([
            (
                "_watermill_message_uuid".into(),
                Value::BulkString(b"wm-stream-specific".to_vec()),
            ),
            (
                "metadata".into(),
                Value::BulkString(rmp_serde::to_vec_named(&metadata).expect("metadata")),
            ),
            ("payload".into(), Value::BulkString(b"mosaic-bytes".to_vec())),
        ]),
        milliseconds_elapsed_from_delivery: None,
        delivered_count: None,
    };

    let decoded = decode_entry(
        "mapset.mosaic",
        &entry,
        false,
        Arc::new(crate::codec::WatermillStreamCodec::default()),
    );

    let claimed = match decoded {
        FetchedEntry::Decoded(c) => c,
        FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
    };
    assert_eq!(claimed.message.uid, "wm-stream-specific");
    assert_eq!(claimed.message.topic.as_str(), "mapset.mosaic");
    assert_eq!(claimed.message.payload, bytes::Bytes::from_static(b"mosaic-bytes"));
}

#[test]
fn decode_entry_preserves_eventbus_json_with_default_codec() {
    let codec = EventbusJsonStreamCodec::default();
    let bytes = codec
        .encode_fields(
            crate::codec::EncodeContext {
                stream: "native.events",
            },
            &Message {
                uid: "native-id".into(),
                topic: eventbus_core::Topic::new("native.events").expect("topic"),
                key: String::new(),
                kind: "native.kind".into(),
                source: "native".into(),
                occurred_at: Utc::now(),
                headers: HashMap::new(),
                payload: bytes::Bytes::from_static(b"native-payload"),
                content_type: None,
                event_version: None,
                idempotency_key: None,
                expires_at: None,
                trace_uid: None,
                correlation_uid: None,
            },
        )
        .expect("encode fields")
        .remove(0)
        .1;

    let entry = StreamId {
        id: "11-0".into(),
        map: HashMap::from([("message".into(), Value::BulkString(bytes))]),
        milliseconds_elapsed_from_delivery: None,
        delivered_count: None,
    };

    let decoded = decode_entry("native.events", &entry, false, Arc::new(codec));
    let claimed = match decoded {
        FetchedEntry::Decoded(c) => c,
        FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
    };
    assert_eq!(claimed.message.uid, "native-id");
    assert_eq!(claimed.message.payload, bytes::Bytes::from_static(b"native-payload"));
}
```

- [ ] **Step 2: Run tests to verify RED or guard against regression**

Run:

```powershell
cargo test -p eventbus-redis --features watermill decode_entry_uses_stream_specific decode_entry_preserves_eventbus_json -- --nocapture
```

Expected before Task 2 implementation: compile failure. Expected after Task 2 and Task 3: PASS. If these pass immediately after prior tasks, keep them as integration guard tests and proceed.

- [ ] **Step 3: Tighten backend helper behavior if tests reveal gaps**

If the test fails because `decode_entry` does not pass stream context, update the helper to call:

```rust
codec.decode_fields(
    DecodeContext {
        stream,
        redis_id: &id,
    },
    &fields,
)
```

If the test fails because Redis values do not convert into bytes, update `entry_fields` so `BulkString` stays raw and `SimpleString` converts by UTF-8 bytes.

- [ ] **Step 4: Run full Redis test suite with Watermill feature**

Run:

```powershell
cargo test -p eventbus-redis --features watermill -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit Task 5**

Run:

```powershell
git add crates/eventbus-redis/src/redis.rs
git commit -m "Cover mixed Redis stream decode paths" -m "Backend tests now prove Redis entries without the legacy message field can decode through a stream-selected field codec while existing JSON entries remain readable." -m "Constraint: Watermill producers write multiple Redis fields, not the legacy message field" -m "Confidence: high" -m "Scope-risk: narrow" -m "Tested: cargo test -p eventbus-redis --features watermill -- --nocapture" -m "Not-tested: External Redis server round trip"
```

## Task 6: Documentation and Final Verification

**Files:**
- Modify: `crates/eventbus-redis/README.md`
- Modify: `crates/eventbus-contract/src/lib.rs`
- Modify: `crates/eventbus-redis/src/lib.rs`

- [ ] **Step 1: Write documentation update**

Update `crates/eventbus-redis/README.md` wire format section to include:

````markdown
## Wire format

`EventbusJsonStreamCodec` is the default. It encodes each `Message` inside a
`{"message":{...}}` envelope stored in the `"message"` field of each Stream
entry. This preserves the 0.2 default behavior.

With the `watermill` feature enabled, `WatermillStreamCodec` can read Go
Watermill canonical Redis Stream entries:

```text
_watermill_message_uuid = raw string
metadata                = msgpack map<string,string>
payload                 = raw bytes
```

Configure it per stream when consuming Watermill-produced streams:

```rust
use std::sync::Arc;
use eventbus_redis::{RedisBackend, WatermillStreamCodec};

let backend = RedisBackend::new(conn);
backend.set_stream_read_codec("mapset.mosaic", Arc::new(WatermillStreamCodec::default()));
```

Default writes remain in the eventbus JSON envelope. To consume mixed streams,
configure stream-specific read codecs or opt into `AutoDetectRedisStreamCodec`
per stream.
````

Update crate docs in `crates/eventbus-redis/src/lib.rs` to mention:

```rust
//! - [`EventbusJsonStreamCodec`] — default Redis field codec for the eventbus
//!   JSON envelope.
//! - [`WatermillStreamCodec`] — optional `watermill` feature codec for Go
//!   Watermill canonical Redis Stream entries.
```

- [ ] **Step 2: Verify docs compile**

Run:

```powershell
cargo test -p eventbus-redis --features watermill --doc
```

Expected: PASS.

- [ ] **Step 3: Run full focused verification**

Run:

```powershell
cargo fmt --check
cargo clippy -p eventbus-redis --features watermill
cargo test -p eventbus-redis --features watermill -- --nocapture
cargo test -p eventbus-contract --features redis-watermill -- --nocapture
```

Expected: all commands PASS with no warnings requiring code changes.

- [ ] **Step 4: Fix formatting or lint issues using TDD-safe refactor**

If `cargo fmt --check` fails, run:

```powershell
cargo fmt
```

Then rerun:

```powershell
cargo fmt --check
```

If clippy fails, make the smallest refactor that preserves behavior, then rerun the test commands from Step 3.

- [ ] **Step 5: Commit Task 6**

Run:

```powershell
git add crates/eventbus-redis/README.md crates/eventbus-redis/src/lib.rs crates/eventbus-contract/src/lib.rs
git commit -m "Document Redis Watermill stream compatibility" -m "Users can now opt into Watermill canonical Redis Stream decoding per stream while existing eventbus JSON read and write behavior remains the default." -m "Constraint: Compatibility mode must be discoverable without changing eventbus-core docs into Redis-specific docs" -m "Confidence: high" -m "Scope-risk: narrow" -m "Tested: cargo fmt --check; cargo clippy -p eventbus-redis --features watermill; cargo test -p eventbus-redis --features watermill -- --nocapture; cargo test -p eventbus-contract --features redis-watermill -- --nocapture" -m "Not-tested: Live Go Watermill producer fixture"
```

## Final Verification

- [ ] Run:

```powershell
cargo fmt --check
cargo clippy -p eventbus-redis --features watermill
cargo clippy -p eventbus-contract --features redis-watermill
cargo test -p eventbus-redis --features watermill -- --nocapture
cargo test -p eventbus-contract --features redis-watermill -- --nocapture
```

Expected: PASS. These package-level commands avoid applying facade-only features to crates that do not define them.

- [ ] Inspect final diff:

```powershell
git status --short --branch
git diff --stat origin/main...HEAD
```

Expected: only intentional commits for Redis field codecs, Watermill feature, tests, and docs appear. Existing unrelated working-tree changes remain untouched unless the user explicitly asks to include them.
