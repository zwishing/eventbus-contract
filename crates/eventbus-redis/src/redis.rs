//! Redis Stream backend using [`redis-rs`](https://github.com/redis-rs/redis-rs).
//!
//! Enable with the `redis-backend` cargo feature:
//! ```toml
//! eventbus-core = { path = "...", features = ["redis-backend"] }
//! ```
//!
//! Wire format is compatible with the Go `StreamBus` — messages are
//! serialised as JSON inside a `{"message": ...}` envelope stored in the
//! `"message"` field of each Redis Stream entry by default. [`RedisBackend`]
//! also supports field-level codec registration, with reads resolving
//! subscription, then group, then stream, then default, and writes resolving
//! stream, then default.
//! Override the default [`crate::codec::JsonCodec`] via
//! [`RedisBackend::with_codec`] when wire-compat with the Go implementation is
//! not required, or register field-level codecs for specific streams and
//! consumers.
//!
//! # Connection security
//!
//! [`RedisBackend`] takes an already-connected [`MultiplexedConnection`]; the
//! caller is responsible for choosing the connection URL and any TLS / auth
//! settings:
//!
//! - Use a `rediss://` URL (note the double `s`) to negotiate TLS. The
//!   `rustls`/`native-tls` flavours are gated by `redis-rs` features —
//!   pick one in your downstream `Cargo.toml`.
//! - Use a URL of the form `redis://:<password>@host` (or `redis://user:<password>@host`
//!   for ACL) to authenticate.
//!
//! This crate does not require, default to, or downgrade TLS — the
//! [`MultiplexedConnection`] is treated as opaque. Production deployments
//! should connect over `rediss://` and ensure the server certificate is
//! validated against a known CA.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamId, StreamRangeReply, StreamReadReply};
use redis::{FromRedisValue, Value};

use crate::codec::{
    DecodeContext, EncodeContext, EventbusJsonStreamCodec, JsonCodec, RedisStreamCodec,
    RedisStreamFields,
};
use eventbus_core::stream::{
    ClaimedMessage, FetchedEntry, StreamBackend, StreamBus, StreamBusOptions,
};
use eventbus_core::{Codec, EventBusError, Message, PartialDeliveryState, HEADER_RETRY_ATTEMPT};

#[cfg(test)]
const REDIS_FIELD_MESSAGE: &str = "message";

/// Pre-decode upper bound on the raw envelope size (8 MiB).
///
/// Stops adversarial / runaway producers from forcing the codec to allocate
/// arbitrarily large structures. Roughly 2× the default 4 MiB payload cap to
/// account for any envelope/encoding overhead (base64 + JSON framing in the
/// default JSON codec).
const MAX_RAW_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

/// A [`StreamBackend`] backed by a real Redis connection.
///
/// The wire format is delegated to codecs; the default [`JsonCodec`] wrapped by
/// [`EventbusJsonStreamCodec`] matches the Go `StreamBus` envelope so the two
/// implementations interop. Swap in a binary codec via
/// [`RedisBackend::with_codec`] when wire compat is not required and
/// throughput matters, or register field-level codecs for specific streams,
/// groups, and subscriptions.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use eventbus_core::stream::{StreamBus, StreamBusOptions};
/// use eventbus_redis::{stream_bus_from_connection, RedisBackend};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let conn = client.get_multiplexed_async_connection().await?;
///
/// // Option A: via convenience constructor (uses JsonCodec).
/// let bus = stream_bus_from_connection(conn.clone(), StreamBusOptions::default())?;
///
/// // Option B: explicit backend construction.
/// let backend = Arc::new(RedisBackend::new(conn));
/// let bus = StreamBus::new(backend, StreamBusOptions::default())?;
/// # Ok(())
/// # }
/// ```
///
/// XAUTOCLAIM cursor key — kept as a tuple (rather than a formatted string)
/// so the `DashMap` lookup avoids one allocation per call.
type ReclaimCursorKey = (String, String, String);
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

    fn set_stream_read_codec(&self, stream: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.stream_read.insert(stream.to_string(), codec);
    }

    fn set_group_read_codec(&self, stream: &str, group: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.group_read
            .insert((stream.to_string(), group.to_string()), codec);
    }

    fn set_subscription_read_codec(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        codec: Arc<dyn RedisStreamCodec>,
    ) {
        self.subscription_read.insert(
            (stream.to_string(), group.to_string(), consumer.to_string()),
            codec,
        );
    }

    fn set_stream_write_codec(&self, stream: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.stream_write.insert(stream.to_string(), codec);
    }
}

pub struct RedisBackend {
    conn: MultiplexedConnection,
    /// Field-level wire-format registry. Defaults to JSON in a `message` field.
    registry: CodecRegistry,
    /// Per-(stream, group, consumer) XAUTOCLAIM start-id cursor.
    ///
    /// [`DashMap`] gives lock-free reads and shard-level write contention only
    /// when two consumers share a shard — far cheaper than a single
    /// `Mutex<HashMap>` under high consumer counts, and the synchronous API
    /// means no `.await` can span a held reference (shared references are
    /// released via `entry_ref` clone before any async work).
    reclaim_starts: DashMap<ReclaimCursorKey, String>,
}

impl RedisBackend {
    /// Construct a backend using the default [`JsonCodec`] wire format.
    pub fn new(conn: MultiplexedConnection) -> Self {
        Self::with_codec(conn, Arc::new(JsonCodec))
    }

    /// Construct a backend with a user-supplied [`Codec`].
    ///
    /// Use this when wire-compat with the Go `StreamBus` is not required and
    /// you want to swap in a binary codec for throughput.
    pub fn with_codec(conn: MultiplexedConnection, codec: Arc<dyn Codec>) -> Self {
        Self {
            conn,
            registry: CodecRegistry::new(Arc::new(EventbusJsonStreamCodec::from_core_codec(codec))),
            reclaim_starts: DashMap::new(),
        }
    }

    /// Register a read codec for every consumer reading from `stream`.
    ///
    /// Read resolution prefers subscription-specific codecs first, then
    /// group-level codecs, then this stream-level codec, and finally the
    /// default codec.
    pub fn set_stream_read_codec(&self, stream: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.registry.set_stream_read_codec(stream, codec);
    }

    /// Register a read codec for every consumer in the `(stream, group)` pair.
    ///
    /// This takes precedence over stream-level read codecs and falls behind
    /// subscription-specific codecs.
    pub fn set_group_read_codec(
        &self,
        stream: &str,
        group: &str,
        codec: Arc<dyn RedisStreamCodec>,
    ) {
        self.registry.set_group_read_codec(stream, group, codec);
    }

    /// Register a read codec for a single `(stream, group, consumer)` subscription.
    ///
    /// This is the highest-priority read override in the registry.
    pub fn set_subscription_read_codec(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        codec: Arc<dyn RedisStreamCodec>,
    ) {
        self.registry
            .set_subscription_read_codec(stream, group, consumer, codec);
    }

    /// Register a write codec for `stream`.
    ///
    /// Writes use the stream-specific codec when present and otherwise fall
    /// back to the default write codec.
    pub fn set_stream_write_codec(&self, stream: &str, codec: Arc<dyn RedisStreamCodec>) {
        self.registry.set_stream_write_codec(stream, codec);
    }

    /// Register [`crate::codec::WatermillStreamCodec`] as the read codec for `stream`.
    #[cfg(feature = "watermill")]
    pub fn set_watermill_read_stream(&self, stream: impl Into<String>) {
        let stream = stream.into();
        self.set_stream_read_codec(&stream, Arc::new(crate::codec::WatermillStreamCodec));
    }

    /// Register [`crate::codec::AutoDetectRedisStreamCodec`] as the read codec for `stream`.
    #[cfg(feature = "watermill")]
    pub fn set_auto_detect_read_stream(&self, stream: impl Into<String>) {
        let stream = stream.into();
        self.set_stream_read_codec(
            &stream,
            Arc::new(crate::codec::AutoDetectRedisStreamCodec::default()),
        );
    }
}

// ---------------------------------------------------------------------------
// Convenience constructor
// ---------------------------------------------------------------------------

/// Create a [`StreamBus`] backed by a real Redis connection.
///
/// Shorthand for wrapping the connection in a [`RedisBackend`] (with the
/// default [`JsonCodec`]) and calling [`StreamBus::new`].
pub fn stream_bus_from_connection(
    conn: MultiplexedConnection,
    options: StreamBusOptions,
) -> Result<StreamBus<RedisBackend>, EventBusError> {
    StreamBus::new(Arc::new(RedisBackend::new(conn)), options)
}

// ---------------------------------------------------------------------------
// StreamBackend implementation
// ---------------------------------------------------------------------------

impl StreamBackend for RedisBackend {
    async fn create_group(
        &self,
        stream: &str,
        group: &str,
        start_id: &str,
    ) -> Result<(), EventBusError> {
        let mut conn = self.conn.clone();
        match redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg(start_id)
            .arg("MKSTREAM")
            .query_async::<()>(&mut conn)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if is_busygroup(&err) => Ok(()),
            Err(err) => Err(EventBusError::source(
                format!("create consumer group for stream {stream}"),
                err,
            )),
        }
    }

    async fn publish(&self, stream: &str, message: Message) -> Result<String, EventBusError> {
        let codec = self.registry.write_codec(stream);
        let fields = codec.encode_fields(EncodeContext { stream }, &message)?;

        let mut conn = self.conn.clone();
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream).arg("*");
        for (field, bytes) in fields {
            cmd.arg(field).arg(bytes);
        }
        let id: String = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| EventBusError::source(format!("xadd to {stream}"), e))?;

        Ok(id)
    }

    async fn reclaim_idle(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        count: usize,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        let mut conn = self.conn.clone();
        let cursor_key: ReclaimCursorKey =
            (stream.to_string(), group.to_string(), consumer.to_string());
        // Scope the Ref so no shard lock is held across the `.await` below.
        let start = self
            .reclaim_starts
            .get(&cursor_key)
            .map(|entry| entry.value().clone())
            .unwrap_or_else(|| "0-0".to_string());

        // XAUTOCLAIM <stream> <group> <consumer> <min-idle-ms> <start> COUNT <n>
        let raw: Value = redis::cmd("XAUTOCLAIM")
            .arg(stream)
            .arg(group)
            .arg(consumer)
            .arg(min_idle.as_millis() as u64)
            .arg(&start)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await
            .map_err(|e| EventBusError::source(format!("xautoclaim on {stream}"), e))?;

        let codec = self.registry.read_codec(stream, group, consumer);
        let (next_start, claimed) = parse_autoclaim(raw, stream, codec)?;
        self.reclaim_starts.insert(cursor_key, next_start);
        Ok(claimed)
    }

    async fn read_new(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
        timeout: Duration,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        let mut conn = self.conn.clone();

        let result: Result<StreamReadReply, _> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg(consumer)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(timeout.as_millis() as u64)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .query_async(&mut conn)
            .await;

        let reply = match result {
            Ok(r) => r,
            // Redis returns nil when no new messages are available.
            Err(err) if is_nil_response(&err) => return Ok(Vec::new()),
            Err(err) => {
                return Err(EventBusError::source(
                    format!("xreadgroup on {stream}"),
                    err,
                ))
            }
        };

        // Per-entry decoding: a single corrupt payload becomes a `Malformed`
        // entry rather than poisoning the whole batch (and looping forever
        // because XREADGROUP already moved the entry into the PEL).
        let codec = self.registry.read_codec(stream, group, consumer);
        Ok(reply
            .keys
            .iter()
            .flat_map(|k| k.ids.iter())
            .map(|entry| decode_entry(stream, entry, false, Arc::clone(&codec)))
            .collect())
    }

    async fn ack(&self, stream: &str, group: &str, message_id: &str) -> Result<(), EventBusError> {
        let mut conn = self.conn.clone();
        let _: i64 = redis::cmd("XACK")
            .arg(stream)
            .arg(group)
            .arg(message_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| EventBusError::source(format!("xack {message_id}"), e))?;
        Ok(())
    }

    /// Single-command XACK for N ids — one RTT for the whole batch.
    ///
    /// This is the throughput knob that turns ack rate from
    /// `(1 / RTT)` into `(batch_size / RTT)` — typically 20×+ on LAN Redis.
    async fn ack_many(
        &self,
        stream: &str,
        group: &str,
        message_ids: &[String],
    ) -> Result<(), EventBusError> {
        if message_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn.clone();
        let mut cmd = redis::cmd("XACK");
        cmd.arg(stream).arg(group);
        for id in message_ids {
            cmd.arg(id);
        }
        let _: i64 = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| EventBusError::source(format!("xack batch on {stream}"), e))?;
        Ok(())
    }

    async fn forget_consumer(&self, stream: &str, group: &str, consumer: &str) {
        let key: ReclaimCursorKey = (stream.to_string(), group.to_string(), consumer.to_string());
        self.reclaim_starts.remove(&key);
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Decode a single Redis Stream entry (`StreamId`) into a `FetchedEntry`.
///
/// On success returns `Decoded(ClaimedMessage)`; on any per-entry failure
/// (missing field, oversize raw payload, codec error) returns
/// `Malformed { id, error }` so the bus layer can ack + DLQ + observe
/// instead of poisoning the whole batch.
fn decode_entry(
    stream: &str,
    entry: &StreamId,
    redelivered: bool,
    codec: Arc<dyn RedisStreamCodec>,
) -> FetchedEntry {
    let id = entry.id.clone();

    let fields = match entry_fields(entry) {
        Ok(fields) => fields,
        Err(error) => return FetchedEntry::Malformed { id, error },
    };

    let mut total_raw_bytes = 0usize;
    for (field, bytes) in &fields {
        if bytes.len() > MAX_RAW_PAYLOAD_BYTES {
            return FetchedEntry::Malformed {
                id: id.clone(),
                error: EventBusError::Serialization(format!(
                    "entry {id} field '{field}' raw payload {} bytes exceeds MAX_RAW_PAYLOAD_BYTES {}",
                    bytes.len(),
                    MAX_RAW_PAYLOAD_BYTES,
                )),
            };
        }

        total_raw_bytes = total_raw_bytes.saturating_add(bytes.len());
    }

    if total_raw_bytes > MAX_RAW_PAYLOAD_BYTES {
        return FetchedEntry::Malformed {
            id: id.clone(),
            error: EventBusError::Serialization(format!(
                "entry {id} raw payload {total_raw_bytes} bytes exceeds MAX_RAW_PAYLOAD_BYTES {} across {} fields",
                MAX_RAW_PAYLOAD_BYTES,
                fields.len(),
            )),
        };
    }

    let mut message = match codec.decode_fields(
        DecodeContext {
            stream,
            redis_id: &id,
        },
        &fields,
    ) {
        Ok(m) => m,
        Err(error) => return FetchedEntry::Malformed { id, error },
    };

    // Hoist header values into typed fields once, here at the wire boundary,
    // so consumers can rely on `Message::idempotency_key()` / `schema()` /
    // `trace_context()` without each call re-reading headers.
    message.normalize();

    let attempt = retry_attempt(&message) + 1;
    let now = Utc::now();

    FetchedEntry::Decoded(ClaimedMessage {
        id,
        message: Arc::new(message),
        state: PartialDeliveryState {
            attempt,
            first_received: now,
            last_received: now,
            redelivered,
        },
    })
}

/// Parse the raw `XAUTOCLAIM` response into claimed messages.
///
/// Response shape: `[next-start-id, [entries...], [deleted-ids...]]`
fn parse_autoclaim(
    raw: Value,
    stream: &str,
    codec: Arc<dyn RedisStreamCodec>,
) -> Result<(String, Vec<FetchedEntry>), EventBusError> {
    let items = match raw {
        Value::Array(v) if v.len() >= 2 => v,
        Value::Nil => return Ok(("0-0".to_string(), Vec::new())),
        _ => {
            return Err(EventBusError::Serialization(
                "unexpected XAUTOCLAIM response".into(),
            ))
        }
    };
    let next_start: String = FromRedisValue::from_redis_value(items[0].clone())
        .map_err(|err| EventBusError::source("decode XAUTOCLAIM cursor", err))?;

    // items[1] has the same format as XRANGE output → parse as StreamRangeReply.
    let range: StreamRangeReply = FromRedisValue::from_redis_value(items[1].clone())
        .map_err(|err| EventBusError::source("decode XAUTOCLAIM entries", err))?;

    // Per-entry decoding (no `?`) so a corrupt entry becomes `Malformed`
    // instead of poisoning the whole reclaim batch.
    let claimed = range
        .ids
        .iter()
        .map(|entry| decode_entry(stream, entry, true, Arc::clone(&codec)))
        .collect();

    Ok((next_start, claimed))
}

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

fn retry_attempt(msg: &Message) -> u32 {
    msg.headers
        .get(HEADER_RETRY_ATTEMPT)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Redis returns `ERR BUSYGROUP ...` when a consumer group already exists.
///
/// Prefer the typed [`redis::RedisError::code()`] over string matching so a
/// future redis-rs error-formatting change cannot silently regress this check.
fn is_busygroup(err: &redis::RedisError) -> bool {
    err.code() == Some("BUSYGROUP")
}

/// When XREADGROUP has no new messages Redis returns nil, which surfaces as
/// an `UnexpectedReturnType` deserialization error against `StreamReadReply`.
///
/// `UnexpectedReturnType` can also arise from genuine deserialization bugs;
/// callers should make sure they reach this check only on `XREADGROUP` paths
/// where a nil reply is the expected idle-empty signal.
fn is_nil_response(err: &redis::RedisError) -> bool {
    matches!(err.kind(), redis::ErrorKind::UnexpectedReturnType)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::codec::{EventbusJsonStreamCodec, RedisStreamCodec};

    fn assert_stream_backend<T: StreamBackend>() {}

    #[test]
    fn redis_backend_implements_stream_backend() {
        assert_stream_backend::<RedisBackend>();
    }

    fn malformed_id(entry: &FetchedEntry) -> Option<&str> {
        match entry {
            FetchedEntry::Malformed { id, .. } => Some(id.as_str()),
            FetchedEntry::Decoded(_) => None,
        }
    }

    #[test]
    fn decode_entry_reports_invalid_payload_as_malformed() {
        let entry = StreamId {
            id: "1-0".into(),
            map: HashMap::from([(
                REDIS_FIELD_MESSAGE.into(),
                Value::BulkString(b"not-json".to_vec()),
            )]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry(
            "orders.created",
            &entry,
            false,
            Arc::new(EventbusJsonStreamCodec::default()),
        );
        assert_eq!(malformed_id(&decoded), Some("1-0"));
    }

    #[test]
    fn parse_autoclaim_surfaces_malformed_entry() {
        let raw = Value::Array(vec![
            Value::BulkString(b"0-0".to_vec()),
            Value::Array(vec![Value::Array(vec![
                Value::BulkString(b"1-0".to_vec()),
                Value::Array(vec![
                    Value::BulkString(REDIS_FIELD_MESSAGE.as_bytes().to_vec()),
                    Value::BulkString(b"not-json".to_vec()),
                ]),
            ])]),
            Value::Array(vec![]),
        ]);

        let (_, entries) = parse_autoclaim(
            raw,
            "orders.created",
            Arc::new(EventbusJsonStreamCodec::default()),
        )
        .expect("parse autoclaim");
        assert_eq!(entries.len(), 1);
        assert_eq!(malformed_id(&entries[0]), Some("1-0"));
    }

    #[test]
    fn parse_autoclaim_returns_next_cursor_and_entries() {
        let codec = JsonCodec;
        let bytes = codec
            .encode(&Message {
                uid: "msg-1".into(),
                topic: eventbus_core::Topic::new("orders.created").expect("topic"),
                key: "order-1".into(),
                kind: "orders.created".into(),
                source: "tests".into(),
                occurred_at: Utc::now(),
                headers: HashMap::new(),
                payload: bytes::Bytes::new(),
                content_type: None,
                event_version: None,
                idempotency_key: None,
                expires_at: None,
                trace_uid: None,
                correlation_uid: None,
            })
            .expect("encode message");
        let raw = Value::Array(vec![
            Value::BulkString(b"42-0".to_vec()),
            Value::Array(vec![Value::Array(vec![
                Value::BulkString(b"1-0".to_vec()),
                Value::Array(vec![
                    Value::BulkString(REDIS_FIELD_MESSAGE.as_bytes().to_vec()),
                    Value::BulkString(bytes),
                ]),
            ])]),
            Value::Array(vec![]),
        ]);

        let (cursor, entries) = parse_autoclaim(
            raw,
            "orders.created",
            Arc::new(EventbusJsonStreamCodec::default()),
        )
        .expect("parse xautoclaim");

        assert_eq!(cursor, "42-0");
        assert_eq!(entries.len(), 1);
        let claimed = match &entries[0] {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { .. } => panic!("expected decoded"),
        };
        assert_eq!(claimed.id, "1-0");
    }

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
            map: HashMap::from([("payload".into(), Value::BulkString(b"raw-payload".to_vec()))]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry("custom.stream", &entry, false, Arc::new(PayloadOnlyCodec));

        let claimed = match decoded {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
        };
        assert_eq!(claimed.message.topic.as_str(), "custom.stream");
        assert_eq!(
            claimed.message.payload,
            bytes::Bytes::from_static(b"raw-payload")
        );
    }

    #[cfg(feature = "watermill")]
    #[test]
    fn decode_entry_uses_stream_specific_codec_for_watermill_without_message_field() {
        let metadata = HashMap::<String, String>::new();
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
                (
                    "payload".into(),
                    Value::BulkString(b"mosaic-bytes".to_vec()),
                ),
            ]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry(
            "mapset.mosaic",
            &entry,
            false,
            Arc::new(crate::codec::WatermillStreamCodec),
        );

        let claimed = match decoded {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
        };
        assert_eq!(claimed.message.uid, "wm-stream-specific");
        assert_eq!(claimed.message.topic.as_str(), "mapset.mosaic");
        assert_eq!(
            claimed.message.payload,
            bytes::Bytes::from_static(b"mosaic-bytes")
        );
        assert_eq!(
            claimed.message.idempotency_key.as_deref(),
            Some("wm-stream-specific")
        );
    }

    #[cfg(feature = "watermill")]
    #[test]
    fn decode_entry_uses_auto_detect_codec_for_watermill_without_message_field() {
        let entry = StreamId {
            id: "10-1".into(),
            map: HashMap::from([
                (
                    "_watermill_message_uuid".into(),
                    Value::BulkString(b"wm-auto-entry".to_vec()),
                ),
                ("payload".into(), Value::BulkString(b"mosaic-auto".to_vec())),
            ]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry(
            "mapset.mosaic",
            &entry,
            false,
            Arc::new(crate::codec::AutoDetectRedisStreamCodec::default()),
        );

        let claimed = match decoded {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { error, .. } => panic!("expected decoded, got {error:?}"),
        };
        assert_eq!(claimed.message.uid, "wm-auto-entry");
        assert_eq!(claimed.message.topic.as_str(), "mapset.mosaic");
        assert_eq!(
            claimed.message.payload,
            bytes::Bytes::from_static(b"mosaic-auto")
        );
    }

    #[test]
    fn decode_entry_preserves_eventbus_json_with_default_codec() {
        let codec = EventbusJsonStreamCodec::default();
        let mut fields = codec
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
            .expect("encode fields");
        let (_, bytes) = fields.remove(0);
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
        assert_eq!(
            claimed.message.payload,
            bytes::Bytes::from_static(b"native-payload")
        );
    }

    #[test]
    fn decode_entry_rejects_total_raw_field_bytes_over_limit() {
        #[derive(Debug)]
        struct NeverCodec;

        impl RedisStreamCodec for NeverCodec {
            fn name(&self) -> &str {
                "never"
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
                _ctx: crate::codec::DecodeContext<'_>,
                _fields: &crate::codec::RedisStreamFields,
            ) -> Result<Message, EventBusError> {
                panic!("oversize entries must be rejected before codec decode")
            }

            fn can_decode(&self, _fields: &crate::codec::RedisStreamFields) -> bool {
                true
            }
        }

        let half_plus_one = (MAX_RAW_PAYLOAD_BYTES / 2) + 1;
        let entry = StreamId {
            id: "oversize-0".into(),
            map: HashMap::from([
                ("a".into(), Value::BulkString(vec![0; half_plus_one])),
                ("b".into(), Value::BulkString(vec![0; half_plus_one])),
            ]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let decoded = decode_entry("oversize.stream", &entry, false, Arc::new(NeverCodec));
        match decoded {
            FetchedEntry::Malformed { error, .. } => {
                let msg = error.to_string();
                assert!(msg.contains("raw payload"));
                assert!(msg.contains("MAX_RAW_PAYLOAD_BYTES"));
            }
            FetchedEntry::Decoded(_) => panic!("expected malformed oversize entry"),
        }
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
        assert_eq!(
            registry.read_codec("other", "other", "other").name(),
            "json"
        );
    }

    #[test]
    fn registry_prefers_stream_write_codec_then_default_write_codec() {
        let registry = CodecRegistry::new(Arc::new(EventbusJsonStreamCodec::default()));
        registry.set_stream_write_codec("s", Arc::new(NamedTestCodec("stream-write")));

        assert_eq!(registry.write_codec("s").name(), "stream-write");
        assert_eq!(registry.write_codec("other").name(), "json");
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
}
