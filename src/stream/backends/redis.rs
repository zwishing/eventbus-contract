//! Redis Stream backend using [`redis-rs`](https://github.com/redis-rs/redis-rs).
//!
//! Enable with the `redis-backend` cargo feature:
//! ```toml
//! eventbus-contract = { path = "...", features = ["redis-backend"] }
//! ```
//!
//! Wire format is compatible with the Go `StreamBus` — messages are
//! serialised as JSON inside a `{"message": ...}` envelope stored in the
//! `"message"` field of each Redis Stream entry.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamId, StreamRangeReply, StreamReadReply};
use redis::{FromRedisValue, Value};

use crate::codec::JsonCodec;
use crate::{Codec, DeliveryState, EventBusError, Message, HEADER_RETRY_ATTEMPT};

use crate::stream::backend::{ClaimedMessage, StreamBackend};
use crate::stream::bus::{StreamBus, StreamBusOptions};

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
/// The wire format is delegated to a [`Codec`]; the default [`JsonCodec`]
/// matches the Go `StreamBus` envelope so the two implementations interop.
/// Swap in a binary codec via [`RedisBackend::with_codec`] when wire compat
/// is not required and throughput matters.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use eventbus_contract::stream::{RedisBackend, StreamBus, StreamBusOptions};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let conn = client.get_multiplexed_async_connection().await?;
///
/// // Option A: via convenience constructor (uses JsonCodec).
/// let bus = StreamBus::from_connection(conn.clone(), StreamBusOptions::default())?;
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

pub struct RedisBackend {
    conn: MultiplexedConnection,
    /// Wire-format codec. Defaults to [`JsonCodec`] (Go-compatible envelope).
    codec: Arc<dyn Codec>,
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
            codec,
            reclaim_starts: DashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Convenience constructor on StreamBus
// ---------------------------------------------------------------------------

impl StreamBus<RedisBackend> {
    /// Create a bus backed by a real Redis connection.
    ///
    /// This is shorthand for wrapping the connection in a [`RedisBackend`] and
    /// calling [`StreamBus::new`].
    pub fn from_connection(
        conn: MultiplexedConnection,
        options: StreamBusOptions,
    ) -> Result<Self, EventBusError> {
        Self::new(Arc::new(RedisBackend::new(conn)), options)
    }
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
        let bytes = self.codec.encode(&message)?;

        let mut conn = self.conn.clone();
        let id: String = redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg(REDIS_FIELD_MESSAGE)
            .arg(bytes.as_slice())
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
    ) -> Result<Vec<ClaimedMessage>, EventBusError> {
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

        let (next_start, claimed) = parse_autoclaim(raw, self.codec.as_ref())?;
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
    ) -> Result<Vec<ClaimedMessage>, EventBusError> {
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

        reply
            .keys
            .iter()
            .flat_map(|k| k.ids.iter())
            .map(|entry| decode_entry(entry, false, self.codec.as_ref()))
            .collect::<Result<Vec<_>, _>>()
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

/// Decode a single Redis Stream entry (`StreamId`) into a `ClaimedMessage`.
///
/// Pulls the codec-encoded payload out of the `message` field, enforces the
/// pre-decode raw-size guard, then delegates to the codec.
fn decode_entry(
    entry: &StreamId,
    redelivered: bool,
    codec: &dyn Codec,
) -> Result<ClaimedMessage, EventBusError> {
    let val = entry.map.get(REDIS_FIELD_MESSAGE).ok_or_else(|| {
        EventBusError::Serialization(format!(
            "entry {} missing '{REDIS_FIELD_MESSAGE}'",
            entry.id
        ))
    })?;

    // Codecs may produce either text or binary; accept both Redis Value shapes.
    let bytes: Vec<u8> = match val {
        Value::BulkString(b) => b.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        other => FromRedisValue::from_redis_value(other.clone())
            .map(|s: String| s.into_bytes())
            .map_err(|e| EventBusError::source("read message value", e))?,
    };

    if bytes.len() > MAX_RAW_PAYLOAD_BYTES {
        return Err(EventBusError::Serialization(format!(
            "entry {} raw payload {} bytes exceeds MAX_RAW_PAYLOAD_BYTES {}",
            entry.id,
            bytes.len(),
            MAX_RAW_PAYLOAD_BYTES,
        )));
    }

    let mut message = codec.decode(&bytes)?;

    // Hoist header values into typed fields once, here at the wire boundary,
    // so consumers can rely on `Message::idempotency_key()` / `schema()` /
    // `trace_context()` without each call re-reading headers.
    message.normalize();

    let attempt = retry_attempt(&message) + 1;
    let now = Utc::now();

    Ok(ClaimedMessage {
        id: entry.id.clone(),
        message: Arc::new(message),
        state: DeliveryState {
            attempt,
            max_attempt: 0, // filled by bus layer from SubscriptionConfig
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
    codec: &dyn Codec,
) -> Result<(String, Vec<ClaimedMessage>), EventBusError> {
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

    let claimed = range
        .ids
        .iter()
        .map(|entry| decode_entry(entry, true, codec))
        .collect::<Result<Vec<_>, _>>()?;

    Ok((next_start, claimed))
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

    use super::*;

    fn assert_stream_backend<T: StreamBackend>() {}

    #[test]
    fn redis_backend_implements_stream_backend() {
        assert_stream_backend::<RedisBackend>();
    }

    #[test]
    fn decode_entry_reports_invalid_payload() {
        let entry = StreamId {
            id: "1-0".into(),
            map: HashMap::from([(
                REDIS_FIELD_MESSAGE.into(),
                Value::BulkString(b"not-json".to_vec()),
            )]),
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        assert!(decode_entry(&entry, false, &JsonCodec).is_err());
    }

    #[test]
    fn parse_autoclaim_reports_invalid_entry_payload() {
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

        assert!(parse_autoclaim(raw, &JsonCodec).is_err());
    }

    #[test]
    fn parse_autoclaim_returns_next_cursor_and_entries() {
        let codec = JsonCodec;
        let bytes = codec
            .encode(&Message {
                uid: "msg-1".into(),
                topic: "orders.created".into(),
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

        let (cursor, claimed) = parse_autoclaim(raw, &codec).expect("parse xautoclaim");

        assert_eq!(cursor, "42-0");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, "1-0");
    }
}
