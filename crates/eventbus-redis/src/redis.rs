//! Redis Stream backend using [`redis-rs`](https://github.com/redis-rs/redis-rs).
//!
//! Enable with the `redis-backend` cargo feature:
//! ```toml
//! eventbus-core = { path = "...", features = ["redis-backend"] }
//! ```
//!
//! Wire format is compatible with the Go `StreamBus` — messages are
//! serialised as JSON inside a `{"message": ...}` envelope stored in the
//! `"message"` field of each Redis Stream entry. Override the default
//! [`JsonCodec`](crate::codec::JsonCodec) via [`RedisBackend::with_codec`]
//! when wire-compat with the Go implementation is not required.
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

use crate::codec::JsonCodec;
use eventbus_core::stream::{
    ClaimedMessage, FetchedEntry, StreamBackend, StreamBus, StreamBusOptions,
};
use eventbus_core::{Codec, EventBusError, Message, PartialDeliveryState, HEADER_RETRY_ATTEMPT};

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
        Ok(reply
            .keys
            .iter()
            .flat_map(|k| k.ids.iter())
            .map(|entry| decode_entry(entry, false, self.codec.as_ref()))
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
fn decode_entry(entry: &StreamId, redelivered: bool, codec: &dyn Codec) -> FetchedEntry {
    let id = entry.id.clone();

    let Some(val) = entry.map.get(REDIS_FIELD_MESSAGE) else {
        return FetchedEntry::Malformed {
            id: id.clone(),
            error: EventBusError::Serialization(format!(
                "entry {id} missing '{REDIS_FIELD_MESSAGE}'"
            )),
        };
    };

    // Codecs may produce either text or binary; accept both Redis Value shapes.
    let bytes: Vec<u8> = match val {
        Value::BulkString(b) => b.clone(),
        Value::SimpleString(s) => s.as_bytes().to_vec(),
        other => match FromRedisValue::from_redis_value(other.clone()) {
            Ok(s) => {
                let s: String = s;
                s.into_bytes()
            }
            Err(e) => {
                return FetchedEntry::Malformed {
                    id,
                    error: EventBusError::source("read message value", e),
                };
            }
        },
    };

    if bytes.len() > MAX_RAW_PAYLOAD_BYTES {
        return FetchedEntry::Malformed {
            id: id.clone(),
            error: EventBusError::Serialization(format!(
                "entry {id} raw payload {} bytes exceeds MAX_RAW_PAYLOAD_BYTES {}",
                bytes.len(),
                MAX_RAW_PAYLOAD_BYTES,
            )),
        };
    }

    let mut message = match codec.decode(&bytes) {
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
    codec: &dyn Codec,
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
        .map(|entry| decode_entry(entry, true, codec))
        .collect();

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

        let decoded = decode_entry(&entry, false, &JsonCodec);
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

        let (_, entries) = parse_autoclaim(raw, &JsonCodec).expect("parse autoclaim");
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

        let (cursor, entries) = parse_autoclaim(raw, &codec).expect("parse xautoclaim");

        assert_eq!(cursor, "42-0");
        assert_eq!(entries.len(), 1);
        let claimed = match &entries[0] {
            FetchedEntry::Decoded(c) => c,
            FetchedEntry::Malformed { .. } => panic!("expected decoded"),
        };
        assert_eq!(claimed.id, "1-0");
    }
}
