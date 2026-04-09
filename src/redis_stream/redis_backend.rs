//! Redis Stream backend using [`redis-rs`](https://github.com/redis-rs/redis-rs).
//!
//! Enable with the `redis-backend` cargo feature:
//! ```toml
//! eventbus-contract = { path = "...", features = ["redis-backend"] }
//! ```
//!
//! Wire format is compatible with the Go `RedisStreamBus` — messages are
//! serialised as JSON inside a `{"message": ...}` envelope stored in the
//! `"message"` field of each Redis Stream entry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamId, StreamRangeReply, StreamReadReply};
use redis::{FromRedisValue, Value};

use crate::{DeliveryState, EventBusError, Message, HEADER_RETRY_ATTEMPT};

use super::backend::{ClaimedMessage, StreamBackend};
use super::bus::{RedisStreamBus, RedisStreamBusOptions};

const REDIS_FIELD_MESSAGE: &str = "message";

/// JSON envelope matching Go's `redisStreamPayload`.
#[derive(serde::Serialize, serde::Deserialize)]
struct Payload {
    message: Message,
}

/// A [`StreamBackend`] backed by a real Redis connection.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use eventbus_contract::redis_stream::{RedisBackend, RedisStreamBus, RedisStreamBusOptions};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let conn = client.get_multiplexed_async_connection().await?;
///
/// // Option A: via convenience constructor
/// let bus = RedisStreamBus::from_connection(conn.clone(), RedisStreamBusOptions::default())?;
///
/// // Option B: explicit backend construction
/// let backend = Arc::new(RedisBackend::new(conn));
/// let bus = RedisStreamBus::new(backend, RedisStreamBusOptions::default())?;
/// # Ok(())
/// # }
/// ```
pub struct RedisBackend {
    conn: MultiplexedConnection,
    reclaim_starts: Mutex<HashMap<String, String>>,
}

impl RedisBackend {
    pub fn new(conn: MultiplexedConnection) -> Self {
        Self {
            conn,
            reclaim_starts: Mutex::new(HashMap::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Convenience constructor on RedisStreamBus
// ---------------------------------------------------------------------------

impl RedisStreamBus<RedisBackend> {
    /// Create a bus backed by a real Redis connection.
    ///
    /// This is shorthand for wrapping the connection in a [`RedisBackend`] and
    /// calling [`RedisStreamBus::new`].
    pub fn from_connection(
        conn: MultiplexedConnection,
        options: RedisStreamBusOptions,
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
            Err(err) => Err(EventBusError::Connection(format!(
                "create consumer group for stream {stream}: {err}"
            ))),
        }
    }

    async fn publish(&self, stream: &str, message: Message) -> Result<String, EventBusError> {
        let json = serde_json::to_string(&Payload { message })
            .map_err(|e| EventBusError::Serialization(e.to_string()))?;

        let mut conn = self.conn.clone();
        let id: String = redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg(REDIS_FIELD_MESSAGE)
            .arg(&json)
            .query_async(&mut conn)
            .await
            .map_err(|e| EventBusError::Connection(format!("xadd to {stream}: {e}")))?;

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
        let cursor_key = format!("{stream}:{group}:{consumer}");
        let start = self
            .reclaim_starts
            .lock()
            .map_err(|_| EventBusError::Internal("reclaim cursor mutex poisoned".into()))?
            .get(&cursor_key)
            .cloned()
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
            .map_err(|e| EventBusError::Connection(format!("xautoclaim on {stream}: {e}")))?;

        let (next_start, claimed) = parse_autoclaim(raw)?;
        self.reclaim_starts
            .lock()
            .map_err(|_| EventBusError::Internal("reclaim cursor mutex poisoned".into()))?
            .insert(cursor_key, next_start);
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
                return Err(EventBusError::Connection(format!(
                    "xreadgroup on {stream}: {err}"
                )))
            }
        };

        reply
            .keys
            .iter()
            .flat_map(|k| k.ids.iter())
            .map(|entry| decode_entry(entry, false))
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
            .map_err(|e| EventBusError::Connection(format!("xack {message_id}: {e}")))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Decode a single Redis Stream entry (`StreamId`) into a `ClaimedMessage`.
fn decode_entry(entry: &StreamId, redelivered: bool) -> Result<ClaimedMessage, EventBusError> {
    let val = entry.map.get(REDIS_FIELD_MESSAGE).ok_or_else(|| {
        EventBusError::Serialization(format!(
            "entry {} missing '{REDIS_FIELD_MESSAGE}'",
            entry.id
        ))
    })?;

    let json: String = FromRedisValue::from_redis_value(val.clone())
        .map_err(|e| EventBusError::Serialization(format!("read message value: {e}")))?;

    let payload: Payload = serde_json::from_str(&json)
        .map_err(|e| EventBusError::Serialization(format!("decode entry {}: {e}", entry.id)))?;

    let attempt = retry_attempt(&payload.message) + 1;
    let now = Utc::now();

    Ok(ClaimedMessage {
        id: entry.id.clone(),
        message: payload.message,
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
fn parse_autoclaim(raw: Value) -> Result<(String, Vec<ClaimedMessage>), EventBusError> {
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
        .map_err(|err| EventBusError::Serialization(format!("decode XAUTOCLAIM cursor: {err}")))?;

    // items[1] has the same format as XRANGE output → parse as StreamRangeReply.
    let range: StreamRangeReply = FromRedisValue::from_redis_value(items[1].clone())
        .map_err(|err| EventBusError::Serialization(format!("decode XAUTOCLAIM entries: {err}")))?;

    let claimed = range
        .ids
        .iter()
        .map(|entry| decode_entry(entry, true))
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
fn is_busygroup(err: &redis::RedisError) -> bool {
    err.to_string().contains("BUSYGROUP")
}

/// When XREADGROUP has no new messages Redis returns nil, which surfaces as
/// either a nil value or an `UnexpectedReturnType` deserialization error.
fn is_nil_response(err: &redis::RedisError) -> bool {
    err.is_nil() || matches!(err.kind(), redis::ErrorKind::UnexpectedReturnType)
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

        assert!(decode_entry(&entry, false).is_err());
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

        assert!(parse_autoclaim(raw).is_err());
    }

    #[test]
    fn parse_autoclaim_returns_next_cursor_and_entries() {
        let json = serde_json::to_string(&Payload {
            message: Message {
                uid: "msg-1".into(),
                topic: "orders.created".into(),
                key: "order-1".into(),
                kind: "orders.created".into(),
                source: "tests".into(),
                occurred_at: Utc::now(),
                headers: HashMap::new(),
                payload: vec![],
                content_type: None,
                event_version: None,
                idempotency_key: None,
                expires_at: None,
                trace_uid: None,
                correlation_uid: None,
            },
        })
        .expect("serialize payload");
        let raw = Value::Array(vec![
            Value::BulkString(b"42-0".to_vec()),
            Value::Array(vec![Value::Array(vec![
                Value::BulkString(b"1-0".to_vec()),
                Value::Array(vec![
                    Value::BulkString(REDIS_FIELD_MESSAGE.as_bytes().to_vec()),
                    Value::BulkString(json.into_bytes()),
                ]),
            ])]),
            Value::Array(vec![]),
        ]);

        let (cursor, claimed) = parse_autoclaim(raw).expect("parse xautoclaim");

        assert_eq!(cursor, "42-0");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, "1-0");
    }
}
