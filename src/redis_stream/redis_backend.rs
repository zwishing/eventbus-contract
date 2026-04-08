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

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamId, StreamRangeReply, StreamReadReply};
use redis::{FromRedisValue, Value};

use crate::{DeliveryState, EventBusError, Message};

use super::backend::{ClaimedMessage, StreamBackend};
use super::bus::{RedisStreamBus, RedisStreamBusOptions};

const REDIS_FIELD_MESSAGE: &str = "message";
const HEADER_RETRY_ATTEMPT: &str = "retry-attempt";

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
}

impl RedisBackend {
    pub fn new(conn: MultiplexedConnection) -> Self {
        Self { conn }
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

        // XAUTOCLAIM <stream> <group> <consumer> <min-idle-ms> <start> COUNT <n>
        let raw: Value = redis::cmd("XAUTOCLAIM")
            .arg(stream)
            .arg(group)
            .arg(consumer)
            .arg(min_idle.as_millis() as u64)
            .arg("0-0")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await
            .map_err(|e| EventBusError::Connection(format!("xautoclaim on {stream}: {e}")))?;

        parse_autoclaim(raw)
    }

    async fn read_new(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
    ) -> Result<Vec<ClaimedMessage>, EventBusError> {
        let mut conn = self.conn.clone();

        // XREADGROUP without BLOCK returns immediately.
        let result: Result<StreamReadReply, _> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg(consumer)
            .arg("COUNT")
            .arg(count)
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

        Ok(reply
            .keys
            .iter()
            .flat_map(|k| k.ids.iter())
            .filter_map(|entry| decode_entry(entry, false).ok())
            .collect())
    }

    async fn ack(
        &self,
        stream: &str,
        group: &str,
        message_id: &str,
    ) -> Result<(), EventBusError> {
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

    async fn wait_for_messages(&self, timeout: Duration) {
        tokio::time::sleep(timeout).await;
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Decode a single Redis Stream entry (`StreamId`) into a `ClaimedMessage`.
fn decode_entry(entry: &StreamId, redelivered: bool) -> Result<ClaimedMessage, EventBusError> {
    let val = entry.map.get(REDIS_FIELD_MESSAGE).ok_or_else(|| {
        EventBusError::Serialization(format!("entry {} missing '{REDIS_FIELD_MESSAGE}'", entry.id))
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
fn parse_autoclaim(raw: Value) -> Result<Vec<ClaimedMessage>, EventBusError> {
    let items = match raw {
        Value::Array(v) if v.len() >= 2 => v,
        Value::Nil => return Ok(Vec::new()),
        _ => return Ok(Vec::new()),
    };

    // items[1] has the same format as XRANGE output → parse as StreamRangeReply.
    let range: StreamRangeReply = match FromRedisValue::from_redis_value(items[1].clone()) {
        Ok(r) => r,
        Err(_) => return Ok(Vec::new()),
    };

    Ok(range
        .ids
        .iter()
        .filter_map(|entry| decode_entry(entry, true).ok())
        .collect())
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

/// When XREADGROUP has no new messages Redis returns nil, which causes
/// an `UnexpectedReturnType` error during deserialization.
fn is_nil_response(err: &redis::RedisError) -> bool {
    matches!(err.kind(), redis::ErrorKind::UnexpectedReturnType)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_stream_backend<T: StreamBackend>() {}

    #[test]
    fn redis_backend_implements_stream_backend() {
        assert_stream_backend::<RedisBackend>();
    }
}
