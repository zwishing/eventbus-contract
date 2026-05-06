//! Built-in [`Codec`] implementations.
//!
//! Currently:
//! - [`JsonCodec`]: wire-compatible with the Go `StreamBus`. Encodes a
//!   `{"message": {...}}` envelope (matching `redisStreamPayload` in Go).
//!   Available with the `redis-backend` feature (which pulls in `serde_json`).

#[cfg(feature = "redis-backend")]
pub use json::JsonCodec;

#[cfg(feature = "redis-backend")]
mod json {
    use serde::{Deserialize, Serialize};

    use crate::{Codec, EventBusError, Message};

    /// JSON codec wrapping [`Message`] in a `{"message": ...}` envelope.
    ///
    /// The envelope shape matches Go's `redisStreamPayload` so producers and
    /// consumers from either language can read each other's streams.
    #[derive(Debug, Default, Clone, Copy)]
    pub struct JsonCodec;

    #[derive(Serialize)]
    struct PayloadRef<'a> {
        message: &'a Message,
    }

    #[derive(Deserialize)]
    struct PayloadOwned {
        message: Message,
    }

    impl Codec for JsonCodec {
        fn name(&self) -> &str {
            "json"
        }

        fn encode(&self, msg: &Message) -> Result<Vec<u8>, EventBusError> {
            // PayloadRef avoids cloning Message just to serialize it.
            serde_json::to_vec(&PayloadRef { message: msg })
                .map_err(|e| EventBusError::source("json encode", e))
        }

        fn decode(&self, bytes: &[u8]) -> Result<Message, EventBusError> {
            let payload: PayloadOwned = serde_json::from_slice(bytes)
                .map_err(|e| EventBusError::source("json decode", e))?;
            Ok(payload.message)
        }
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;

        use chrono::Utc;

        use super::*;

        fn sample() -> Message {
            Message {
                uid: "u".into(),
                topic: "t".into(),
                key: "k".into(),
                kind: "Kind".into(),
                source: "s".into(),
                occurred_at: Utc::now(),
                headers: HashMap::new(),
                payload: bytes::Bytes::from_static(b"hello"),
                content_type: Some("application/json".into()),
                event_version: Some("v1".into()),
                idempotency_key: Some("idem".into()),
                expires_at: None,
                trace_uid: None,
                correlation_uid: None,
            }
        }

        #[test]
        fn round_trip_preserves_message() {
            let codec = JsonCodec;
            let msg = sample();
            let bytes = codec.encode(&msg).expect("encode");
            let decoded = codec.decode(&bytes).expect("decode");
            assert_eq!(decoded.uid, msg.uid);
            assert_eq!(decoded.topic, msg.topic);
            assert_eq!(decoded.payload, msg.payload);
        }

        #[test]
        fn wire_format_uses_message_envelope() {
            let codec = JsonCodec;
            let bytes = codec.encode(&sample()).expect("encode");
            let s = std::str::from_utf8(&bytes).expect("utf-8");
            assert!(
                s.starts_with(r#"{"message":"#),
                "wire format must match Go's redisStreamPayload, got: {s}"
            );
        }
    }
}
