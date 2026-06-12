//! Built-in [`Codec`](eventbus_core::Codec) implementations for the
//! `eventbus-redis` crate.
//!
//! Currently:
//! - [`JsonCodec`]: wire-compatible with the Go `StreamBus`. Encodes a
//!   `{"message": {...}}` envelope (matching `redisStreamPayload` in Go).

use std::collections::HashMap;
use std::sync::Arc;

use eventbus_core::{Codec, EventBusError, Message};

pub use json::JsonCodec;

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
    /// Returns the underlying wire codec name for compatibility and observability.
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

/// Redis stream wrapper that preserves the wrapped wire codec identity in [`Self::name`].
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

mod json {
    use serde::{Deserialize, Serialize};

    use eventbus_core::Message;

    use super::{Codec, EventBusError};

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
                topic: eventbus_core::Topic::new("t").expect("topic"),
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
            let value: serde_json::Value = serde_json::from_slice(&bytes).expect("json value");
            assert!(
                value
                    .get("message")
                    .is_some_and(serde_json::Value::is_object),
                "wire format must match Go's redisStreamPayload, got: {value}"
            );
        }
    }
}

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
        let message = sample_message();
        let expected = JsonCodec.encode(&message).expect("json encode");
        let fields = codec
            .encode_fields(
                EncodeContext {
                    stream: "native.events",
                },
                &message,
            )
            .expect("encode fields");

        assert_eq!(fields, vec![("message".to_string(), expected)]);
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
            .encode_fields(
                EncodeContext {
                    stream: "native.events",
                },
                &sample_message(),
            )
            .expect("encode fields");

        assert_eq!(
            fields,
            vec![("message".to_string(), b"constant-wire".to_vec())]
        );
    }
}
