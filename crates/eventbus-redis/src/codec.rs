//! Built-in [`Codec`](eventbus_core::Codec) implementations for the
//! `eventbus-redis` crate.
//!
//! Currently:
//! - [`JsonCodec`]: wire-compatible with the Go `StreamBus`. Encodes a
//!   `{"message": {...}}` envelope (matching `redisStreamPayload` in Go).

use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "watermill")]
use eventbus_core::HEADER_IDEMPOTENCY_KEY;
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
            Arc::new(WatermillStreamCodec),
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
            "auto-detect Redis stream codec cannot be used for writes".into(),
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
        let metadata = rmp_serde::to_vec_named(&msg.headers)
            .map_err(|err| EventBusError::source("watermill metadata encode", err))?;

        Ok(vec![
            (
                "_watermill_message_uuid".to_string(),
                msg.uid.as_bytes().to_vec(),
            ),
            ("metadata".to_string(), metadata),
            ("payload".to_string(), msg.payload.to_vec()),
        ])
    }

    fn decode_fields(
        &self,
        ctx: DecodeContext<'_>,
        fields: &RedisStreamFields,
    ) -> Result<Message, EventBusError> {
        let uid = decode_utf8_field(fields, "_watermill_message_uuid")?;
        let mut headers = decode_watermill_metadata(fields)?;
        let payload = fields.get("payload").cloned().ok_or_else(|| {
            EventBusError::Serialization("watermill entry missing 'payload' field".into())
        })?;
        headers.insert("_watermill_message_uuid".to_string(), uid.clone());
        let idempotency_key = headers
            .entry(HEADER_IDEMPOTENCY_KEY.to_string())
            .or_insert_with(|| uid.clone())
            .clone();

        let kind = first_header(&headers, &["event_type", "type", "kind"])
            .unwrap_or_else(|| "watermill.message".to_string());
        let source = first_header(&headers, &["producer", "source"])
            .unwrap_or_else(|| "watermill".to_string());
        let content_type = first_header(&headers, &["content-type", "content_type"]);
        let event_version = first_header(&headers, &["event-version", "event_version"]);
        let key = first_header(&headers, &["key", "partition_key"]).unwrap_or_default();

        Ok(Message {
            uid: uid.clone(),
            topic: eventbus_core::Topic::new(ctx.stream)?,
            key,
            kind,
            source,
            occurred_at: chrono::Utc::now(),
            headers,
            payload: bytes::Bytes::from(payload),
            content_type,
            event_version,
            idempotency_key: Some(idempotency_key),
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
fn first_header(headers: &HashMap<String, String>, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| headers.get(*name).cloned())
}

#[cfg(feature = "watermill")]
fn decode_utf8_field(fields: &RedisStreamFields, field: &str) -> Result<String, EventBusError> {
    let value = fields.get(field).ok_or_else(|| {
        EventBusError::Serialization(format!("watermill entry missing '{field}' field"))
    })?;

    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|err| EventBusError::source(format!("watermill {field} decode"), err))
}

#[cfg(feature = "watermill")]
fn decode_watermill_metadata(
    fields: &RedisStreamFields,
) -> Result<HashMap<String, String>, EventBusError> {
    match fields.get("metadata") {
        Some(metadata) => rmp_serde::from_slice(metadata)
            .map_err(|err| EventBusError::source("watermill metadata decode", err)),
        None => Ok(HashMap::new()),
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

#[cfg(all(test, feature = "watermill"))]
mod watermill_stream_codec_tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use eventbus_core::{Message, HEADER_IDEMPOTENCY_KEY};
    use rmp_serde::to_vec_named;

    use super::{
        DecodeContext, EncodeContext, RedisStreamCodec, RedisStreamFields, WatermillStreamCodec,
    };

    fn sample_metadata() -> HashMap<String, String> {
        HashMap::from([
            (
                "event_type".to_string(),
                "billing.invoice.created".to_string(),
            ),
            ("producer".to_string(), "ledger-service".to_string()),
            ("content-type".to_string(), "application/json".to_string()),
            ("event-version".to_string(), "v3".to_string()),
            ("tenant".to_string(), "acme".to_string()),
        ])
    }

    fn canonical_fields() -> RedisStreamFields {
        HashMap::from([
            ("_watermill_message_uuid".to_string(), b"wm-123".to_vec()),
            (
                "metadata".to_string(),
                to_vec_named(&sample_metadata()).expect("metadata"),
            ),
            ("payload".to_string(), br#"{"ok":true}"#.to_vec()),
        ])
    }

    fn decode_context() -> DecodeContext<'static> {
        DecodeContext {
            stream: "billing.invoices",
            redis_id: "1710000000000-0",
        }
    }

    fn sample_message() -> Message {
        Message {
            uid: "wm-encode-1".into(),
            topic: eventbus_core::Topic::new("billing.invoices").expect("topic"),
            key: "wm-encode-1".into(),
            kind: "billing.invoice.created".into(),
            source: "ledger-service".into(),
            occurred_at: chrono::Utc::now(),
            headers: sample_metadata(),
            payload: Bytes::from_static(br#"{"ok":true}"#),
            content_type: Some("application/json".into()),
            event_version: Some("v3".into()),
            idempotency_key: Some("wm-encode-1".into()),
            expires_at: None,
            trace_uid: None,
            correlation_uid: None,
        }
    }

    #[test]
    fn watermill_stream_codec_decodes_canonical_entry() {
        let codec = WatermillStreamCodec;
        let decoded = codec
            .decode_fields(decode_context(), &canonical_fields())
            .expect("decode");

        assert_eq!(decoded.uid, "wm-123");
        assert_eq!(decoded.topic.as_str(), "billing.invoices");
        assert_eq!(decoded.headers.get("tenant"), Some(&"acme".to_string()));
        assert_eq!(
            decoded
                .headers
                .get("_watermill_message_uuid")
                .map(String::as_str),
            Some("wm-123")
        );
        assert_eq!(
            decoded
                .headers
                .get(HEADER_IDEMPOTENCY_KEY)
                .map(String::as_str),
            Some("wm-123")
        );
        assert_eq!(decoded.kind, "billing.invoice.created");
        assert_eq!(decoded.source, "ledger-service");
        assert_eq!(decoded.content_type.as_deref(), Some("application/json"));
        assert_eq!(decoded.event_version.as_deref(), Some("v3"));
        assert_eq!(decoded.idempotency_key.as_deref(), Some("wm-123"));
        assert_eq!(decoded.payload, Bytes::from_static(br#"{"ok":true}"#));
    }

    #[test]
    fn watermill_stream_codec_decodes_without_metadata_using_defaults() {
        let codec = WatermillStreamCodec;
        let fields = HashMap::from([
            (
                "_watermill_message_uuid".to_string(),
                b"wm-no-meta".to_vec(),
            ),
            ("payload".to_string(), b"raw-payload".to_vec()),
        ]);

        let decoded = codec
            .decode_fields(decode_context(), &fields)
            .expect("decode");

        assert_eq!(decoded.kind, "watermill.message");
        assert_eq!(decoded.source, "watermill");
        assert_eq!(
            decoded
                .headers
                .get("_watermill_message_uuid")
                .map(String::as_str),
            Some("wm-no-meta")
        );
        assert_eq!(
            decoded
                .headers
                .get(HEADER_IDEMPOTENCY_KEY)
                .map(String::as_str),
            Some("wm-no-meta")
        );
        assert_eq!(decoded.idempotency_key.as_deref(), Some("wm-no-meta"));
        assert_eq!(decoded.payload, Bytes::from_static(b"raw-payload"));
    }

    #[test]
    fn watermill_stream_codec_preserves_metadata_idempotency_key() {
        let codec = WatermillStreamCodec;
        let mut metadata = sample_metadata();
        metadata.insert(
            HEADER_IDEMPOTENCY_KEY.to_string(),
            "metadata-idem".to_string(),
        );
        let fields = HashMap::from([
            ("_watermill_message_uuid".to_string(), b"wm-123".to_vec()),
            (
                "metadata".to_string(),
                to_vec_named(&metadata).expect("metadata"),
            ),
            ("payload".to_string(), b"raw-payload".to_vec()),
        ]);

        let decoded = codec
            .decode_fields(decode_context(), &fields)
            .expect("decode");

        assert_eq!(decoded.uid, "wm-123");
        assert_eq!(decoded.idempotency_key.as_deref(), Some("metadata-idem"));
        assert_eq!(
            decoded
                .headers
                .get(HEADER_IDEMPOTENCY_KEY)
                .map(String::as_str),
            Some("metadata-idem")
        );
    }

    #[test]
    fn watermill_stream_codec_errors_when_uuid_missing() {
        let codec = WatermillStreamCodec;
        let fields = HashMap::from([
            (
                "metadata".to_string(),
                to_vec_named(&sample_metadata()).expect("metadata"),
            ),
            ("payload".to_string(), b"raw-payload".to_vec()),
        ]);

        let err = codec
            .decode_fields(decode_context(), &fields)
            .expect_err("missing uuid should fail");

        assert!(
            err.to_string().contains("_watermill_message_uuid"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn watermill_stream_codec_errors_when_payload_missing() {
        let codec = WatermillStreamCodec;
        let mut fields = canonical_fields();
        fields.remove("payload");

        let err = codec
            .decode_fields(decode_context(), &fields)
            .expect_err("missing payload should fail");

        assert!(
            err.to_string().contains("payload"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn watermill_stream_codec_errors_when_metadata_is_corrupt() {
        let codec = WatermillStreamCodec;
        let fields = HashMap::from([
            (
                "_watermill_message_uuid".to_string(),
                b"wm-bad-meta".to_vec(),
            ),
            ("metadata".to_string(), vec![0xc1, 0x00, 0xff]),
            ("payload".to_string(), b"raw-payload".to_vec()),
        ]);

        let err = codec
            .decode_fields(decode_context(), &fields)
            .expect_err("corrupt metadata should fail");

        assert!(
            err.to_string().contains("metadata"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn watermill_stream_codec_errors_when_metadata_value_is_not_string() {
        let codec = WatermillStreamCodec;
        let metadata = HashMap::from([("event_type".to_string(), 7_u64)]);
        let fields = HashMap::from([
            (
                "_watermill_message_uuid".to_string(),
                b"wm-bad-meta-value".to_vec(),
            ),
            (
                "metadata".to_string(),
                to_vec_named(&metadata).expect("metadata"),
            ),
            ("payload".to_string(), b"raw-payload".to_vec()),
        ]);

        let err = codec
            .decode_fields(decode_context(), &fields)
            .expect_err("non-string metadata should fail");

        assert!(
            err.to_string().contains("metadata"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn watermill_stream_codec_writes_canonical_entry() {
        let codec = WatermillStreamCodec;
        let fields = codec
            .encode_fields(
                EncodeContext {
                    stream: "billing.invoices",
                },
                &sample_message(),
            )
            .expect("encode");

        let fields = HashMap::<_, _>::from_iter(fields);
        assert_eq!(
            fields.get("_watermill_message_uuid"),
            Some(&b"wm-encode-1".to_vec())
        );
        let encoded_metadata = fields
            .get("metadata")
            .expect("metadata field should be present");
        let decoded_metadata: HashMap<String, String> =
            rmp_serde::from_slice(encoded_metadata).expect("metadata should decode");
        assert_eq!(decoded_metadata, sample_metadata());
        assert_eq!(fields.get("payload"), Some(&br#"{"ok":true}"#.to_vec()));
    }

    #[test]
    fn watermill_stream_codec_detects_canonical_entries() {
        let codec = WatermillStreamCodec;
        let fields = canonical_fields();

        assert!(codec.can_decode(&fields));
        assert!(!codec.can_decode(&HashMap::from([(
            "_watermill_message_uuid".to_string(),
            b"wm-only".to_vec(),
        )])));
    }
}

#[cfg(all(test, feature = "watermill"))]
mod auto_detect_stream_codec_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

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
            Arc::new(EventbusJsonStreamCodec::default()),
            Arc::new(WatermillStreamCodec),
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
        assert_eq!(decoded.payload, bytes::Bytes::from_static(b"json-payload"));
    }

    #[test]
    fn auto_detect_decodes_watermill_when_watermill_fields_exist() {
        let fields = HashMap::from([
            ("_watermill_message_uuid".to_string(), b"wm-auto".to_vec()),
            ("payload".to_string(), b"raw".to_vec()),
        ]);
        let codec = AutoDetectRedisStreamCodec::new(vec![
            Arc::new(EventbusJsonStreamCodec::default()),
            Arc::new(WatermillStreamCodec),
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
        assert_eq!(decoded.topic.as_str(), "mapset.auto");
        assert_eq!(decoded.payload, bytes::Bytes::from_static(b"raw"));
    }

    #[test]
    fn auto_detect_reports_field_names_when_no_codec_matches() {
        let fields = HashMap::from([
            ("payload".to_string(), b"raw".to_vec()),
            ("unknown".to_string(), b"value".to_vec()),
        ]);
        let codec = AutoDetectRedisStreamCodec::default();

        let err = codec
            .decode_fields(
                DecodeContext {
                    stream: "unknown.stream",
                    redis_id: "3-0",
                },
                &fields,
            )
            .expect_err("unknown fields should fail");

        let message = err.to_string();
        assert!(message.contains("payload"), "unexpected error: {err}");
        assert!(message.contains("unknown"), "unexpected error: {err}");
    }

    #[test]
    fn auto_detect_is_decode_only() {
        let codec = AutoDetectRedisStreamCodec::default();
        let err = codec
            .encode_fields(
                super::EncodeContext {
                    stream: "native.events",
                },
                &sample_message(),
            )
            .expect_err("auto-detect writes should fail");

        assert!(
            err.to_string().contains("writes"),
            "unexpected error: {err}"
        );
    }
}
