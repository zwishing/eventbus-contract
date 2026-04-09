use crate::eventbus::Message;

// ---------------------------------------------------------------------------
// Well-known header keys
// ---------------------------------------------------------------------------

pub const HEADER_CONTENT_TYPE: &str = "content-type";
pub const HEADER_EVENT_VERSION: &str = "event-version";
pub const HEADER_TRACE_PARENT: &str = "traceparent";
pub const HEADER_TRACE_STATE: &str = "tracestate";
pub const HEADER_BAGGAGE: &str = "baggage";
pub const HEADER_IDEMPOTENCY_KEY: &str = "idempotency-key";
pub const HEADER_RETRY_ATTEMPT: &str = "retry-attempt";
pub const HEADER_RETRY_REASON: &str = "retry-reason";
pub const HEADER_DEAD_LETTER_REASON: &str = "dead-letter-reason";

// ---------------------------------------------------------------------------
// Schema descriptor
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct SchemaDescriptor {
    pub content_type: String,
    pub event_version: String,
}

impl SchemaDescriptor {
    pub fn validate(&self) -> Result<(), crate::error::EventBusError> {
        if self.content_type.trim().is_empty() {
            return Err(crate::error::EventBusError::Validation(
                "content type is required".into(),
            ));
        }
        if self.event_version.trim().is_empty() {
            return Err(crate::error::EventBusError::Validation(
                "event version is required".into(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Trace context
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct TraceContext {
    pub trace_parent: Option<String>,
    pub trace_state: Option<String>,
    pub baggage: Option<String>,
    pub trace_uid: Option<String>,
    pub correlation_uid: Option<String>,
}

// ---------------------------------------------------------------------------
// Message extensions
// ---------------------------------------------------------------------------

impl Message {
    pub fn set_schema(&mut self, content_type: &str, event_version: &str) {
        self.headers
            .insert(HEADER_CONTENT_TYPE.into(), content_type.into());
        self.headers
            .insert(HEADER_EVENT_VERSION.into(), event_version.into());
        self.content_type = Some(content_type.into());
        self.event_version = Some(event_version.into());
    }

    pub fn schema(&self) -> SchemaDescriptor {
        let content_type = self
            .content_type
            .clone()
            .or_else(|| self.headers.get(HEADER_CONTENT_TYPE).cloned())
            .unwrap_or_default();

        let event_version = self
            .event_version
            .clone()
            .or_else(|| self.headers.get(HEADER_EVENT_VERSION).cloned())
            .unwrap_or_default();

        SchemaDescriptor {
            content_type,
            event_version,
        }
    }

    pub fn set_trace_context(&mut self, ctx: &TraceContext) {
        if let Some(ref tp) = ctx.trace_parent {
            self.headers.insert(HEADER_TRACE_PARENT.into(), tp.clone());
        }
        if let Some(ref ts) = ctx.trace_state {
            self.headers.insert(HEADER_TRACE_STATE.into(), ts.clone());
        }
        if let Some(ref b) = ctx.baggage {
            self.headers.insert(HEADER_BAGGAGE.into(), b.clone());
        }
        self.trace_uid = ctx.trace_uid.clone();
        self.correlation_uid = ctx.correlation_uid.clone();
    }

    pub fn trace_context(&self) -> TraceContext {
        TraceContext {
            trace_parent: self.headers.get(HEADER_TRACE_PARENT).cloned(),
            trace_state: self.headers.get(HEADER_TRACE_STATE).cloned(),
            baggage: self.headers.get(HEADER_BAGGAGE).cloned(),
            trace_uid: self.trace_uid.clone(),
            correlation_uid: self.correlation_uid.clone(),
        }
    }

    pub fn set_idempotency_key(&mut self, key: &str) {
        self.idempotency_key = Some(key.into());
        self.headers
            .insert(HEADER_IDEMPOTENCY_KEY.into(), key.into());
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key
            .as_deref()
            .or_else(|| self.headers.get(HEADER_IDEMPOTENCY_KEY).map(|s| s.as_str()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eventbus::Headers;
    use chrono::Utc;

    fn test_message() -> Message {
        Message {
            uid: "test-uid".into(),
            topic: "test.topic".into(),
            key: "key".into(),
            kind: "test.kind".into(),
            source: "test".into(),
            occurred_at: Utc::now(),
            headers: Headers::new(),
            payload: vec![],
            content_type: None,
            event_version: None,
            idempotency_key: None,
            expires_at: None,
            trace_uid: None,
            correlation_uid: None,
        }
    }

    #[test]
    fn schema_roundtrip() {
        let mut msg = test_message();
        msg.set_schema("application/json", "v1");

        let schema = msg.schema();
        assert_eq!(schema.content_type, "application/json");
        assert_eq!(schema.event_version, "v1");
    }

    #[test]
    fn trace_context_roundtrip() {
        let mut msg = test_message();
        let trace_uid = "trace-uid".to_string();
        let correlation_uid = "corr-uid".to_string();

        msg.set_trace_context(&TraceContext {
            trace_parent: Some("00-abc-def-01".into()),
            trace_state: Some("vendor=value".into()),
            baggage: Some("key=value".into()),
            trace_uid: Some(trace_uid.clone()),
            correlation_uid: Some(correlation_uid.clone()),
        });

        let ctx = msg.trace_context();
        assert_eq!(ctx.trace_parent.as_deref(), Some("00-abc-def-01"));
        assert_eq!(ctx.trace_uid.as_deref(), Some("trace-uid"));
        assert_eq!(ctx.correlation_uid.as_deref(), Some("corr-uid"));
    }

    #[test]
    fn idempotency_key_roundtrip() {
        let mut msg = test_message();
        msg.set_idempotency_key("idem-123");

        assert_eq!(msg.idempotency_key(), Some("idem-123"));
        assert_eq!(
            msg.headers.get(HEADER_IDEMPOTENCY_KEY).map(|s| s.as_str()),
            Some("idem-123"),
        );
    }

    #[test]
    fn idempotency_key_falls_back_to_header() {
        let mut msg = test_message();
        msg.headers
            .insert(HEADER_IDEMPOTENCY_KEY.into(), "from-header".into());

        assert_eq!(msg.idempotency_key(), Some("from-header"));
    }

    #[test]
    fn schema_descriptor_validate() {
        let valid = SchemaDescriptor {
            content_type: "application/json".into(),
            event_version: "v1".into(),
        };
        assert!(valid.validate().is_ok());

        let invalid = SchemaDescriptor {
            content_type: "".into(),
            event_version: "v1".into(),
        };
        assert!(invalid.validate().is_err());
    }
}
