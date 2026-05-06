use thiserror::Error;

/// Errors returned by the event bus.
///
/// The enum is marked `#[non_exhaustive]` so new variants can be added in a
/// non-breaking way; external crates must include a `_ => ...` arm when
/// matching exhaustively.
///
/// Prefer [`EventBusError::source`] when wrapping an underlying error from a
/// backend or codec — it preserves the causal chain via [`std::error::Error::source`],
/// allowing tracing/observability stacks to render the full chain without
/// losing typed information to stringification.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EventBusError {
    #[error("internal error: {0}")]
    Internal(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("invalid state transition: {from} -> {to}")]
    InvalidTransition { from: String, to: String },

    #[error("connection error: {0}")]
    Connection(String),

    #[error("timeout: {0}")]
    Timeout(String),

    /// An underlying error from a backend, codec, or dependency, with its
    /// source chain preserved. Construct via [`EventBusError::source`].
    #[error("{context}")]
    Source {
        context: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl EventBusError {
    /// Wrap an underlying error with a human-readable context string while
    /// preserving the source chain for `error.source()` traversal.
    ///
    /// # Example
    ///
    /// ```ignore
    /// fn parse_payload(bytes: &[u8]) -> Result<Event, EventBusError> {
    ///     serde_json::from_slice(bytes)
    ///         .map_err(|e| EventBusError::source("decoding event payload", e))
    /// }
    /// ```
    pub fn source<E>(context: impl Into<String>, err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Source {
            context: context.into(),
            source: Box::new(err),
        }
    }
}
