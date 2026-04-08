use thiserror::Error;

#[derive(Debug, Error)]
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
}
