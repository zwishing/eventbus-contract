use chrono::{DateTime, Utc};
use eventbus_core::Topic;

#[derive(Debug, Clone)]
pub struct ConsumerMessageRecord {
    pub uid: String,
    pub message_uid: String,
    pub consumer_group: String,
    pub topic: Topic,
    pub consumer_name: String,
    pub processed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}
