use crate::eventbus::Topic;
use chrono::{DateTime, Utc};

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
