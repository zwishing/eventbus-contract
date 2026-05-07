use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize<S>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&STANDARD.encode(value))
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    STANDARD
        .decode(value)
        .map(Bytes::from)
        .map_err(D::Error::custom)
}
