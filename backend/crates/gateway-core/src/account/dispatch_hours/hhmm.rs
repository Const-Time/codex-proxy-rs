use chrono::NaiveTime;
use serde::{Deserialize, Deserializer, Serializer, de::Error};

pub fn serialize<S: Serializer>(time: &NaiveTime, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&time.format("%H:%M"))
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<NaiveTime, D::Error> {
    let text = String::deserialize(deserializer)?;
    let time = NaiveTime::parse_from_str(&text, "%H:%M").map_err(D::Error::custom)?;
    if time.format("%H:%M").to_string() != text {
        return Err(D::Error::custom("expected HH:MM"));
    }
    Ok(time)
}
