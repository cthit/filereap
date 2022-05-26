use chrono::Duration;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    /// The folder from which to reap
    pub path: PathBuf,

    /// Whether to treat the files as btrfs subvolumes
    #[serde(default)]
    pub btrfs: bool,

    pub periods: Vec<ConfPeriod>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum SimpleDuration {
    Weeks(i64),
    Days(i64),
    Hours(i64),
    Minutes(i64),
    Seconds(i64),
}

impl From<SimpleDuration> for Duration {
    fn from(simple: SimpleDuration) -> Duration {
        match simple {
            SimpleDuration::Weeks(weeks) => Duration::weeks(weeks),
            SimpleDuration::Days(days) => Duration::days(days),
            SimpleDuration::Hours(hours) => Duration::hours(hours),
            SimpleDuration::Minutes(minutes) => Duration::minutes(minutes),
            SimpleDuration::Seconds(seconds) => Duration::seconds(seconds),
        }
    }
}

#[derive(Debug, Deserialize, Hash, PartialEq, Eq)]
pub struct ConfPeriod {
    /// The total duration of this period
    #[serde(deserialize_with = "parse_simple_duration")]
    pub period_length: SimpleDuration,

    /// The size of chunks in this period. Each chunk should hold 1 file.
    #[serde(deserialize_with = "parse_simple_duration")]
    pub chunk_size: SimpleDuration,
}

impl ConfPeriod {
    pub fn chunk_count(&self) -> i64 {
        let period_length: Duration = self.period_length.into();
        let chunk_size: Duration = self.chunk_size.into();
        period_length.num_milliseconds() / chunk_size.num_milliseconds()
    }
}

fn parse_simple_duration<'de, D>(d: D) -> Result<SimpleDuration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = d.deserialize_string(StringVisitor)?;
    let s = s.trim();

    let suffix = s.chars().rev().next().unwrap();
    let value = &s[..s.len() - suffix.len_utf8()];

    let value: u64 = value.parse().expect("failed to parse duration value");
    let value = value as i64;

    use SimpleDuration::*;
    Ok(match suffix.to_ascii_lowercase() {
        's' => Seconds(value),
        'm' => Minutes(value),
        'h' => Hours(value),
        'd' => Days(value),
        'w' => Weeks(value),
        _ => panic!("unknown unit of duration"),
    })
}

struct StringVisitor;

impl<'de> Visitor<'de> for StringVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string")
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(value.to_string())
    }
}
