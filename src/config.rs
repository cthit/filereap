use chrono::Duration;
use serde::de::Visitor;
use serde::{de::Error, Deserialize, Deserializer};
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

/// Deserialize a [SimpleDuration] from a string like "3d" or "24h".
fn parse_simple_duration<'de, D>(d: D) -> Result<SimpleDuration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = d.deserialize_str(StrVisitor)?;
    let s = s.trim();

    if s.contains(char::is_whitespace) {
        return Err(D::Error::custom("duration can't include whitespace"));
    }

    let suffix = s
        .chars()
        .next_back()
        .ok_or_else(|| D::Error::custom("duration can't be empty"))?;

    if suffix.is_ascii_digit() {
        return Err(D::Error::custom(
            r#"specify duration with a suffix, i.e. "24h""#,
        ));
    }

    let value = &s[..s.len() - suffix.len_utf8()];

    let value: u64 = value
        .parse()
        .map_err(|e| D::Error::custom(format!("failed to parse duration value: {e}")))?;
    let value = value as i64;

    use SimpleDuration::*;
    Ok(match suffix.to_ascii_lowercase() {
        's' => Seconds(value),
        'm' => Minutes(value),
        'h' => Hours(value),
        'd' => Days(value),
        'w' => Weeks(value),
        d => return Err(D::Error::custom(format!("unknown unit of duration: {d:?}"))),
    })
}

struct StrVisitor;

impl<'de> Visitor<'de> for StrVisitor {
    type Value = &'de str;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string")
    }

    fn visit_borrowed_str<E>(self, s: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(s)
    }
}
