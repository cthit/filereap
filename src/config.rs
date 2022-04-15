use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Deserialize)]
pub struct Config {
    /// The folder from which to reap
    pub path: PathBuf,

    /// Whether to treat the files as btrfs subvolumes
    #[serde(default)]
    pub btrfs: bool,

    pub periods: Vec<ConfPeriod>,
}

#[derive(Deserialize)]
pub struct ConfPeriod {
    /// The total duration of this period
    #[serde(deserialize_with = "parse_duration")]
    pub period_length: Duration,

    /// The size of chunks in this period. Each chunk should hold 1 file.
    #[serde(deserialize_with = "parse_duration")]
    pub chunk_size: Duration,
}

fn parse_duration<'de, D>(d: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = d.deserialize_string(StringVisitor)?;

    let mut duration = Duration::ZERO;

    for part in s.split_whitespace() {
        if part.len() < 2 {
            continue;
        }

        let suffix = part.chars().rev().next().unwrap();
        let value = &part[..part.len() - suffix.len_utf8()];

        let value: u32 = value.parse().expect("failed to parse duration value");

        let second: Duration = Duration::from_secs(1);
        let minute: Duration = second * 60;
        let hour: Duration = minute * 60;
        let day: Duration = hour * 24;
        let week: Duration = day * 7;
        let year: Duration = day * 365;

        let unit = match suffix.to_ascii_lowercase() {
            's' => second,
            'm' => minute,
            'h' => hour,
            'd' => day,
            'w' => week,
            'y' => year,
            _ => panic!("unknown unit of duration"),
        };

        duration += unit * value;
    }

    if duration == Duration::ZERO {
        panic!("Invalid duration: Zero");
    }

    Ok(duration)
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
