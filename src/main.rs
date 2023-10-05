//! CLI to delete dated files in a directory according to a time period config.
//! Useful for backup directories that accumulate files over time.

mod config;

#[macro_use]
extern crate log;

use chrono::{DateTime, Duration, FixedOffset, Local};
use clap::{ArgAction, Parser};
use config::{ConfPeriod, Config, SimpleDuration};
use eyre::{eyre, Context};
use log::LevelFilter;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

type FileName = DateTime<FixedOffset>;

#[derive(Parser)]
#[command(version)]
struct Opt {
    config: PathBuf,

    /// Log more stuff
    #[clap(long, short, action = ArgAction::Count)]
    verbose: u8,

    /// Do not output anything but errors.
    #[clap(long, short)]
    quiet: bool,

    /// Do not delete anything
    #[clap(long, short)]
    dry_run: bool,
}

fn main() -> eyre::Result<()> {
    let opt = Opt::parse();
    color_eyre::install()?;

    let log_level = match opt.verbose {
        0 if opt.quiet => LevelFilter::Error,
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        2.. => LevelFilter::Trace,
    };

    pretty_env_logger::formatted_builder()
        .filter(None, log_level)
        .init();

    run(&opt)
}

fn run(opt: &Opt) -> eyre::Result<()> {
    let config = fs::read_to_string(&opt.config)
        .wrap_err_with(|| format!("Failed to read config file {:?}", opt.config))?;

    let config: Config = toml::from_str(&config).wrap_err("Failed to parse config file")?;

    debug!("periods:");
    for period in &config.periods {
        debug!(
            "  length={:?}, chunk_size={:?}",
            period.period_length, period.chunk_size
        );
    }

    info!("scanning directory {:?}", config.path);

    let mut files = BinaryHeap::new();

    let dir_err = || format!("Failed to read directory {:?}", config.path);

    for entry in fs::read_dir(&config.path).wrap_err_with(dir_err)? {
        let name = entry.wrap_err_with(dir_err)?.file_name();
        let name = name.to_string_lossy();
        if let Ok(time) = DateTime::parse_from_rfc3339(&name) {
            trace!("found \"{name}\"");
            files.push(time);
        } else {
            trace!("ignoring \"{name}\", couldn't parse filename as rfc3339");
        }
    }
    let files = files.into_sorted_vec();

    let now = Local::now();
    let keep_files = check_files_to_keep(now, &config.periods, &files);

    info!("final decision:");
    for &file in &files {
        let keep_file = keep_files.contains(&file);

        if keep_file {
            debug!("  {file} KEEP");
        } else {
            info!("  {file} DELETE");
            if opt.dry_run {
                debug!("dry run enabled, file not deleted");
            } else {
                delete_file(&config, file)?;
            }
        }
    }

    Ok(())
}

fn check_files_to_keep(
    now: DateTime<Local>,
    periods: &[ConfPeriod],
    files: &[FileName],
) -> HashSet<FileName> {
    let mut files = files.to_vec();

    debug_assert_eq!(
        files,
        {
            let mut sorted = files.clone();
            sorted.sort();
            sorted
        },
        "file list must be sorted"
    );

    let mut chunked_files = HashMap::new();

    let mut cursor = now;

    'period: for period in periods {
        let first_chunk = ChunkTime::of(period, cursor);
        let start_index = first_chunk.index();
        let stop_index = start_index - period.chunk_count();

        trace!("period {period:?}:");
        trace!("  first chunk: {first_chunk:?}");
        trace!("  index range: {start_index}..{stop_index}");

        'chunk: loop {
            let file = match files.pop() {
                Some(file) => file,
                None => break 'period,
            };

            let file_chunk = ChunkTime::of(period, file.into());

            let index = file_chunk.index();

            trace!("{file}:");
            trace!("  comparing to period {period:?}");
            trace!("  is in chunk {file_chunk:?}");
            trace!("  with index {index}");

            if index <= stop_index {
                trace!("  not in this period, checking next");
                files.push(file);
                cursor = file.into();
                break 'chunk;
            }

            trace!("  keeping for this period");
            chunked_files.insert((period, file_chunk), file);
        }
    }

    chunked_files.values().copied().collect()
}

fn delete_file(config: &Config, file: FileName) -> eyre::Result<()> {
    let file_path = config.path.join(file.to_rfc3339());

    if config.btrfs {
        trace!("btrfs subvolume delete {file_path:?}");
        use std::process::Command;
        let output = Command::new("btrfs")
            .args(["subvolume", "delete"])
            .arg(&file_path)
            .output()
            .wrap_err("failed to run 'btrfs subvolume delete'")?;

        if !output.status.success() {
            let msg = String::from_utf8(output.stderr)
                .unwrap_or_else(|_| "Failed to capture stderr".to_string());

            return Err(
                eyre!("btrfs subvolume delete exited with code {}", output.status)
                    .wrap_err(msg)
                    .wrap_err(format!("Failed to delete subvolume {file_path:?}")),
            );
        };
    } else if file_path.is_dir() {
        trace!("rm -r {file_path:?}");
        fs::remove_dir_all(&file_path)
            .wrap_err_with(|| format!("Failed to remove directory {file_path:?}"))?;
    } else {
        trace!("rm {file_path:?}");
        fs::remove_file(&file_path)
            .wrap_err_with(|| format!("Failed to remove file {file_path:?}"))?;
    }

    Ok(())
}

const EPOCH_STR: &str = "1900-01-01T00:00:00+00:00";
fn epoch() -> DateTime<Local> {
    DateTime::parse_from_rfc3339(EPOCH_STR)
        .expect("Failed to parse epoch")
        .into()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChunkTime {
    /// The value of the time of the chunk, e.g. how many seconds
    pub value: i64,

    /// The time unit of the chunk, e.g. seconds
    pub unit: fn(i64) -> Duration,

    /// A number of whole [unit]s since the epoch
    ///
    /// This value corresponds to a time within the chunk
    pub since_epoch: i64,
}

impl ChunkTime {
    //pub fn next(self) -> ChunkTime {
    //    Self {
    //        since_epoch: self.since_epoch + self.value,
    //        ..self
    //    }
    //}

    pub fn index(&self) -> i64 {
        self.since_epoch / self.value
    }

    pub fn start(&self) -> DateTime<Local> {
        /// compute the largest multiple of `b`, that is smaller than `a`
        fn last_mul_of(a: i64, b: i64) -> i64 {
            a / b * b
        }

        epoch() + (self.unit)(last_mul_of(self.since_epoch, self.value))
    }

    pub fn of(period: &ConfPeriod, time: DateTime<Local>) -> Self {
        let since_epoch = time - epoch();

        use SimpleDuration::*;
        match period.chunk_size {
            Seconds(s) => ChunkTime {
                unit: Duration::seconds,
                value: s,
                since_epoch: since_epoch.num_seconds(),
            },
            Minutes(m) => ChunkTime {
                unit: Duration::minutes,
                value: m,
                since_epoch: since_epoch.num_minutes(),
            },
            Hours(h) => ChunkTime {
                unit: Duration::hours,
                value: h,
                since_epoch: since_epoch.num_hours(),
            },
            Days(d) => ChunkTime {
                unit: Duration::days,
                value: d,
                since_epoch: since_epoch.num_days(),
            },
            Weeks(w) => ChunkTime {
                unit: Duration::weeks,
                value: w,
                since_epoch: since_epoch.num_weeks(),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::{ConfPeriod, SimpleDuration};
    use chrono::DateTime;

    #[test]
    fn chunk_of_period_hours() {
        let period = ConfPeriod {
            period_length: SimpleDuration::Weeks(1),
            chunk_size: SimpleDuration::Hours(12),
        };

        let tests = [
            (
                "2020-01-01T12:00:00+00:00", // time
                "2020-01-01T12:00:00+00:00", // expected chunk start
                87659,                       // expected chunk index
            ),
            (
                "2020-01-02T12:00:00+00:00",
                "2020-01-02T12:00:00+00:00",
                87661,
            ),
            (
                "2020-01-03T12:00:00+00:00",
                "2020-01-03T12:00:00+00:00",
                87663,
            ),
            (
                "2020-01-04T12:00:00+00:00",
                "2020-01-04T12:00:00+00:00",
                87665,
            ),
        ];

        for (time, expected_chunk_start, expected_chunk_index) in tests {
            let time: DateTime<Local> = DateTime::parse_from_rfc3339(time).unwrap().into();
            let expected_chunk_start = DateTime::parse_from_rfc3339(expected_chunk_start).unwrap();

            let chunk = ChunkTime::of(&period, time);

            assert_eq!(chunk.start(), expected_chunk_start);
            assert_eq!(chunk.index(), expected_chunk_index);
        }
    }

    #[test]
    fn chunk_of_period_days() {
        let period = ConfPeriod {
            period_length: SimpleDuration::Days(15),
            chunk_size: SimpleDuration::Days(3),
        };

        let tests = [
            (
                "2020-01-01T12:00:00+00:00", // time
                "2019-12-30T00:00:00+00:00", // expected chunk start
                14609,                       // expected chunk index
            ),
            (
                "2020-01-02T12:00:00+00:00",
                "2020-01-02T00:00:00+00:00",
                14610,
            ),
            (
                "2020-01-03T12:00:00+00:00",
                "2020-01-02T00:00:00+00:00",
                14610,
            ),
            (
                "2020-01-04T12:00:00+00:00",
                "2020-01-02T00:00:00+00:00",
                14610,
            ),
        ];

        for (time, expected_chunk_start, expected_chunk_index) in tests {
            let time: DateTime<Local> = DateTime::parse_from_rfc3339(time).unwrap().into();
            let expected_chunk_start = DateTime::parse_from_rfc3339(expected_chunk_start).unwrap();

            let chunk = ChunkTime::of(&period, time);

            assert_eq!(chunk.start(), expected_chunk_start);
            assert_eq!(chunk.index(), expected_chunk_index);
        }
    }

    #[test]
    fn delete_files() {
        use SimpleDuration::*;

        let periods = [
            ConfPeriod {
                period_length: Hours(6),
                chunk_size: Seconds(1),
            },
            ConfPeriod {
                period_length: Hours(6),
                chunk_size: Hours(1),
            },
            ConfPeriod {
                period_length: Days(8),
                chunk_size: Days(2),
            },
        ];

        let input = [
            "2020-01-01T01:00:00+00:00",
            "2020-01-01T02:00:00+00:00",
            "2020-01-01T03:00:00+00:00",
            "2020-01-01T04:00:00+00:00",
            "2020-01-01T05:00:00+00:00",
            "2020-01-01T06:00:00+00:00",
            "2020-01-01T07:00:00+00:00",
            "2020-01-01T08:00:00+00:00",
            "2020-01-01T09:00:00+00:00",
            "2020-01-01T10:00:00+00:00",
            "2020-01-01T10:00:32+00:00",
            "2020-01-01T10:00:33+00:00",
            "2020-01-01T10:00:34+00:00",
            "2020-01-01T11:00:00+00:00",
            "2020-01-01T12:00:00+00:00",
            "2020-01-01T13:00:00+00:00",
            "2020-01-01T14:00:00+00:00",
            "2020-01-01T15:00:00+00:00",
            "2020-01-01T16:00:00+00:00",
            "2020-01-01T17:00:00+00:00",
            "2020-01-01T18:00:00+00:00",
            "2020-01-01T19:00:00+00:00",
            "2020-01-01T20:00:00+00:00",
            "2020-01-01T21:00:00+00:00",
            "2020-01-01T22:00:00+00:00",
            "2020-01-01T23:00:00+00:00",
            "2020-01-02T00:00:00+00:00",
            "2020-01-02T01:00:00+00:00",
            "2020-01-02T02:00:00+00:00",
            "2020-01-02T03:00:00+00:00",
            "2020-01-02T04:00:00+00:00",
            "2020-01-02T05:00:00+00:00",
            "2020-01-02T06:00:00+00:00",
            "2020-01-02T07:00:00+00:00",
            "2020-01-02T08:00:00+00:00",
            "2020-01-02T09:00:00+00:00",
            "2020-01-02T10:00:00+00:00",
            "2020-01-02T11:00:00+00:00",
            "2020-01-02T12:00:00+00:00",
            "2020-01-02T13:00:00+00:00",
            "2020-01-02T14:00:00+00:00",
            "2020-01-02T15:00:00+00:00",
            "2020-01-02T16:00:00+00:00",
            "2020-01-02T17:00:00+00:00",
            "2020-01-02T18:00:00+00:00",
            "2020-01-02T19:00:00+00:00",
            "2020-01-02T20:00:00+00:00",
            "2020-01-02T21:00:00+00:00",
            "2020-01-02T22:00:00+00:00",
            "2020-01-02T23:00:00+00:00",
            "2020-01-03T00:00:00+00:00",
            "2020-01-03T01:00:00+00:00",
            "2020-01-03T02:00:00+00:00",
            "2020-01-03T03:00:00+00:00",
            "2020-01-03T04:00:00+00:00",
            "2020-01-03T05:00:00+00:00",
            "2020-01-03T06:00:00+00:00",
            "2020-01-03T07:00:00+00:00",
            "2020-01-03T08:00:00+00:00",
            "2020-01-03T09:00:00+00:00",
            "2020-01-03T10:00:00+00:00",
            "2020-01-03T11:00:00+00:00",
            "2020-01-03T12:00:00+00:00",
            "2020-01-03T13:00:00+00:00",
            "2020-01-03T14:00:00+00:00",
            "2020-01-03T14:00:10+00:00",
            "2020-01-03T14:00:20+00:00",
            "2020-01-03T15:00:00+00:00",
            "2020-01-03T16:00:00+00:00",
            "2020-01-03T17:00:00+00:00",
            "2020-01-03T18:00:00+00:00",
            "2020-01-03T19:00:00+00:00",
            "2020-01-03T20:00:00+00:00",
            "2020-01-03T21:00:00+00:00",
            "2020-01-03T22:00:30+00:00",
            "2020-01-03T22:00:31+00:00",
            "2020-01-03T22:00:32+00:00",
            "2020-01-03T22:00:33+00:00",
            "2020-01-03T23:00:00+00:00",
        ];
        let input = input.map(|date| DateTime::parse_from_rfc3339(date).unwrap());

        let expected_output = [
            "2020-01-01T01:00:00+00:00",
            "2020-01-02T00:00:00+00:00",
            "2020-01-03T00:00:00+00:00",
            "2020-01-03T13:00:00+00:00",
            "2020-01-03T14:00:00+00:00",
            "2020-01-03T15:00:00+00:00",
            "2020-01-03T16:00:00+00:00",
            "2020-01-03T17:00:00+00:00",
            "2020-01-03T18:00:00+00:00",
            "2020-01-03T19:00:00+00:00",
            "2020-01-03T20:00:00+00:00",
            "2020-01-03T21:00:00+00:00",
            "2020-01-03T22:00:30+00:00",
            "2020-01-03T22:00:31+00:00",
            "2020-01-03T22:00:32+00:00",
            "2020-01-03T22:00:33+00:00",
            "2020-01-03T23:00:00+00:00",
        ];
        let expected_output: HashSet<_> = expected_output
            .into_iter()
            .map(|date| DateTime::parse_from_rfc3339(date).unwrap())
            .collect();

        let start_time = DateTime::parse_from_rfc3339("2020-01-04T00:00:00+00:00").unwrap();

        let output = check_files_to_keep(start_time.into(), &periods, &input).unwrap();

        assert_eq!(output, expected_output);
    }
}
