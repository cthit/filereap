mod config;

use chrono::{DateTime, FixedOffset, Local};
#[macro_use]
extern crate log;

use clap::Parser;
use config::Config;
use log::LevelFilter;
use std::collections::{BinaryHeap, HashSet};
use std::fs;
use std::io;
use std::path::PathBuf;
use thiserror::Error;

type FileName = DateTime<FixedOffset>;

#[derive(Parser)]
struct Opt {
    config: PathBuf,

    /// Log more stuff
    #[clap(long, short, parse(from_occurrences))]
    verbose: u8,

    /// Do not output anything but errors.
    #[clap(long, short)]
    quiet: bool,

    /// Do not delete anything
    #[clap(long, short)]
    dry_run: bool,
}

#[derive(Debug, Error)]
enum Error {
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),

    #[error("Failed to parse config: {0}")]
    ParseConfig(#[from] toml::de::Error),

    #[error("Managed to overflow a DateTime. What did you do??")]
    DateTimeOverflow,

    #[error("Failed to delete btrfs subvolume: {0}")]
    DeleteSubvolume(String),
}

fn main() {
    let opt = Opt::parse();

    let log_level = match opt.verbose {
        0 if opt.quiet => LevelFilter::Error,
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        2.. => LevelFilter::Trace,
    };

    pretty_env_logger::formatted_builder()
        .filter(None, log_level)
        .init();

    if let Err(e) = run(&opt) {
        println!("{e}");
    }
}

fn run(opt: &Opt) -> Result<(), Error> {
    let config = fs::read_to_string(&opt.config)?;
    let config: Config = toml::from_str(&config)?;
    debug!("periods:");
    for period in &config.periods {
        debug!(
            "  length={:?}, chunk_size={:?}",
            period.period_length, period.chunk_size
        );
    }

    info!("scanning directory {:?}", config.path);

    let mut files = BinaryHeap::new();
    for entry in fs::read_dir(&config.path)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if let Ok(time) = DateTime::parse_from_rfc3339(&name) {
            trace!("found \"{name}\"");
            files.push(time);
        }
    }
    let files = files.into_sorted_vec();

    let keep_files = check_files_to_keep(&config, &files)?;

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

fn check_files_to_keep(config: &Config, files: &[FileName]) -> Result<HashSet<FileName>, Error> {
    let mut files = files.to_vec();

    let mut keep_files = HashSet::new();

    let now = Local::now();
    let mut cursor = now;

    for period in &config.periods {
        if files.is_empty() {
            trace!("no more files, skipping remaining periods");
            break;
        }

        let period_length = chrono::Duration::from_std(period.period_length)
            .map_err(|_| Error::DateTimeOverflow)?;
        let chunk_size =
            chrono::Duration::from_std(period.chunk_size).map_err(|_| Error::DateTimeOverflow)?;

        if period_length < chunk_size {
            panic!("invalid period configuration");
        }

        // NOTE: we are looking backwards in time, so all checks and additions need to be inverted
        let period_end = cursor - period_length;

        while cursor > period_end {
            if files.is_empty() {
                trace!("no more files, skipping remaining chunks");
                break;
            }

            let start_of_chunk = cursor;
            let end_of_chunk = cursor - chunk_size;
            cursor = end_of_chunk;

            let mut chunk_file_to_keep = None;

            trace!("processing chunk {end_of_chunk} -> {start_of_chunk}");
            loop {
                let file = match files.pop() {
                    Some(file) => file,
                    None => break,
                };

                if file > start_of_chunk {
                    trace!("{file} outside of chunk bounds. ignoring.");
                    keep_files.insert(file);
                } else if file > end_of_chunk {
                    trace!("{file} is in chunk. beaten by {chunk_file_to_keep:?}");
                    chunk_file_to_keep.get_or_insert(file);
                } else {
                    trace!("reached end of chunk");
                    files.push(file); // put the file back in the queue
                    break;
                }
            }

            if let Some(file) = chunk_file_to_keep {
                trace!("keeping files {file}");
                keep_files.insert(file);
            }
        }

        cursor = period_end;
    }

    Ok(keep_files)
}

fn delete_file(config: &Config, file: FileName) -> Result<(), Error> {
    let file_path = config.path.join(file.to_rfc3339());

    if config.btrfs {
        trace!("btrfs subvolume delete {file_path:?}");
        use std::process::Command;
        let output = Command::new("btrfs")
            .args(["subvolume", "delete"])
            .arg(file_path)
            .output()?;

        if !output.status.success() {
            let msg = String::from_utf8(output.stderr)
                .unwrap_or_else(|_| "Failed to capture stderr".to_string());
            return Err(Error::DeleteSubvolume(msg));
        };
    } else {
        if file_path.is_dir() {
            trace!("rm -r {file_path:?}");
            fs::remove_dir_all(file_path)?;
        } else {
            trace!("rm {file_path:?}");
            fs::remove_file(file_path)?;
        }
    }

    Ok(())
}
