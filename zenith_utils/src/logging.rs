use slog::{Drain, Level};
use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use anyhow::{Context, Result};

pub fn init(
    log_filename: impl AsRef<Path>,
    daemonize: bool,
) -> Result<(slog_scope::GlobalLoggerGuard, File)> {
    // Don't open the same file for output multiple times;
    // the different fds could overwrite each other's output.
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_filename)
        .with_context(|| format!("failed to open {:?}", log_filename.as_ref()))?;

    // we are cloning and returning log file in order to allow redirecting daemonized stdout and stderr to it
    // if we do not use daemonization (e.g. in docker) it is better to log to stdout directly
    // for example to be in line with docker log command which expects logs comimg from stdout
    let guard = if daemonize {
        let decorator = slog_term::PlainSyncDecorator::new(log_file.try_clone()?);
        let drain = slog_term::FullFormat::new(decorator)
            .build()
            .filter_level(Level::Debug)
            .fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        slog_scope::set_global_logger(logger)
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator)
            .build()
            .filter_level(Level::Debug)
            .fuse();
        let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        slog_scope::set_global_logger(logger)
    };

    // initialise forwarding of std log calls
    slog_stdlog::init()?;

    Ok((guard, log_file))
}
