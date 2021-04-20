//
// Main entry point for the Page Server executable
//

use log::*;
use std::fs;
use std::io;
use std::process::exit;
use std::thread;
use std::fs::{File, OpenOptions};

use anyhow::{Context, Result};
use clap::{App, Arg};
use daemonize::Daemonize;

use slog::Drain;

use pageserver::page_service;
use pageserver::tui;
//use pageserver::walreceiver;
use pageserver::PageServerConf;

fn zenith_repo_dir() -> String {
    // Find repository path
    match std::env::var_os("ZENITH_REPO_DIR") {
        Some(val) => String::from(val.to_str().unwrap()),
        None => ".zenith".into(),
    }
}

fn main() -> Result<()> {
    let arg_matches = App::new("Zenith page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .arg(Arg::with_name("listen")
                 .short("l")
                 .long("listen")
                 .takes_value(true)
                 .help("listen for incoming page requests on ip:port (default: 127.0.0.1:5430)"))
        .arg(Arg::with_name("interactive")
                 .short("i")
                 .long("interactive")
                 .takes_value(false)
                 .help("Interactive mode"))
        .arg(Arg::with_name("daemonize")
                 .short("d")
                 .long("daemonize")
                 .takes_value(false)
                 .help("Run in the background"))
        .get_matches();

    let mut conf = PageServerConf {
        daemonize: false,
        interactive: false,
        listen_addr: "127.0.0.1:5430".parse().unwrap()
    };

    if arg_matches.is_present("daemonize") {
        conf.daemonize = true;
    }

    if arg_matches.is_present("interactive") {
        conf.interactive = true;
    }

    if conf.daemonize && conf.interactive {
        eprintln!("--daemonize is not allowed with --interactive: choose one");
        exit(1);
    }

    if let Some(addr) = arg_matches.value_of("listen") {
        conf.listen_addr = addr.parse()?;
    }

    start_pageserver(&conf)
}

fn start_pageserver(conf: &PageServerConf) -> Result<()> {
    // Initialize logger
    let _scope_guard = init_logging(&conf)?;
    let _log_guard = slog_stdlog::init()?;

    // Note: this `info!(...)` macro comes from `log` crate
    info!("standard logging redirected to slog");

    let tui_thread: Option<thread::JoinHandle<()>>;
    if conf.interactive {
        // Initialize the UI
        tui_thread = Some(
            thread::Builder::new()
                .name("UI thread".into())
                .spawn(|| {
                    let _ = tui::ui_main();
                })
                .unwrap(),
        );
        //threads.push(tui_thread);
    } else {
        tui_thread = None;
    }

    if conf.daemonize {
        info!("daemonizing...");

        let repodir = zenith_repo_dir();

        // There should'n be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fprintf's or backtraces.
        let log_filename = repodir.clone() + "pageserver.log";
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_filename)
            .with_context(|| format!("failed to open {:?}", &log_filename))?;
        let stderr = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_filename)
            .with_context(|| format!("failed to open {:?}", &log_filename))?;

        let daemonize = Daemonize::new()
            .pid_file(repodir.clone() + "/pageserver.pid")
            .working_directory(repodir)
            .stdout(stdout)
            .stderr(stderr);

        match daemonize.start() {
            Ok(_) => info!("Success, daemonized"),
            Err(e) => error!("Error, {}", e),
        }
    }
    else
    {
        // change into the repository directory. In daemon mode, Daemonize
        // does this for us.
        let repodir = zenith_repo_dir();
        std::env::set_current_dir(&repodir)?;
        info!("Changed current directory to repository in {}", &repodir);
    }

    let mut threads = Vec::new();

    // TODO: Check that it looks like a valid repository before going further

    // Create directory for wal-redo datadirs
    match fs::create_dir("wal-redo") {
        Ok(_) => {}
        Err(e) => match e.kind() {
            io::ErrorKind::AlreadyExists => {}
            _ => {
                anyhow::bail!("Failed to create wal-redo data directory: {}", e);
            }
        },
    }

    // GetPage@LSN requests are served by another thread. (It uses async I/O,
    // but the code in page_service sets up it own thread pool for that)
    let conf_copy = conf.clone();
    let page_server_thread = thread::Builder::new()
        .name("Page Service thread".into())
        .spawn(move || {
            // thread code
            page_service::thread_main(&conf_copy);
        })
        .unwrap();
    threads.push(page_server_thread);

    if tui_thread.is_some() {
        // The TUI thread exits when the user asks to Quit.
        tui_thread.unwrap().join().unwrap();
    } else {
        // In non-interactive mode, wait forever.
        for t in threads {
            t.join().unwrap()
        }
    }
    Ok(())
}

fn init_logging(conf: &PageServerConf) -> Result<slog_scope::GlobalLoggerGuard, io::Error> {
    if conf.interactive {
        Ok(tui::init_logging())
    } else if conf.daemonize {
        let log = zenith_repo_dir() + "/pageserver.log";
        let log_file = File::create(&log).map_err(|err| {
            // We failed to initialize logging, so we can't log this message with error!
            eprintln!("Could not create log file {:?}: {}", log, err);
            err
        })?;
        let decorator = slog_term::PlainSyncDecorator::new(log_file);
        let drain = slog_term::CompactFormat::new(decorator).build();
        let drain = slog::Filter::new(drain, |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Debug) {
                return true;
            }
            return false;
        });
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        Ok(slog_scope::set_global_logger(logger))
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();
        let drain = slog::Filter::new(drain, |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Info) {
                return true;
            }
            if record.level().is_at_least(slog::Level::Debug)
                && record.module().starts_with("pageserver")
            {
                return true;
            }
            return false;
        })
        .fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        Ok(slog_scope::set_global_logger(logger))
    }
}
