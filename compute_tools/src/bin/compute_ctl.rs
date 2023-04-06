//!
//! Postgres wrapper (`compute_ctl`) is intended to be run as a Docker entrypoint or as a `systemd`
//! `ExecStart` option. It will handle all the `Neon` specifics during compute node
//! initialization:
//! - `compute_ctl` accepts cluster (compute node) specification as a JSON file.
//! - Every start is a fresh start, so the data directory is removed and
//!   initialized again on each run.
//! - Next it will put configuration files into the `PGDATA` directory.
//! - Sync safekeepers and get commit LSN.
//! - Get `basebackup` from pageserver using the returned on the previous step LSN.
//! - Try to start `postgres` and wait until it is ready to accept connections.
//! - Check and alter/drop/create roles and databases.
//! - Hang waiting on the `postmaster` process to exit.
//!
//! Also `compute_ctl` spawns two separate service threads:
//! - `compute-monitor` checks the last Postgres activity timestamp and saves it
//!   into the shared `ComputeNode`;
//! - `http-endpoint` runs a Hyper HTTP API server, which serves readiness and the
//!   last activity requests.
//!
//! If the `vm-informant` binary is present at `/bin/vm-informant`, it will also be started. For VM
//! compute nodes, `vm-informant` communicates with the VM autoscaling system. It coordinates
//! downscaling and (eventually) will request immediate upscaling under resource pressure.
//!
//! Usage example:
//! ```sh
//! compute_ctl -D /var/db/postgres/compute \
//!             -C 'postgresql://cloud_admin@localhost/postgres' \
//!             -S /var/db/postgres/specs/current.json \
//!             -b /usr/local/bin/postgres
//! ```
//!
use std::fs::File;
use std::panic;
use std::path::Path;
use std::process::exit;
use std::sync::{Arc, Condvar, Mutex};
use std::{thread, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Arg;
use tracing::{error, info};
use url::Url;

use compute_tools::compute::{ComputeMetrics, ComputeNode, ComputeState, ComputeStatus};
use compute_tools::http::api::launch_http_server;
use compute_tools::logger::*;
use compute_tools::monitor::launch_monitor;
use compute_tools::params::*;
use compute_tools::pg_helpers::*;
use compute_tools::spec::*;

fn main() -> Result<()> {
    init_tracing_and_logging(DEFAULT_LOG_LEVEL)?;

    let matches = cli().get_matches();

    let pgdata = matches
        .get_one::<String>("pgdata")
        .expect("PGDATA path is required");
    let connstr = matches
        .get_one::<String>("connstr")
        .expect("Postgres connection string is required");
    let spec_json = matches.get_one::<String>("spec");
    let spec_path = matches.get_one::<String>("spec-path");

    let compute_id = matches.get_one::<String>("compute-id");
    let control_plane_uri = matches.get_one::<String>("control-plane-uri");

    // Try to use just 'postgres' if no path is provided
    let pgbin = matches.get_one::<String>("pgbin").unwrap();

    let mut spec = Default::default();
    let mut spec_set = false;
    let mut live_config_allowed = false;
    match spec_json {
        // First, try to get cluster spec from the cli argument
        Some(json) => {
            spec = serde_json::from_str(json)?;
            spec_set = true;
        }
        None => {
            // Second, try to read it from the file if path is provided
            if let Some(sp) = spec_path {
                let path = Path::new(sp);
                let file = File::open(path)?;
                spec = serde_json::from_reader(file)?;
                spec_set = true;
            } else if let Some(id) = compute_id {
                if let Some(cp_base) = control_plane_uri {
                    live_config_allowed = true;
                    if let Ok(s) = get_spec_from_control_plane(cp_base, id) {
                        spec = s;
                        spec_set = true;
                    }
                } else {
                    panic!("must specify both --control-plane-uri and --compute-id or none");
                }
            } else {
                panic!(
                    "compute spec should be provided by one of the following ways: \
                    --spec OR --spec-path OR --control-plane-uri and --compute-id"
                );
            }
        }
    };

    let mut new_state = ComputeState::new();
    if spec_set {
        new_state.spec = spec;
    }
    let compute_node = ComputeNode {
        start_time: Utc::now(),
        connstr: Url::parse(connstr).context("cannot parse connstr as a URL")?,
        pgdata: pgdata.to_string(),
        pgbin: pgbin.to_string(),
        live_config_allowed,
        metrics: ComputeMetrics::default(),
        state: Mutex::new(new_state),
        state_changed: Condvar::new(),
    };
    let compute = Arc::new(compute_node);

    // Launch http service first, so we were able to serve control-plane
    // requests, while configuration is still in progress.
    let _http_handle = launch_http_server(&compute).expect("cannot launch http endpoint thread");

    if !spec_set {
        // No spec provided, hang waiting for it.
        info!("no compute spec provided, waiting");
        let mut state = compute.state.lock().unwrap();
        while state.status != ComputeStatus::ConfigurationPending {
            state = compute.state_changed.wait(state).unwrap();

            if state.status == ComputeStatus::ConfigurationPending {
                info!("got spec, continue configuration");
                // Spec is already set by the http server handler.
                break;
            }
        }
    }

    // We got all we need, fill in the state.
    let mut state = compute.state.lock().unwrap();
    let pageserver_connstr = state
        .spec
        .cluster
        .settings
        .find("neon.pageserver_connstring")
        .expect("pageserver connstr should be provided");
    let storage_auth_token = state.spec.storage_auth_token.clone();
    let tenant = state
        .spec
        .cluster
        .settings
        .find("neon.tenant_id")
        .expect("tenant id should be provided");
    let timeline = state
        .spec
        .cluster
        .settings
        .find("neon.timeline_id")
        .expect("tenant id should be provided");
    let startup_tracing_context = state.spec.startup_tracing_context.clone();

    state.pageserver_connstr = pageserver_connstr;
    state.storage_auth_token = storage_auth_token;
    state.tenant = tenant;
    state.timeline = timeline;
    state.status = ComputeStatus::Init;
    compute.state_changed.notify_all();
    drop(state);

    // Extract OpenTelemetry context for the startup actions from the spec, and
    // attach it to the current tracing context.
    //
    // This is used to propagate the context for the 'start_compute' operation
    // from the neon control plane. This allows linking together the wider
    // 'start_compute' operation that creates the compute container, with the
    // startup actions here within the container.
    //
    // Switch to the startup context here, and exit it once the startup has
    // completed and Postgres is up and running.
    //
    // NOTE: This is supposed to only cover the *startup* actions. Once
    // postgres is configured and up-and-running, we exit this span. Any other
    // actions that are performed on incoming HTTP requests, for example, are
    // performed in separate spans.
    let startup_context_guard = if let Some(ref carrier) = startup_tracing_context {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry::sdk::propagation::TraceContextPropagator;
        Some(TraceContextPropagator::new().extract(carrier).attach())
    } else {
        None
    };

    // Launch remaining service threads
    let _monitor_handle = launch_monitor(&compute).expect("cannot launch compute monitor thread");

    // Start Postgres
    let mut delay_exit = false;
    let mut exit_code = None;
    let pg = match compute.start_compute() {
        Ok(pg) => Some(pg),
        Err(err) => {
            error!("could not start the compute node: {:?}", err);
            let mut state = compute.state.lock().unwrap();
            state.error = Some(format!("{:?}", err));
            state.status = ComputeStatus::Failed;
            drop(state);
            delay_exit = true;
            None
        }
    };

    // Wait for the child Postgres process forever. In this state Ctrl+C will
    // propagate to Postgres and it will be shut down as well.
    if let Some(mut pg) = pg {
        // Startup is finished, exit the startup tracing span
        drop(startup_context_guard);

        let ecode = pg
            .wait()
            .expect("failed to start waiting on Postgres process");
        info!("Postgres exited with code {}, shutting down", ecode);
        exit_code = ecode.code()
    }

    if let Err(err) = compute.check_for_core_dumps() {
        error!("error while checking for core dumps: {err:?}");
    }

    // If launch failed, keep serving HTTP requests for a while, so the cloud
    // control plane can get the actual error.
    if delay_exit {
        info!("giving control plane 30s to collect the error before shutdown");
        thread::sleep(Duration::from_secs(30));
    }

    info!("shutting down tracing");
    // Shutdown trace pipeline gracefully, so that it has a chance to send any
    // pending traces before we exit.
    tracing_utils::shutdown_tracing();

    info!("shutting down");
    exit(exit_code.unwrap_or(1))
}

fn cli() -> clap::Command {
    // Env variable is set by `cargo`
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    clap::Command::new("compute_ctl")
        .version(version)
        .arg(
            Arg::new("connstr")
                .short('C')
                .long("connstr")
                .value_name("DATABASE_URL")
                .required(true),
        )
        .arg(
            Arg::new("pgdata")
                .short('D')
                .long("pgdata")
                .value_name("DATADIR")
                .required(true),
        )
        .arg(
            Arg::new("pgbin")
                .short('b')
                .long("pgbin")
                .default_value("postgres")
                .value_name("POSTGRES_PATH"),
        )
        .arg(
            Arg::new("spec")
                .short('s')
                .long("spec")
                .value_name("SPEC_JSON"),
        )
        .arg(
            Arg::new("spec-path")
                .short('S')
                .long("spec-path")
                .value_name("SPEC_PATH"),
        )
        .arg(
            Arg::new("compute-id")
                .short('i')
                .long("compute-id")
                .value_name("COMPUTE_ID"),
        )
        .arg(
            Arg::new("control-plane-uri")
                .short('p')
                .long("control-plane-uri")
                .value_name("CONTROL_PLANE_API_BASE_URI"),
        )
}

#[test]
fn verify_cli() {
    cli().debug_assert()
}
