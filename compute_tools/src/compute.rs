//
// XXX: This starts to be scarry similar to the `PostgresNode` from `control_plane`,
// but there are several things that makes `PostgresNode` usage inconvenient in the
// cloud:
// - it inherits from `LocalEnv`, which contains **all-all** the information about
//   a complete service running
// - it uses `PageServerNode` with information about http endpoint, which we do not
//   need in the cloud again
// - many tiny pieces like, for example, we do not use `pg_ctl` in the cloud
//
// Thus, to use `PostgresNode` in the cloud, we need to 'mock' a bunch of required
// attributes (not required for the cloud). Yet, it is still tempting to unify these
// `PostgresNode` and `ComputeNode` and use one in both places.
//
// TODO: stabilize `ComputeNode` and think about using it in the `control_plane`.
//
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use postgres::{Client, NoTls};
use serde::Serialize;
use tokio_postgres;
use tracing::{info, instrument, warn};

use crate::checker::create_writability_check_data;
use crate::config;
use crate::pg_helpers::*;
use crate::spec::*;

/// Compute node info shared across several `compute_ctl` threads.
pub struct ComputeNode {
    pub start_time: DateTime<Utc>,
    // Url type maintains proper escaping
    pub connstr: url::Url,
    pub pgdata: String,
    pub pgbin: String,
    pub metrics: ComputeMetrics,
    /// We should only allow live re- / configuration of the compute node if
    /// it uses 'pull model', i.e. it can go to control-plane and fetch
    /// the latest configuration. Otherwise, there could be a case:
    /// - we start compute with some spec provided as argument
    /// - we push new spec and it does reconfiguration
    /// - but then something happens and compute pod / VM is destroyed,
    ///   so k8s controller starts it again with the **old** spec
    /// and the same for empty computes:
    /// - we started compute without any spec
    /// - we push spec and it does configuration
    /// - but then it is restarted without any spec again
    pub live_config_allowed: bool,
    /// Volatile part of the `ComputeNode`, which should be used under `Mutex`.
    /// To allow HTTP API server to serving status requests, while configuration
    /// is in progress, lock should be held only for short periods of time to do
    /// read/write, not the whole configuration process.
    pub state: Mutex<ComputeState>,
    /// `Condvar` to allow notifying waiters about state changes.
    pub state_changed: Condvar,
}

#[derive(Clone, Debug)]
pub struct ComputeState {
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity
    pub last_active: DateTime<Utc>,
    pub error: Option<String>,
    pub spec: ComputeSpec,
    pub tenant: String,
    pub timeline: String,
    pub pageserver_connstr: String,
    pub storage_auth_token: Option<String>,
}

impl ComputeState {
    pub fn new() -> Self {
        Self {
            status: ComputeStatus::Empty,
            last_active: Utc::now(),
            error: None,
            spec: ComputeSpec::default(),
            tenant: String::new(),
            timeline: String::new(),
            pageserver_connstr: String::new(),
            storage_auth_token: None,
        }
    }
}

impl Default for ComputeState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum ComputeStatus {
    // Spec wasn't provided at start, waiting for it to be
    // provided by control-plane.
    Empty,
    // Compute configuration was requested.
    ConfigurationPending,
    // Compute node has spec and initial startup and
    // configuration is in progress.
    Init,
    // Compute is configured and running.
    Running,
    // Either startup or configuration failed,
    // compute will exit soon or is waiting for
    // control-plane to terminate it.
    Failed,
}

#[derive(Default, Serialize)]
pub struct ComputeMetrics {
    pub sync_safekeepers_ms: AtomicU64,
    pub basebackup_ms: AtomicU64,
    pub config_ms: AtomicU64,
    pub total_startup_ms: AtomicU64,
}

impl ComputeNode {
    pub fn set_status(&self, status: ComputeStatus) {
        let mut state = self.state.lock().unwrap();
        state.status = status;
        self.state_changed.notify_all();
    }

    pub fn get_status(&self) -> ComputeStatus {
        self.state.lock().unwrap().status
    }

    // Remove `pgdata` directory and create it again with right permissions.
    fn create_pgdata(&self) -> Result<()> {
        // Ignore removal error, likely it is a 'No such file or directory (os error 2)'.
        // If it is something different then create_dir() will error out anyway.
        let _ok = fs::remove_dir_all(&self.pgdata);
        fs::create_dir(&self.pgdata)?;
        fs::set_permissions(&self.pgdata, fs::Permissions::from_mode(0o700))?;

        Ok(())
    }

    // Get basebackup from the libpq connection to pageserver using `connstr` and
    // unarchive it to `pgdata` directory overriding all its previous content.
    #[instrument(skip(self, compute_state))]
    fn get_basebackup(&self, compute_state: &ComputeState, lsn: &str) -> Result<()> {
        let start_time = Utc::now();

        let mut config = postgres::Config::from_str(&compute_state.pageserver_connstr)?;

        // Use the storage auth token from the config file, if given.
        // Note: this overrides any password set in the connection string.
        if let Some(storage_auth_token) = &compute_state.storage_auth_token {
            info!("Got storage auth token from spec file");
            config.password(storage_auth_token);
        } else {
            info!("Storage auth token not set");
        }

        let mut client = config.connect(NoTls)?;
        let basebackup_cmd = match lsn {
            "0/0" => format!(
                "basebackup {} {}",
                &compute_state.tenant, &compute_state.timeline
            ), // First start of the compute
            _ => format!(
                "basebackup {} {} {}",
                &compute_state.tenant, &compute_state.timeline, lsn
            ),
        };
        let copyreader = client.copy_out(basebackup_cmd.as_str())?;

        // Read the archive directly from the `CopyOutReader`
        //
        // Set `ignore_zeros` so that unpack() reads all the Copy data and
        // doesn't stop at the end-of-archive marker. Otherwise, if the server
        // sends an Error after finishing the tarball, we will not notice it.
        let mut ar = tar::Archive::new(copyreader);
        ar.set_ignore_zeros(true);
        ar.unpack(&self.pgdata)?;

        self.metrics.basebackup_ms.store(
            Utc::now()
                .signed_duration_since(start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );

        Ok(())
    }

    // Run `postgres` in a special mode with `--sync-safekeepers` argument
    // and return the reported LSN back to the caller.
    #[instrument(skip(self, storage_auth_token))]
    fn sync_safekeepers(&self, storage_auth_token: Option<String>) -> Result<String> {
        let start_time = Utc::now();

        let sync_handle = Command::new(&self.pgbin)
            .args(["--sync-safekeepers"])
            .env("PGDATA", &self.pgdata) // we cannot use -D in this mode
            .envs(if let Some(storage_auth_token) = &storage_auth_token {
                vec![("NEON_AUTH_TOKEN", storage_auth_token)]
            } else {
                vec![]
            })
            .stdout(Stdio::piped())
            .spawn()
            .expect("postgres --sync-safekeepers failed to start");

        // `postgres --sync-safekeepers` will print all log output to stderr and
        // final LSN to stdout. So we pipe only stdout, while stderr will be automatically
        // redirected to the caller output.
        let sync_output = sync_handle
            .wait_with_output()
            .expect("postgres --sync-safekeepers failed");

        if !sync_output.status.success() {
            anyhow::bail!(
                "postgres --sync-safekeepers exited with non-zero status: {}. stdout: {}",
                sync_output.status,
                String::from_utf8(sync_output.stdout)
                    .expect("postgres --sync-safekeepers exited, and stdout is not utf-8"),
            );
        }

        self.metrics.sync_safekeepers_ms.store(
            Utc::now()
                .signed_duration_since(start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );

        let lsn = String::from(String::from_utf8(sync_output.stdout)?.trim());

        Ok(lsn)
    }

    /// Do all the preparations like PGDATA directory creation, configuration,
    /// safekeepers sync, basebackup, etc.
    #[instrument(skip(self, compute_state))]
    pub fn prepare_pgdata(&self, compute_state: &ComputeState) -> Result<()> {
        let spec = &compute_state.spec;
        let pgdata_path = Path::new(&self.pgdata);

        // Remove/create an empty pgdata directory and put configuration there.
        self.create_pgdata()?;
        config::write_postgres_conf(&pgdata_path.join("postgresql.conf"), spec)?;

        info!("starting safekeepers syncing");
        let lsn = self
            .sync_safekeepers(compute_state.storage_auth_token.clone())
            .with_context(|| "failed to sync safekeepers")?;
        info!("safekeepers synced at LSN {}", lsn);

        info!(
            "getting basebackup@{} from pageserver {}",
            lsn, &compute_state.pageserver_connstr
        );
        self.get_basebackup(compute_state, &lsn).with_context(|| {
            format!(
                "failed to get basebackup@{} from pageserver {}",
                lsn, &compute_state.pageserver_connstr
            )
        })?;

        // Update pg_hba.conf received with basebackup.
        update_pg_hba(pgdata_path)?;

        Ok(())
    }

    /// Start Postgres as a child process and manage DBs/roles.
    /// After that this will hang waiting on the postmaster process to exit.
    #[instrument(skip(self))]
    pub fn start_postgres(
        &self,
        storage_auth_token: Option<String>,
    ) -> Result<std::process::Child> {
        let pgdata_path = Path::new(&self.pgdata);

        // Run postgres as a child process.
        let mut pg = Command::new(&self.pgbin)
            .args(["-D", &self.pgdata])
            .envs(if let Some(storage_auth_token) = &storage_auth_token {
                vec![("NEON_AUTH_TOKEN", storage_auth_token)]
            } else {
                vec![]
            })
            .spawn()
            .expect("cannot start postgres process");

        wait_for_postgres(&mut pg, pgdata_path)?;

        Ok(pg)
    }

    /// Do initial configuration of the already started Postgres.
    #[instrument(skip(self, compute_state))]
    pub fn apply_config(&self, compute_state: &ComputeState) -> Result<()> {
        // If connection fails,
        // it may be the old node with `zenith_admin` superuser.
        //
        // In this case we need to connect with old `zenith_admin` name
        // and create new user. We cannot simply rename connected user,
        // but we can create a new one and grant it all privileges.
        let mut client = match Client::connect(self.connstr.as_str(), NoTls) {
            Err(e) => {
                info!(
                    "cannot connect to postgres: {}, retrying with `zenith_admin` username",
                    e
                );
                let mut zenith_admin_connstr = self.connstr.clone();

                zenith_admin_connstr
                    .set_username("zenith_admin")
                    .map_err(|_| anyhow::anyhow!("invalid connstr"))?;

                let mut client = Client::connect(zenith_admin_connstr.as_str(), NoTls)?;
                client.simple_query("CREATE USER cloud_admin WITH SUPERUSER")?;
                client.simple_query("GRANT zenith_admin TO cloud_admin")?;
                drop(client);

                // reconnect with connsting with expected name
                Client::connect(self.connstr.as_str(), NoTls)?
            }
            Ok(client) => client,
        };

        // Proceed with post-startup configuration. Note, that order of operations is important.
        handle_roles(&compute_state.spec, &mut client)?;
        handle_databases(&compute_state.spec, &mut client)?;
        handle_role_deletions(&compute_state.spec, self.connstr.as_str(), &mut client)?;
        handle_grants(&compute_state.spec, self.connstr.as_str(), &mut client)?;
        create_writability_check_data(&mut client)?;
        handle_extensions(&compute_state.spec, &mut client)?;

        // 'Close' connection
        drop(client);

        info!(
            "finished configuration of compute for project {}",
            compute_state.spec.cluster.cluster_id
        );

        Ok(())
    }

    #[instrument(skip(self))]
    pub fn start_compute(&self) -> Result<std::process::Child> {
        let compute_state = self.state.lock().unwrap().clone();
        info!(
            "starting compute for project {}, operation {}, tenant {}, timeline {}",
            compute_state.spec.cluster.cluster_id,
            compute_state.spec.operation_uuid.as_ref().unwrap(),
            compute_state.tenant,
            compute_state.timeline,
        );

        self.prepare_pgdata(&compute_state)?;

        let start_time = Utc::now();

        let pg = self.start_postgres(compute_state.storage_auth_token.clone())?;

        self.apply_config(&compute_state)?;

        let startup_end_time = Utc::now();
        self.metrics.config_ms.store(
            startup_end_time
                .signed_duration_since(start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );
        self.metrics.total_startup_ms.store(
            startup_end_time
                .signed_duration_since(self.start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );

        self.set_status(ComputeStatus::Running);

        Ok(pg)
    }

    // Look for core dumps and collect backtraces.
    //
    // EKS worker nodes have following core dump settings:
    //   /proc/sys/kernel/core_pattern -> core
    //   /proc/sys/kernel/core_uses_pid -> 1
    //   ulimint -c -> unlimited
    // which results in core dumps being written to postgres data directory as core.<pid>.
    //
    // Use that as a default location and pattern, except macos where core dumps are written
    // to /cores/ directory by default.
    pub fn check_for_core_dumps(&self) -> Result<()> {
        let core_dump_dir = match std::env::consts::OS {
            "macos" => Path::new("/cores/"),
            _ => Path::new(&self.pgdata),
        };

        // Collect core dump paths if any
        info!("checking for core dumps in {}", core_dump_dir.display());
        let files = fs::read_dir(core_dump_dir)?;
        let cores = files.filter_map(|entry| {
            let entry = entry.ok()?;
            let _ = entry.file_name().to_str()?.strip_prefix("core.")?;
            Some(entry.path())
        });

        // Print backtrace for each core dump
        for core_path in cores {
            warn!(
                "core dump found: {}, collecting backtrace",
                core_path.display()
            );

            // Try first with gdb
            let backtrace = Command::new("gdb")
                .args(["--batch", "-q", "-ex", "bt", &self.pgbin])
                .arg(&core_path)
                .output();

            // Try lldb if no gdb is found -- that is handy for local testing on macOS
            let backtrace = match backtrace {
                Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                    warn!("cannot find gdb, trying lldb");
                    Command::new("lldb")
                        .arg("-c")
                        .arg(&core_path)
                        .args(["--batch", "-o", "bt all", "-o", "quit"])
                        .output()
                }
                _ => backtrace,
            }?;

            warn!(
                "core dump backtrace: {}",
                String::from_utf8_lossy(&backtrace.stdout)
            );
            warn!(
                "debugger stderr: {}",
                String::from_utf8_lossy(&backtrace.stderr)
            );
        }

        Ok(())
    }

    /// Select `pg_stat_statements` data and return it as a stringified JSON
    pub async fn collect_insights(&self) -> String {
        let mut result_rows: Vec<String> = Vec::new();
        let connect_result = tokio_postgres::connect(self.connstr.as_str(), NoTls).await;
        let (client, connection) = connect_result.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let result = client
            .simple_query(
                "SELECT
    row_to_json(pg_stat_statements)
FROM
    pg_stat_statements
WHERE
    userid != 'cloud_admin'::regrole::oid
ORDER BY
    (mean_exec_time + mean_plan_time) DESC
LIMIT 100",
            )
            .await;

        if let Ok(raw_rows) = result {
            for message in raw_rows.iter() {
                if let postgres::SimpleQueryMessage::Row(row) = message {
                    if let Some(json) = row.get(0) {
                        result_rows.push(json.to_string());
                    }
                }
            }

            format!("{{\"pg_stat_statements\": [{}]}}", result_rows.join(","))
        } else {
            "{{\"pg_stat_statements\": []}}".to_string()
        }
    }
}
