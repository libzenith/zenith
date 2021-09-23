//
// Main entry point for the Page Server executable
//

use log::*;
use pageserver::defaults::*;
use serde::{Deserialize, Serialize};
use std::{
    env,
    net::TcpListener,
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
    thread,
};
use zenith_utils::{auth::JwtAuth, logging, postgres_backend::AuthType};

use anyhow::{bail, ensure, Result};
use clap::{App, Arg, ArgMatches};
use daemonize::Daemonize;

use pageserver::{
    branches, http, page_service, tenant_mgr, PageServerConf, RelishStorageConfig, S3Config,
    LOG_FILE_NAME,
};
use zenith_utils::http::endpoint;

/// String arguments that can be declared via CLI or config file
#[derive(Serialize, Deserialize)]
struct CfgFileParams {
    listen_pg_addr: Option<String>,
    listen_http_addr: Option<String>,
    checkpoint_distance: Option<String>,
    checkpoint_period: Option<String>,
    gc_horizon: Option<String>,
    gc_period: Option<String>,
    pg_distrib_dir: Option<String>,
    auth_validation_public_key_path: Option<String>,
    auth_type: Option<String>,
    // see https://github.com/alexcrichton/toml-rs/blob/6c162e6562c3e432bf04c82a3d1d789d80761a86/examples/enum_external.rs for enum deserialisation examples
    relish_storage: Option<RelishStorage>,
}

#[derive(Serialize, Deserialize, Clone)]
enum RelishStorage {
    Local {
        local_path: String,
    },
    AwsS3 {
        bucket_name: String,
        bucket_region: String,
        #[serde(skip_serializing)]
        access_key_id: Option<String>,
        #[serde(skip_serializing)]
        secret_access_key: Option<String>,
    },
}

impl CfgFileParams {
    /// Extract string arguments from CLI
    fn from_args(arg_matches: &ArgMatches) -> Self {
        let get_arg = |arg_name: &str| -> Option<String> {
            arg_matches.value_of(arg_name).map(str::to_owned)
        };

        let relish_storage = if let Some(local_path) = get_arg("relish-storage-local-path") {
            Some(RelishStorage::Local { local_path })
        } else if let Some((bucket_name, bucket_region)) =
            get_arg("relish-storage-s3-bucket").zip(get_arg("relish-storage-region"))
        {
            Some(RelishStorage::AwsS3 {
                bucket_name,
                bucket_region,
                access_key_id: get_arg("relish-storage-access-key"),
                secret_access_key: get_arg("relish-storage-secret-access-key"),
            })
        } else {
            None
        };

        Self {
            listen_pg_addr: get_arg("listen-pg"),
            listen_http_addr: get_arg("listen-http"),
            checkpoint_distance: get_arg("checkpoint_distance"),
            checkpoint_period: get_arg("checkpoint_period"),
            gc_horizon: get_arg("gc_horizon"),
            gc_period: get_arg("gc_period"),
            pg_distrib_dir: get_arg("postgres-distrib"),
            auth_validation_public_key_path: get_arg("auth-validation-public-key-path"),
            auth_type: get_arg("auth-type"),
            relish_storage,
        }
    }

    /// Fill missing values in `self` with `other`
    fn or(self, other: CfgFileParams) -> Self {
        // TODO cleaner way to do this
        Self {
            listen_pg_addr: self.listen_pg_addr.or(other.listen_pg_addr),
            listen_http_addr: self.listen_http_addr.or(other.listen_http_addr),
            checkpoint_distance: self.checkpoint_distance.or(other.checkpoint_distance),
            checkpoint_period: self.checkpoint_period.or(other.checkpoint_period),
            gc_horizon: self.gc_horizon.or(other.gc_horizon),
            gc_period: self.gc_period.or(other.gc_period),
            pg_distrib_dir: self.pg_distrib_dir.or(other.pg_distrib_dir),
            auth_validation_public_key_path: self
                .auth_validation_public_key_path
                .or(other.auth_validation_public_key_path),
            auth_type: self.auth_type.or(other.auth_type),
            relish_storage: self.relish_storage.or(other.relish_storage),
        }
    }

    /// Create a PageServerConf from these string parameters
    fn try_into_config(&self) -> Result<PageServerConf> {
        let workdir = PathBuf::from(".");

        let listen_pg_addr = match self.listen_pg_addr.as_ref() {
            Some(addr) => addr.clone(),
            None => DEFAULT_PG_LISTEN_ADDR.to_owned(),
        };

        let listen_http_addr = match self.listen_http_addr.as_ref() {
            Some(addr) => addr.clone(),
            None => DEFAULT_HTTP_LISTEN_ADDR.to_owned(),
        };

        let checkpoint_distance: u64 = match self.checkpoint_distance.as_ref() {
            Some(checkpoint_distance_str) => checkpoint_distance_str.parse()?,
            None => DEFAULT_CHECKPOINT_DISTANCE,
        };
        let checkpoint_period = match self.checkpoint_period.as_ref() {
            Some(checkpoint_period_str) => humantime::parse_duration(checkpoint_period_str)?,
            None => DEFAULT_CHECKPOINT_PERIOD,
        };

        let gc_horizon: u64 = match self.gc_horizon.as_ref() {
            Some(horizon_str) => horizon_str.parse()?,
            None => DEFAULT_GC_HORIZON,
        };
        let gc_period = match self.gc_period.as_ref() {
            Some(period_str) => humantime::parse_duration(period_str)?,
            None => DEFAULT_GC_PERIOD,
        };

        let pg_distrib_dir = match self.pg_distrib_dir.as_ref() {
            Some(pg_distrib_dir_str) => PathBuf::from(pg_distrib_dir_str),
            None => env::current_dir()?.join("tmp_install"),
        };

        let auth_validation_public_key_path = self
            .auth_validation_public_key_path
            .as_ref()
            .map(PathBuf::from);

        let auth_type = self
            .auth_type
            .as_ref()
            .map_or(Ok(AuthType::Trust), |auth_type| {
                AuthType::from_str(auth_type)
            })?;

        if !pg_distrib_dir.join("bin/postgres").exists() {
            bail!("Can't find postgres binary at {:?}", pg_distrib_dir);
        }

        if auth_type == AuthType::ZenithJWT {
            ensure!(
                auth_validation_public_key_path.is_some(),
                "Missing auth_validation_public_key_path when auth_type is ZenithJWT"
            );
            let path_ref = auth_validation_public_key_path.as_ref().unwrap();
            ensure!(
                path_ref.exists(),
                format!("Can't find auth_validation_public_key at {:?}", path_ref)
            );
        }

        let relish_storage_config =
            self.relish_storage
                .as_ref()
                .map(|storage_params| match storage_params.clone() {
                    RelishStorage::Local { local_path } => {
                        RelishStorageConfig::LocalFs(PathBuf::from(local_path))
                    }
                    RelishStorage::AwsS3 {
                        bucket_name,
                        bucket_region,
                        access_key_id,
                        secret_access_key,
                    } => RelishStorageConfig::AwsS3(S3Config {
                        bucket_name,
                        bucket_region,
                        access_key_id,
                        secret_access_key,
                    }),
                });

        Ok(PageServerConf {
            daemonize: false,

            listen_pg_addr,
            listen_http_addr,
            checkpoint_distance,
            checkpoint_period,
            gc_horizon,
            gc_period,

            superuser: String::from(DEFAULT_SUPERUSER),

            workdir,

            pg_distrib_dir,

            auth_validation_public_key_path,
            auth_type,
            relish_storage_config,
        })
    }
}

fn main() -> Result<()> {
    let arg_matches = App::new("Zenith page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .arg(
            Arg::with_name("listen-pg")
                .short("l")
                .long("listen-pg")
                .alias("listen") // keep some compatibility
                .takes_value(true)
                .help("listen for incoming page requests on ip:port (default: 127.0.0.1:5430)"),
        )
        .arg(
            Arg::with_name("listen-http")
                .long("listen-http")
                .alias("http_endpoint") // keep some compatibility
                .takes_value(true)
                .help("http endpoint address for for metrics and management API calls ip:port (default: 127.0.0.1:5430)"),
        )
        .arg(
            Arg::with_name("daemonize")
                .short("d")
                .long("daemonize")
                .takes_value(false)
                .help("Run in the background"),
        )
        .arg(
            Arg::with_name("init")
                .long("init")
                .takes_value(false)
                .help("Initialize pageserver repo"),
        )
        .arg(
            Arg::with_name("checkpoint_distance")
                .long("checkpoint_distance")
                .takes_value(true)
                .help("Distance from current LSN to perform checkpoint of in-memory layers"),
        )
        .arg(
            Arg::with_name("checkpoint_period")
                .long("checkpoint_period")
                .takes_value(true)
                .help("Interval between checkpoint iterations"),
        )
        .arg(
            Arg::with_name("gc_horizon")
                .long("gc_horizon")
                .takes_value(true)
                .help("Distance from current LSN to perform all wal records cleanup"),
        )
        .arg(
            Arg::with_name("gc_period")
                .long("gc_period")
                .takes_value(true)
                .help("Interval between garbage collector iterations"),
        )
        .arg(
            Arg::with_name("workdir")
                .short("D")
                .long("workdir")
                .takes_value(true)
                .help("Working directory for the pageserver"),
        )
        .arg(
            Arg::with_name("postgres-distrib")
                .long("postgres-distrib")
                .takes_value(true)
                .help("Postgres distribution directory"),
        )
        .arg(
            Arg::with_name("create-tenant")
                .long("create-tenant")
                .takes_value(true)
                .help("Create tenant during init")
                .requires("init"),
        )
        .arg(
            Arg::with_name("auth-validation-public-key-path")
                .long("auth-validation-public-key-path")
                .takes_value(true)
                .help("Path to public key used to validate jwt signature"),
        )
        .arg(
            Arg::with_name("auth-type")
                .long("auth-type")
                .takes_value(true)
                .help("Authentication scheme type. One of: Trust, MD5, ZenithJWT"),
        )
        .arg(
            Arg::with_name("relish-storage-local-path")
                .long("relish-storage-local-path")
                .takes_value(true)
                .help("Path to the local directory, to be used as an external relish storage")
                .conflicts_with_all(&[
                    "relish-storage-s3-bucket",
                    "relish-storage-region",
                    "relish-storage-access-key",
                    "relish-storage-secret-access-key",
                ]),
        )
        .arg(
            Arg::with_name("relish-storage-s3-bucket")
                .long("relish-storage-s3-bucket")
                .takes_value(true)
                .help("Name of the AWS S3 bucket to use an external relish storage")
                .requires("relish-storage-region"),
        )
        .arg(
            Arg::with_name("relish-storage-region")
                .long("relish-storage-region")
                .takes_value(true)
                .help("Region of the AWS S3 bucket"),
        )
        .arg(
            Arg::with_name("relish-storage-access-key")
                .long("relish-storage-access-key")
                .takes_value(true)
                .help("Credentials to access the AWS S3 bucket"),
        )
        .arg(
            Arg::with_name("relish-storage-secret-access-key")
                .long("relish-storage-secret-access-key")
                .takes_value(true)
                .help("Credentials to access the AWS S3 bucket"),
        )
        .get_matches();

    let workdir = Path::new(arg_matches.value_of("workdir").unwrap_or(".zenith"));
    let cfg_file_path = workdir.canonicalize()?.join("pageserver.toml");

    let args_params = CfgFileParams::from_args(&arg_matches);

    let init = arg_matches.is_present("init");
    let create_tenant = arg_matches.value_of("create-tenant");

    let params = if init {
        // We're initializing the repo, so there's no config file yet
        args_params
    } else {
        // Supplement the CLI arguments with the config file
        let cfg_file_contents = std::fs::read_to_string(&cfg_file_path)?;
        let file_params: CfgFileParams = toml::from_str(&cfg_file_contents)?;
        args_params.or(file_params)
    };

    // Set CWD to workdir for non-daemon modes
    env::set_current_dir(&workdir)?;

    // Ensure the config is valid, even if just init-ing
    let mut conf = params.try_into_config()?;

    conf.daemonize = arg_matches.is_present("daemonize");

    if init && conf.daemonize {
        eprintln!("--daemonize cannot be used with --init");
        exit(1);
    }

    // The configuration is all set up now. Turn it into a 'static
    // that can be freely stored in structs and passed across threads
    // as a ref.
    let conf: &'static PageServerConf = Box::leak(Box::new(conf));

    // Create repo and exit if init was requested
    if init {
        branches::init_pageserver(conf, create_tenant)?;
        // write the config file
        let cfg_file_contents = toml::to_string_pretty(&params)?;
        // TODO support enable-auth flag
        std::fs::write(&cfg_file_path, cfg_file_contents)?;

        return Ok(());
    }

    start_pageserver(conf)
}

fn start_pageserver(conf: &'static PageServerConf) -> Result<()> {
    // Initialize logger
    let (_scope_guard, log_file) = logging::init(LOG_FILE_NAME, conf.daemonize)?;

    // TODO: Check that it looks like a valid repository before going further

    // bind sockets before daemonizing so we report errors early and do not return until we are listening
    info!(
        "Starting pageserver http handler on {}",
        conf.listen_http_addr
    );
    let http_listener = TcpListener::bind(conf.listen_http_addr.clone())?;

    info!(
        "Starting pageserver pg protocol handler on {}",
        conf.listen_pg_addr
    );
    let pageserver_listener = TcpListener::bind(conf.listen_pg_addr.clone())?;

    if conf.daemonize {
        info!("daemonizing...");

        // There shouldn't be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fprintf's or backtraces.
        let stdout = log_file.try_clone().unwrap();
        let stderr = log_file;

        let daemonize = Daemonize::new()
            .pid_file("pageserver.pid")
            .working_directory(".")
            .stdout(stdout)
            .stderr(stderr);

        match daemonize.start() {
            Ok(_) => info!("Success, daemonized"),
            Err(e) => error!("Error, {}", e),
        }
    }

    // Initialize tenant manager.
    tenant_mgr::init(conf);

    // keep join handles for spawned threads
    let mut join_handles = vec![];

    // initialize authentication for incoming connections
    let auth = match &conf.auth_type {
        AuthType::Trust | AuthType::MD5 => None,
        AuthType::ZenithJWT => {
            // unwrap is ok because check is performed when creating config, so path is set and file exists
            let key_path = conf.auth_validation_public_key_path.as_ref().unwrap();
            Some(JwtAuth::from_key_path(key_path)?.into())
        }
    };
    info!("Using auth: {:#?}", conf.auth_type);

    // Spawn a new thread for the http endpoint
    // bind before launching separate thread so the error reported before startup exits
    let cloned = auth.clone();
    let http_endpoint_thread = thread::Builder::new()
        .name("http_endpoint_thread".into())
        .spawn(move || {
            let router = http::make_router(conf, cloned);
            endpoint::serve_thread_main(router, http_listener)
        })?;

    join_handles.push(http_endpoint_thread);

    // Spawn a thread to listen for connections. It will spawn further threads
    // for each connection.
    let page_service_thread = thread::Builder::new()
        .name("Page Service thread".into())
        .spawn(move || {
            page_service::thread_main(conf, auth, pageserver_listener, conf.auth_type)
        })?;

    join_handles.push(page_service_thread);

    let storage_sync_thread =
        pageserver::layered_repository::relish_storage::synced_storage::create_storage_sync_thread(
            conf,
        )?;

    for handle in join_handles.into_iter() {
        handle
            .join()
            .expect("thread panicked")
            .expect("thread exited with an error")
    }
    if let Some(storage_sync_thread) = storage_sync_thread {
        storage_sync_thread.join().expect("thread panicked");
    }
    Ok(())
}
