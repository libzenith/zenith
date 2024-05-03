//! Functions for handling page server configuration options
//!
//! Configuration options can be set in the pageserver.toml configuration
//! file, or on the command line.
//! See also `settings.md` for better description on every parameter.

use anyhow::{anyhow, bail, ensure, Context, Result};
use pageserver_api::{config::MustNotDefault, shard::TenantShardId};
use remote_storage::{RemotePath, RemoteStorageConfig};
use serde;
use serde::de::IntoDeserializer;
use std::env;
use storage_broker::Uri;
use utils::crashsafe::path_with_suffix_extension;
use utils::id::ConnectionId;
use utils::logging::SecretString;

use once_cell::sync::OnceCell;
use reqwest::Url;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use toml_edit::{Document, Item};

use camino::{Utf8Path, Utf8PathBuf};
use postgres_backend::AuthType;
use utils::{
    id::{NodeId, TimelineId},
    logging::LogFormat,
};

use crate::tenant::timeline::GetVectoredImpl;
use crate::tenant::vectored_blob_io::MaxVectoredReadBytes;
use crate::tenant::{config::TenantConfOpt, timeline::GetImpl};
use crate::tenant::{
    TENANTS_SEGMENT_NAME, TENANT_DELETED_MARKER_FILE_NAME, TIMELINES_SEGMENT_NAME,
};
use crate::{disk_usage_eviction_task::DiskUsageEvictionTaskConfig, virtual_file::io_engine};
use crate::{tenant::config::TenantConf, virtual_file};
use crate::{
    IGNORED_TENANT_FILE_NAME, TENANT_CONFIG_NAME, TENANT_HEATMAP_BASENAME,
    TENANT_LOCATION_CONFIG_NAME, TIMELINE_DELETE_MARK_SUFFIX,
};

use self::defaults::DEFAULT_CONCURRENT_TENANT_WARMUP;

use self::defaults::DEFAULT_VIRTUAL_FILE_IO_ENGINE;

pub mod defaults {
    use crate::tenant::config::defaults::*;
    use const_format::formatcp;

    ///
    /// Default built-in configuration file.
    ///
    pub const DEFAULT_CONFIG_FILE: &str = formatcp!(
        r#"
# Initial configuration file created by 'pageserver --init'
#listen_pg_addr = '{DEFAULT_PG_LISTEN_ADDR}'
#listen_http_addr = '{DEFAULT_HTTP_LISTEN_ADDR}'

#wait_lsn_timeout = '{DEFAULT_WAIT_LSN_TIMEOUT}'
#wal_redo_timeout = '{DEFAULT_WAL_REDO_TIMEOUT}'

#page_cache_size = {DEFAULT_PAGE_CACHE_SIZE}
#max_file_descriptors = {DEFAULT_MAX_FILE_DESCRIPTORS}

# initial superuser role name to use when creating a new tenant
#initial_superuser_name = '{DEFAULT_SUPERUSER}'

#broker_endpoint = '{BROKER_DEFAULT_ENDPOINT}'

#log_format = '{DEFAULT_LOG_FORMAT}'

#concurrent_tenant_size_logical_size_queries = '{DEFAULT_CONCURRENT_TENANT_SIZE_LOGICAL_SIZE_QUERIES}'
#concurrent_tenant_warmup = '{DEFAULT_CONCURRENT_TENANT_WARMUP}'

#metric_collection_interval = '{DEFAULT_METRIC_COLLECTION_INTERVAL}'
#cached_metric_collection_interval = '{DEFAULT_CACHED_METRIC_COLLECTION_INTERVAL}'
#synthetic_size_calculation_interval = '{DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL}'

#disk_usage_based_eviction = {{ max_usage_pct = .., min_avail_bytes = .., period = "10s"}}

#background_task_maximum_delay = '{DEFAULT_BACKGROUND_TASK_MAXIMUM_DELAY}'

#ingest_batch_size = {DEFAULT_INGEST_BATCH_SIZE}

#virtual_file_io_engine = '{DEFAULT_VIRTUAL_FILE_IO_ENGINE}'

#get_vectored_impl = '{DEFAULT_GET_VECTORED_IMPL}'

#get_impl = '{DEFAULT_GET_IMPL}'

#max_vectored_read_bytes = '{DEFAULT_MAX_VECTORED_READ_BYTES}'

#validate_vectored_get = '{DEFAULT_VALIDATE_VECTORED_GET}'

#walredo_process_kind = '{DEFAULT_WALREDO_PROCESS_KIND}'

[tenant_config]
#checkpoint_distance = {DEFAULT_CHECKPOINT_DISTANCE} # in bytes
#checkpoint_timeout = {DEFAULT_CHECKPOINT_TIMEOUT}
#compaction_target_size = {DEFAULT_COMPACTION_TARGET_SIZE} # in bytes
#compaction_period = '{DEFAULT_COMPACTION_PERIOD}'
#compaction_threshold = {DEFAULT_COMPACTION_THRESHOLD}

#gc_period = '{DEFAULT_GC_PERIOD}'
#gc_horizon = {DEFAULT_GC_HORIZON}
#image_creation_threshold = {DEFAULT_IMAGE_CREATION_THRESHOLD}
#pitr_interval = '{DEFAULT_PITR_INTERVAL}'

#min_resident_size_override = .. # in bytes
#evictions_low_residence_duration_metric_threshold = '{DEFAULT_EVICTIONS_LOW_RESIDENCE_DURATION_METRIC_THRESHOLD}'

#heatmap_upload_concurrency = {DEFAULT_HEATMAP_UPLOAD_CONCURRENCY}
#secondary_download_concurrency = {DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY}

#ephemeral_bytes_per_memory_kb = {DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB}

[remote_storage]

"#
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageServerConf {
    // Identifier of that particular pageserver so e g safekeepers
    // can safely distinguish different pageservers
    pub id: NodeId,

    /// Example (default): 127.0.0.1:64000
    pub listen_pg_addr: String,
    /// Example (default): 127.0.0.1:9898
    pub listen_http_addr: String,

    /// Current availability zone. Used for traffic metrics.
    pub availability_zone: Option<String>,

    // Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
    pub wait_lsn_timeout: Duration,
    // How long to wait for WAL redo to complete.
    pub wal_redo_timeout: Duration,

    pub superuser: String,

    pub page_cache_size: usize,
    pub max_file_descriptors: usize,

    // Repository directory, relative to current working directory.
    // Normally, the page server changes the current working directory
    // to the repository, and 'workdir' is always '.'. But we don't do
    // that during unit testing, because the current directory is global
    // to the process but different unit tests work on different
    // repositories.
    pub workdir: Utf8PathBuf,

    pub pg_distrib_dir: Utf8PathBuf,

    // Authentication
    /// authentication method for the HTTP mgmt API
    pub http_auth_type: AuthType,
    /// authentication method for libpq connections from compute
    pub pg_auth_type: AuthType,
    /// Path to a file or directory containing public key(s) for verifying JWT tokens.
    /// Used for both mgmt and compute auth, if enabled.
    pub auth_validation_public_key_path: Option<Utf8PathBuf>,

    pub remote_storage_config: Option<RemoteStorageConfig>,

    pub default_tenant_conf: TenantConf,

    /// Storage broker endpoints to connect to.
    pub broker_endpoint: Uri,
    pub broker_keepalive_interval: Duration,

    pub log_format: LogFormat,

    /// Number of tenants which will be concurrently loaded from remote storage proactively on startup or attach.
    ///
    /// A lower value implicitly deprioritizes loading such tenants, vs. other work in the system.
    pub concurrent_tenant_warmup: ConfigurableSemaphore,

    /// Number of concurrent [`Tenant::gather_size_inputs`](crate::tenant::Tenant::gather_size_inputs) allowed.
    pub concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore,
    /// Limit of concurrent [`Tenant::gather_size_inputs`] issued by module `eviction_task`.
    /// The number of permits is the same as `concurrent_tenant_size_logical_size_queries`.
    /// See the comment in `eviction_task` for details.
    ///
    /// [`Tenant::gather_size_inputs`]: crate::tenant::Tenant::gather_size_inputs
    pub eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore,

    // How often to collect metrics and send them to the metrics endpoint.
    pub metric_collection_interval: Duration,
    // How often to send unchanged cached metrics to the metrics endpoint.
    pub cached_metric_collection_interval: Duration,
    pub metric_collection_endpoint: Option<Url>,
    pub metric_collection_bucket: Option<RemoteStorageConfig>,
    pub synthetic_size_calculation_interval: Duration,

    pub disk_usage_based_eviction: Option<DiskUsageEvictionTaskConfig>,

    pub test_remote_failures: u64,

    pub ondemand_download_behavior_treat_error_as_warn: bool,

    /// How long will background tasks be delayed at most after initial load of tenants.
    ///
    /// Our largest initialization completions are in the range of 100-200s, so perhaps 10s works
    /// as we now isolate initial loading, initial logical size calculation and background tasks.
    /// Smaller nodes will have background tasks "not running" for this long unless every timeline
    /// has it's initial logical size calculated. Not running background tasks for some seconds is
    /// not terrible.
    pub background_task_maximum_delay: Duration,

    pub control_plane_api: Option<Url>,

    /// JWT token for use with the control plane API.
    pub control_plane_api_token: Option<SecretString>,

    /// If true, pageserver will make best-effort to operate without a control plane: only
    /// for use in major incidents.
    pub control_plane_emergency_mode: bool,

    /// How many heatmap uploads may be done concurrency: lower values implicitly deprioritize
    /// heatmap uploads vs. other remote storage operations.
    pub heatmap_upload_concurrency: usize,

    /// How many remote storage downloads may be done for secondary tenants concurrently.  Implicitly
    /// deprioritises secondary downloads vs. remote storage operations for attached tenants.
    pub secondary_download_concurrency: usize,

    /// Maximum number of WAL records to be ingested and committed at the same time
    pub ingest_batch_size: u64,

    pub virtual_file_io_engine: virtual_file::IoEngineKind,

    pub get_vectored_impl: GetVectoredImpl,

    pub get_impl: GetImpl,

    pub max_vectored_read_bytes: MaxVectoredReadBytes,

    pub validate_vectored_get: bool,

    /// How many bytes of ephemeral layer content will we allow per kilobyte of RAM.  When this
    /// is exceeded, we start proactively closing ephemeral layers to limit the total amount
    /// of ephemeral data.
    ///
    /// Setting this to zero disables limits on total ephemeral layer size.
    pub ephemeral_bytes_per_memory_kb: usize,

    pub walredo_process_kind: crate::walredo::ProcessKind,
}

/// We do not want to store this in a PageServerConf because the latter may be logged
/// and/or serialized at a whim, while the token is secret. Currently this token is the
/// same for accessing all tenants/timelines, but may become per-tenant/per-timeline in
/// the future, more tokens and auth may arrive for storage broker, completely changing the logic.
/// Hence, we resort to a global variable for now instead of passing the token from the
/// startup code to the connection code through a dozen layers.
pub static SAFEKEEPER_AUTH_TOKEN: OnceCell<Arc<String>> = OnceCell::new();

impl TryFrom<pageserver_api::config::ConfigToml> for PageServerConf {
    type Error = anyhow::Error;
    fn try_from(
        config_toml: pageserver_api::config::ConfigToml,
    ) -> std::prelude::v1::Result<Self, Self::Error> {
        macro_rules! conf {
            (STRAIGHT { $($field:ident,)* } CUSTOM LOGIC { $($custom_field:ident : $custom_value:expr,)* } ) => {
                PageServerConf {
                    $(
                        $field: config_toml.$field.clone(),
                    )*
                    $(
                        $custom_field: $custom_value,
                    )*
                }
            };
        }

        Ok(conf!(
            STRAIGHT
            {
                listen_pg_addr,
                listen_http_addr,
                availability_zone,
                wait_lsn_timeout,
                wal_redo_timeout,
                superuser,
                page_cache_size,
                max_file_descriptors,
                workdir,
                pg_distrib_dir,
                http_auth_type,
                pg_auth_type,
                auth_validation_public_key_path,
                remote_storage_config,
                broker_endpoint,
                broker_keepalive_interval,
                log_format,
                metric_collection_interval,
                cached_metric_collection_interval,
                metric_collection_endpoint,
                metric_collection_bucket,
                synthetic_size_calculation_interval,
                disk_usage_based_eviction,
                test_remote_failures,
                ondemand_download_behavior_treat_error_as_warn,
                background_task_maximum_delay,
                control_plane_api,
                control_plane_api_token,
                control_plane_emergency_mode,
                heatmap_upload_concurrency,
                secondary_download_concurrency,
                ingest_batch_size,
                get_vectored_impl,
                get_impl,
                max_vectored_read_bytes,
                validate_vectored_get,
                ephemeral_bytes_per_memory_kb,
                walredo_process_kind,
            }
            CUSTOM LOGIC
            {
                id: config_toml.id.ok_or("id")?,
                // TenantConf is handled separately
                default_tenant_conf: TenantConf::default(),
                concurrent_tenant_warmup: ConfigurableSemaphore::new({
                   config_toml
                        .concurrent_tenant_warmup
                        .ok_or("concurrent_tenant_warmpup",
                               default.concurrent_tenant_warmup)?
                }),
                concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::new(
                    config_toml
                        .concurrent_tenant_size_logical_size_queries
                        .ok_or("concurrent_tenant_size_logical_size_queries",
                               default.concurrent_tenant_size_logical_size_queries.clone())?
                ),
                eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore::new(
                    // re-use `concurrent_tenant_size_logical_size_queries`
                    config_toml
                        .concurrent_tenant_size_logical_size_queries
                        .ok_or("eviction_task_immitated_concurrent_logical_size_queries",
                               default.concurrent_tenant_size_logical_size_queries.clone())?,
                ),
                virtual_file_io_engine: match config_toml.virtual_file_io_engine {
                    Some(v) => v,
                    None => match crate::virtual_file::io_engine_feature_test().context("auto-detect virtual_file_io_engine")? {
                        io_engine::FeatureTestResult::PlatformPreferred(v) => v, // make no noise
                        io_engine::FeatureTestResult::Worse { engine, remark } => {
                            // TODO: bubble this up to the caller so we can tracing::warn! it.
                            eprintln!("auto-detected IO engine is not platform-preferred: engine={engine:?} remark={remark:?}");
                            engine
                        }
                    },
                },
            }
        ))
    }
}

impl PageServerConf {
    //
    // Repository paths, relative to workdir.
    //

    pub fn tenants_path(&self) -> Utf8PathBuf {
        self.workdir.join(TENANTS_SEGMENT_NAME)
    }

    pub fn deletion_prefix(&self) -> Utf8PathBuf {
        self.workdir.join("deletion")
    }

    pub fn metadata_path(&self) -> Utf8PathBuf {
        self.workdir.join("metadata.json")
    }

    pub fn deletion_list_path(&self, sequence: u64) -> Utf8PathBuf {
        // Encode a version in the filename, so that if we ever switch away from JSON we can
        // increment this.
        const VERSION: u8 = 1;

        self.deletion_prefix()
            .join(format!("{sequence:016x}-{VERSION:02x}.list"))
    }

    pub fn deletion_header_path(&self) -> Utf8PathBuf {
        // Encode a version in the filename, so that if we ever switch away from JSON we can
        // increment this.
        const VERSION: u8 = 1;

        self.deletion_prefix().join(format!("header-{VERSION:02x}"))
    }

    pub fn tenant_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenants_path().join(tenant_shard_id.to_string())
    }

    pub fn tenant_ignore_mark_file_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(IGNORED_TENANT_FILE_NAME)
    }

    /// Points to a place in pageserver's local directory,
    /// where certain tenant's tenantconf file should be located.
    ///
    /// Legacy: superseded by tenant_location_config_path.  Eventually
    /// remove this function.
    pub fn tenant_config_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id).join(TENANT_CONFIG_NAME)
    }

    pub fn tenant_location_config_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TENANT_LOCATION_CONFIG_NAME)
    }

    pub(crate) fn tenant_heatmap_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TENANT_HEATMAP_BASENAME)
    }

    pub fn timelines_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TIMELINES_SEGMENT_NAME)
    }

    pub fn timeline_path(
        &self,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
    ) -> Utf8PathBuf {
        self.timelines_path(tenant_shard_id)
            .join(timeline_id.to_string())
    }

    pub(crate) fn timeline_delete_mark_file_path(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Utf8PathBuf {
        path_with_suffix_extension(
            self.timeline_path(&tenant_shard_id, &timeline_id),
            TIMELINE_DELETE_MARK_SUFFIX,
        )
    }

    pub(crate) fn tenant_deleted_mark_file_path(
        &self,
        tenant_shard_id: &TenantShardId,
    ) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TENANT_DELETED_MARKER_FILE_NAME)
    }

    pub fn traces_path(&self) -> Utf8PathBuf {
        self.workdir.join("traces")
    }

    pub fn trace_path(
        &self,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
        connection_id: &ConnectionId,
    ) -> Utf8PathBuf {
        self.traces_path()
            .join(tenant_shard_id.to_string())
            .join(timeline_id.to_string())
            .join(connection_id.to_string())
    }

    /// Turns storage remote path of a file into its local path.
    pub fn local_path(&self, remote_path: &RemotePath) -> Utf8PathBuf {
        remote_path.with_base(&self.workdir)
    }

    //
    // Postgres distribution paths
    //
    pub fn pg_distrib_dir(&self, pg_version: u32) -> anyhow::Result<Utf8PathBuf> {
        let path = self.pg_distrib_dir.clone();

        #[allow(clippy::manual_range_patterns)]
        match pg_version {
            14 | 15 | 16 => Ok(path.join(format!("v{pg_version}"))),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }

    pub fn pg_bin_dir(&self, pg_version: u32) -> anyhow::Result<Utf8PathBuf> {
        Ok(self.pg_distrib_dir(pg_version)?.join("bin"))
    }
    pub fn pg_lib_dir(&self, pg_version: u32) -> anyhow::Result<Utf8PathBuf> {
        Ok(self.pg_distrib_dir(pg_version)?.join("lib"))
    }

    /// Parse a configuration file (pageserver.toml) into a PageServerConf struct,
    /// validating the input and failing on errors.
    ///
    /// This leaves any options not present in the file in the built-in defaults.
    pub fn parse_and_validate(toml: &Document, workdir: &Utf8Path) -> anyhow::Result<Self> {
        let mut builder = PageServerConfigBuilder::default();
        builder.workdir(workdir.to_owned());

        let mut t_conf = TenantConfOpt::default();

        for (key, item) in toml.iter() {
            match key {
                "listen_pg_addr" => builder.listen_pg_addr(parse_toml_string(key, item)?),
                "listen_http_addr" => builder.listen_http_addr(parse_toml_string(key, item)?),
                "availability_zone" => builder.availability_zone(Some(parse_toml_string(key, item)?)),
                "wait_lsn_timeout" => builder.wait_lsn_timeout(parse_toml_duration(key, item)?),
                "wal_redo_timeout" => builder.wal_redo_timeout(parse_toml_duration(key, item)?),
                "initial_superuser_name" => builder.superuser(parse_toml_string(key, item)?),
                "page_cache_size" => builder.page_cache_size(parse_toml_u64(key, item)? as usize),
                "max_file_descriptors" => {
                    builder.max_file_descriptors(parse_toml_u64(key, item)? as usize)
                }
                "pg_distrib_dir" => {
                    builder.pg_distrib_dir(Utf8PathBuf::from(parse_toml_string(key, item)?))
                }
                "auth_validation_public_key_path" => builder.auth_validation_public_key_path(Some(
                    Utf8PathBuf::from(parse_toml_string(key, item)?),
                )),
                "http_auth_type" => builder.http_auth_type(parse_toml_from_str(key, item)?),
                "pg_auth_type" => builder.pg_auth_type(parse_toml_from_str(key, item)?),
                "remote_storage" => {
                    builder.remote_storage_config(RemoteStorageConfig::from_toml(item)?)
                }
                "tenant_config" => {
                    t_conf = TenantConfOpt::try_from(item.to_owned()).context(format!("failed to parse: '{key}'"))?;
                }
                "id" => builder.id(NodeId(parse_toml_u64(key, item)?)),
                "broker_endpoint" => builder.broker_endpoint(parse_toml_string(key, item)?.parse().context("failed to parse broker endpoint")?),
                "broker_keepalive_interval" => builder.broker_keepalive_interval(parse_toml_duration(key, item)?),
                "log_format" => builder.log_format(
                    LogFormat::from_config(&parse_toml_string(key, item)?)?
                ),
                "concurrent_tenant_warmup" => builder.concurrent_tenant_warmup({
                    let input = parse_toml_string(key, item)?;
                    let permits = input.parse::<usize>().context("expected a number of initial permits, not {s:?}")?;
                    NonZeroUsize::new(permits).context("initial semaphore permits out of range: 0, use other configuration to disable a feature")?
                }),
                "concurrent_tenant_size_logical_size_queries" => builder.concurrent_tenant_size_logical_size_queries({
                    let input = parse_toml_string(key, item)?;
                    let permits = input.parse::<usize>().context("expected a number of initial permits, not {s:?}")?;
                    NonZeroUsize::new(permits).context("initial semaphore permits out of range: 0, use other configuration to disable a feature")?
                }),
                "metric_collection_interval" => builder.metric_collection_interval(parse_toml_duration(key, item)?),
                "cached_metric_collection_interval" => builder.cached_metric_collection_interval(parse_toml_duration(key, item)?),
                "metric_collection_endpoint" => {
                    let endpoint = parse_toml_string(key, item)?.parse().context("failed to parse metric_collection_endpoint")?;
                    builder.metric_collection_endpoint(Some(endpoint));
                },
                "metric_collection_bucket" => {
                    builder.metric_collection_bucket(RemoteStorageConfig::from_toml(item)?)
                }
                "synthetic_size_calculation_interval" =>
                    builder.synthetic_size_calculation_interval(parse_toml_duration(key, item)?),
                "test_remote_failures" => builder.test_remote_failures(parse_toml_u64(key, item)?),
                "disk_usage_based_eviction" => {
                    tracing::info!("disk_usage_based_eviction: {:#?}", &item);
                    builder.disk_usage_based_eviction(
                        deserialize_from_item("disk_usage_based_eviction", item)
                            .context("parse disk_usage_based_eviction")?
                    )
                },
                "ondemand_download_behavior_treat_error_as_warn" => builder.ondemand_download_behavior_treat_error_as_warn(parse_toml_bool(key, item)?),
                "background_task_maximum_delay" => builder.background_task_maximum_delay(parse_toml_duration(key, item)?),
                "control_plane_api" => {
                    let parsed = parse_toml_string(key, item)?;
                    if parsed.is_empty() {
                        builder.control_plane_api(None)
                    } else {
                        builder.control_plane_api(Some(parsed.parse().context("failed to parse control plane URL")?))
                    }
                },
                "control_plane_api_token" => {
                    let parsed = parse_toml_string(key, item)?;
                    if parsed.is_empty() {
                        builder.control_plane_api_token(None)
                    } else {
                        builder.control_plane_api_token(Some(parsed.into()))
                    }
                },
                "control_plane_emergency_mode" => {
                    builder.control_plane_emergency_mode(parse_toml_bool(key, item)?)
                },
                "heatmap_upload_concurrency" => {
                    builder.heatmap_upload_concurrency(parse_toml_u64(key, item)? as usize)
                },
                "secondary_download_concurrency" => {
                    builder.secondary_download_concurrency(parse_toml_u64(key, item)? as usize)
                },
                "ingest_batch_size" => builder.ingest_batch_size(parse_toml_u64(key, item)?),
                "virtual_file_io_engine" => {
                    builder.virtual_file_io_engine(parse_toml_from_str("virtual_file_io_engine", item)?)
                }
                "get_vectored_impl" => {
                    builder.get_vectored_impl(parse_toml_from_str("get_vectored_impl", item)?)
                }
                "get_impl" => {
                    builder.get_impl(parse_toml_from_str("get_impl", item)?)
                }
                "max_vectored_read_bytes" => {
                    let bytes = parse_toml_u64("max_vectored_read_bytes", item)? as usize;
                    builder.get_max_vectored_read_bytes(
                        MaxVectoredReadBytes(
                            NonZeroUsize::new(bytes).expect("Max byte size of vectored read must be greater than 0")))
                }
                "validate_vectored_get" => {
                    builder.get_validate_vectored_get(parse_toml_bool("validate_vectored_get", item)?)
                }
                "ephemeral_bytes_per_memory_kb" => {
                    builder.get_ephemeral_bytes_per_memory_kb(parse_toml_u64("ephemeral_bytes_per_memory_kb", item)? as usize)
                }
                "walredo_process_kind" => {
                    builder.get_walredo_process_kind(parse_toml_from_str("walredo_process_kind", item)?)
                }
                _ => bail!("unrecognized pageserver option '{key}'"),
            }
        }

        let mut conf = builder.build().context("invalid config")?;

        if conf.http_auth_type == AuthType::NeonJWT || conf.pg_auth_type == AuthType::NeonJWT {
            let auth_validation_public_key_path = conf
                .auth_validation_public_key_path
                .get_or_insert_with(|| workdir.join("auth_public_key.pem"));
            ensure!(
                auth_validation_public_key_path.exists(),
                format!(
                    "Can't find auth_validation_public_key at '{auth_validation_public_key_path}'",
                )
            );
        }

        conf.default_tenant_conf = t_conf.merge(TenantConf::default());

        Ok(conf)
    }

    #[cfg(test)]
    pub fn test_repo_dir(test_name: &str) -> Utf8PathBuf {
        let test_output_dir = std::env::var("TEST_OUTPUT").unwrap_or("../tmp_check".into());
        Utf8PathBuf::from(format!("{test_output_dir}/test_{test_name}"))
    }

    pub fn dummy_conf(repo_dir: Utf8PathBuf) -> Self {
        let pg_distrib_dir = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pg_install");

        PageServerConf {
            id: NodeId(0),
            wait_lsn_timeout: Duration::from_secs(60),
            wal_redo_timeout: Duration::from_secs(60),
            page_cache_size: defaults::DEFAULT_PAGE_CACHE_SIZE,
            max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            availability_zone: None,
            superuser: "cloud_admin".to_string(),
            workdir: repo_dir,
            pg_distrib_dir,
            http_auth_type: AuthType::Trust,
            pg_auth_type: AuthType::Trust,
            auth_validation_public_key_path: None,
            remote_storage_config: None,
            default_tenant_conf: TenantConf::default(),
            broker_endpoint: storage_broker::DEFAULT_ENDPOINT.parse().unwrap(),
            broker_keepalive_interval: Duration::from_secs(5000),
            log_format: LogFormat::from_str(defaults::DEFAULT_LOG_FORMAT).unwrap(),
            concurrent_tenant_warmup: ConfigurableSemaphore::new(
                NonZeroUsize::new(DEFAULT_CONCURRENT_TENANT_WARMUP)
                    .expect("Invalid default constant"),
            ),
            concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::default(),
            eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore::default(
            ),
            metric_collection_interval: Duration::from_secs(60),
            cached_metric_collection_interval: Duration::from_secs(60 * 60),
            metric_collection_endpoint: defaults::DEFAULT_METRIC_COLLECTION_ENDPOINT,
            metric_collection_bucket: None,
            synthetic_size_calculation_interval: Duration::from_secs(60),
            disk_usage_based_eviction: None,
            test_remote_failures: 0,
            ondemand_download_behavior_treat_error_as_warn: false,
            background_task_maximum_delay: Duration::ZERO,
            control_plane_api: None,
            control_plane_api_token: None,
            control_plane_emergency_mode: false,
            heatmap_upload_concurrency: defaults::DEFAULT_HEATMAP_UPLOAD_CONCURRENCY,
            secondary_download_concurrency: defaults::DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY,
            ingest_batch_size: defaults::DEFAULT_INGEST_BATCH_SIZE,
            virtual_file_io_engine: DEFAULT_VIRTUAL_FILE_IO_ENGINE.parse().unwrap(),
            get_vectored_impl: defaults::DEFAULT_GET_VECTORED_IMPL.parse().unwrap(),
            get_impl: defaults::DEFAULT_GET_IMPL.parse().unwrap(),
            max_vectored_read_bytes: MaxVectoredReadBytes(
                NonZeroUsize::new(defaults::DEFAULT_MAX_VECTORED_READ_BYTES)
                    .expect("Invalid default constant"),
            ),
            validate_vectored_get: defaults::DEFAULT_VALIDATE_VECTORED_GET,
            ephemeral_bytes_per_memory_kb: defaults::DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB,
            walredo_process_kind: defaults::DEFAULT_WALREDO_PROCESS_KIND.parse().unwrap(),
        }
    }
}

// Helper functions to parse a toml Item

fn parse_toml_string(name: &str, item: &Item) -> Result<String> {
    let s = item
        .as_str()
        .with_context(|| format!("configure option {name} is not a string"))?;
    Ok(s.to_string())
}

fn parse_toml_u64(name: &str, item: &Item) -> Result<u64> {
    // A toml integer is signed, so it cannot represent the full range of an u64. That's OK
    // for our use, though.
    let i: i64 = item
        .as_integer()
        .with_context(|| format!("configure option {name} is not an integer"))?;
    if i < 0 {
        bail!("configure option {name} cannot be negative");
    }
    Ok(i as u64)
}

fn parse_toml_bool(name: &str, item: &Item) -> Result<bool> {
    item.as_bool()
        .with_context(|| format!("configure option {name} is not a bool"))
}

fn parse_toml_duration(name: &str, item: &Item) -> Result<Duration> {
    let s = item
        .as_str()
        .with_context(|| format!("configure option {name} is not a string"))?;

    Ok(humantime::parse_duration(s)?)
}

fn parse_toml_from_str<T>(name: &str, item: &Item) -> anyhow::Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let v = item
        .as_str()
        .with_context(|| format!("configure option {name} is not a string"))?;
    T::from_str(v).map_err(|e| {
        anyhow!(
            "Failed to parse string as {parse_type} for configure option {name}: {e}",
            parse_type = stringify!(T)
        )
    })
}

fn deserialize_from_item<T>(name: &str, item: &Item) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    // ValueDeserializer::new is not public, so use the ValueDeserializer's documented way
    let deserializer = match item.clone().into_value() {
        Ok(value) => value.into_deserializer(),
        Err(item) => anyhow::bail!("toml_edit::Item '{item}' is not a toml_edit::Value"),
    };
    T::deserialize(deserializer).with_context(|| format!("deserializing item for node {name}"))
}

/// Configurable semaphore permits setting.
///
/// Does not allow semaphore permits to be zero, because at runtime initially zero permits and empty
/// semaphore cannot be distinguished, leading any feature using these to await forever (or until
/// new permits are added).
#[derive(Debug, Clone)]
pub struct ConfigurableSemaphore {
    initial_permits: NonZeroUsize,
    inner: std::sync::Arc<tokio::sync::Semaphore>,
}

impl ConfigurableSemaphore {
    pub const DEFAULT_INITIAL: NonZeroUsize = match NonZeroUsize::new(1) {
        Some(x) => x,
        None => panic!("const unwrap is not yet stable"),
    };

    /// Initializse using a non-zero amount of permits.
    ///
    /// Require a non-zero initial permits, because using permits == 0 is a crude way to disable a
    /// feature such as [`Tenant::gather_size_inputs`]. Otherwise any semaphore using future will
    /// behave like [`futures::future::pending`], just waiting until new permits are added.
    ///
    /// [`Tenant::gather_size_inputs`]: crate::tenant::Tenant::gather_size_inputs
    pub fn new(initial_permits: NonZeroUsize) -> Self {
        ConfigurableSemaphore {
            initial_permits,
            inner: std::sync::Arc::new(tokio::sync::Semaphore::new(initial_permits.get())),
        }
    }

    /// Returns the configured amount of permits.
    pub fn initial_permits(&self) -> NonZeroUsize {
        self.initial_permits
    }
}

impl Default for ConfigurableSemaphore {
    fn default() -> Self {
        Self::new(Self::DEFAULT_INITIAL)
    }
}

impl PartialEq for ConfigurableSemaphore {
    fn eq(&self, other: &Self) -> bool {
        // the number of permits can be increased at runtime, so we cannot really fulfill the
        // PartialEq value equality otherwise
        self.initial_permits == other.initial_permits
    }
}

impl Eq for ConfigurableSemaphore {}

impl ConfigurableSemaphore {
    pub fn inner(&self) -> &std::sync::Arc<tokio::sync::Semaphore> {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, num::NonZeroU32};

    use camino_tempfile::{tempdir, Utf8TempDir};
    use pageserver_api::models::EvictionPolicy;
    use remote_storage::{RemoteStorageKind, S3Config};
    use utils::serde_percent::Percent;

    use super::*;
    use crate::DEFAULT_PG_VERSION;

    const ALL_BASE_VALUES_TOML: &str = r#"
# Initial configuration file created by 'pageserver --init'

listen_pg_addr = '127.0.0.1:64000'
listen_http_addr = '127.0.0.1:9898'

wait_lsn_timeout = '111 s'
wal_redo_timeout = '111 s'

page_cache_size = 444
max_file_descriptors = 333

# initial superuser role name to use when creating a new tenant
initial_superuser_name = 'zzzz'
id = 10

metric_collection_interval = '222 s'
cached_metric_collection_interval = '22200 s'
metric_collection_endpoint = 'http://localhost:80/metrics'
synthetic_size_calculation_interval = '333 s'

log_format = 'json'
background_task_maximum_delay = '334 s'

"#;

    #[test]
    fn parse_defaults() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        let broker_endpoint = storage_broker::DEFAULT_ENDPOINT;
        // we have to create dummy values to overcome the validation errors
        let config_string = format!(
            "pg_distrib_dir='{pg_distrib_dir}'\nid=10\nbroker_endpoint = '{broker_endpoint}'",
        );
        let toml = config_string.parse()?;

        let parsed_config = PageServerConf::parse_and_validate(&toml, &workdir)
            .unwrap_or_else(|e| panic!("Failed to parse config '{config_string}', reason: {e:?}"));

        assert_eq!(
            parsed_config,
            PageServerConf {
                id: NodeId(10),
                listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
                listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
                availability_zone: None,
                wait_lsn_timeout: humantime::parse_duration(defaults::DEFAULT_WAIT_LSN_TIMEOUT)?,
                wal_redo_timeout: humantime::parse_duration(defaults::DEFAULT_WAL_REDO_TIMEOUT)?,
                superuser: defaults::DEFAULT_SUPERUSER.to_string(),
                page_cache_size: defaults::DEFAULT_PAGE_CACHE_SIZE,
                max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
                workdir,
                pg_distrib_dir,
                http_auth_type: AuthType::Trust,
                pg_auth_type: AuthType::Trust,
                auth_validation_public_key_path: None,
                remote_storage_config: None,
                default_tenant_conf: TenantConf::default(),
                broker_endpoint: storage_broker::DEFAULT_ENDPOINT.parse().unwrap(),
                broker_keepalive_interval: humantime::parse_duration(
                    storage_broker::DEFAULT_KEEPALIVE_INTERVAL
                )?,
                log_format: LogFormat::from_str(defaults::DEFAULT_LOG_FORMAT).unwrap(),
                concurrent_tenant_warmup: ConfigurableSemaphore::new(
                    NonZeroUsize::new(DEFAULT_CONCURRENT_TENANT_WARMUP).unwrap()
                ),
                concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::default(),
                eviction_task_immitated_concurrent_logical_size_queries:
                    ConfigurableSemaphore::default(),
                metric_collection_interval: humantime::parse_duration(
                    defaults::DEFAULT_METRIC_COLLECTION_INTERVAL
                )?,
                cached_metric_collection_interval: humantime::parse_duration(
                    defaults::DEFAULT_CACHED_METRIC_COLLECTION_INTERVAL
                )?,
                metric_collection_endpoint: defaults::DEFAULT_METRIC_COLLECTION_ENDPOINT,
                metric_collection_bucket: None,
                synthetic_size_calculation_interval: humantime::parse_duration(
                    defaults::DEFAULT_SYNTHETIC_SIZE_CALCULATION_INTERVAL
                )?,
                disk_usage_based_eviction: None,
                test_remote_failures: 0,
                ondemand_download_behavior_treat_error_as_warn: false,
                background_task_maximum_delay: humantime::parse_duration(
                    defaults::DEFAULT_BACKGROUND_TASK_MAXIMUM_DELAY
                )?,
                control_plane_api: None,
                control_plane_api_token: None,
                control_plane_emergency_mode: false,
                heatmap_upload_concurrency: defaults::DEFAULT_HEATMAP_UPLOAD_CONCURRENCY,
                secondary_download_concurrency: defaults::DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY,
                ingest_batch_size: defaults::DEFAULT_INGEST_BATCH_SIZE,
                virtual_file_io_engine: DEFAULT_VIRTUAL_FILE_IO_ENGINE.parse().unwrap(),
                get_vectored_impl: defaults::DEFAULT_GET_VECTORED_IMPL.parse().unwrap(),
                get_impl: defaults::DEFAULT_GET_IMPL.parse().unwrap(),
                max_vectored_read_bytes: MaxVectoredReadBytes(
                    NonZeroUsize::new(defaults::DEFAULT_MAX_VECTORED_READ_BYTES)
                        .expect("Invalid default constant")
                ),
                validate_vectored_get: defaults::DEFAULT_VALIDATE_VECTORED_GET,
                ephemeral_bytes_per_memory_kb: defaults::DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB,
                walredo_process_kind: defaults::DEFAULT_WALREDO_PROCESS_KIND.parse().unwrap(),
            },
            "Correct defaults should be used when no config values are provided"
        );

        Ok(())
    }

    #[test]
    fn parse_basic_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        let broker_endpoint = storage_broker::DEFAULT_ENDPOINT;

        let config_string = format!(
            "{ALL_BASE_VALUES_TOML}pg_distrib_dir='{pg_distrib_dir}'\nbroker_endpoint = '{broker_endpoint}'",
        );
        let toml = config_string.parse()?;

        let parsed_config = PageServerConf::parse_and_validate(&toml, &workdir)
            .unwrap_or_else(|e| panic!("Failed to parse config '{config_string}', reason: {e:?}"));

        assert_eq!(
            parsed_config,
            PageServerConf {
                id: NodeId(10),
                listen_pg_addr: "127.0.0.1:64000".to_string(),
                listen_http_addr: "127.0.0.1:9898".to_string(),
                availability_zone: None,
                wait_lsn_timeout: Duration::from_secs(111),
                wal_redo_timeout: Duration::from_secs(111),
                superuser: "zzzz".to_string(),
                page_cache_size: 444,
                max_file_descriptors: 333,
                workdir,
                pg_distrib_dir,
                http_auth_type: AuthType::Trust,
                pg_auth_type: AuthType::Trust,
                auth_validation_public_key_path: None,
                remote_storage_config: None,
                default_tenant_conf: TenantConf::default(),
                broker_endpoint: storage_broker::DEFAULT_ENDPOINT.parse().unwrap(),
                broker_keepalive_interval: Duration::from_secs(5),
                log_format: LogFormat::Json,
                concurrent_tenant_warmup: ConfigurableSemaphore::new(
                    NonZeroUsize::new(DEFAULT_CONCURRENT_TENANT_WARMUP).unwrap()
                ),
                concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::default(),
                eviction_task_immitated_concurrent_logical_size_queries:
                    ConfigurableSemaphore::default(),
                metric_collection_interval: Duration::from_secs(222),
                cached_metric_collection_interval: Duration::from_secs(22200),
                metric_collection_endpoint: Some(Url::parse("http://localhost:80/metrics")?),
                metric_collection_bucket: None,
                synthetic_size_calculation_interval: Duration::from_secs(333),
                disk_usage_based_eviction: None,
                test_remote_failures: 0,
                ondemand_download_behavior_treat_error_as_warn: false,
                background_task_maximum_delay: Duration::from_secs(334),
                control_plane_api: None,
                control_plane_api_token: None,
                control_plane_emergency_mode: false,
                heatmap_upload_concurrency: defaults::DEFAULT_HEATMAP_UPLOAD_CONCURRENCY,
                secondary_download_concurrency: defaults::DEFAULT_SECONDARY_DOWNLOAD_CONCURRENCY,
                ingest_batch_size: 100,
                virtual_file_io_engine: DEFAULT_VIRTUAL_FILE_IO_ENGINE.parse().unwrap(),
                get_vectored_impl: defaults::DEFAULT_GET_VECTORED_IMPL.parse().unwrap(),
                get_impl: defaults::DEFAULT_GET_IMPL.parse().unwrap(),
                max_vectored_read_bytes: MaxVectoredReadBytes(
                    NonZeroUsize::new(defaults::DEFAULT_MAX_VECTORED_READ_BYTES)
                        .expect("Invalid default constant")
                ),
                validate_vectored_get: defaults::DEFAULT_VALIDATE_VECTORED_GET,
                ephemeral_bytes_per_memory_kb: defaults::DEFAULT_EPHEMERAL_BYTES_PER_MEMORY_KB,
                walredo_process_kind: defaults::DEFAULT_WALREDO_PROCESS_KIND.parse().unwrap(),
            },
            "Should be able to parse all basic config values correctly"
        );

        Ok(())
    }

    #[test]
    fn parse_remote_fs_storage_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        let broker_endpoint = "http://127.0.0.1:7777";

        let local_storage_path = tempdir.path().join("local_remote_storage");

        let identical_toml_declarations = &[
            format!(
                r#"[remote_storage]
local_path = '{local_storage_path}'"#,
            ),
            format!("remote_storage={{local_path='{local_storage_path}'}}"),
        ];

        for remote_storage_config_str in identical_toml_declarations {
            let config_string = format!(
                r#"{ALL_BASE_VALUES_TOML}
pg_distrib_dir='{pg_distrib_dir}'
broker_endpoint = '{broker_endpoint}'

{remote_storage_config_str}"#,
            );

            let toml = config_string.parse()?;

            let parsed_remote_storage_config = PageServerConf::parse_and_validate(&toml, &workdir)
                .unwrap_or_else(|e| {
                    panic!("Failed to parse config '{config_string}', reason: {e:?}")
                })
                .remote_storage_config
                .expect("Should have remote storage config for the local FS");

            assert_eq!(
                parsed_remote_storage_config,
                RemoteStorageConfig {
                    storage: RemoteStorageKind::LocalFs(local_storage_path.clone()),
                    timeout: RemoteStorageConfig::DEFAULT_TIMEOUT,
                },
                "Remote storage config should correctly parse the local FS config and fill other storage defaults"
            );
        }
        Ok(())
    }

    #[test]
    fn parse_remote_s3_storage_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;

        let bucket_name = "some-sample-bucket".to_string();
        let bucket_region = "eu-north-1".to_string();
        let prefix_in_bucket = "test_prefix".to_string();
        let endpoint = "http://localhost:5000".to_string();
        let max_concurrent_syncs = NonZeroUsize::new(111).unwrap();
        let max_sync_errors = NonZeroU32::new(222).unwrap();
        let s3_concurrency_limit = NonZeroUsize::new(333).unwrap();
        let broker_endpoint = "http://127.0.0.1:7777";

        let identical_toml_declarations = &[
            format!(
                r#"[remote_storage]
max_concurrent_syncs = {max_concurrent_syncs}
max_sync_errors = {max_sync_errors}
bucket_name = '{bucket_name}'
bucket_region = '{bucket_region}'
prefix_in_bucket = '{prefix_in_bucket}'
endpoint = '{endpoint}'
concurrency_limit = {s3_concurrency_limit}"#
            ),
            format!(
                "remote_storage={{max_concurrent_syncs={max_concurrent_syncs}, max_sync_errors={max_sync_errors}, bucket_name='{bucket_name}',\
                bucket_region='{bucket_region}', prefix_in_bucket='{prefix_in_bucket}', endpoint='{endpoint}', concurrency_limit={s3_concurrency_limit}}}",
            ),
        ];

        for remote_storage_config_str in identical_toml_declarations {
            let config_string = format!(
                r#"{ALL_BASE_VALUES_TOML}
pg_distrib_dir='{pg_distrib_dir}'
broker_endpoint = '{broker_endpoint}'

{remote_storage_config_str}"#,
            );

            let toml = config_string.parse()?;

            let parsed_remote_storage_config = PageServerConf::parse_and_validate(&toml, &workdir)
                .unwrap_or_else(|e| {
                    panic!("Failed to parse config '{config_string}', reason: {e:?}")
                })
                .remote_storage_config
                .expect("Should have remote storage config for S3");

            assert_eq!(
                parsed_remote_storage_config,
                RemoteStorageConfig {
                    storage: RemoteStorageKind::AwsS3(S3Config {
                        bucket_name: bucket_name.clone(),
                        bucket_region: bucket_region.clone(),
                        prefix_in_bucket: Some(prefix_in_bucket.clone()),
                        endpoint: Some(endpoint.clone()),
                        concurrency_limit: s3_concurrency_limit,
                        max_keys_per_list_response: None,
                        upload_storage_class: None,
                    }),
                    timeout: RemoteStorageConfig::DEFAULT_TIMEOUT,
                },
                "Remote storage config should correctly parse the S3 config"
            );
        }
        Ok(())
    }

    #[test]
    fn parse_tenant_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;

        let broker_endpoint = "http://127.0.0.1:7777";
        let trace_read_requests = true;

        let config_string = format!(
            r#"{ALL_BASE_VALUES_TOML}
pg_distrib_dir='{pg_distrib_dir}'
broker_endpoint = '{broker_endpoint}'

[tenant_config]
trace_read_requests = {trace_read_requests}"#,
        );

        let toml = config_string.parse()?;

        let conf = PageServerConf::parse_and_validate(&toml, &workdir)?;
        assert_eq!(
            conf.default_tenant_conf.trace_read_requests, trace_read_requests,
            "Tenant config from pageserver config file should be parsed and udpated values used as defaults for all tenants",
        );

        Ok(())
    }

    #[test]
    fn parse_incorrect_tenant_config() -> anyhow::Result<()> {
        let config_string = r#"
            [tenant_config]
            checkpoint_distance = -1 # supposed to be an u64
        "#
        .to_string();

        let toml: Document = config_string.parse()?;
        let item = toml.get("tenant_config").unwrap();
        let error = TenantConfOpt::try_from(item.to_owned()).unwrap_err();

        let expected_error_str = "checkpoint_distance: invalid value: integer `-1`, expected u64";
        assert_eq!(error.to_string(), expected_error_str);

        Ok(())
    }

    #[test]
    fn parse_override_tenant_config() -> anyhow::Result<()> {
        let config_string = r#"tenant_config={ min_resident_size_override =  400 }"#.to_string();

        let toml: Document = config_string.parse()?;
        let item = toml.get("tenant_config").unwrap();
        let conf = TenantConfOpt::try_from(item.to_owned()).unwrap();

        assert_eq!(conf.min_resident_size_override, Some(400));

        Ok(())
    }

    #[test]
    fn eviction_pageserver_config_parse() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;

        let pageserver_conf_toml = format!(
            r#"pg_distrib_dir = "{pg_distrib_dir}"
metric_collection_endpoint = "http://sample.url"
metric_collection_interval = "10min"
id = 222

[disk_usage_based_eviction]
max_usage_pct = 80
min_avail_bytes = 0
period = "10s"

[tenant_config]
evictions_low_residence_duration_metric_threshold = "20m"

[tenant_config.eviction_policy]
kind = "LayerAccessThreshold"
period = "20m"
threshold = "20m"
"#,
        );
        let toml: Document = pageserver_conf_toml.parse()?;
        let conf = PageServerConf::parse_and_validate(&toml, &workdir)?;

        assert_eq!(conf.pg_distrib_dir, pg_distrib_dir);
        assert_eq!(
            conf.metric_collection_endpoint,
            Some("http://sample.url".parse().unwrap())
        );
        assert_eq!(
            conf.metric_collection_interval,
            Duration::from_secs(10 * 60)
        );
        assert_eq!(
            conf.default_tenant_conf
                .evictions_low_residence_duration_metric_threshold,
            Duration::from_secs(20 * 60)
        );
        assert_eq!(conf.id, NodeId(222));
        assert_eq!(
            conf.disk_usage_based_eviction,
            Some(DiskUsageEvictionTaskConfig {
                max_usage_pct: Percent::new(80).unwrap(),
                min_avail_bytes: 0,
                period: Duration::from_secs(10),
                #[cfg(feature = "testing")]
                mock_statvfs: None,
                eviction_order: crate::disk_usage_eviction_task::EvictionOrder::AbsoluteAccessed,
            })
        );

        match &conf.default_tenant_conf.eviction_policy {
            EvictionPolicy::LayerAccessThreshold(eviction_threshold) => {
                assert_eq!(eviction_threshold.period, Duration::from_secs(20 * 60));
                assert_eq!(eviction_threshold.threshold, Duration::from_secs(20 * 60));
            }
            other => unreachable!("Unexpected eviction policy tenant settings: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn parse_imitation_only_pageserver_config() {
        let tempdir = tempdir().unwrap();
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir).unwrap();

        let pageserver_conf_toml = format!(
            r#"pg_distrib_dir = "{pg_distrib_dir}"
metric_collection_endpoint = "http://sample.url"
metric_collection_interval = "10min"
id = 222

[tenant_config]
evictions_low_residence_duration_metric_threshold = "20m"

[tenant_config.eviction_policy]
kind = "OnlyImitiate"
period = "20m"
threshold = "20m"
"#,
        );
        let toml: Document = pageserver_conf_toml.parse().unwrap();
        let conf = PageServerConf::parse_and_validate(&toml, &workdir).unwrap();

        match &conf.default_tenant_conf.eviction_policy {
            EvictionPolicy::OnlyImitiate(t) => {
                assert_eq!(t.period, Duration::from_secs(20 * 60));
                assert_eq!(t.threshold, Duration::from_secs(20 * 60));
            }
            other => unreachable!("Unexpected eviction policy tenant settings: {other:?}"),
        }
    }

    fn prepare_fs(tempdir: &Utf8TempDir) -> anyhow::Result<(Utf8PathBuf, Utf8PathBuf)> {
        let tempdir_path = tempdir.path();

        let workdir = tempdir_path.join("workdir");
        fs::create_dir_all(&workdir)?;

        let pg_distrib_dir = tempdir_path.join("pg_distrib");
        let pg_distrib_dir_versioned = pg_distrib_dir.join(format!("v{DEFAULT_PG_VERSION}"));
        fs::create_dir_all(&pg_distrib_dir_versioned)?;
        let postgres_bin_dir = pg_distrib_dir_versioned.join("bin");
        fs::create_dir_all(&postgres_bin_dir)?;
        fs::write(postgres_bin_dir.join("postgres"), "I'm postgres, trust me")?;

        Ok((workdir, pg_distrib_dir))
    }
}
