use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use std::path::PathBuf;
use std::time::Duration;

use lazy_static::lazy_static;
use zenith_metrics::{register_int_gauge_vec, IntGaugeVec};

pub mod basebackup;
pub mod branches;
pub mod http;
pub mod layered_repository;
pub mod page_service;
pub mod relish;
pub mod repository;
pub mod restore_local_repo;
pub mod tenant_mgr;
pub mod waldecoder;
pub mod walreceiver;
pub mod walredo;

pub mod defaults {
    use std::time::Duration;

    pub const DEFAULT_PG_LISTEN_PORT: u16 = 64000;
    pub const DEFAULT_PG_LISTEN_ADDR: &str = "127.0.0.1:64000"; // can't format! const yet...
    pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 9898;
    pub const DEFAULT_HTTP_LISTEN_ADDR: &str = "127.0.0.1:9898";

    pub const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_GC_PERIOD: Duration = Duration::from_secs(100);

    pub const DEFAULT_SUPERUSER: &str = "zenith_admin";
}

lazy_static! {
    static ref LIVE_CONNECTIONS_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "pageserver_live_connections_count",
        "Number of live network connections",
        &["pageserver_connection_kind"]
    )
    .expect("failed to define a metric");
}

pub const LOG_FILE_NAME: &str = "pageserver.log";

#[derive(Debug, Clone)]
pub struct PageServerConf {
    pub daemonize: bool,
    pub listen_pg_addr: String,
    pub listen_http_addr: String,
    pub gc_horizon: u64,
    pub gc_period: Duration,
    pub superuser: String,

    // Repository directory, relative to current working directory.
    // Normally, the page server changes the current working directory
    // to the repository, and 'workdir' is always '.'. But we don't do
    // that during unit testing, because the current directory is global
    // to the process but different unit tests work on different
    // repositories.
    pub workdir: PathBuf,

    pub pg_distrib_dir: PathBuf,

    pub auth_type: AuthType,

    pub auth_validation_public_key_path: Option<PathBuf>,
}

impl PageServerConf {
    //
    // Repository paths, relative to workdir.
    //

    fn tenants_path(&self) -> PathBuf {
        self.workdir.join("tenants")
    }

    fn tenant_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenants_path().join(tenantid.to_string())
    }

    fn tags_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join("refs").join("tags")
    }

    fn tag_path(&self, tag_name: &str, tenantid: &ZTenantId) -> PathBuf {
        self.tags_path(tenantid).join(tag_name)
    }

    fn branches_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join("refs").join("branches")
    }

    fn branch_path(&self, branch_name: &str, tenantid: &ZTenantId) -> PathBuf {
        self.branches_path(tenantid).join(branch_name)
    }

    fn timelines_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join("timelines")
    }

    fn timeline_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timelines_path(tenantid).join(timelineid.to_string())
    }

    fn ancestor_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timeline_path(timelineid, tenantid).join("ancestor")
    }

    fn wal_dir_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timeline_path(timelineid, tenantid).join("wal")
    }

    //
    // Postgres distribution paths
    //

    pub fn pg_bin_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("bin")
    }

    pub fn pg_lib_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("lib")
    }

    #[cfg(test)]
    fn test_repo_dir(test_name: &str) -> PathBuf {
        PathBuf::from(format!("../tmp_check/test_{}", test_name))
    }

    #[cfg(test)]
    fn dummy_conf(repo_dir: PathBuf) -> Self {
        PageServerConf {
            daemonize: false,
            gc_horizon: 64 * 1024 * 1024,
            gc_period: Duration::from_secs(10),
            listen_pg_addr: "127.0.0.1:5430".to_string(),
            listen_http_addr: "127.0.0.1:9898".to_string(),
            superuser: "zenith_admin".to_string(),
            workdir: repo_dir,
            pg_distrib_dir: "".into(),
            auth_type: AuthType::Trust,
            auth_validation_public_key_path: None,
        }
    }
}
