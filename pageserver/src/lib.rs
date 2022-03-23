pub mod basebackup;
pub mod config;
pub mod http;
pub mod import_datadir;
pub mod layered_repository;
pub mod page_cache;
pub mod page_service;
pub mod relish;
pub mod remote_storage;
pub mod repository;
pub mod tenant_mgr;
pub mod tenant_threads;
pub mod thread_mgr;
pub mod timelines;
pub mod virtual_file;
pub mod walingest;
pub mod walreceiver;
pub mod walrecord;
pub mod walredo;

use lazy_static::lazy_static;
use tracing::info;
use zenith_metrics::{register_int_gauge_vec, IntGaugeVec};
use zenith_utils::{
    postgres_backend,
    zid::{ZTenantId, ZTimelineId},
};

use crate::thread_mgr::ThreadKind;

lazy_static! {
    static ref LIVE_CONNECTIONS_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "pageserver_live_connections_count",
        "Number of live network connections",
        &["pageserver_connection_kind"]
    )
    .expect("failed to define a metric");
}

pub const LOG_FILE_NAME: &str = "pageserver.log";

/// Config for the Repository checkpointer
#[derive(Debug, Clone, Copy)]
pub enum CheckpointConfig {
    // Flush in-memory data that is older than this
    Distance(u64),
    // Flush all in-memory data
    Flush,
    // Flush all in-memory data and reconstruct all page images
    Forced,
}

pub fn shutdown_pageserver() {
    // Shut down the libpq endpoint thread. This prevents new connections from
    // being accepted.
    thread_mgr::shutdown_threads(Some(ThreadKind::LibpqEndpointListener), None, None);

    // Shut down any page service threads.
    postgres_backend::set_pgbackend_shutdown_requested();
    thread_mgr::shutdown_threads(Some(ThreadKind::PageRequestHandler), None, None);

    // Shut down all the tenants. This flushes everything to disk and kills
    // the checkpoint and GC threads.
    tenant_mgr::shutdown_all_tenants();

    // Stop syncing with remote storage.
    //
    // FIXME: Does this wait for the sync thread to finish syncing what's queued up?
    // Should it?
    thread_mgr::shutdown_threads(Some(ThreadKind::StorageSync), None, None);

    // Shut down the HTTP endpoint last, so that you can still check the server's
    // status while it's shutting down.
    thread_mgr::shutdown_threads(Some(ThreadKind::HttpEndpointListener), None, None);

    // There should be nothing left, but let's be sure
    thread_mgr::shutdown_threads(None, None, None);

    info!("Shut down successfully completed");
    std::process::exit(0);
}
