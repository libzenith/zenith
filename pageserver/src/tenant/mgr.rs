//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use std::collections::{hash_map, HashMap};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;

use anyhow::Context;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tracing::*;

use remote_storage::GenericRemoteStorage;
use utils::crashsafe;

use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind};
use crate::tenant::config::TenantConfOpt;
use crate::tenant::{create_tenant_files, CreateTenantFilesMode, Tenant, TenantState};
use crate::IGNORED_TENANT_FILE_NAME;

use utils::fs_ext::PathExt;
use utils::id::{TenantId, TimelineId};

/// The tenants known to the pageserver.
/// The enum variants are used to distinguish the different states that the pageserver can be in.
enum TenantsMap {
    /// [`init_tenant_mgr`] is not done yet.
    Initializing,
    /// [`init_tenant_mgr`] is done, all on-disk tenants have been loaded.
    /// New tenants can be added using [`tenant_map_insert`].
    Open(HashMap<TenantId, Arc<Tenant>>),
    /// The pageserver has entered shutdown mode via [`shutdown_all_tenants`].
    /// Existing tenants are still accessible, but no new tenants can be created.
    ShuttingDown(HashMap<TenantId, Arc<Tenant>>),
}

impl TenantsMap {
    fn get(&self, tenant_id: &TenantId) -> Option<&Arc<Tenant>> {
        match self {
            TenantsMap::Initializing => None,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m.get(tenant_id),
        }
    }
    fn remove(&mut self, tenant_id: &TenantId) -> Option<Arc<Tenant>> {
        match self {
            TenantsMap::Initializing => None,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m.remove(tenant_id),
        }
    }
}

static TENANTS: Lazy<RwLock<TenantsMap>> = Lazy::new(|| RwLock::new(TenantsMap::Initializing));

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the tenant once download is completed.
#[instrument(skip(conf, remote_storage))]
pub async fn init_tenant_mgr(
    conf: &'static PageServerConf,
    remote_storage: Option<GenericRemoteStorage>,
) -> anyhow::Result<()> {
    // Scan local filesystem for attached tenants
    let tenants_dir = conf.tenants_path();

    let mut tenants = HashMap::new();

    let mut dir_entries = fs::read_dir(&tenants_dir)
        .await
        .with_context(|| format!("Failed to list tenants dir {tenants_dir:?}"))?;

    let ctx = RequestContext::todo_child(TaskKind::Startup, DownloadBehavior::Warn);

    loop {
        match dir_entries.next_entry().await {
            Ok(None) => break,
            Ok(Some(dir_entry)) => {
                let tenant_dir_path = dir_entry.path();
                if crate::is_temporary(&tenant_dir_path) {
                    info!(
                        "Found temporary tenant directory, removing: {}",
                        tenant_dir_path.display()
                    );
                    if let Err(e) = fs::remove_dir_all(&tenant_dir_path).await {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            tenant_dir_path.display(),
                            e
                        );
                    }
                } else {
                    // This case happens if we crash during attach before creating the attach marker file
                    let is_empty = tenant_dir_path.is_empty_dir().with_context(|| {
                        format!("Failed to check whether {tenant_dir_path:?} is an empty dir")
                    })?;
                    if is_empty {
                        info!("removing empty tenant directory {tenant_dir_path:?}");
                        if let Err(e) = fs::remove_dir(&tenant_dir_path).await {
                            error!(
                                "Failed to remove empty tenant directory '{}': {e:#}",
                                tenant_dir_path.display()
                            )
                        }
                        continue;
                    }

                    let tenant_ignore_mark_file = tenant_dir_path.join(IGNORED_TENANT_FILE_NAME);
                    if tenant_ignore_mark_file.exists() {
                        info!("Found an ignore mark file {tenant_ignore_mark_file:?}, skipping the tenant");
                        continue;
                    }

                    match schedule_local_tenant_processing(
                        conf,
                        &tenant_dir_path,
                        remote_storage.clone(),
                        &ctx,
                    ) {
                        Ok(tenant) => {
                            tenants.insert(tenant.tenant_id(), tenant);
                        }
                        Err(e) => {
                            error!("Failed to collect tenant files from dir {tenants_dir:?} for entry {dir_entry:?}, reason: {e:#}");
                        }
                    }
                }
            }
            Err(e) => {
                // On error, print it, but continue with the other tenants. If we error out
                // here, the pageserver startup fails altogether, causing outage for *all*
                // tenants. That seems worse.
                error!(
                    "Failed to list tenants dir entry in directory {tenants_dir:?}, reason: {e:?}"
                );
            }
        }
    }

    info!("Processed {} local tenants at startup", tenants.len());

    let mut tenants_map = TENANTS.write().await;
    assert!(matches!(&*tenants_map, &TenantsMap::Initializing));
    *tenants_map = TenantsMap::Open(tenants);
    Ok(())
}

pub fn schedule_local_tenant_processing(
    conf: &'static PageServerConf,
    tenant_path: &Path,
    remote_storage: Option<GenericRemoteStorage>,
    ctx: &RequestContext,
) -> anyhow::Result<Arc<Tenant>> {
    anyhow::ensure!(
        tenant_path.is_dir(),
        "Cannot load tenant from path {tenant_path:?}, it either does not exist or not a directory"
    );
    anyhow::ensure!(
        !crate::is_temporary(tenant_path),
        "Cannot load tenant from temporary path {tenant_path:?}"
    );
    anyhow::ensure!(
        !tenant_path.is_empty_dir().with_context(|| {
            format!("Failed to check whether {tenant_path:?} is an empty dir")
        })?,
        "Cannot load tenant from empty directory {tenant_path:?}"
    );

    let tenant_id = tenant_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<TenantId>()
        .with_context(|| {
            format!("Could not parse tenant id out of the tenant dir name in path {tenant_path:?}")
        })?;

    let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(tenant_id);
    anyhow::ensure!(
        !conf.tenant_ignore_mark_file_path(tenant_id).exists(),
        "Cannot load tenant, ignore mark found at {tenant_ignore_mark:?}"
    );

    let tenant = if conf.tenant_attaching_mark_file_path(&tenant_id).exists() {
        info!("tenant {tenant_id} has attaching mark file, resuming its attach operation");
        if let Some(remote_storage) = remote_storage {
            match Tenant::spawn_attach(conf, tenant_id, remote_storage, ctx) {
                Ok(tenant) => tenant,
                Err(e) => {
                    error!("Failed to spawn_attach tenant {tenant_id}, reason: {e:#}");
                    Tenant::create_broken_tenant(conf, tenant_id, format!("{e:#}"))
                }
            }
        } else {
            warn!("tenant {tenant_id} has attaching mark file, but pageserver has no remote storage configured");
            Tenant::create_broken_tenant(
                conf,
                tenant_id,
                "attaching mark file present but no remote storage configured".to_string(),
            )
        }
    } else {
        info!("tenant {tenant_id} is assumed to be loadable, starting load operation");
        // Start loading the tenant into memory. It will initially be in Loading state.
        Tenant::spawn_load(conf, tenant_id, remote_storage, ctx)
    };
    Ok(tenant)
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
/// NB: We leave the tenants in the map, so that they remain accessible through
/// the management API until we shut it down. If we removed the shut-down tenants
/// from the tenants map, the management API would return 404 for these tenants,
/// because TenantsMap::get() now returns `None`.
/// That could be easily misinterpreted by control plane, the consumer of the
/// management API. For example, it could attach the tenant on a different pageserver.
/// We would then be in split-brain once this pageserver restarts.
pub async fn shutdown_all_tenants() {
    // Prevent new tenants from being created.
    let tenants_to_shut_down = {
        let mut m = TENANTS.write().await;
        match &mut *m {
            TenantsMap::Initializing => {
                *m = TenantsMap::ShuttingDown(HashMap::default());
                info!("tenants map is empty");
                return;
            }
            TenantsMap::Open(tenants) => {
                let tenants_clone = tenants.clone();
                *m = TenantsMap::ShuttingDown(std::mem::take(tenants));
                tenants_clone
            }
            TenantsMap::ShuttingDown(_) => {
                error!("already shutting down, this function isn't supposed to be called more than once");
                return;
            }
        }
    };

    let mut tenants_to_freeze_and_flush = Vec::with_capacity(tenants_to_shut_down.len());
    for (_, tenant) in tenants_to_shut_down {
        if tenant.is_active() {
            // updates tenant state, forbidding new GC and compaction iterations from starting
            tenant.set_stopping();
            tenants_to_freeze_and_flush.push(tenant);
        }
    }

    // Shut down all existing walreceiver connections and stop accepting the new ones.
    task_mgr::shutdown_tasks(Some(TaskKind::WalReceiverManager), None, None).await;

    // Ok, no background tasks running anymore. Flush any remaining data in
    // memory to disk.
    //
    // We assume that any incoming connections that might request pages from
    // the tenant have already been terminated by the caller, so there
    // should be no more activity in any of the repositories.
    //
    // On error, log it but continue with the shutdown for other tenants.
    for tenant in tenants_to_freeze_and_flush {
        let tenant_id = tenant.tenant_id();
        debug!("shutdown tenant {tenant_id}");

        if let Err(err) = tenant.freeze_and_flush().await {
            error!("Could not checkpoint tenant {tenant_id} during shutdown: {err:?}");
        }
    }
}

pub async fn create_tenant(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    remote_storage: Option<GenericRemoteStorage>,
    ctx: &RequestContext,
) -> Result<Arc<Tenant>, TenantMapInsertError> {
    tenant_map_insert(tenant_id, || {
        // We're holding the tenants lock in write mode while doing local IO.
        // If this section ever becomes contentious, introduce a new `TenantState::Creating`
        // and do the work in that state.
        let tenant_directory = super::create_tenant_files(conf, tenant_conf, tenant_id, CreateTenantFilesMode::Create)?;
        // TODO: tenant directory remains on disk if we bail out from here on.
        //       See https://github.com/neondatabase/neon/issues/4233

        let created_tenant =
            schedule_local_tenant_processing(conf, &tenant_directory, remote_storage, ctx)?;
        // TODO: tenant object & its background loops remain, untracked in tenant map, if we fail here.
        //      See https://github.com/neondatabase/neon/issues/4233

        let crated_tenant_id = created_tenant.tenant_id();
        anyhow::ensure!(
                tenant_id == crated_tenant_id,
                "loaded created tenant has unexpected tenant id (expect {tenant_id} != actual {crated_tenant_id})",
            );
        Ok(created_tenant)
    }).await
}

pub async fn set_new_tenant_config(
    conf: &'static PageServerConf,
    new_tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
) -> Result<(), TenantStateError> {
    info!("configuring tenant {tenant_id}");
    let tenant = get_tenant(tenant_id, true).await?;

    let tenant_config_path = conf.tenant_config_path(tenant_id);
    Tenant::persist_tenant_config(
        &tenant.tenant_id(),
        &tenant_config_path,
        new_tenant_conf,
        false,
    )?;
    tenant.set_new_tenant_config(new_tenant_conf);
    Ok(())
}

/// Gets the tenant from the in-memory data, erroring if it's absent or is not fitting to the query.
/// `active_only = true` allows to query only tenants that are ready for operations, erroring on other kinds of tenants.
pub async fn get_tenant(
    tenant_id: TenantId,
    active_only: bool,
) -> Result<Arc<Tenant>, TenantStateError> {
    let m = TENANTS.read().await;
    let tenant = m
        .get(&tenant_id)
        .ok_or(TenantStateError::NotFound(tenant_id))?;
    if active_only && !tenant.is_active() {
        Err(TenantStateError::NotActive(tenant_id))
    } else {
        Ok(Arc::clone(tenant))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DeleteTimelineError {
    #[error("Tenant {0}")]
    Tenant(#[from] TenantStateError),

    #[error("Timeline {0}")]
    Timeline(#[from] crate::tenant::DeleteTimelineError),
}

pub async fn delete_timeline(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    ctx: &RequestContext,
) -> Result<(), DeleteTimelineError> {
    let tenant = get_tenant(tenant_id, true).await?;
    tenant.delete_timeline(timeline_id, ctx).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum TenantStateError {
    #[error("Tenant {0} not found")]
    NotFound(TenantId),
    #[error("Tenant {0} is stopping")]
    IsStopping(TenantId),
    #[error("Tenant {0} is not active")]
    NotActive(TenantId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub async fn detach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    detach_ignored: bool,
) -> Result<(), TenantStateError> {
    let local_files_cleanup_operation = |tenant_id_to_clean| async move {
        let local_tenant_directory = conf.tenant_path(&tenant_id_to_clean);
        fs::remove_dir_all(&local_tenant_directory)
            .await
            .with_context(|| {
                format!("local tenant directory {local_tenant_directory:?} removal")
            })?;
        Ok(())
    };

    let removal_result =
        remove_tenant_from_memory(tenant_id, local_files_cleanup_operation(tenant_id)).await;

    // Ignored tenants are not present in memory and will bail the removal from memory operation.
    // Before returning the error, check for ignored tenant removal case — we only need to clean its local files then.
    if detach_ignored && matches!(removal_result, Err(TenantStateError::NotFound(_))) {
        let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(tenant_id);
        if tenant_ignore_mark.exists() {
            info!("Detaching an ignored tenant");
            local_files_cleanup_operation(tenant_id)
                .await
                .with_context(|| format!("Ignored tenant {tenant_id} local files cleanup"))?;
            return Ok(());
        }
    }

    removal_result
}

pub async fn load_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    remote_storage: Option<GenericRemoteStorage>,
    ctx: &RequestContext,
) -> Result<(), TenantMapInsertError> {
    tenant_map_insert(tenant_id, || {
        let tenant_path = conf.tenant_path(&tenant_id);
        let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(tenant_id);
        if tenant_ignore_mark.exists() {
            std::fs::remove_file(&tenant_ignore_mark)
                .with_context(|| format!("Failed to remove tenant ignore mark {tenant_ignore_mark:?} during tenant loading"))?;
        }

        let new_tenant = schedule_local_tenant_processing(conf, &tenant_path, remote_storage, ctx)
            .with_context(|| {
                format!("Failed to schedule tenant processing in path {tenant_path:?}")
            })?;

        Ok(new_tenant)
    }).await?;
    Ok(())
}

pub async fn ignore_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
) -> Result<(), TenantStateError> {
    remove_tenant_from_memory(tenant_id, async {
        let ignore_mark_file = conf.tenant_ignore_mark_file_path(tenant_id);
        fs::File::create(&ignore_mark_file)
            .await
            .context("Failed to create ignore mark file")
            .and_then(|_| {
                crashsafe::fsync_file_and_parent(&ignore_mark_file)
                    .context("Failed to fsync ignore mark file")
            })
            .with_context(|| format!("Failed to crate ignore mark for tenant {tenant_id}"))?;
        Ok(())
    })
    .await
}

#[derive(Debug, thiserror::Error)]
pub enum TenantMapListError {
    #[error("tenant map is still initiailizing")]
    Initializing,
}

///
/// Get list of tenants, for the mgmt API
///
pub async fn list_tenants() -> Result<Vec<(TenantId, TenantState)>, TenantMapListError> {
    let tenants = TENANTS.read().await;
    let m = match &*tenants {
        TenantsMap::Initializing => return Err(TenantMapListError::Initializing),
        TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m,
    };
    Ok(m.iter()
        .map(|(id, tenant)| (*id, tenant.current_state()))
        .collect())
}

/// Execute Attach mgmt API command.
///
/// Downloading all the tenant data is performed in the background, this merely
/// spawns the background task and returns quickly.
pub async fn attach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    tenant_conf: TenantConfOpt,
    remote_storage: GenericRemoteStorage,
    ctx: &RequestContext,
) -> Result<(), TenantMapInsertError> {
    tenant_map_insert(tenant_id, || {
        let tenant_dir = create_tenant_files(conf, tenant_conf, tenant_id, CreateTenantFilesMode::Attach)?;
        // TODO: tenant directory remains on disk if we bail out from here on.
        //       See https://github.com/neondatabase/neon/issues/4233

        // Without the attach marker, schedule_local_tenant_processing will treat the attached tenant as fully attached
        let marker_file_exists = conf
            .tenant_attaching_mark_file_path(&tenant_id)
            .try_exists()
            .context("check for attach marker file existence")?;
        anyhow::ensure!(marker_file_exists, "create_tenant_files should have created the attach marker file");

        let attached_tenant = schedule_local_tenant_processing(conf, &tenant_dir, Some(remote_storage), ctx)?;
        // TODO: tenant object & its background loops remain, untracked in tenant map, if we fail here.
        //      See https://github.com/neondatabase/neon/issues/4233

        let attached_tenant_id = attached_tenant.tenant_id();
        anyhow::ensure!(
            tenant_id == attached_tenant_id,
            "loaded created tenant has unexpected tenant id (expect {tenant_id} != actual {attached_tenant_id})",
        );
        Ok(attached_tenant)
    })
    .await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum TenantMapInsertError {
    #[error("tenant map is still initializing")]
    StillInitializing,
    #[error("tenant map is shutting down")]
    ShuttingDown,
    #[error("tenant {0} already exists, state: {1:?}")]
    TenantAlreadyExists(TenantId, TenantState),
    #[error(transparent)]
    Closure(#[from] anyhow::Error),
}

/// Give the given closure access to the tenants map entry for the given `tenant_id`, iff that
/// entry is vacant. The closure is responsible for creating the tenant object and inserting
/// it into the tenants map through the vacnt entry that it receives as argument.
///
/// NB: the closure should return quickly because the current implementation of tenants map
/// serializes access through an `RwLock`.
async fn tenant_map_insert<F>(
    tenant_id: TenantId,
    insert_fn: F,
) -> Result<Arc<Tenant>, TenantMapInsertError>
where
    F: FnOnce() -> anyhow::Result<Arc<Tenant>>,
{
    let mut guard = TENANTS.write().await;
    let m = match &mut *guard {
        TenantsMap::Initializing => return Err(TenantMapInsertError::StillInitializing),
        TenantsMap::ShuttingDown(_) => return Err(TenantMapInsertError::ShuttingDown),
        TenantsMap::Open(m) => m,
    };
    match m.entry(tenant_id) {
        hash_map::Entry::Occupied(e) => Err(TenantMapInsertError::TenantAlreadyExists(
            tenant_id,
            e.get().current_state(),
        )),
        hash_map::Entry::Vacant(v) => match insert_fn() {
            Ok(tenant) => {
                v.insert(tenant.clone());
                Ok(tenant)
            }
            Err(e) => Err(TenantMapInsertError::Closure(e)),
        },
    }
}

/// Stops and removes the tenant from memory, if it's not [`TenantState::Stopping`] already, bails otherwise.
/// Allows to remove other tenant resources manually, via `tenant_cleanup`.
/// If the cleanup fails, tenant will stay in memory in [`TenantState::Broken`] state, and another removal
/// operation would be needed to remove it.
async fn remove_tenant_from_memory<V, F>(
    tenant_id: TenantId,
    tenant_cleanup: F,
) -> Result<V, TenantStateError>
where
    F: std::future::Future<Output = anyhow::Result<V>>,
{
    // It's important to keep the tenant in memory after the final cleanup, to avoid cleanup races.
    // The exclusive lock here ensures we don't miss the tenant state updates before trying another removal.
    // tenant-wde cleanup operations may take some time (removing the entire tenant directory), we want to
    // avoid holding the lock for the entire process.
    {
        let tenants_accessor = TENANTS.write().await;
        match tenants_accessor.get(&tenant_id) {
            Some(tenant) => match tenant.current_state() {
                TenantState::Attaching
                | TenantState::Loading
                | TenantState::Broken { .. }
                | TenantState::Active => tenant.set_stopping(),
                TenantState::Stopping => return Err(TenantStateError::IsStopping(tenant_id)),
            },
            None => return Err(TenantStateError::NotFound(tenant_id)),
        }
    }

    // shutdown all tenant and timeline tasks: gc, compaction, page service)
    // No new tasks will be started for this tenant because it's in `Stopping` state.
    // Hence, once we're done here, the `tenant_cleanup` callback can mutate tenant on-disk state freely.
    task_mgr::shutdown_tasks(None, Some(tenant_id), None).await;

    match tenant_cleanup
        .await
        .with_context(|| format!("Failed to run cleanup for tenant {tenant_id}"))
    {
        Ok(hook_value) => {
            let mut tenants_accessor = TENANTS.write().await;
            if tenants_accessor.remove(&tenant_id).is_none() {
                warn!("Tenant {tenant_id} got removed from memory before operation finished");
            }
            Ok(hook_value)
        }
        Err(e) => {
            let tenants_accessor = TENANTS.read().await;
            match tenants_accessor.get(&tenant_id) {
                Some(tenant) => {
                    tenant.set_broken(e.to_string());
                }
                None => {
                    warn!("Tenant {tenant_id} got removed from memory");
                    return Err(TenantStateError::NotFound(tenant_id));
                }
            }
            Err(TenantStateError::Other(e))
        }
    }
}

use {
    crate::repository::GcResult, pageserver_api::models::TimelineGcRequest,
    utils::http::error::ApiError,
};

pub async fn immediate_gc(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    gc_req: TimelineGcRequest,
    ctx: &RequestContext,
) -> Result<tokio::sync::oneshot::Receiver<Result<GcResult, anyhow::Error>>, ApiError> {
    let guard = TENANTS.read().await;
    let tenant = guard
        .get(&tenant_id)
        .map(Arc::clone)
        .with_context(|| format!("tenant {tenant_id}"))
        .map_err(ApiError::NotFound)?;

    let gc_horizon = gc_req.gc_horizon.unwrap_or_else(|| tenant.get_gc_horizon());
    // Use tenant's pitr setting
    let pitr = tenant.get_pitr_interval();

    // Run in task_mgr to avoid race with tenant_detach operation
    let ctx = ctx.detached_child(TaskKind::GarbageCollector, DownloadBehavior::Download);
    let (task_done, wait_task_done) = tokio::sync::oneshot::channel();
    task_mgr::spawn(
        &tokio::runtime::Handle::current(),
        TaskKind::GarbageCollector,
        Some(tenant_id),
        Some(timeline_id),
        &format!("timeline_gc_handler garbage collection run for tenant {tenant_id} timeline {timeline_id}"),
        false,
        async move {
            fail::fail_point!("immediate_gc_task_pre");
            let result = tenant
                .gc_iteration(Some(timeline_id), gc_horizon, pitr, &ctx)
                .instrument(info_span!("manual_gc", tenant = %tenant_id, timeline = %timeline_id))
                .await;
                // FIXME: `gc_iteration` can return an error for multiple reasons; we should handle it
                // better once the types support it.
            match task_done.send(result) {
                Ok(_) => (),
                Err(result) => error!("failed to send gc result: {result:?}"),
            }
            Ok(())
        }
    );

    // drop the guard until after we've spawned the task so that timeline shutdown will wait for the task
    drop(guard);

    Ok(wait_task_done)
}

#[cfg(feature = "testing")]
pub async fn immediate_compact(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    ctx: &RequestContext,
) -> Result<tokio::sync::oneshot::Receiver<anyhow::Result<()>>, ApiError> {
    let guard = TENANTS.read().await;

    let tenant = guard
        .get(&tenant_id)
        .map(Arc::clone)
        .with_context(|| format!("tenant {tenant_id}"))
        .map_err(ApiError::NotFound)?;

    let timeline = tenant
        .get_timeline(timeline_id, true)
        .map_err(ApiError::NotFound)?;

    // Run in task_mgr to avoid race with tenant_detach operation
    let ctx = ctx.detached_child(TaskKind::Compaction, DownloadBehavior::Download);
    let (task_done, wait_task_done) = tokio::sync::oneshot::channel();
    task_mgr::spawn(
        &tokio::runtime::Handle::current(),
        TaskKind::Compaction,
        Some(tenant_id),
        Some(timeline_id),
        &format!(
            "timeline_compact_handler compaction run for tenant {tenant_id} timeline {timeline_id}"
        ),
        false,
        async move {
            let result = timeline
                .compact(&ctx)
                .instrument(
                    info_span!("manual_compact", tenant = %tenant_id, timeline = %timeline_id),
                )
                .await;

            match task_done.send(result) {
                Ok(_) => (),
                Err(result) => error!("failed to send compaction result: {result:?}"),
            }
            Ok(())
        },
    );

    // drop the guard until after we've spawned the task so that timeline shutdown will wait for the task
    drop(guard);

    Ok(wait_task_done)
}
