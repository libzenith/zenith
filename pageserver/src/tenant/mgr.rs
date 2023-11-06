//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use camino::{Utf8DirEntry, Utf8Path, Utf8PathBuf};
use rand::{distributions::Alphanumeric, Rng};
use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use utils::timeout::{timeout_cancellable, TimeoutCancellableError};

use anyhow::Context;
use once_cell::sync::Lazy;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;

use remote_storage::GenericRemoteStorage;
use utils::crashsafe;

use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::control_plane_client::{
    ControlPlaneClient, ControlPlaneGenerationsApi, RetryForeverError,
};
use crate::deletion_queue::DeletionQueueClient;
use crate::metrics::TENANT_MANAGER as METRICS;
use crate::task_mgr::{self, TaskKind};
use crate::tenant::config::{AttachmentMode, LocationConf, LocationMode, TenantConfOpt};
use crate::tenant::delete::DeleteTenantFlow;
use crate::tenant::{create_tenant_files, AttachedTenantConf, SpawnMode, Tenant, TenantState};
use crate::{InitializationOrder, IGNORED_TENANT_FILE_NAME, TEMP_FILE_SUFFIX};

use utils::crashsafe::path_with_suffix_extension;
use utils::fs_ext::PathExt;
use utils::generation::Generation;
use utils::id::{TenantId, TimelineId};

use super::delete::DeleteTenantError;
use super::timeline::delete::DeleteTimelineFlow;
use super::TenantSharedResources;

/// For a tenant that appears in TenantsMap, it may either be
/// - `Attached`: has a full Tenant object, is elegible to service
///    reads and ingest WAL.
/// - `Secondary`: is only keeping a local cache warm.
///
/// Secondary is a totally distinct state rather than being a mode of a `Tenant`, because
/// that way we avoid having to carefully switch a tenant's ingestion etc on and off during
/// its lifetime, and we can preserve some important safety invariants like `Tenant` always
/// having a properly acquired generation (Secondary doesn't need a generation)
pub(crate) enum TenantSlot {
    Attached(Arc<Tenant>),
    Secondary,
    /// In this state, other administrative operations acting on the TenantId should
    /// block, or return a retry indicator equivalent to HTTP 503.
    InProgress(utils::completion::Barrier),
}

impl std::fmt::Debug for TenantSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Attached(tenant) => write!(f, "Attached({})", tenant.current_state()),
            Self::Secondary => write!(f, "Secondary"),
            Self::InProgress(_) => write!(f, "InProgress"),
        }
    }
}

impl TenantSlot {
    /// Return the `Tenant` in this slot if attached, else None
    fn get_attached(&self) -> Option<&Arc<Tenant>> {
        match self {
            Self::Attached(t) => Some(t),
            Self::Secondary => None,
            Self::InProgress(_) => None,
        }
    }
}

/// The tenants known to the pageserver.
/// The enum variants are used to distinguish the different states that the pageserver can be in.
pub(crate) enum TenantsMap {
    /// [`init_tenant_mgr`] is not done yet.
    Initializing,
    /// [`init_tenant_mgr`] is done, all on-disk tenants have been loaded.
    /// New tenants can be added using [`tenant_map_acquire_slot`].
    Open(HashMap<TenantId, TenantSlot>),
    /// The pageserver has entered shutdown mode via [`shutdown_all_tenants`].
    /// Existing tenants are still accessible, but no new tenants can be created.
    ShuttingDown(HashMap<TenantId, TenantSlot>),
}

impl TenantsMap {
    /// Convenience function for typical usage, where we want to get a `Tenant` object, for
    /// working with attached tenants.  If the TenantId is in the map but in Secondary state,
    /// None is returned.
    pub(crate) fn get(&self, tenant_id: &TenantId) -> Option<&Arc<Tenant>> {
        match self {
            TenantsMap::Initializing => None,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => {
                m.get(tenant_id).and_then(TenantSlot::get_attached)
            }
        }
    }

    pub(crate) fn remove(&mut self, tenant_id: &TenantId) -> Option<TenantSlot> {
        match self {
            TenantsMap::Initializing => None,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m.remove(tenant_id),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            TenantsMap::Initializing => 0,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m.len(),
        }
    }
}

/// This is "safe" in that that it won't leave behind a partially deleted directory
/// at the original path, because we rename with TEMP_FILE_SUFFIX before starting deleting
/// the contents.
///
/// This is pageserver-specific, as it relies on future processes after a crash to check
/// for TEMP_FILE_SUFFIX when loading things.
async fn safe_remove_tenant_dir_all(path: impl AsRef<Utf8Path>) -> std::io::Result<()> {
    let tmp_path = safe_rename_tenant_dir(path).await?;
    fs::remove_dir_all(tmp_path).await
}

async fn safe_rename_tenant_dir(path: impl AsRef<Utf8Path>) -> std::io::Result<Utf8PathBuf> {
    let parent = path
        .as_ref()
        .parent()
        // It is invalid to call this function with a relative path.  Tenant directories
        // should always have a parent.
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Path must be absolute",
        ))?;
    let rand_suffix = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect::<String>()
        + TEMP_FILE_SUFFIX;
    let tmp_path = path_with_suffix_extension(&path, &rand_suffix);
    fs::rename(path.as_ref(), &tmp_path).await?;
    fs::File::open(parent).await?.sync_all().await?;
    Ok(tmp_path)
}

static TENANTS: Lazy<std::sync::RwLock<TenantsMap>> =
    Lazy::new(|| std::sync::RwLock::new(TenantsMap::Initializing));

/// Create a directory, including parents.  This does no fsyncs and makes
/// no guarantees about the persistence of the resulting metadata: for
/// use when creating dirs for use as cache.
async fn unsafe_create_dir_all(path: &Utf8PathBuf) -> std::io::Result<()> {
    let mut dirs_to_create = Vec::new();
    let mut path: &Utf8Path = path.as_ref();

    // Figure out which directories we need to create.
    loop {
        let meta = tokio::fs::metadata(path).await;
        match meta {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("non-directory found in path: {path}"),
                ));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        dirs_to_create.push(path);

        match path.parent() {
            Some(parent) => path = parent,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("can't find parent of path '{path}'"),
                ));
            }
        }
    }

    // Create directories from parent to child.
    for &path in dirs_to_create.iter().rev() {
        tokio::fs::create_dir(path).await?;
    }

    Ok(())
}

fn emergency_generations(
    tenant_confs: &HashMap<TenantId, anyhow::Result<LocationConf>>,
) -> HashMap<TenantId, Generation> {
    tenant_confs
        .iter()
        .filter_map(|(tid, lc)| {
            let lc = match lc {
                Ok(lc) => lc,
                Err(_) => return None,
            };
            let gen = match &lc.mode {
                LocationMode::Attached(alc) => Some(alc.generation),
                LocationMode::Secondary(_) => None,
            };

            gen.map(|g| (*tid, g))
        })
        .collect()
}

async fn init_load_generations(
    conf: &'static PageServerConf,
    tenant_confs: &HashMap<TenantId, anyhow::Result<LocationConf>>,
    resources: &TenantSharedResources,
    cancel: &CancellationToken,
) -> anyhow::Result<Option<HashMap<TenantId, Generation>>> {
    let generations = if conf.control_plane_emergency_mode {
        error!(
            "Emergency mode!  Tenants will be attached unsafely using their last known generation"
        );
        emergency_generations(tenant_confs)
    } else if let Some(client) = ControlPlaneClient::new(conf, cancel) {
        info!("Calling control plane API to re-attach tenants");
        // If we are configured to use the control plane API, then it is the source of truth for what tenants to load.
        match client.re_attach().await {
            Ok(tenants) => tenants,
            Err(RetryForeverError::ShuttingDown) => {
                anyhow::bail!("Shut down while waiting for control plane re-attach response")
            }
        }
    } else {
        info!("Control plane API not configured, tenant generations are disabled");
        return Ok(None);
    };

    // The deletion queue needs to know about the startup attachment state to decide which (if any) stored
    // deletion list entries may still be valid.  We provide that by pushing a recovery operation into
    // the queue. Sequential processing of te queue ensures that recovery is done before any new tenant deletions
    // are processed, even though we don't block on recovery completing here.
    //
    // Must only do this if remote storage is enabled, otherwise deletion queue
    // is not running and channel push will fail.
    if resources.remote_storage.is_some() {
        resources
            .deletion_queue_client
            .recover(generations.clone())?;
    }

    Ok(Some(generations))
}

/// Given a directory discovered in the pageserver's tenants/ directory, attempt
/// to load a tenant config from it.
///
/// If file is missing, return Ok(None)
fn load_tenant_config(
    conf: &'static PageServerConf,
    dentry: Utf8DirEntry,
) -> anyhow::Result<Option<(TenantId, anyhow::Result<LocationConf>)>> {
    let tenant_dir_path = dentry.path().to_path_buf();
    if crate::is_temporary(&tenant_dir_path) {
        info!("Found temporary tenant directory, removing: {tenant_dir_path}");
        // No need to use safe_remove_tenant_dir_all because this is already
        // a temporary path
        if let Err(e) = std::fs::remove_dir_all(&tenant_dir_path) {
            error!(
                "Failed to remove temporary directory '{}': {:?}",
                tenant_dir_path, e
            );
        }
        return Ok(None);
    }

    // This case happens if we crash during attachment before writing a config into the dir
    let is_empty = tenant_dir_path
        .is_empty_dir()
        .with_context(|| format!("Failed to check whether {tenant_dir_path:?} is an empty dir"))?;
    if is_empty {
        info!("removing empty tenant directory {tenant_dir_path:?}");
        if let Err(e) = std::fs::remove_dir(&tenant_dir_path) {
            error!(
                "Failed to remove empty tenant directory '{}': {e:#}",
                tenant_dir_path
            )
        }
        return Ok(None);
    }

    let tenant_ignore_mark_file = tenant_dir_path.join(IGNORED_TENANT_FILE_NAME);
    if tenant_ignore_mark_file.exists() {
        info!("Found an ignore mark file {tenant_ignore_mark_file:?}, skipping the tenant");
        return Ok(None);
    }

    let tenant_id = match tenant_dir_path
        .file_name()
        .unwrap_or_default()
        .parse::<TenantId>()
    {
        Ok(id) => id,
        Err(_) => {
            warn!("Invalid tenant path (garbage in our repo directory?): {tenant_dir_path}",);
            return Ok(None);
        }
    };

    Ok(Some((
        tenant_id,
        Tenant::load_tenant_config(conf, &tenant_id),
    )))
}

/// Initial stage of load: walk the local tenants directory, clean up any temp files,
/// and load configurations for the tenants we found.
///
/// Do this in parallel, because we expect 10k+ tenants, so serial execution can take
/// seconds even on reasonably fast drives.
async fn init_load_tenant_configs(
    conf: &'static PageServerConf,
) -> anyhow::Result<HashMap<TenantId, anyhow::Result<LocationConf>>> {
    let tenants_dir = conf.tenants_path();

    let dentries = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<Utf8DirEntry>> {
        let dir_entries = tenants_dir
            .read_dir_utf8()
            .with_context(|| format!("Failed to list tenants dir {tenants_dir:?}"))?;

        Ok(dir_entries.collect::<Result<Vec<_>, std::io::Error>>()?)
    })
    .await??;

    let mut configs = HashMap::new();

    let mut join_set = JoinSet::new();
    for dentry in dentries {
        join_set.spawn_blocking(move || load_tenant_config(conf, dentry));
    }

    while let Some(r) = join_set.join_next().await {
        if let Some((tenant_id, tenant_config)) = r?? {
            configs.insert(tenant_id, tenant_config);
        }
    }

    Ok(configs)
}

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the tenant once download is completed.
#[instrument(skip_all)]
pub async fn init_tenant_mgr(
    conf: &'static PageServerConf,
    resources: TenantSharedResources,
    init_order: InitializationOrder,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut tenants = HashMap::new();

    let ctx = RequestContext::todo_child(TaskKind::Startup, DownloadBehavior::Warn);

    // Scan local filesystem for attached tenants
    let tenant_configs = init_load_tenant_configs(conf).await?;

    // Determine which tenants are to be attached
    let tenant_generations =
        init_load_generations(conf, &tenant_configs, &resources, &cancel).await?;

    // Construct `Tenant` objects and start them running
    for (tenant_id, location_conf) in tenant_configs {
        let tenant_dir_path = conf.tenant_path(&tenant_id);

        let mut location_conf = match location_conf {
            Ok(l) => l,
            Err(e) => {
                warn!(%tenant_id, "Marking tenant broken, failed to {e:#}");

                tenants.insert(
                    tenant_id,
                    TenantSlot::Attached(Tenant::create_broken_tenant(
                        conf,
                        tenant_id,
                        format!("{}", e),
                    )),
                );
                continue;
            }
        };

        let generation = if let Some(generations) = &tenant_generations {
            // We have a generation map: treat it as the authority for whether
            // this tenant is really attached.
            if let Some(gen) = generations.get(&tenant_id) {
                *gen
            } else {
                match &location_conf.mode {
                    LocationMode::Secondary(_) => {
                        // We do not require the control plane's permission for secondary mode
                        // tenants, because they do no remote writes and hence require no
                        // generation number
                        info!(%tenant_id, "Loaded tenant in secondary mode");
                        tenants.insert(tenant_id, TenantSlot::Secondary);
                    }
                    LocationMode::Attached(_) => {
                        // TODO: augment re-attach API to enable the control plane to
                        // instruct us about secondary attachments.  That way, instead of throwing
                        // away local state, we can gracefully fall back to secondary here, if the control
                        // plane tells us so.
                        // (https://github.com/neondatabase/neon/issues/5377)
                        info!(%tenant_id, "Detaching tenant, control plane omitted it in re-attach response");
                        if let Err(e) = safe_remove_tenant_dir_all(&tenant_dir_path).await {
                            error!(%tenant_id,
                                "Failed to remove detached tenant directory '{tenant_dir_path}': {e:?}",
                            );
                        }
                    }
                };

                continue;
            }
        } else {
            // Legacy mode: no generation information, any tenant present
            // on local disk may activate
            info!(%tenant_id, "Starting tenant in legacy mode, no generation",);
            Generation::none()
        };

        // Presence of a generation number implies attachment: attach the tenant
        // if it wasn't already, and apply the generation number.
        location_conf.attach_in_generation(generation);
        Tenant::persist_tenant_config(conf, &tenant_id, &location_conf).await?;

        match tenant_spawn(
            conf,
            tenant_id,
            &tenant_dir_path,
            resources.clone(),
            AttachedTenantConf::try_from(location_conf)?,
            Some(init_order.clone()),
            &TENANTS,
            SpawnMode::Normal,
            &ctx,
        ) {
            Ok(tenant) => {
                tenants.insert(tenant.tenant_id(), TenantSlot::Attached(tenant));
            }
            Err(e) => {
                error!(%tenant_id, "Failed to start tenant: {e:#}");
            }
        }
    }

    info!("Processed {} local tenants at startup", tenants.len());

    let mut tenants_map = TENANTS.write().unwrap();
    assert!(matches!(&*tenants_map, &TenantsMap::Initializing));
    METRICS.tenant_slots.set(tenants.len() as u64);
    *tenants_map = TenantsMap::Open(tenants);
    Ok(())
}

/// Wrapper for Tenant::spawn that checks invariants before running, and inserts
/// a broken tenant in the map if Tenant::spawn fails.
#[allow(clippy::too_many_arguments)]
pub(crate) fn tenant_spawn(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    tenant_path: &Utf8Path,
    resources: TenantSharedResources,
    location_conf: AttachedTenantConf,
    init_order: Option<InitializationOrder>,
    tenants: &'static std::sync::RwLock<TenantsMap>,
    mode: SpawnMode,
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

    let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(&tenant_id);
    anyhow::ensure!(
        !conf.tenant_ignore_mark_file_path(&tenant_id).exists(),
        "Cannot load tenant, ignore mark found at {tenant_ignore_mark:?}"
    );

    info!("Attaching tenant {tenant_id}");
    let tenant = match Tenant::spawn(
        conf,
        tenant_id,
        resources,
        location_conf,
        init_order,
        tenants,
        mode,
        ctx,
    ) {
        Ok(tenant) => tenant,
        Err(e) => {
            error!("Failed to spawn tenant {tenant_id}, reason: {e:#}");
            Tenant::create_broken_tenant(conf, tenant_id, format!("{e:#}"))
        }
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
#[instrument(skip_all)]
pub(crate) async fn shutdown_all_tenants() {
    shutdown_all_tenants0(&TENANTS).await
}

async fn shutdown_all_tenants0(tenants: &std::sync::RwLock<TenantsMap>) {
    use utils::completion;

    // Atomically, 1. extract the list of tenants to shut down and 2. prevent creation of new tenants.
    let (in_progress_ops, tenants_to_shut_down) = {
        let mut m = tenants.write().unwrap();
        match &mut *m {
            TenantsMap::Initializing => {
                *m = TenantsMap::ShuttingDown(HashMap::default());
                info!("tenants map is empty");
                return;
            }
            TenantsMap::Open(tenants) => {
                let mut shutdown_state = HashMap::new();
                let mut in_progress_ops = Vec::new();
                let mut tenants_to_shut_down = Vec::new();

                for (k, v) in tenants.drain() {
                    match v {
                        TenantSlot::Attached(t) => {
                            tenants_to_shut_down.push(t.clone());
                            shutdown_state.insert(k, TenantSlot::Attached(t));
                        }
                        TenantSlot::Secondary => {
                            shutdown_state.insert(k, TenantSlot::Secondary);
                        }
                        TenantSlot::InProgress(notify) => {
                            // InProgress tenants are not visible in TenantsMap::ShuttingDown: we will
                            // wait for their notifications to fire in this function.
                            in_progress_ops.push(notify);
                        }
                    }
                }
                *m = TenantsMap::ShuttingDown(shutdown_state);
                (in_progress_ops, tenants_to_shut_down)
            }
            TenantsMap::ShuttingDown(_) => {
                // TODO: it is possible that detach and shutdown happen at the same time. as a
                // result, during shutdown we do not wait for detach.
                error!("already shutting down, this function isn't supposed to be called more than once");
                return;
            }
        }
    };

    info!(
        "Waiting for {} InProgress tenants and {} Attached tenants to shut down",
        in_progress_ops.len(),
        tenants_to_shut_down.len()
    );

    for barrier in in_progress_ops {
        barrier.wait().await;
    }

    info!(
        "InProgress tenants shut down, waiting for {} Attached tenants to shut down",
        tenants_to_shut_down.len()
    );
    let started_at = std::time::Instant::now();
    let mut join_set = JoinSet::new();
    for tenant in tenants_to_shut_down {
        let tenant_id = tenant.get_tenant_id();
        join_set.spawn(
            async move {
                let freeze_and_flush = true;

                let res = {
                    let (_guard, shutdown_progress) = completion::channel();
                    tenant.shutdown(shutdown_progress, freeze_and_flush).await
                };

                if let Err(other_progress) = res {
                    // join the another shutdown in progress
                    other_progress.wait().await;
                }

                // we cannot afford per tenant logging here, because if s3 is degraded, we are
                // going to log too many lines

                debug!("tenant successfully stopped");
            }
            .instrument(info_span!("shutdown", %tenant_id)),
        );
    }

    let total = join_set.len();
    let mut panicked = 0;
    let mut buffering = true;
    const BUFFER_FOR: std::time::Duration = std::time::Duration::from_millis(500);
    let mut buffered = std::pin::pin!(tokio::time::sleep(BUFFER_FOR));

    while !join_set.is_empty() {
        tokio::select! {
            Some(joined) = join_set.join_next() => {
                match joined {
                    Ok(()) => {}
                    Err(join_error) if join_error.is_cancelled() => {
                        unreachable!("we are not cancelling any of the futures");
                    }
                    Err(join_error) if join_error.is_panic() => {
                        // cannot really do anything, as this panic is likely a bug
                        panicked += 1;
                    }
                    Err(join_error) => {
                        warn!("unknown kind of JoinError: {join_error}");
                    }
                }
                if !buffering {
                    // buffer so that every 500ms since the first update (or starting) we'll log
                    // how far away we are; this is because we will get SIGKILL'd at 10s, and we
                    // are not able to log *then*.
                    buffering = true;
                    buffered.as_mut().reset(tokio::time::Instant::now() + BUFFER_FOR);
                }
            },
            _ = &mut buffered, if buffering => {
                buffering = false;
                info!(remaining = join_set.len(), total, elapsed_ms = started_at.elapsed().as_millis(), "waiting for tenants to shutdown");
            }
        }
    }

    if panicked > 0 {
        warn!(
            panicked,
            total, "observed panicks while shutting down tenants"
        );
    }

    // caller will log how long we took
}

pub(crate) async fn create_tenant(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    generation: Generation,
    resources: TenantSharedResources,
    ctx: &RequestContext,
) -> Result<Arc<Tenant>, TenantMapInsertError> {
    let location_conf = LocationConf::attached_single(tenant_conf, generation);

    let slot_guard = tenant_map_acquire_slot(&tenant_id, TenantSlotAcquireMode::MustNotExist)?;
    let tenant_path = super::create_tenant_files(conf, &location_conf, &tenant_id).await?;

    let created_tenant = tenant_spawn(
        conf,
        tenant_id,
        &tenant_path,
        resources,
        AttachedTenantConf::try_from(location_conf)?,
        None,
        &TENANTS,
        SpawnMode::Create,
        ctx,
    )?;
    // TODO: tenant object & its background loops remain, untracked in tenant map, if we fail here.
    //      See https://github.com/neondatabase/neon/issues/4233

    let created_tenant_id = created_tenant.tenant_id();
    if tenant_id != created_tenant_id {
        return Err(TenantMapInsertError::Other(anyhow::anyhow!(
            "loaded created tenant has unexpected tenant id (expect {tenant_id} != actual {created_tenant_id})",
        )));
    }

    slot_guard.upsert(TenantSlot::Attached(created_tenant.clone()))?;

    Ok(created_tenant)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SetNewTenantConfigError {
    #[error(transparent)]
    GetTenant(#[from] GetTenantError),
    #[error(transparent)]
    Persist(anyhow::Error),
}

pub(crate) async fn set_new_tenant_config(
    conf: &'static PageServerConf,
    new_tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
) -> Result<(), SetNewTenantConfigError> {
    info!("configuring tenant {tenant_id}");
    let tenant = get_tenant(tenant_id, true)?;

    // This is a legacy API that only operates on attached tenants: the preferred
    // API to use is the location_config/ endpoint, which lets the caller provide
    // the full LocationConf.
    let location_conf = LocationConf::attached_single(new_tenant_conf, tenant.generation);

    Tenant::persist_tenant_config(conf, &tenant_id, &location_conf)
        .await
        .map_err(SetNewTenantConfigError::Persist)?;
    tenant.set_new_tenant_config(new_tenant_conf);
    Ok(())
}

#[instrument(skip_all, fields(%tenant_id))]
pub(crate) async fn upsert_location(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    new_location_config: LocationConf,
    broker_client: storage_broker::BrokerClientChannel,
    remote_storage: Option<GenericRemoteStorage>,
    deletion_queue_client: DeletionQueueClient,
    ctx: &RequestContext,
) -> Result<(), anyhow::Error> {
    info!("configuring tenant location {tenant_id} to state {new_location_config:?}");

    // Special case fast-path for updates to Tenant: if our upsert is only updating configuration,
    // then we do not need to set the slot to InProgress, we can just call into the
    // existng tenant.
    {
        let locked = TENANTS.read().unwrap();
        let peek_slot = tenant_map_peek_slot(&locked, &tenant_id, TenantSlotPeekMode::Write)?;
        match (&new_location_config.mode, peek_slot) {
            (LocationMode::Attached(attach_conf), Some(TenantSlot::Attached(tenant))) => {
                if attach_conf.generation == tenant.generation {
                    // A transition from Attached to Attached in the same generation, we may
                    // take our fast path and just provide the updated configuration
                    // to the tenant.
                    tenant.set_new_location_config(AttachedTenantConf::try_from(
                        new_location_config,
                    )?);

                    // Persist the new config in the background, to avoid holding up any
                    // locks while we do so.
                    // TODO

                    return Ok(());
                } else {
                    // Different generations, fall through to general case
                }
            }
            _ => {
                // Not an Attached->Attached transition, fall through to general case
            }
        }
    }

    // General case for upserts to TenantsMap, excluding the case above: we will substitute an
    // InProgress value to the slot while we make whatever changes are required.  The state for
    // the tenant is inaccessible to the outside world while we are doing this, but that is sensible:
    // the state is ill-defined while we're in transition.  Transitions are async, but fast: we do
    // not do significant I/O, and shutdowns should be prompt via cancellation tokens.
    let mut slot_guard = tenant_map_acquire_slot(&tenant_id, TenantSlotAcquireMode::Any)?;

    if let Some(TenantSlot::Attached(tenant)) = slot_guard.get_old_value() {
        // The case where we keep a Tenant alive was covered above in the special case
        // for Attached->Attached transitions in the same generation.  By this point,
        // if we see an attached tenant we know it will be discarded and should be
        // shut down.
        let (_guard, progress) = utils::completion::channel();

        match tenant.get_attach_mode() {
            AttachmentMode::Single | AttachmentMode::Multi => {
                // Before we leave our state as the presumed holder of the latest generation,
                // flush any outstanding deletions to reduce the risk of leaking objects.
                deletion_queue_client.flush_advisory()
            }
            AttachmentMode::Stale => {
                // If we're stale there's not point trying to flush deletions
            }
        };

        info!("Shutting down attached tenant");
        match tenant.shutdown(progress, false).await {
            Ok(()) => {}
            Err(barrier) => {
                info!("Shutdown already in progress, waiting for it to complete");
                barrier.wait().await;
            }
        }
        slot_guard.drop_old_value().expect("We just shut it down");
    }

    let tenant_path = conf.tenant_path(&tenant_id);

    let new_slot = match &new_location_config.mode {
        LocationMode::Secondary(_) => {
            let tenant_path = conf.tenant_path(&tenant_id);
            // Directory doesn't need to be fsync'd because if we crash it can
            // safely be recreated next time this tenant location is configured.
            unsafe_create_dir_all(&tenant_path)
                .await
                .with_context(|| format!("Creating {tenant_path}"))?;

            Tenant::persist_tenant_config(conf, &tenant_id, &new_location_config)
                .await
                .map_err(SetNewTenantConfigError::Persist)?;

            TenantSlot::Secondary
        }
        LocationMode::Attached(_attach_config) => {
            let timelines_path = conf.timelines_path(&tenant_id);

            // Directory doesn't need to be fsync'd because we do not depend on
            // it to exist after crashes: it may be recreated when tenant is
            // re-attached, see https://github.com/neondatabase/neon/issues/5550
            unsafe_create_dir_all(&timelines_path)
                .await
                .with_context(|| format!("Creating {timelines_path}"))?;

            Tenant::persist_tenant_config(conf, &tenant_id, &new_location_config)
                .await
                .map_err(SetNewTenantConfigError::Persist)?;

            let tenant = tenant_spawn(
                conf,
                tenant_id,
                &tenant_path,
                TenantSharedResources {
                    broker_client,
                    remote_storage,
                    deletion_queue_client,
                },
                AttachedTenantConf::try_from(new_location_config)?,
                None,
                &TENANTS,
                SpawnMode::Normal,
                ctx,
            )?;

            TenantSlot::Attached(tenant)
        }
    };

    slot_guard.upsert(new_slot)?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GetTenantError {
    #[error("Tenant {0} not found")]
    NotFound(TenantId),
    #[error("Tenant {0} is not active")]
    NotActive(TenantId),
    /// Broken is logically a subset of NotActive, but a distinct error is useful as
    /// NotActive is usually a retryable state for API purposes, whereas Broken
    /// is a stuck error state
    #[error("Tenant is broken: {0}")]
    Broken(String),

    // Initializing or shutting down: cannot authoritatively say whether we have this tenant
    #[error("Tenant map is not available: {0}")]
    MapState(#[from] TenantMapError),
}

/// Gets the tenant from the in-memory data, erroring if it's absent or is not fitting to the query.
/// `active_only = true` allows to query only tenants that are ready for operations, erroring on other kinds of tenants.
///
/// This method is cancel-safe.
pub(crate) fn get_tenant(
    tenant_id: TenantId,
    active_only: bool,
) -> Result<Arc<Tenant>, GetTenantError> {
    let locked = TENANTS.read().unwrap();
    let peek_slot = tenant_map_peek_slot(&locked, &tenant_id, TenantSlotPeekMode::Read)?;

    match peek_slot {
        Some(TenantSlot::Attached(tenant)) => match tenant.current_state() {
            TenantState::Broken {
                reason,
                backtrace: _,
            } if active_only => Err(GetTenantError::Broken(reason)),
            TenantState::Active => Ok(Arc::clone(tenant)),
            _ => {
                if active_only {
                    Err(GetTenantError::NotActive(tenant_id))
                } else {
                    Ok(Arc::clone(tenant))
                }
            }
        },
        Some(TenantSlot::InProgress(_)) => Err(GetTenantError::NotActive(tenant_id)),
        None | Some(TenantSlot::Secondary) => Err(GetTenantError::NotFound(tenant_id)),
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum GetActiveTenantError {
    /// We may time out either while TenantSlot is InProgress, or while the Tenant
    /// is in a non-Active state
    #[error(
        "Timed out waiting {wait_time:?} for tenant active state. Latest state: {latest_state:?}"
    )]
    WaitForActiveTimeout {
        latest_state: Option<TenantState>,
        wait_time: Duration,
    },

    /// The TenantSlot is absent, or in secondary mode
    #[error(transparent)]
    NotFound(#[from] GetTenantError),

    /// Cancellation token fired while we were waiting
    #[error("cancelled")]
    Cancelled,

    /// Tenant exists, but is in a state that cannot become active (e.g. Stopping, Broken)
    #[error("will not become active.  Current state: {0}")]
    WillNotBecomeActive(TenantState),
}

/// Get a [`Tenant`] in its active state. If the tenant_id is currently in [`TenantSlot::InProgress`]
/// state, then wait for up to `timeout`.  If the [`Tenant`] is not currently in [`TenantState::Active`],
/// then wait for up to `timeout` (minus however long we waited for the slot).
pub(crate) async fn get_active_tenant_with_timeout(
    tenant_id: TenantId,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<Arc<Tenant>, GetActiveTenantError> {
    enum WaitFor {
        Barrier(utils::completion::Barrier),
        Tenant(Arc<Tenant>),
    }

    let wait_start = Instant::now();
    let deadline = wait_start + timeout;

    let wait_for = {
        let locked = TENANTS.read().unwrap();
        let peek_slot = tenant_map_peek_slot(&locked, &tenant_id, TenantSlotPeekMode::Read)
            .map_err(GetTenantError::MapState)?;
        match peek_slot {
            Some(TenantSlot::Attached(tenant)) => {
                match tenant.current_state() {
                    TenantState::Active => {
                        // Fast path: we don't need to do any async waiting.
                        return Ok(tenant.clone());
                    }
                    _ => WaitFor::Tenant(tenant.clone()),
                }
            }
            Some(TenantSlot::Secondary) => {
                return Err(GetActiveTenantError::NotFound(GetTenantError::NotActive(
                    tenant_id,
                )))
            }
            Some(TenantSlot::InProgress(barrier)) => WaitFor::Barrier(barrier.clone()),
            None => {
                return Err(GetActiveTenantError::NotFound(GetTenantError::NotFound(
                    tenant_id,
                )))
            }
        }
    };

    let tenant = match wait_for {
        WaitFor::Barrier(barrier) => {
            tracing::debug!("Waiting for tenant InProgress state to pass...");
            timeout_cancellable(
                deadline.duration_since(Instant::now()),
                cancel,
                barrier.wait(),
            )
            .await
            .map_err(|e| match e {
                TimeoutCancellableError::Timeout => GetActiveTenantError::WaitForActiveTimeout {
                    latest_state: None,
                    wait_time: wait_start.elapsed(),
                },
                TimeoutCancellableError::Cancelled => GetActiveTenantError::Cancelled,
            })?;
            {
                let locked = TENANTS.read().unwrap();
                let peek_slot = tenant_map_peek_slot(&locked, &tenant_id, TenantSlotPeekMode::Read)
                    .map_err(GetTenantError::MapState)?;
                match peek_slot {
                    Some(TenantSlot::Attached(tenant)) => tenant.clone(),
                    _ => {
                        return Err(GetActiveTenantError::NotFound(GetTenantError::NotActive(
                            tenant_id,
                        )))
                    }
                }
            }
        }
        WaitFor::Tenant(tenant) => tenant,
    };

    tracing::debug!("Waiting for tenant to enter active state...");
    match timeout_cancellable(
        deadline.duration_since(Instant::now()),
        cancel,
        tenant.wait_to_become_active(),
    )
    .await
    {
        Ok(Ok(())) => Ok(tenant),
        Ok(Err(e)) => Err(e),
        Err(TimeoutCancellableError::Timeout) => {
            let latest_state = tenant.current_state();
            if latest_state == TenantState::Active {
                Ok(tenant)
            } else {
                Err(GetActiveTenantError::WaitForActiveTimeout {
                    latest_state: Some(latest_state),
                    wait_time: timeout,
                })
            }
        }
        Err(TimeoutCancellableError::Cancelled) => Err(GetActiveTenantError::Cancelled),
    }
}

pub(crate) async fn delete_tenant(
    conf: &'static PageServerConf,
    remote_storage: Option<GenericRemoteStorage>,
    tenant_id: TenantId,
) -> Result<(), DeleteTenantError> {
    // We acquire a SlotGuard during this function to protect against concurrent
    // changes while the ::prepare phase of DeleteTenantFlow executes, but then
    // have to return the Tenant to the map while the background deletion runs.
    //
    // TODO: refactor deletion to happen outside the lifetime of a Tenant.
    // Currently, deletion requires a reference to the tenants map in order to
    // keep the Tenant in the map until deletion is complete, and then remove
    // it at the end.
    //
    // See https://github.com/neondatabase/neon/issues/5080

    let mut slot_guard = tenant_map_acquire_slot(&tenant_id, TenantSlotAcquireMode::MustExist)?;

    // unwrap is safe because we used MustExist mode when acquiring
    let tenant = match slot_guard.get_old_value().as_ref().unwrap() {
        TenantSlot::Attached(tenant) => tenant.clone(),
        _ => {
            // Express "not attached" as equivalent to "not found"
            return Err(DeleteTenantError::NotAttached);
        }
    };

    let result = DeleteTenantFlow::run(conf, remote_storage, &TENANTS, tenant).await;

    // The Tenant goes back into the map in Stopping state, it will eventually be removed by DeleteTenantFLow
    slot_guard.revert();
    result
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeleteTimelineError {
    #[error("Tenant {0}")]
    Tenant(#[from] GetTenantError),

    #[error("Timeline {0}")]
    Timeline(#[from] crate::tenant::DeleteTimelineError),
}

pub(crate) async fn delete_timeline(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    _ctx: &RequestContext,
) -> Result<(), DeleteTimelineError> {
    let tenant = get_tenant(tenant_id, true)?;
    DeleteTimelineFlow::run(&tenant, timeline_id, false).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TenantStateError {
    #[error("Tenant {0} is stopping")]
    IsStopping(TenantId),
    #[error(transparent)]
    SlotError(#[from] TenantSlotError),
    #[error(transparent)]
    SlotUpsertError(#[from] TenantSlotUpsertError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub(crate) async fn detach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    detach_ignored: bool,
    deletion_queue_client: &DeletionQueueClient,
) -> Result<(), TenantStateError> {
    let tmp_path = detach_tenant0(
        conf,
        &TENANTS,
        tenant_id,
        detach_ignored,
        deletion_queue_client,
    )
    .await?;
    // Although we are cleaning up the tenant, this task is not meant to be bound by the lifetime of the tenant in memory.
    // After a tenant is detached, there are no more task_mgr tasks for that tenant_id.
    let task_tenant_id = None;
    task_mgr::spawn(
        task_mgr::BACKGROUND_RUNTIME.handle(),
        TaskKind::MgmtRequest,
        task_tenant_id,
        None,
        "tenant_files_delete",
        false,
        async move {
            fs::remove_dir_all(tmp_path.as_path())
                .await
                .with_context(|| format!("tenant directory {:?} deletion", tmp_path))
        },
    );
    Ok(())
}

async fn detach_tenant0(
    conf: &'static PageServerConf,
    tenants: &std::sync::RwLock<TenantsMap>,
    tenant_id: TenantId,
    detach_ignored: bool,
    deletion_queue_client: &DeletionQueueClient,
) -> Result<Utf8PathBuf, TenantStateError> {
    let tenant_dir_rename_operation = |tenant_id_to_clean| async move {
        let local_tenant_directory = conf.tenant_path(&tenant_id_to_clean);
        safe_rename_tenant_dir(&local_tenant_directory)
            .await
            .with_context(|| format!("local tenant directory {local_tenant_directory:?} rename"))
    };

    let removal_result =
        remove_tenant_from_memory(tenants, tenant_id, tenant_dir_rename_operation(tenant_id)).await;

    // Flush pending deletions, so that they have a good chance of passing validation
    // before this tenant is potentially re-attached elsewhere.
    deletion_queue_client.flush_advisory();

    // Ignored tenants are not present in memory and will bail the removal from memory operation.
    // Before returning the error, check for ignored tenant removal case — we only need to clean its local files then.
    if detach_ignored
        && matches!(
            removal_result,
            Err(TenantStateError::SlotError(TenantSlotError::NotFound(_)))
        )
    {
        let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(&tenant_id);
        if tenant_ignore_mark.exists() {
            info!("Detaching an ignored tenant");
            let tmp_path = tenant_dir_rename_operation(tenant_id)
                .await
                .with_context(|| format!("Ignored tenant {tenant_id} local directory rename"))?;
            return Ok(tmp_path);
        }
    }

    removal_result
}

pub(crate) async fn load_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    generation: Generation,
    broker_client: storage_broker::BrokerClientChannel,
    remote_storage: Option<GenericRemoteStorage>,
    deletion_queue_client: DeletionQueueClient,
    ctx: &RequestContext,
) -> Result<(), TenantMapInsertError> {
    let slot_guard = tenant_map_acquire_slot(&tenant_id, TenantSlotAcquireMode::MustNotExist)?;
    let tenant_path = conf.tenant_path(&tenant_id);

    let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(&tenant_id);
    if tenant_ignore_mark.exists() {
        std::fs::remove_file(&tenant_ignore_mark).with_context(|| {
            format!(
                "Failed to remove tenant ignore mark {tenant_ignore_mark:?} during tenant loading"
            )
        })?;
    }

    let resources = TenantSharedResources {
        broker_client,
        remote_storage,
        deletion_queue_client,
    };

    let mut location_conf =
        Tenant::load_tenant_config(conf, &tenant_id).map_err(TenantMapInsertError::Other)?;
    location_conf.attach_in_generation(generation);

    Tenant::persist_tenant_config(conf, &tenant_id, &location_conf).await?;

    let new_tenant = tenant_spawn(
        conf,
        tenant_id,
        &tenant_path,
        resources,
        AttachedTenantConf::try_from(location_conf)?,
        None,
        &TENANTS,
        SpawnMode::Normal,
        ctx,
    )
    .with_context(|| format!("Failed to schedule tenant processing in path {tenant_path:?}"))?;

    slot_guard.upsert(TenantSlot::Attached(new_tenant))?;
    Ok(())
}

pub(crate) async fn ignore_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
) -> Result<(), TenantStateError> {
    ignore_tenant0(conf, &TENANTS, tenant_id).await
}

async fn ignore_tenant0(
    conf: &'static PageServerConf,
    tenants: &std::sync::RwLock<TenantsMap>,
    tenant_id: TenantId,
) -> Result<(), TenantStateError> {
    remove_tenant_from_memory(tenants, tenant_id, async {
        let ignore_mark_file = conf.tenant_ignore_mark_file_path(&tenant_id);
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
pub(crate) enum TenantMapListError {
    #[error("tenant map is still initiailizing")]
    Initializing,
}

///
/// Get list of tenants, for the mgmt API
///
pub(crate) async fn list_tenants() -> Result<Vec<(TenantId, TenantState)>, TenantMapListError> {
    let tenants = TENANTS.read().unwrap();
    let m = match &*tenants {
        TenantsMap::Initializing => return Err(TenantMapListError::Initializing),
        TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m,
    };
    Ok(m.iter()
        .filter_map(|(id, tenant)| match tenant {
            TenantSlot::Attached(tenant) => Some((*id, tenant.current_state())),
            TenantSlot::Secondary => None,
            TenantSlot::InProgress(_) => None,
        })
        .collect())
}

/// Execute Attach mgmt API command.
///
/// Downloading all the tenant data is performed in the background, this merely
/// spawns the background task and returns quickly.
pub(crate) async fn attach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    generation: Generation,
    tenant_conf: TenantConfOpt,
    resources: TenantSharedResources,
    ctx: &RequestContext,
) -> Result<(), TenantMapInsertError> {
    let slot_guard = tenant_map_acquire_slot(&tenant_id, TenantSlotAcquireMode::MustNotExist)?;
    let location_conf = LocationConf::attached_single(tenant_conf, generation);
    let tenant_dir = create_tenant_files(conf, &location_conf, &tenant_id).await?;
    // TODO: tenant directory remains on disk if we bail out from here on.
    //       See https://github.com/neondatabase/neon/issues/4233

    let attached_tenant = tenant_spawn(
        conf,
        tenant_id,
        &tenant_dir,
        resources,
        AttachedTenantConf::try_from(location_conf)?,
        None,
        &TENANTS,
        SpawnMode::Normal,
        ctx,
    )?;
    // TODO: tenant object & its background loops remain, untracked in tenant map, if we fail here.
    //      See https://github.com/neondatabase/neon/issues/4233

    let attached_tenant_id = attached_tenant.tenant_id();
    if tenant_id != attached_tenant_id {
        return Err(TenantMapInsertError::Other(anyhow::anyhow!(
            "loaded created tenant has unexpected tenant id (expect {tenant_id} != actual {attached_tenant_id})",
        )));
    }

    slot_guard.upsert(TenantSlot::Attached(attached_tenant))?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TenantMapInsertError {
    #[error(transparent)]
    SlotError(#[from] TenantSlotError),
    #[error(transparent)]
    SlotUpsertError(#[from] TenantSlotUpsertError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Superset of TenantMapError: issues that can occur when acquiring a slot
/// for a particular tenant ID.
#[derive(Debug, thiserror::Error)]
pub enum TenantSlotError {
    /// When acquiring a slot with the expectation that the tenant already exists.
    #[error("Tenant {0} not found")]
    NotFound(TenantId),

    /// When acquiring a slot with the expectation that the tenant does not already exist.
    #[error("tenant {0} already exists, state: {1:?}")]
    AlreadyExists(TenantId, TenantState),

    #[error("tenant {0} already exists in but is not attached")]
    Conflict(TenantId),

    // Tried to read a slot that is currently being mutated by another administrative
    // operation.
    #[error("tenant has a state change in progress, try again later")]
    InProgress,

    #[error(transparent)]
    MapState(#[from] TenantMapError),
}

/// Superset of TenantMapError: issues that can occur when using a SlotGuard
/// to insert a new value.
#[derive(Debug, thiserror::Error)]
pub enum TenantSlotUpsertError {
    /// An error where the slot is in an unexpected state, indicating a code bug
    #[error("Internal error updating Tenant")]
    InternalError(Cow<'static, str>),

    #[error(transparent)]
    MapState(#[from] TenantMapError),
}

#[derive(Debug)]
enum TenantSlotDropError {
    /// It is only legal to drop a TenantSlot if its contents are fully shut down
    NotShutdown,
}

/// Errors that can happen any time we are walking the tenant map to try and acquire
/// the TenantSlot for a particular tenant.
#[derive(Debug, thiserror::Error)]
pub enum TenantMapError {
    // Tried to read while initializing
    #[error("tenant map is still initializing")]
    StillInitializing,

    // Tried to read while shutting down
    #[error("tenant map is shutting down")]
    ShuttingDown,
}

/// Guards a particular tenant_id's content in the TenantsMap.  While this
/// structure exists, the TenantsMap will contain a [`TenantSlot::InProgress`]
/// for this tenant, which acts as a marker for any operations targeting
/// this tenant to retry later, or wait for the InProgress state to end.
///
/// This structure enforces the important invariant that we do not have overlapping
/// tasks that will try use local storage for a the same tenant ID: we enforce that
/// the previous contents of a slot have been shut down before the slot can be
/// left empty or used for something else
///
/// Holders of a SlotGuard should explicitly dispose of it, using either `upsert`
/// to provide a new value, or `revert` to put the slot back into its initial
/// state.  If the SlotGuard is dropped without calling either of these, then
/// we will leave the slot empty if our `old_value` is already shut down, else
/// we will replace the slot with `old_value` (equivalent to doing a revert).
///
/// The `old_value` may be dropped before the SlotGuard is dropped, by calling
/// `drop_old_value`.  It is an error to call this without shutting down
/// the conents of `old_value`.
pub struct SlotGuard {
    tenant_id: TenantId,
    old_value: Option<TenantSlot>,
    upserted: bool,

    /// [`TenantSlot::InProgress`] carries the corresponding Barrier: it will
    /// release any waiters as soon as this SlotGuard is dropped.
    _completion: utils::completion::Completion,
}

unsafe impl Send for SlotGuard {}
unsafe impl Sync for SlotGuard {}

impl SlotGuard {
    fn new(
        tenant_id: TenantId,
        old_value: Option<TenantSlot>,
        completion: utils::completion::Completion,
    ) -> Self {
        Self {
            tenant_id,
            old_value,
            upserted: false,
            _completion: completion,
        }
    }

    /// Take any value that was present in the slot before we acquired ownership
    /// of it: in state transitions, this will be the old state.
    fn get_old_value(&mut self) -> &Option<TenantSlot> {
        &self.old_value
    }

    /// Emplace a new value in the slot.  This consumes the guard, and after
    /// returning, the slot is no longer protected from concurrent changes.
    fn upsert(mut self, new_value: TenantSlot) -> Result<(), TenantSlotUpsertError> {
        if !self.old_value_is_shutdown() {
            // This is a bug: callers should never try to drop an old value without
            // shutting it down
            return Err(TenantSlotUpsertError::InternalError(
                "Old TenantSlot value not shut down".into(),
            ));
        }

        let replaced = {
            let mut locked = TENANTS.write().unwrap();

            if let TenantSlot::InProgress(_) = new_value {
                // It is never expected to try and upsert InProgress via this path: it should
                // only be written via the tenant_map_acquire_slot path.  If we hit this it's a bug.
                return Err(TenantSlotUpsertError::InternalError(
                    "Attempt to upsert an InProgress state".into(),
                ));
            }

            let m = match &mut *locked {
                TenantsMap::Initializing => return Err(TenantMapError::StillInitializing.into()),
                TenantsMap::ShuttingDown(_) => {
                    return Err(TenantMapError::ShuttingDown.into());
                }
                TenantsMap::Open(m) => m,
            };

            let replaced = m.insert(self.tenant_id, new_value);
            self.upserted = true;

            METRICS.tenant_slots.set(m.len() as u64);

            replaced
        };

        // Sanity check: on an upsert we should always be replacing an InProgress marker
        match replaced {
            Some(TenantSlot::InProgress(_)) => {
                // Expected case: we find our InProgress in the map: nothing should have
                // replaced it because the code that acquires slots will not grant another
                // one for the same TenantId.
                Ok(())
            }
            None => {
                METRICS.unexpected_errors.inc();
                error!(
                    tenant_id = %self.tenant_id,
                    "Missing InProgress marker during tenant upsert, this is a bug."
                );
                Err(TenantSlotUpsertError::InternalError(
                    "Missing InProgress marker during tenant upsert".into(),
                ))
            }
            Some(slot) => {
                METRICS.unexpected_errors.inc();
                error!(tenant_id=%self.tenant_id, "Unexpected contents of TenantSlot during upsert, this is a bug.  Contents: {:?}", slot);
                Err(TenantSlotUpsertError::InternalError(
                    "Unexpected contents of TenantSlot".into(),
                ))
            }
        }
    }

    /// Replace the InProgress slot with whatever was in the guard when we started
    fn revert(mut self) {
        if let Some(value) = self.old_value.take() {
            match self.upsert(value) {
                Err(TenantSlotUpsertError::InternalError(_)) => {
                    // We already logged the error, nothing else we can do.
                }
                Err(TenantSlotUpsertError::MapState(_)) => {
                    // If the map is shutting down, we need not replace anything
                }
                Ok(()) => {}
            }
        }
    }

    /// We may never drop our old value until it is cleanly shut down: otherwise we might leave
    /// rogue background tasks that would write to the local tenant directory that this guard
    /// is responsible for protecting
    fn old_value_is_shutdown(&self) -> bool {
        match self.old_value.as_ref() {
            Some(TenantSlot::Attached(tenant)) => {
                // TODO: PR #5711 will add a gate that enables properly checking that
                // shutdown completed.
                matches!(
                    tenant.current_state(),
                    TenantState::Stopping { .. } | TenantState::Broken { .. }
                )
            }
            Some(TenantSlot::Secondary) => {
                // TODO: when adding secondary mode tenants, this will check for shutdown
                // in the same way that we do for `Tenant` above
                true
            }
            Some(TenantSlot::InProgress(_)) => {
                // A SlotGuard cannot be constructed for a slot that was already InProgress
                unreachable!()
            }
            None => true,
        }
    }

    /// The guard holder is done with the old value of the slot: they are obliged to already
    /// shut it down before we reach this point.
    fn drop_old_value(&mut self) -> Result<(), TenantSlotDropError> {
        if !self.old_value_is_shutdown() {
            Err(TenantSlotDropError::NotShutdown)
        } else {
            self.old_value.take();
            Ok(())
        }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if self.upserted {
            return;
        }
        // Our old value is already shutdown, or it never existed: it is safe
        // for us to fully release the TenantSlot back into an empty state

        let mut locked = TENANTS.write().unwrap();

        let m = match &mut *locked {
            TenantsMap::Initializing => {
                // There is no map, this should never happen.
                return;
            }
            TenantsMap::ShuttingDown(_) => {
                // When we transition to shutdown, InProgress elements are removed
                // from the map, so we do not need to clean up our Inprogress marker.
                // See [`shutdown_all_tenants0`]
                return;
            }
            TenantsMap::Open(m) => m,
        };

        use std::collections::hash_map::Entry;
        match m.entry(self.tenant_id) {
            Entry::Occupied(mut entry) => {
                if !matches!(entry.get(), TenantSlot::InProgress(_)) {
                    METRICS.unexpected_errors.inc();
                    error!(tenant_id=%self.tenant_id, "Unexpected contents of TenantSlot during drop, this is a bug.  Contents: {:?}", entry.get());
                }

                if self.old_value_is_shutdown() {
                    entry.remove();
                } else {
                    entry.insert(self.old_value.take().unwrap());
                }
            }
            Entry::Vacant(_) => {
                METRICS.unexpected_errors.inc();
                error!(
                    tenant_id = %self.tenant_id,
                    "Missing InProgress marker during SlotGuard drop, this is a bug."
                );
            }
        }

        METRICS.tenant_slots.set(m.len() as u64);
    }
}

enum TenantSlotPeekMode {
    /// In Read mode, peek will be permitted to see the slots even if the pageserver is shutting down
    Read,
    /// In Write mode, trying to peek at a slot while the pageserver is shutting down is an error
    Write,
}

fn tenant_map_peek_slot<'a>(
    tenants: &'a std::sync::RwLockReadGuard<'a, TenantsMap>,
    tenant_id: &TenantId,
    mode: TenantSlotPeekMode,
) -> Result<Option<&'a TenantSlot>, TenantMapError> {
    let m = match tenants.deref() {
        TenantsMap::Initializing => return Err(TenantMapError::StillInitializing),
        TenantsMap::ShuttingDown(m) => match mode {
            TenantSlotPeekMode::Read => m,
            TenantSlotPeekMode::Write => {
                return Err(TenantMapError::ShuttingDown);
            }
        },
        TenantsMap::Open(m) => m,
    };

    Ok(m.get(tenant_id))
}

enum TenantSlotAcquireMode {
    /// Acquire the slot irrespective of current state, or whether it already exists
    Any,
    /// Return an error if trying to acquire a slot and it doesn't already exist
    MustExist,
    /// Return an error if trying to acquire a slot and it already exists
    MustNotExist,
}

fn tenant_map_acquire_slot(
    tenant_id: &TenantId,
    mode: TenantSlotAcquireMode,
) -> Result<SlotGuard, TenantSlotError> {
    tenant_map_acquire_slot_impl(tenant_id, &TENANTS, mode)
}

fn tenant_map_acquire_slot_impl(
    tenant_id: &TenantId,
    tenants: &std::sync::RwLock<TenantsMap>,
    mode: TenantSlotAcquireMode,
) -> Result<SlotGuard, TenantSlotError> {
    use TenantSlotAcquireMode::*;
    METRICS.tenant_slot_writes.inc();

    let mut locked = tenants.write().unwrap();
    let span = tracing::info_span!("acquire_slot", %tenant_id);
    let _guard = span.enter();

    let m = match &mut *locked {
        TenantsMap::Initializing => return Err(TenantMapError::StillInitializing.into()),
        TenantsMap::ShuttingDown(_) => return Err(TenantMapError::ShuttingDown.into()),
        TenantsMap::Open(m) => m,
    };

    use std::collections::hash_map::Entry;
    let entry = m.entry(*tenant_id);
    match entry {
        Entry::Vacant(v) => match mode {
            MustExist => {
                tracing::debug!("Vacant && MustExist: return NotFound");
                Err(TenantSlotError::NotFound(*tenant_id))
            }
            _ => {
                let (completion, barrier) = utils::completion::channel();
                v.insert(TenantSlot::InProgress(barrier));
                tracing::debug!("Vacant, inserted InProgress");
                Ok(SlotGuard::new(*tenant_id, None, completion))
            }
        },
        Entry::Occupied(mut o) => {
            // Apply mode-driven checks
            match (o.get(), mode) {
                (TenantSlot::InProgress(_), _) => {
                    tracing::debug!("Occupied, failing for InProgress");
                    Err(TenantSlotError::InProgress)
                }
                (slot, MustNotExist) => match slot {
                    TenantSlot::Attached(tenant) => {
                        tracing::debug!("Attached && MustNotExist, return AlreadyExists");
                        Err(TenantSlotError::AlreadyExists(
                            *tenant_id,
                            tenant.current_state(),
                        ))
                    }
                    _ => {
                        // FIXME: the AlreadyExists error assumes that we have a Tenant
                        // to get the state from
                        tracing::debug!("Occupied & MustNotExist, return AlreadyExists");
                        Err(TenantSlotError::AlreadyExists(
                            *tenant_id,
                            TenantState::Broken {
                                reason: "Present but not attached".to_string(),
                                backtrace: "".to_string(),
                            },
                        ))
                    }
                },
                _ => {
                    // Happy case: the slot was not in any state that violated our mode
                    let (completion, barrier) = utils::completion::channel();
                    let old_value = o.insert(TenantSlot::InProgress(barrier));
                    tracing::debug!("Occupied, replaced with InProgress");
                    Ok(SlotGuard::new(*tenant_id, Some(old_value), completion))
                }
            }
        }
    }
}

/// Stops and removes the tenant from memory, if it's not [`TenantState::Stopping`] already, bails otherwise.
/// Allows to remove other tenant resources manually, via `tenant_cleanup`.
/// If the cleanup fails, tenant will stay in memory in [`TenantState::Broken`] state, and another removal
/// operation would be needed to remove it.
async fn remove_tenant_from_memory<V, F>(
    tenants: &std::sync::RwLock<TenantsMap>,
    tenant_id: TenantId,
    tenant_cleanup: F,
) -> Result<V, TenantStateError>
where
    F: std::future::Future<Output = anyhow::Result<V>>,
{
    use utils::completion;

    let mut slot_guard =
        tenant_map_acquire_slot_impl(&tenant_id, tenants, TenantSlotAcquireMode::MustExist)?;

    // The SlotGuard allows us to manipulate the Tenant object without fear of some
    // concurrent API request doing something else for the same tenant ID.
    let attached_tenant = match slot_guard.get_old_value() {
        Some(TenantSlot::Attached(t)) => Some(t),
        _ => None,
    };

    // allow pageserver shutdown to await for our completion
    let (_guard, progress) = completion::channel();

    // If the tenant was attached, shut it down gracefully.  For secondary
    // locations this part is not necessary
    match &attached_tenant {
        Some(attached_tenant) => {
            // whenever we remove a tenant from memory, we don't want to flush and wait for upload
            let freeze_and_flush = false;

            // shutdown is sure to transition tenant to stopping, and wait for all tasks to complete, so
            // that we can continue safely to cleanup.
            match attached_tenant.shutdown(progress, freeze_and_flush).await {
                Ok(()) => {}
                Err(_other) => {
                    // if pageserver shutdown or other detach/ignore is already ongoing, we don't want to
                    // wait for it but return an error right away because these are distinct requests.
                    slot_guard.revert();
                    return Err(TenantStateError::IsStopping(tenant_id));
                }
            }
        }
        None => {
            // Nothing to wait on when not attached, proceed.
        }
    }

    match tenant_cleanup
        .await
        .with_context(|| format!("Failed to run cleanup for tenant {tenant_id}"))
    {
        Ok(hook_value) => {
            // Success: drop the old TenantSlot::Attached.
            slot_guard
                .drop_old_value()
                .expect("We just called shutdown");

            Ok(hook_value)
        }
        Err(e) => {
            // If we had a Tenant, set it to Broken and put it back in the TenantsMap
            if let Some(attached_tenant) = attached_tenant {
                attached_tenant.set_broken(e.to_string()).await;
            }
            // Leave the broken tenant in the map
            slot_guard.revert();

            Err(TenantStateError::Other(e))
        }
    }
}

use {
    crate::repository::GcResult, pageserver_api::models::TimelineGcRequest,
    utils::http::error::ApiError,
};

pub(crate) async fn immediate_gc(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    gc_req: TimelineGcRequest,
    ctx: &RequestContext,
) -> Result<tokio::sync::oneshot::Receiver<Result<GcResult, anyhow::Error>>, ApiError> {
    let guard = TENANTS.read().unwrap();
    let tenant = guard
        .get(&tenant_id)
        .map(Arc::clone)
        .with_context(|| format!("tenant {tenant_id}"))
        .map_err(|e| ApiError::NotFound(e.into()))?;

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
                .instrument(info_span!("manual_gc", %tenant_id, %timeline_id))
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tracing::{info_span, Instrument};

    use crate::tenant::mgr::TenantSlot;

    use super::{super::harness::TenantHarness, TenantsMap};

    #[tokio::test]
    async fn shutdown_awaits_in_progress_tenant() {
        // Test that if an InProgress tenant is in the map during shutdown, the shutdown will gracefully
        // wait for it to complete before proceeding.

        let (t, _ctx) = TenantHarness::create("shutdown_awaits_in_progress_tenant")
            .unwrap()
            .load()
            .await;

        // harness loads it to active, which is forced and nothing is running on the tenant

        let id = t.tenant_id();

        // tenant harness configures the logging and we cannot escape it
        let _e = info_span!("testing", tenant_id = %id).entered();

        let tenants = HashMap::from([(id, TenantSlot::Attached(t.clone()))]);
        let tenants = Arc::new(std::sync::RwLock::new(TenantsMap::Open(tenants)));

        // Invoke remove_tenant_from_memory with a cleanup hook that blocks until we manually
        // permit it to proceed: that will stick the tenant in InProgress

        let (until_cleanup_completed, can_complete_cleanup) = utils::completion::channel();
        let (until_cleanup_started, cleanup_started) = utils::completion::channel();
        let mut remove_tenant_from_memory_task = {
            let jh = tokio::spawn({
                let tenants = tenants.clone();
                async move {
                    let cleanup = async move {
                        drop(until_cleanup_started);
                        can_complete_cleanup.wait().await;
                        anyhow::Ok(())
                    };
                    super::remove_tenant_from_memory(&tenants, id, cleanup).await
                }
                .instrument(info_span!("foobar", tenant_id = %id))
            });

            // now the long cleanup should be in place, with the stopping state
            cleanup_started.wait().await;
            jh
        };

        let mut shutdown_task = {
            let (until_shutdown_started, shutdown_started) = utils::completion::channel();

            let shutdown_task = tokio::spawn(async move {
                drop(until_shutdown_started);
                super::shutdown_all_tenants0(&tenants).await;
            });

            shutdown_started.wait().await;
            shutdown_task
        };

        let long_time = std::time::Duration::from_secs(15);
        tokio::select! {
            _ = &mut shutdown_task => unreachable!("shutdown should block on remove_tenant_from_memory completing"),
            _ = &mut remove_tenant_from_memory_task => unreachable!("remove_tenant_from_memory_task should not complete until explicitly unblocked"),
            _ = tokio::time::sleep(long_time) => {},
        }

        drop(until_cleanup_completed);

        // Now that we allow it to proceed, shutdown should complete immediately
        remove_tenant_from_memory_task.await.unwrap().unwrap();
        shutdown_task.await.unwrap();
    }
}
