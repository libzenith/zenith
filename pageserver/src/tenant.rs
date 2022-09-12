//!
//! Timeline repository implementation that keeps old data in files on disk, and
//! the recent changes in memory. See tenant/*_layer.rs files.
//! The functions here are responsible for locating the correct layer for the
//! get/put call, walking back the timeline branching history as needed.
//!
//! The files are stored in the .neon/tenants/<tenantid>/timelines/<timelineid>
//! directory. See docs/pageserver-storage.md for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{bail, ensure, Context, Result};
use tokio::sync::watch;
use tracing::*;

use std::cmp::min;
use std::collections::hash_map;
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::num::NonZeroU64;
use std::ops::Bound::Included;
use std::path::Path;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use self::metadata::TimelineMetadata;
use crate::config::PageServerConf;
use crate::metrics::remove_tenant_metrics;
use crate::storage_sync::index::RemoteIndex;
use crate::tenant_config::{TenantConf, TenantConfOpt};

use crate::metrics::STORAGE_TIME;
use crate::repository::GcResult;
use crate::task_mgr;
use crate::virtual_file::VirtualFile;
use crate::walredo::WalRedoManager;
use crate::CheckpointConfig;

use toml_edit;
use utils::{
    crashsafe_dir,
    lsn::{Lsn, RecordLsn},
    zid::{ZTenantId, ZTimelineId},
};

mod blob_io;
pub mod block_io;
mod delta_layer;
mod disk_btree;
pub(crate) mod ephemeral_file;
mod filename;
mod image_layer;
mod inmemory_layer;
mod layer_map;
pub mod metadata;
mod par_fsync;
mod storage_layer;

mod timeline;

use storage_layer::Layer;

pub use timeline::Timeline;

// re-export this function so that page_cache.rs can use it.
pub use crate::tenant::ephemeral_file::writeback as writeback_ephemeral_file;

// re-export for use in storage_sync.rs
pub use crate::tenant::metadata::save_metadata;

// re-export for use in walreceiver
pub use crate::tenant::timeline::WalReceiverInfo;

/// Parts of the `.neon/tenants/<tenantid>/timelines/<timelineid>` directory prefix.
pub const TIMELINES_SEGMENT_NAME: &str = "timelines";

///
/// Tenant consists of multiple timelines. Keep them in a hash table.
///
pub struct Tenant {
    // Global pageserver config parameters
    pub conf: &'static PageServerConf,

    state: watch::Sender<TenantState>,

    // Overridden tenant-specific config parameters.
    // We keep TenantConfOpt sturct here to preserve the information
    // about parameters that are not set.
    // This is necessary to allow global config updates.
    tenant_conf: Arc<RwLock<TenantConfOpt>>,

    tenant_id: ZTenantId,
    timelines: Mutex<HashMap<ZTimelineId, Arc<Timeline>>>,
    // This mutex prevents creation of new timelines during GC.
    // Adding yet another mutex (in addition to `timelines`) is needed because holding
    // `timelines` mutex during all GC iteration (especially with enforced checkpoint)
    // may block for a long time `get_timeline`, `get_timelines_state`,... and other operations
    // with timelines, which in turn may cause dropping replication connection, expiration of wait_for_lsn
    // timeout...
    gc_cs: Mutex<()>,
    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,

    // provides access to timeline data sitting in the remote storage
    // supposed to be used for retrieval of remote consistent lsn in walreceiver
    remote_index: RemoteIndex,

    /// Makes every timeline to backup their files to remote storage.
    upload_layers: bool,
}

/// A state of a tenant in pageserver's memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TenantState {
    /// Tenant is fully operational, its background jobs might be running or not.
    Active { background_jobs_running: bool },
    /// A tenant is recognized by pageserver, but not yet ready to operate:
    /// e.g. not present locally and being downloaded or being read into memory from the file system.
    Paused,
    /// A tenant is recognized by the pageserver, but no longer used for any operations, as failed to get activated.
    Broken,
}

/// A repository corresponds to one .neon directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
impl Tenant {
    /// Get Timeline handle for given zenith timeline ID.
    /// This function is idempotent. It doesn't change internal state in any way.
    pub fn get_timeline(&self, timeline_id: ZTimelineId) -> anyhow::Result<Arc<Timeline>> {
        self.timelines
            .lock()
            .unwrap()
            .get(&timeline_id)
            .with_context(|| {
                format!(
                    "Timeline {} was not found for tenant {}",
                    timeline_id,
                    self.tenant_id()
                )
            })
            .map(Arc::clone)
    }

    /// Lists timelines the tenant contains.
    /// Up to tenant's implementation to omit certain timelines that ar not considered ready for use.
    pub fn list_timelines(&self) -> Vec<(ZTimelineId, Arc<Timeline>)> {
        self.timelines
            .lock()
            .unwrap()
            .iter()
            .map(|(timeline_id, timeline_entry)| (*timeline_id, Arc::clone(timeline_entry)))
            .collect()
    }

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    /// Initdb lsn is provided for timeline impl to be able to perform checks for some operations against it.
    pub fn create_empty_timeline(
        &self,
        new_timeline_id: ZTimelineId,
        initdb_lsn: Lsn,
    ) -> Result<Arc<Timeline>> {
        // XXX: keep the lock to avoid races during timeline creation
        let mut timelines = self.timelines.lock().unwrap();

        anyhow::ensure!(
            timelines.get(&new_timeline_id).is_none(),
            "Timeline {new_timeline_id} already exists"
        );

        let timeline_path = self.conf.timeline_path(&new_timeline_id, &self.tenant_id);
        if timeline_path.exists() {
            bail!("Timeline directory already exists, but timeline is missing in repository map. This is a bug.")
        }

        // Create the timeline directory, and write initial metadata to file.
        crashsafe_dir::create_dir_all(timeline_path)?;

        let new_metadata =
            TimelineMetadata::new(Lsn(0), None, None, Lsn(0), initdb_lsn, initdb_lsn);
        save_metadata(
            self.conf,
            new_timeline_id,
            self.tenant_id,
            &new_metadata,
            true,
        )?;

        let new_timeline =
            self.initialize_new_timeline(new_timeline_id, new_metadata, &mut timelines)?;
        new_timeline.layers.write().unwrap().next_open_layer_at = Some(initdb_lsn);

        if let hash_map::Entry::Vacant(v) = timelines.entry(new_timeline_id) {
            v.insert(Arc::clone(&new_timeline));
        }

        Ok(new_timeline)
    }

    /// Branch a timeline
    pub fn branch_timeline(
        &self,
        src: ZTimelineId,
        dst: ZTimelineId,
        start_lsn: Option<Lsn>,
    ) -> Result<Arc<Timeline>> {
        // We need to hold this lock to prevent GC from starting at the same time. GC scans the directory to learn
        // about timelines, so otherwise a race condition is possible, where we create new timeline and GC
        // concurrently removes data that is needed by the new timeline.
        let _gc_cs = self.gc_cs.lock().unwrap();

        // In order for the branch creation task to not wait for GC/compaction,
        // we need to make sure that the starting LSN of the child branch is not out of scope midway by
        //
        // 1. holding the GC lock to prevent overwritting timeline's GC data
        // 2. checking both the latest GC cutoff LSN and latest GC info of the source timeline
        //
        // Step 2 is to avoid initializing the new branch using data removed by past GC iterations
        // or in-queue GC iterations.

        // XXX: keep the lock to avoid races during timeline creation
        let mut timelines = self.timelines.lock().unwrap();
        let src_timeline = timelines
            .get(&src)
            // message about timeline being remote is one .context up in the stack
            .ok_or_else(|| anyhow::anyhow!("unknown timeline id: {src}"))?;

        let latest_gc_cutoff_lsn = src_timeline.get_latest_gc_cutoff_lsn();

        // If no start LSN is specified, we branch the new timeline from the source timeline's last record LSN
        let start_lsn = start_lsn.unwrap_or_else(|| {
            let lsn = src_timeline.get_last_record_lsn();
            info!("branching timeline {dst} from timeline {src} at last record LSN: {lsn}");
            lsn
        });

        // Check if the starting LSN is out of scope because it is less than
        // 1. the latest GC cutoff LSN or
        // 2. the planned GC cutoff LSN, which is from an in-queue GC iteration.
        src_timeline
            .check_lsn_is_in_scope(start_lsn, &latest_gc_cutoff_lsn)
            .context(format!(
                "invalid branch start lsn: less than latest GC cutoff {}",
                *latest_gc_cutoff_lsn,
            ))?;
        {
            let gc_info = src_timeline.gc_info.read().unwrap();
            let cutoff = min(gc_info.pitr_cutoff, gc_info.horizon_cutoff);
            if start_lsn < cutoff {
                bail!(format!(
                    "invalid branch start lsn: less than planned GC cutoff {cutoff}"
                ));
            }
        }

        // Determine prev-LSN for the new timeline. We can only determine it if
        // the timeline was branched at the current end of the source timeline.
        let RecordLsn {
            last: src_last,
            prev: src_prev,
        } = src_timeline.get_last_record_rlsn();
        let dst_prev = if src_last == start_lsn {
            Some(src_prev)
        } else {
            None
        };

        // create a new timeline directory
        let timelinedir = self.conf.timeline_path(&dst, &self.tenant_id);
        crashsafe_dir::create_dir(&timelinedir)?;

        // Create the metadata file, noting the ancestor of the new timeline.
        // There is initially no data in it, but all the read-calls know to look
        // into the ancestor.
        let metadata = TimelineMetadata::new(
            start_lsn,
            dst_prev,
            Some(src),
            start_lsn,
            *src_timeline.latest_gc_cutoff_lsn.read(),
            src_timeline.initdb_lsn,
        );
        crashsafe_dir::create_dir_all(self.conf.timeline_path(&dst, &self.tenant_id))?;
        save_metadata(self.conf, dst, self.tenant_id, &metadata, true)?;

        let new_timeline = self.initialize_new_timeline(dst, metadata, &mut timelines)?;
        timelines.insert(dst, Arc::clone(&new_timeline));

        info!("branched timeline {dst} from {src} at {start_lsn}");

        Ok(new_timeline)
    }

    /// perform one garbage collection iteration, removing old data files from disk.
    /// this function is periodically called by gc task.
    /// also it can be explicitly requested through page server api 'do_gc' command.
    ///
    /// 'timelineid' specifies the timeline to GC, or None for all.
    /// `horizon` specifies delta from last lsn to preserve all object versions (pitr interval).
    /// `checkpoint_before_gc` parameter is used to force compaction of storage before GC
    /// to make tests more deterministic.
    /// TODO Do we still need it or we can call checkpoint explicitly in tests where needed?
    pub fn gc_iteration(
        &self,
        target_timeline_id: Option<ZTimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        let timeline_str = target_timeline_id
            .map(|x| x.to_string())
            .unwrap_or_else(|| "-".to_string());

        STORAGE_TIME
            .with_label_values(&["gc", &self.tenant_id.to_string(), &timeline_str])
            .observe_closure_duration(|| {
                self.gc_iteration_internal(target_timeline_id, horizon, pitr, checkpoint_before_gc)
            })
    }

    /// Perform one compaction iteration.
    /// This function is periodically called by compactor task.
    /// Also it can be explicitly requested per timeline through page server
    /// api's 'compact' command.
    pub fn compaction_iteration(&self) -> Result<()> {
        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // compactions.  We don't want to block everything else while the
        // compaction runs.
        let timelines = self.timelines.lock().unwrap();
        let timelines_to_compact = timelines
            .iter()
            .map(|(timelineid, timeline)| (*timelineid, timeline.clone()))
            .collect::<Vec<_>>();
        drop(timelines);

        for (timelineid, timeline) in &timelines_to_compact {
            let _entered =
                info_span!("compact", timeline = %timelineid, tenant = %self.tenant_id).entered();
            timeline.compact()?;
        }

        Ok(())
    }

    /// Flush all in-memory data to disk.
    ///
    /// Used at graceful shutdown.
    ///
    pub fn checkpoint(&self) -> Result<()> {
        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // checkpoints. We don't want to block everything else while the
        // checkpoint runs.
        let timelines = self.timelines.lock().unwrap();
        let timelines_to_compact = timelines
            .iter()
            .map(|(timelineid, timeline)| (*timelineid, Arc::clone(timeline)))
            .collect::<Vec<_>>();
        drop(timelines);

        for (timelineid, timeline) in &timelines_to_compact {
            let _entered =
                info_span!("checkpoint", timeline = %timelineid, tenant = %self.tenant_id)
                    .entered();
            timeline.checkpoint(CheckpointConfig::Flush)?;
        }

        Ok(())
    }

    /// Removes timeline-related in-memory data
    pub fn delete_timeline(&self, timeline_id: ZTimelineId) -> anyhow::Result<()> {
        // in order to be retriable detach needs to be idempotent
        // (or at least to a point that each time the detach is called it can make progress)
        let mut timelines = self.timelines.lock().unwrap();

        // Ensure that there are no child timelines **attached to that pageserver**,
        // because detach removes files, which will break child branches
        let children_exist = timelines
            .iter()
            .any(|(_, entry)| entry.get_ancestor_timeline_id() == Some(timeline_id));

        ensure!(
            !children_exist,
            "Cannot detach timeline which has child timelines"
        );
        let timeline_entry = match timelines.entry(timeline_id) {
            Entry::Occupied(e) => e,
            Entry::Vacant(_) => bail!("timeline not found"),
        };

        let layer_removal_guard = timeline_entry.get().layer_removal_guard()?;

        let local_timeline_directory = self.conf.timeline_path(&timeline_id, &self.tenant_id);
        std::fs::remove_dir_all(&local_timeline_directory).with_context(|| {
            format!(
                "Failed to remove local timeline directory '{}'",
                local_timeline_directory.display()
            )
        })?;
        info!("detach removed files");

        drop(layer_removal_guard);
        timeline_entry.remove();

        Ok(())
    }

    pub fn init_attach_timelines(
        &self,
        timelines: HashMap<ZTimelineId, TimelineMetadata>,
    ) -> anyhow::Result<()> {
        let sorted_timelines = if timelines.len() == 1 {
            timelines.into_iter().collect()
        } else if !timelines.is_empty() {
            tree_sort_timelines(timelines)?
        } else {
            warn!("No timelines to attach received");
            return Ok(());
        };

        let mut timelines_accessor = self.timelines.lock().unwrap();
        for (timeline_id, metadata) in sorted_timelines {
            let timeline = self
                .initialize_new_timeline(timeline_id, metadata, &mut timelines_accessor)
                .with_context(|| format!("Failed to initialize timeline {timeline_id}"))?;

            match timelines_accessor.entry(timeline.timeline_id) {
                hash_map::Entry::Occupied(_) => anyhow::bail!(
                    "Found freshly initialized timeline {} in the tenant map",
                    timeline.timeline_id
                ),
                hash_map::Entry::Vacant(v) => {
                    v.insert(timeline);
                }
            }
        }

        Ok(())
    }

    /// Allows to retrieve remote timeline index from the tenant. Used in walreceiver to grab remote consistent lsn.
    pub fn get_remote_index(&self) -> &RemoteIndex {
        &self.remote_index
    }

    pub fn current_state(&self) -> TenantState {
        *self.state.borrow()
    }

    pub fn is_active(&self) -> bool {
        matches!(self.current_state(), TenantState::Active { .. })
    }

    pub fn should_run_tasks(&self) -> bool {
        matches!(
            self.current_state(),
            TenantState::Active {
                background_jobs_running: true
            }
        )
    }

    /// Changes tenant status to active, if it was not broken before.
    /// Otherwise, ignores the state change, logging an error.
    pub fn activate(&self, enable_background_jobs: bool) {
        self.set_state(TenantState::Active {
            background_jobs_running: enable_background_jobs,
        });
    }

    pub fn set_state(&self, new_state: TenantState) {
        match (self.current_state(), new_state) {
            (equal_state_1, equal_state_2) if equal_state_1 == equal_state_2 => {
                debug!("Ignoring new state, equal to the existing one: {equal_state_2:?}");
            }
            (TenantState::Broken, _) => {
                error!("Ignoring state update {new_state:?} for broken tenant");
            }
            (_, new_state) => {
                self.state.send_replace(new_state);
                if self.should_run_tasks() {
                    // Spawn gc and compaction loops. The loops will shut themselves
                    // down when they notice that the tenant is inactive.
                    crate::tenant_tasks::start_background_loops(self.tenant_id);
                }
            }
        }
    }

    pub fn subscribe_for_state_updates(&self) -> watch::Receiver<TenantState> {
        self.state.subscribe()
    }
}

/// Given a Vec of timelines and their ancestors (timeline_id, ancestor_id),
/// perform a topological sort, so that the parent of each timeline comes
/// before the children.
fn tree_sort_timelines(
    timelines: HashMap<ZTimelineId, TimelineMetadata>,
) -> Result<Vec<(ZTimelineId, TimelineMetadata)>> {
    let mut result = Vec::with_capacity(timelines.len());

    let mut now = Vec::with_capacity(timelines.len());
    // (ancestor, children)
    let mut later: HashMap<ZTimelineId, Vec<(ZTimelineId, TimelineMetadata)>> =
        HashMap::with_capacity(timelines.len());

    for (timeline_id, metadata) in timelines {
        if let Some(ancestor_id) = metadata.ancestor_timeline() {
            let children = later.entry(ancestor_id).or_default();
            children.push((timeline_id, metadata));
        } else {
            now.push((timeline_id, metadata));
        }
    }

    while let Some((timeline_id, metadata)) = now.pop() {
        result.push((timeline_id, metadata));
        // All children of this can be loaded now
        if let Some(mut children) = later.remove(&timeline_id) {
            now.append(&mut children);
        }
    }

    // All timelines should be visited now. Unless there were timelines with missing ancestors.
    if !later.is_empty() {
        for (missing_id, orphan_ids) in later {
            for (orphan_id, _) in orphan_ids {
                error!("could not load timeline {orphan_id} because its ancestor timeline {missing_id} could not be loaded");
            }
        }
        bail!("could not load tenant because some timelines are missing ancestors");
    }

    Ok(result)
}

/// Private functions
impl Tenant {
    pub fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    pub fn get_checkpoint_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_timeout
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_timeout)
    }

    pub fn get_compaction_target_size(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_target_size
            .unwrap_or(self.conf.default_tenant_conf.compaction_target_size)
    }

    pub fn get_compaction_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_period
            .unwrap_or(self.conf.default_tenant_conf.compaction_period)
    }

    pub fn get_compaction_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_threshold
            .unwrap_or(self.conf.default_tenant_conf.compaction_threshold)
    }

    pub fn get_gc_horizon(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .gc_horizon
            .unwrap_or(self.conf.default_tenant_conf.gc_horizon)
    }

    pub fn get_gc_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .gc_period
            .unwrap_or(self.conf.default_tenant_conf.gc_period)
    }

    pub fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    pub fn get_pitr_interval(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .pitr_interval
            .unwrap_or(self.conf.default_tenant_conf.pitr_interval)
    }

    pub fn get_wal_receiver_connect_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .walreceiver_connect_timeout
            .unwrap_or(self.conf.default_tenant_conf.walreceiver_connect_timeout)
    }

    pub fn get_lagging_wal_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .lagging_wal_timeout
            .unwrap_or(self.conf.default_tenant_conf.lagging_wal_timeout)
    }

    pub fn get_max_lsn_wal_lag(&self) -> NonZeroU64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .max_lsn_wal_lag
            .unwrap_or(self.conf.default_tenant_conf.max_lsn_wal_lag)
    }

    pub fn update_tenant_config(&self, new_tenant_conf: TenantConfOpt) {
        self.tenant_conf.write().unwrap().update(&new_tenant_conf);
    }

    fn initialize_new_timeline(
        &self,
        new_timeline_id: ZTimelineId,
        new_metadata: TimelineMetadata,
        timelines: &mut MutexGuard<HashMap<ZTimelineId, Arc<Timeline>>>,
    ) -> anyhow::Result<Arc<Timeline>> {
        let ancestor = match new_metadata.ancestor_timeline() {
            Some(ancestor_timeline_id) => Some(
                timelines
                    .get(&ancestor_timeline_id)
                    .cloned()
                    .with_context(|| {
                        format!(
                        "Timeline's {new_timeline_id} ancestor {ancestor_timeline_id} was not found"
                    )
                    })?,
            ),
            None => None,
        };

        let new_disk_consistent_lsn = new_metadata.disk_consistent_lsn();

        let new_timeline = Arc::new(Timeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            new_metadata,
            ancestor,
            new_timeline_id,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            self.upload_layers,
        ));

        new_timeline
            .load_layer_map(new_disk_consistent_lsn)
            .context("failed to load layermap")?;

        new_timeline.launch_wal_receiver()?;

        Ok(new_timeline)
    }

    pub fn new(
        conf: &'static PageServerConf,
        tenant_conf: TenantConfOpt,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        tenant_id: ZTenantId,
        remote_index: RemoteIndex,
        upload_layers: bool,
    ) -> Tenant {
        let (state, _) = watch::channel(TenantState::Paused);
        Tenant {
            tenant_id,
            conf,
            tenant_conf: Arc::new(RwLock::new(tenant_conf)),
            timelines: Mutex::new(HashMap::new()),
            gc_cs: Mutex::new(()),
            walredo_mgr,
            remote_index,
            upload_layers,
            state,
        }
    }

    /// Locate and load config
    pub fn load_tenant_config(
        conf: &'static PageServerConf,
        tenant_id: ZTenantId,
    ) -> anyhow::Result<TenantConfOpt> {
        let target_config_path = TenantConf::path(conf, tenant_id);
        let target_config_display = target_config_path.display();

        info!("loading tenantconf from {target_config_display}");

        // FIXME If the config file is not found, assume that we're attaching
        // a detached tenant and config is passed via attach command.
        // https://github.com/neondatabase/neon/issues/1555
        if !target_config_path.exists() {
            info!("tenant config not found in {target_config_display}");
            return Ok(TenantConfOpt::default());
        }

        // load and parse file
        let config = fs::read_to_string(&target_config_path).with_context(|| {
            format!("Failed to load config from path '{target_config_display}'")
        })?;

        let toml = config.parse::<toml_edit::Document>().with_context(|| {
            format!("Failed to parse config from file '{target_config_display}' as toml file")
        })?;

        let mut tenant_conf = TenantConfOpt::default();
        for (key, item) in toml.iter() {
            match key {
                "tenant_config" => {
                    tenant_conf = PageServerConf::parse_toml_tenant_conf(item).with_context(|| {
                        format!("Failed to parse config from file '{target_config_display}' as pageserver config")
                    })?;
                }
                _ => bail!("config file {target_config_display} has unrecognized pageserver option '{key}'"),

            }
        }

        Ok(tenant_conf)
    }

    pub fn persist_tenant_config(
        target_config_path: &Path,
        tenant_conf: TenantConfOpt,
        first_save: bool,
    ) -> anyhow::Result<()> {
        let _enter = info_span!("saving tenantconf").entered();
        info!("persisting tenantconf to {}", target_config_path.display());

        // TODO this will prepend comments endlessly
        let mut conf_content = r#"# This file contains a specific per-tenant's config.
#  It is read in case of pageserver restart.

[tenant_config]
"#
        .to_string();

        // Convert the config to a toml file.
        conf_content += &toml_edit::easy::to_string(&tenant_conf)?;

        let mut target_config_file = VirtualFile::open_with_options(
            target_config_path,
            OpenOptions::new().write(true).create_new(first_save),
        )?;

        target_config_file
            .write(conf_content.as_bytes())
            .context("Failed to write toml bytes into file")
            .and_then(|_| {
                target_config_file
                    .sync_all()
                    .context("Faile to fsync config file")
            })
            .with_context(|| {
                format!(
                    "Failed to write config file into path '{}'",
                    target_config_path.display()
                )
            })?;

        // fsync the parent directory to ensure the directory entry is durable
        if first_save {
            target_config_path
                .parent()
                .context("Config file does not have a parent")
                .and_then(|target_config_parent| {
                    File::open(target_config_parent).context("Failed to open config parent")
                })
                .and_then(|tenant_dir| {
                    tenant_dir
                        .sync_all()
                        .context("Failed to fsync config parent")
                })
                .with_context(|| {
                    format!(
                        "Failed to fsync on firts save for config {}",
                        target_config_path.display()
                    )
                })?;
        }

        Ok(())
    }

    //
    // How garbage collection works:
    //
    //                    +--bar------------->
    //                   /
    //             +----+-----foo---------------->
    //            /
    // ----main--+-------------------------->
    //                \
    //                 +-----baz-------->
    //
    //
    // 1. Grab 'gc_cs' mutex to prevent new timelines from being created
    // 2. Scan all timelines, and on each timeline, make note of the
    //    all the points where other timelines have been branched off.
    //    We will refrain from removing page versions at those LSNs.
    // 3. For each timeline, scan all layer files on the timeline.
    //    Remove all files for which a newer file exists and which
    //    don't cover any branch point LSNs.
    //
    // TODO:
    // - if a relation has a non-incremental persistent layer on a child branch, then we
    //   don't need to keep that in the parent anymore. But currently
    //   we do.
    fn gc_iteration_internal(
        &self,
        target_timeline_id: Option<ZTimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> Result<GcResult> {
        let _span_guard =
            info_span!("gc iteration", tenant = %self.tenant_id, timeline = ?target_timeline_id)
                .entered();
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        // grab mutex to prevent new timelines from being created here.
        let gc_cs = self.gc_cs.lock().unwrap();

        let timelines = self.timelines.lock().unwrap();

        // Scan all timelines. For each timeline, remember the timeline ID and
        // the branch point where it was created.
        let mut all_branchpoints: BTreeSet<(ZTimelineId, Lsn)> = BTreeSet::new();
        let timeline_ids = {
            if let Some(target_timeline_id) = target_timeline_id.as_ref() {
                if timelines.get(target_timeline_id).is_none() {
                    bail!("gc target timeline does not exist")
                }
            };

            timelines
                .iter()
                .map(|(timeline_id, timeline_entry)| {
                    // This is unresolved question for now, how to do gc in presence of remote timelines
                    // especially when this is combined with branching.
                    // Somewhat related: https://github.com/zenithdb/zenith/issues/999
                    if let Some(ancestor_timeline_id) = &timeline_entry.get_ancestor_timeline_id() {
                        // If target_timeline is specified, we only need to know branchpoints of its children
                        if let Some(timelineid) = target_timeline_id {
                            if ancestor_timeline_id == &timelineid {
                                all_branchpoints.insert((
                                    *ancestor_timeline_id,
                                    timeline_entry.get_ancestor_lsn(),
                                ));
                            }
                        }
                        // Collect branchpoints for all timelines
                        else {
                            all_branchpoints
                                .insert((*ancestor_timeline_id, timeline_entry.get_ancestor_lsn()));
                        }
                    }

                    *timeline_id
                })
                .collect::<Vec<_>>()
        };
        drop(timelines);

        // Ok, we now know all the branch points.
        // Update the GC information for each timeline.
        let mut gc_timelines = Vec::with_capacity(timeline_ids.len());
        for timeline_id in timeline_ids {
            // Timeline is known to be local and loaded.
            let timeline = self
                .get_timeline(timeline_id)
                .with_context(|| format!("Timeline {timeline_id} was not found"))?;

            // If target_timeline is specified, ignore all other timelines
            if let Some(target_timelineid) = target_timeline_id {
                if timeline_id != target_timelineid {
                    continue;
                }
            }

            if let Some(cutoff) = timeline.get_last_record_lsn().checked_sub(horizon) {
                let branchpoints: Vec<Lsn> = all_branchpoints
                    .range((
                        Included((timeline_id, Lsn(0))),
                        Included((timeline_id, Lsn(u64::MAX))),
                    ))
                    .map(|&x| x.1)
                    .collect();
                timeline.update_gc_info(branchpoints, cutoff, pitr)?;

                gc_timelines.push(timeline);
            }
        }
        drop(gc_cs);

        // Perform GC for each timeline.
        //
        // Note that we don't hold the GC lock here because we don't want
        // to delay the branch creation task, which requires the GC lock.
        // A timeline GC iteration can be slow because it may need to wait for
        // compaction (both require `layer_removal_cs` lock),
        // but the GC iteration can run concurrently with branch creation.
        //
        // See comments in [`Tenant::branch_timeline`] for more information
        // about why branch creation task can run concurrently with timeline's GC iteration.
        for timeline in gc_timelines {
            if task_mgr::is_shutdown_requested() {
                // We were requested to shut down. Stop and return with the progress we
                // made.
                break;
            }

            // If requested, force flush all in-memory layers to disk first,
            // so that they too can be garbage collected. That's
            // used in tests, so we want as deterministic results as possible.
            if checkpoint_before_gc {
                timeline.checkpoint(CheckpointConfig::Forced)?;
                info!(
                    "timeline {} checkpoint_before_gc done",
                    timeline.timeline_id
                );
            }

            let result = timeline.gc()?;
            totals += result;
        }

        totals.elapsed = now.elapsed();
        Ok(totals)
    }

    pub fn tenant_id(&self) -> ZTenantId {
        self.tenant_id
    }
}

impl Drop for Tenant {
    fn drop(&mut self) {
        remove_tenant_metrics(&self.tenant_id);
    }
}
/// Dump contents of a layer file to stdout.
pub fn dump_layerfile_from_path(path: &Path, verbose: bool) -> Result<()> {
    use std::os::unix::fs::FileExt;

    // All layer files start with a two-byte "magic" value, to identify the kind of
    // file.
    let file = File::open(path)?;
    let mut header_buf = [0u8; 2];
    file.read_exact_at(&mut header_buf, 0)?;

    match u16::from_be_bytes(header_buf) {
        crate::IMAGE_FILE_MAGIC => {
            image_layer::ImageLayer::new_for_path(path, file)?.dump(verbose)?
        }
        crate::DELTA_FILE_MAGIC => {
            delta_layer::DeltaLayer::new_for_path(path, file)?.dump(verbose)?
        }
        magic => bail!("unrecognized magic identifier: {:?}", magic),
    }

    Ok(())
}

#[cfg(test)]
pub mod harness {
    use bytes::{Bytes, BytesMut};
    use once_cell::sync::Lazy;
    use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
    use std::{fs, path::PathBuf};
    use utils::lsn::Lsn;

    use crate::storage_sync::index::RemoteIndex;
    use crate::{
        config::PageServerConf,
        repository::Key,
        tenant::Tenant,
        walrecord::ZenithWalRecord,
        walredo::{WalRedoError, WalRedoManager},
    };

    use super::metadata::metadata_path;
    use super::*;
    use crate::tenant_config::{TenantConf, TenantConfOpt};
    use hex_literal::hex;
    use utils::zid::{ZTenantId, ZTimelineId};

    pub const TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("11223344556677881122334455667788"));
    pub const NEW_TIMELINE_ID: ZTimelineId =
        ZTimelineId::from_array(hex!("AA223344556677881122334455667788"));

    /// Convenience function to create a page image with given string as the only content
    #[allow(non_snake_case)]
    pub fn TEST_IMG(s: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        buf.resize(64, 0);

        buf.freeze()
    }

    static LOCK: Lazy<RwLock<()>> = Lazy::new(|| RwLock::new(()));

    impl From<TenantConf> for TenantConfOpt {
        fn from(tenant_conf: TenantConf) -> Self {
            Self {
                checkpoint_distance: Some(tenant_conf.checkpoint_distance),
                checkpoint_timeout: Some(tenant_conf.checkpoint_timeout),
                compaction_target_size: Some(tenant_conf.compaction_target_size),
                compaction_period: Some(tenant_conf.compaction_period),
                compaction_threshold: Some(tenant_conf.compaction_threshold),
                gc_horizon: Some(tenant_conf.gc_horizon),
                gc_period: Some(tenant_conf.gc_period),
                image_creation_threshold: Some(tenant_conf.image_creation_threshold),
                pitr_interval: Some(tenant_conf.pitr_interval),
                walreceiver_connect_timeout: Some(tenant_conf.walreceiver_connect_timeout),
                lagging_wal_timeout: Some(tenant_conf.lagging_wal_timeout),
                max_lsn_wal_lag: Some(tenant_conf.max_lsn_wal_lag),
            }
        }
    }

    pub struct TenantHarness<'a> {
        pub conf: &'static PageServerConf,
        pub tenant_conf: TenantConf,
        pub tenant_id: ZTenantId,

        pub lock_guard: (
            Option<RwLockReadGuard<'a, ()>>,
            Option<RwLockWriteGuard<'a, ()>>,
        ),
    }

    impl<'a> TenantHarness<'a> {
        pub fn create(test_name: &'static str) -> Result<Self> {
            Self::create_internal(test_name, false)
        }
        pub fn create_exclusive(test_name: &'static str) -> Result<Self> {
            Self::create_internal(test_name, true)
        }
        fn create_internal(test_name: &'static str, exclusive: bool) -> Result<Self> {
            let lock_guard = if exclusive {
                (None, Some(LOCK.write().unwrap()))
            } else {
                (Some(LOCK.read().unwrap()), None)
            };

            let repo_dir = PageServerConf::test_repo_dir(test_name);
            let _ = fs::remove_dir_all(&repo_dir);
            fs::create_dir_all(&repo_dir)?;

            let conf = PageServerConf::dummy_conf(repo_dir);
            // Make a static copy of the config. This can never be free'd, but that's
            // OK in a test.
            let conf: &'static PageServerConf = Box::leak(Box::new(conf));

            let tenant_conf = TenantConf::dummy_conf();

            let tenant_id = ZTenantId::generate();
            fs::create_dir_all(conf.tenant_path(&tenant_id))?;
            fs::create_dir_all(conf.timelines_path(&tenant_id))?;

            Ok(Self {
                conf,
                tenant_conf,
                tenant_id,
                lock_guard,
            })
        }

        pub fn load(&self) -> Tenant {
            self.try_load().expect("failed to load test tenant")
        }

        pub fn try_load(&self) -> Result<Tenant> {
            let walredo_mgr = Arc::new(TestRedoManager);

            let tenant = Tenant::new(
                self.conf,
                TenantConfOpt::from(self.tenant_conf),
                walredo_mgr,
                self.tenant_id,
                RemoteIndex::default(),
                false,
            );
            // populate tenant with locally available timelines
            let mut timelines_to_load = HashMap::new();
            for timeline_dir_entry in fs::read_dir(self.conf.timelines_path(&self.tenant_id))
                .expect("should be able to read timelines dir")
            {
                let timeline_dir_entry = timeline_dir_entry?;
                let timeline_id: ZTimelineId = timeline_dir_entry
                    .path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .parse()?;

                let timeline_metadata = load_metadata(self.conf, timeline_id, self.tenant_id)?;
                timelines_to_load.insert(timeline_id, timeline_metadata);
            }
            tenant.init_attach_timelines(timelines_to_load)?;

            Ok(tenant)
        }

        pub fn timeline_path(&self, timeline_id: &ZTimelineId) -> PathBuf {
            self.conf.timeline_path(timeline_id, &self.tenant_id)
        }
    }

    fn load_metadata(
        conf: &'static PageServerConf,
        timeline_id: ZTimelineId,
        tenant_id: ZTenantId,
    ) -> anyhow::Result<TimelineMetadata> {
        let metadata_path = metadata_path(conf, timeline_id, tenant_id);
        let metadata_bytes = std::fs::read(&metadata_path).with_context(|| {
            format!(
                "Failed to read metadata bytes from path {}",
                metadata_path.display()
            )
        })?;
        TimelineMetadata::from_bytes(&metadata_bytes).with_context(|| {
            format!(
                "Failed to parse metadata bytes from path {}",
                metadata_path.display()
            )
        })
    }

    // Mock WAL redo manager that doesn't do much
    pub struct TestRedoManager;

    impl WalRedoManager for TestRedoManager {
        fn request_redo(
            &self,
            key: Key,
            lsn: Lsn,
            base_img: Option<Bytes>,
            records: Vec<(Lsn, ZenithWalRecord)>,
        ) -> Result<Bytes, WalRedoError> {
            let s = format!(
                "redo for {} to get to {}, with {} and {} records",
                key,
                lsn,
                if base_img.is_some() {
                    "base image"
                } else {
                    "no base image"
                },
                records.len()
            );
            println!("{}", s);

            Ok(TEST_IMG(&s))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::metadata::METADATA_FILE_NAME;
    use super::*;
    use crate::keyspace::KeySpaceAccum;
    use crate::repository::{Key, Value};
    use crate::tenant::harness::*;
    use bytes::BytesMut;
    use hex_literal::hex;
    use once_cell::sync::Lazy;
    use rand::{thread_rng, Rng};

    static TEST_KEY: Lazy<Key> =
        Lazy::new(|| Key::from_slice(&hex!("112222222233333333444444445500000001")));

    #[test]
    fn test_basic() -> Result<()> {
        let tenant = TenantHarness::create("test_basic")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x10), &Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x20), &Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        assert_eq!(tline.get(*TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));

        Ok(())
    }

    #[test]
    fn no_duplicate_timelines() -> Result<()> {
        let tenant = TenantHarness::create("no_duplicate_timelines")?.load();
        let _ = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        match tenant.create_empty_timeline(TIMELINE_ID, Lsn(0)) {
            Ok(_) => panic!("duplicate timeline creation should fail"),
            Err(e) => assert_eq!(
                e.to_string(),
                format!("Timeline {TIMELINE_ID} already exists")
            ),
        }

        Ok(())
    }

    /// Convenience function to create a page image with given string as the only content
    pub fn test_value(s: &str) -> Value {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        Value::Image(buf.freeze())
    }

    ///
    /// Test branch creation
    ///
    #[test]
    fn test_branch() -> Result<()> {
        let tenant = TenantHarness::create("test_branch")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        let writer = tline.writer();
        use std::str::from_utf8;

        #[allow(non_snake_case)]
        let TEST_KEY_A: Key = Key::from_hex("112222222233333333444444445500000001").unwrap();
        #[allow(non_snake_case)]
        let TEST_KEY_B: Key = Key::from_hex("112222222233333333444444445500000002").unwrap();

        // Insert a value on the timeline
        writer.put(TEST_KEY_A, Lsn(0x20), &test_value("foo at 0x20"))?;
        writer.put(TEST_KEY_B, Lsn(0x20), &test_value("foobar at 0x20"))?;
        writer.finish_write(Lsn(0x20));

        writer.put(TEST_KEY_A, Lsn(0x30), &test_value("foo at 0x30"))?;
        writer.finish_write(Lsn(0x30));
        writer.put(TEST_KEY_A, Lsn(0x40), &test_value("foo at 0x40"))?;
        writer.finish_write(Lsn(0x40));

        //assert_current_logical_size(&tline, Lsn(0x40));

        // Branch the history, modify relation differently on the new timeline
        tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x30)))?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");
        let new_writer = newtline.writer();
        new_writer.put(TEST_KEY_A, Lsn(0x40), &test_value("bar at 0x40"))?;
        new_writer.finish_write(Lsn(0x40));

        // Check page contents on both branches
        assert_eq!(
            from_utf8(&tline.get(TEST_KEY_A, Lsn(0x40))?)?,
            "foo at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_A, Lsn(0x40))?)?,
            "bar at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_B, Lsn(0x40))?)?,
            "foobar at 0x20"
        );

        //assert_current_logical_size(&tline, Lsn(0x40));

        Ok(())
    }

    fn make_some_layers(tline: &Timeline, start_lsn: Lsn) -> Result<()> {
        let mut lsn = start_lsn;
        #[allow(non_snake_case)]
        {
            let writer = tline.writer();
            // Create a relation on the timeline
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
        }
        tline.checkpoint(CheckpointConfig::Forced)?;
        {
            let writer = tline.writer();
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
        }
        tline.checkpoint(CheckpointConfig::Forced)
    }

    #[test]
    fn test_prohibit_branch_creation_on_garbage_collected_data() -> Result<()> {
        let tenant =
            TenantHarness::create("test_prohibit_branch_creation_on_garbage_collected_data")?
                .load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        // FIXME: this doesn't actually remove any layer currently, given how the checkpointing
        // and compaction works. But it does set the 'cutoff' point so that the cross check
        // below should fail.
        tenant.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;

        // try to branch at lsn 25, should fail because we already garbage collected the data
        match tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x25))) {
            Ok(_) => panic!("branching should have failed"),
            Err(err) => {
                assert!(err.to_string().contains("invalid branch start lsn"));
                assert!(err
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("we might've already garbage collected needed data"))
            }
        }

        Ok(())
    }

    #[test]
    fn test_prohibit_branch_creation_on_pre_initdb_lsn() -> Result<()> {
        let tenant =
            TenantHarness::create("test_prohibit_branch_creation_on_pre_initdb_lsn")?.load();

        tenant.create_empty_timeline(TIMELINE_ID, Lsn(0x50))?;
        // try to branch at lsn 0x25, should fail because initdb lsn is 0x50
        match tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x25))) {
            Ok(_) => panic!("branching should have failed"),
            Err(err) => {
                assert!(&err.to_string().contains("invalid branch start lsn"));
                assert!(&err
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("is earlier than latest GC horizon"));
            }
        }

        Ok(())
    }

    /*
    // FIXME: This currently fails to error out. Calling GC doesn't currently
    // remove the old value, we'd need to work a little harder
    #[test]
    fn test_prohibit_get_for_garbage_collected_data() -> Result<()> {
        let repo =
            RepoHarness::create("test_prohibit_get_for_garbage_collected_data")?
            .load();

        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;
        let latest_gc_cutoff_lsn = tline.get_latest_gc_cutoff_lsn();
        assert!(*latest_gc_cutoff_lsn > Lsn(0x25));
        match tline.get(*TEST_KEY, Lsn(0x25)) {
            Ok(_) => panic!("request for page should have failed"),
            Err(err) => assert!(err.to_string().contains("not found at")),
        }
        Ok(())
    }
     */

    #[test]
    fn test_retain_data_in_parent_which_is_needed_for_child() -> Result<()> {
        let tenant =
            TenantHarness::create("test_retain_data_in_parent_which_is_needed_for_child")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");
        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        tenant.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;
        assert!(newtline.get(*TEST_KEY, Lsn(0x25)).is_ok());

        Ok(())
    }
    #[test]
    fn test_parent_keeps_data_forever_after_branching() -> Result<()> {
        let tenant =
            TenantHarness::create("test_parent_keeps_data_forever_after_branching")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        make_some_layers(tline.as_ref(), Lsn(0x20))?;

        tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID)
            .expect("Should have a local timeline");

        make_some_layers(newtline.as_ref(), Lsn(0x60))?;

        // run gc on parent
        tenant.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;

        // Check that the data is still accessible on the branch.
        assert_eq!(
            newtline.get(*TEST_KEY, Lsn(0x50))?,
            TEST_IMG(&format!("foo at {}", Lsn(0x40)))
        );

        Ok(())
    }

    #[test]
    fn timeline_load() -> Result<()> {
        const TEST_NAME: &str = "timeline_load";
        let harness = TenantHarness::create(TEST_NAME)?;
        {
            let tenant = harness.load();
            let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0x8000))?;
            make_some_layers(tline.as_ref(), Lsn(0x8000))?;
            tline.checkpoint(CheckpointConfig::Forced)?;
        }

        let tenant = harness.load();
        tenant
            .get_timeline(TIMELINE_ID)
            .expect("cannot load timeline");

        Ok(())
    }

    #[test]
    fn timeline_load_with_ancestor() -> Result<()> {
        const TEST_NAME: &str = "timeline_load_with_ancestor";
        let harness = TenantHarness::create(TEST_NAME)?;
        // create two timelines
        {
            let tenant = harness.load();
            let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

            make_some_layers(tline.as_ref(), Lsn(0x20))?;
            tline.checkpoint(CheckpointConfig::Forced)?;

            tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;

            let newtline = tenant
                .get_timeline(NEW_TIMELINE_ID)
                .expect("Should have a local timeline");

            make_some_layers(newtline.as_ref(), Lsn(0x60))?;
            tline.checkpoint(CheckpointConfig::Forced)?;
        }

        // check that both of them are initially unloaded
        let tenant = harness.load();

        // check that both, child and ancestor are loaded
        let _child_tline = tenant
            .get_timeline(NEW_TIMELINE_ID)
            .expect("cannot get child timeline loaded");

        let _ancestor_tline = tenant
            .get_timeline(TIMELINE_ID)
            .expect("cannot get ancestor timeline loaded");

        Ok(())
    }

    #[test]
    fn corrupt_metadata() -> Result<()> {
        const TEST_NAME: &str = "corrupt_metadata";
        let harness = TenantHarness::create(TEST_NAME)?;
        let tenant = harness.load();

        tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;
        drop(tenant);

        let metadata_path = harness.timeline_path(&TIMELINE_ID).join(METADATA_FILE_NAME);

        assert!(metadata_path.is_file());

        let mut metadata_bytes = std::fs::read(&metadata_path)?;
        assert_eq!(metadata_bytes.len(), 512);
        metadata_bytes[8] ^= 1;
        std::fs::write(metadata_path, metadata_bytes)?;

        let err = harness.try_load().err().expect("should fail");
        assert!(err
            .to_string()
            .starts_with("Failed to parse metadata bytes from path"));

        let mut found_error_message = false;
        let mut err_source = err.source();
        while let Some(source) = err_source {
            if source.to_string() == "metadata checksum mismatch" {
                found_error_message = true;
                break;
            }
            err_source = source.source();
        }
        assert!(
            found_error_message,
            "didn't find the corrupted metadata error"
        );

        Ok(())
    }

    #[test]
    fn test_images() -> Result<()> {
        let tenant = TenantHarness::create("test_images")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x10), &Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x20), &Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x30), &Value::Image(TEST_IMG("foo at 0x30")))?;
        writer.finish_write(Lsn(0x30));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x40), &Value::Image(TEST_IMG("foo at 0x40")))?;
        writer.finish_write(Lsn(0x40));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced)?;
        tline.compact()?;

        assert_eq!(tline.get(*TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x30))?, TEST_IMG("foo at 0x30"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x40))?, TEST_IMG("foo at 0x40"));

        Ok(())
    }

    //
    // Insert 1000 key-value pairs with increasing keys, checkpoint,
    // repeat 50 times.
    //
    #[test]
    fn test_bulk_insert() -> Result<()> {
        let tenant = TenantHarness::create("test_bulk_insert")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        let mut lsn = Lsn(0x10);

        let mut keyspace = KeySpaceAccum::new();

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        let mut blknum = 0;
        for _ in 0..50 {
            for _ in 0..10000 {
                test_key.field6 = blknum;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                writer.finish_write(lsn);
                drop(writer);

                keyspace.add_key(test_key);

                lsn = Lsn(lsn.0 + 0x10);
                blknum += 1;
            }

            let cutoff = tline.get_last_record_lsn();

            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO)?;
            tline.checkpoint(CheckpointConfig::Forced)?;
            tline.compact()?;
            tline.gc()?;
        }

        Ok(())
    }

    #[test]
    fn test_random_updates() -> Result<()> {
        let tenant = TenantHarness::create("test_random_updates")?.load();
        let tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut keyspace = KeySpaceAccum::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer();
            writer.put(
                test_key,
                lsn,
                &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
            )?;
            writer.finish_write(lsn);
            updated[blknum] = lsn;
            drop(writer);

            keyspace.add_key(test_key);
        }

        for _ in 0..50 {
            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                writer.finish_write(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn)?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of checkpoint, compaction, and GC
            println!("checkpointing {}", lsn);
            let cutoff = tline.get_last_record_lsn();
            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO)?;
            tline.checkpoint(CheckpointConfig::Forced)?;
            tline.compact()?;
            tline.gc()?;
        }

        Ok(())
    }

    #[test]
    fn test_traverse_branches() -> Result<()> {
        let tenant = TenantHarness::create("test_traverse_branches")?.load();
        let mut tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut keyspace = KeySpaceAccum::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer();
            writer.put(
                test_key,
                lsn,
                &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
            )?;
            writer.finish_write(lsn);
            updated[blknum] = lsn;
            drop(writer);

            keyspace.add_key(test_key);
        }

        let mut tline_id = TIMELINE_ID;
        for _ in 0..50 {
            let new_tline_id = ZTimelineId::generate();
            tenant.branch_timeline(tline_id, new_tline_id, Some(lsn))?;
            tline = tenant
                .get_timeline(new_tline_id)
                .expect("Should have the branched timeline");
            tline_id = new_tline_id;

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                println!("updating {} at {}", blknum, lsn);
                writer.finish_write(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn)?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of checkpoint, compaction, and GC
            println!("checkpointing {}", lsn);
            let cutoff = tline.get_last_record_lsn();
            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO)?;
            tline.checkpoint(CheckpointConfig::Forced)?;
            tline.compact()?;
            tline.gc()?;
        }

        Ok(())
    }

    #[test]
    fn test_traverse_ancestors() -> Result<()> {
        let tenant = TenantHarness::create("test_traverse_ancestors")?.load();
        let mut tline = tenant.create_empty_timeline(TIMELINE_ID, Lsn(0))?;

        const NUM_KEYS: usize = 100;
        const NUM_TLINES: usize = 50;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        // Track page mutation lsns across different timelines.
        let mut updated = [[Lsn(0); NUM_KEYS]; NUM_TLINES];

        let mut lsn = Lsn(0);
        let mut tline_id = TIMELINE_ID;

        #[allow(clippy::needless_range_loop)]
        for idx in 0..NUM_TLINES {
            let new_tline_id = ZTimelineId::generate();
            tenant.branch_timeline(tline_id, new_tline_id, Some(lsn))?;
            tline = tenant
                .get_timeline(new_tline_id)
                .expect("Should have the branched timeline");
            tline_id = new_tline_id;

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} {} at {}", idx, blknum, lsn))),
                )?;
                println!("updating [{}][{}] at {}", idx, blknum, lsn);
                writer.finish_write(lsn);
                drop(writer);
                updated[idx][blknum] = lsn;
            }
        }

        // Read pages from leaf timeline across all ancestors.
        for (idx, lsns) in updated.iter().enumerate() {
            for (blknum, lsn) in lsns.iter().enumerate() {
                // Skip empty mutations.
                if lsn.0 == 0 {
                    continue;
                }
                println!("checking [{idx}][{blknum}] at {lsn}");
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, *lsn)?,
                    TEST_IMG(&format!("{idx} {blknum} at {lsn}"))
                );
            }
        }
        Ok(())
    }
}
