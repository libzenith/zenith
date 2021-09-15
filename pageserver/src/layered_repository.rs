//!
//! Zenith repository implementation that keeps old data in files on disk, and
//! the recent changes in memory. See layered_repository/*_layer.rs files.
//! The functions here are responsible for locating the correct layer for the
//! get/put call, tracing timeline branching history as needed.
//!
//! The files are stored in the .zenith/tenants/<tenantid>/timelines/<timelineid>
//! directory. See layered_repository/README for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use lazy_static::lazy_static;
use log::*;
use postgres_ffi::pg_constants::BLCKSZ;
use serde::{Deserialize, Serialize};

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::Write;
use std::ops::Bound::Included;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use std::{fs, thread};

use crate::layered_repository::inmemory_layer::FreezeLayers;
use crate::relish::*;
use crate::repository::{GcResult, Repository, Timeline, WALRecord};
use crate::restore_local_repo::import_timeline_wal;
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};

use zenith_metrics::{register_histogram, register_int_gauge_vec, Histogram, IntGaugeVec};
use zenith_metrics::{register_histogram_vec, HistogramVec};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::{AtomicLsn, Lsn, RecordLsn};
use zenith_utils::seqwait::SeqWait;

mod blob;
mod delta_layer;
mod filename;
mod image_layer;
mod inmemory_layer;
mod layer_map;
mod storage_layer;

use delta_layer::DeltaLayer;
use image_layer::ImageLayer;

use filename::{DeltaFileName, ImageFileName};
use inmemory_layer::InMemoryLayer;
use layer_map::LayerMap;
use storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, SegmentTag, RELISH_SEG_SIZE,
};

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

// Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

// Flush out an inmemory layer, if it's holding WAL older than this.
// This puts a backstop on how much WAL needs to be re-digested if the
// page server crashes.
//
// FIXME: This current value is very low. I would imagine something like 1 GB or 10 GB
// would be more appropriate. But a low value forces the code to be exercised more,
// which is good for now to trigger bugs.
static OLDEST_INMEM_DISTANCE: i128 = 16 * 1024 * 1024;

// Metrics collected on operations on the storage repository.
lazy_static! {
    static ref STORAGE_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_storage_time",
        "Time spent on storage operations",
        &["operation"]
    )
    .expect("failed to define a metric");
}

// Metrics collected on operations on the storage repository.
lazy_static! {
    static ref RECONSTRUCT_TIME: Histogram = register_histogram!(
        "pageserver_getpage_reconstruct_time",
        "FIXME Time spent on storage operations"
    )
    .expect("failed to define a metric");
}

lazy_static! {
    // NOTE: can be zero if pageserver was restarted and there hasn't been any
    // activity yet.
    static ref LOGICAL_TIMELINE_SIZE: IntGaugeVec = register_int_gauge_vec!(
        "pageserver_logical_timeline_size",
        "Logical timeline size (bytes)",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric");
}

///
/// Repository consists of multiple timelines. Keep them in a hash table.
///
pub struct LayeredRepository {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelines: Mutex<HashMap<ZTimelineId, Arc<LayeredTimeline>>>,

    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
}

/// Public interface
impl Repository for LayeredRepository {
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        Ok(self.get_timeline_locked(timelineid, &mut timelines)?)
    }

    fn create_empty_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        // Create the timeline directory, and write initial metadata to file.
        std::fs::create_dir_all(self.conf.timeline_path(&timelineid, &self.tenantid))?;

        let metadata = TimelineMetadata {
            disk_consistent_lsn: Lsn(0),
            prev_record_lsn: None,
            ancestor_timeline: None,
            ancestor_lsn: Lsn(0),
        };
        Self::save_metadata(self.conf, timelineid, self.tenantid, &metadata)?;

        let timeline = LayeredTimeline::new(
            self.conf,
            metadata,
            None,
            timelineid,
            self.tenantid,
            self.walredo_mgr.clone(),
            0,
        )?;

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());
        Ok(timeline_rc)
    }

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()> {
        let src_timeline = self.get_timeline(src)?;

        let RecordLsn {
            last: src_last,
            prev: src_prev,
        } = src_timeline.get_last_record_rlsn();

        // Use src_prev from the source timeline only if we branched at the last record.
        let dst_prev = if src_last == start_lsn {
            Some(src_prev)
        } else {
            None
        };

        // Create the metadata file, noting the ancestor of the new timeline.
        // There is initially no data in it, but all the read-calls know to look
        // into the ancestor.
        let metadata = TimelineMetadata {
            disk_consistent_lsn: start_lsn,
            prev_record_lsn: dst_prev,
            ancestor_timeline: Some(src),
            ancestor_lsn: start_lsn,
        };
        std::fs::create_dir_all(self.conf.timeline_path(&dst, &self.tenantid))?;
        Self::save_metadata(self.conf, dst, self.tenantid, &metadata)?;

        info!("branched timeline {} from {} at {}", dst, src, start_lsn);

        Ok(())
    }

    /// Public entry point to GC. All the logic is in the private
    /// gc_iteration_internal function, this public facade just wraps it for
    /// metrics collection.
    fn gc_iteration(
        &self,
        target_timelineid: Option<ZTimelineId>,
        horizon: u64,
        compact: bool,
    ) -> Result<GcResult> {
        STORAGE_TIME
            .with_label_values(&["gc"])
            .observe_closure_duration(|| {
                self.gc_iteration_internal(target_timelineid, horizon, compact)
            })
    }
}

/// Private functions
impl LayeredRepository {
    // Implementation of the public `get_timeline` function. This differs from the public
    // interface in that the caller must already hold the mutex on the 'timelines' hashmap.
    fn get_timeline_locked(
        &self,
        timelineid: ZTimelineId,
        timelines: &mut HashMap<ZTimelineId, Arc<LayeredTimeline>>,
    ) -> Result<Arc<LayeredTimeline>> {
        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let metadata = Self::load_metadata(self.conf, timelineid, self.tenantid)?;

                // Recurse to look up the ancestor timeline.
                //
                // TODO: If you have a very deep timeline history, this could become
                // expensive. Perhaps delay this until we need to look up a page in
                // ancestor.
                let ancestor = if let Some(ancestor_timelineid) = metadata.ancestor_timeline {
                    Some(self.get_timeline_locked(ancestor_timelineid, timelines)?)
                } else {
                    None
                };

                let mut timeline = LayeredTimeline::new(
                    self.conf,
                    metadata,
                    ancestor,
                    timelineid,
                    self.tenantid,
                    self.walredo_mgr.clone(),
                    0, // init with 0 and update after layers are loaded
                )?;

                // List the layers on disk, and load them into the layer map
                timeline.load_layer_map()?;

                // needs to be after load_layer_map
                timeline.init_current_logical_size()?;

                let timeline = Arc::new(timeline);

                // Load any new WAL after the last checkpoint into memory.
                info!(
                    "Loading WAL for timeline {} starting at {}",
                    timelineid,
                    timeline.get_last_record_lsn()
                );
                let wal_dir = self
                    .conf
                    .timeline_path(&timelineid, &self.tenantid)
                    .join("wal");
                import_timeline_wal(&wal_dir, timeline.as_ref(), timeline.get_last_record_lsn())?;

                if cfg!(debug_assertions) {
                    // check again after wal loading
                    Self::assert_size_calculation_matches_offloaded(Arc::clone(&timeline));
                }

                timelines.insert(timelineid, timeline.clone());
                Ok(timeline.clone())
            }
        }
    }

    pub fn new(
        conf: &'static PageServerConf,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        tenantid: ZTenantId,
    ) -> LayeredRepository {
        LayeredRepository {
            tenantid,
            conf,
            timelines: Mutex::new(HashMap::new()),
            walredo_mgr,
        }
    }

    ///
    /// Launch the checkpointer thread in given repository.
    ///
    pub fn launch_checkpointer_thread(conf: &'static PageServerConf, rc: Arc<LayeredRepository>) {
        let _thread = std::thread::Builder::new()
            .name("Checkpointer thread".into())
            .spawn(move || {
                // FIXME: relaunch it? Panic is not good.
                rc.checkpoint_loop(conf).expect("Checkpointer thread died");
            })
            .unwrap();
    }

    ///
    /// Checkpointer thread's main loop
    ///
    fn checkpoint_loop(&self, conf: &'static PageServerConf) -> Result<()> {
        loop {
            std::thread::sleep(conf.gc_period);

            info!("checkpointer thread for tenant {} waking up", self.tenantid);

            // checkpoint timelines that have accumulated more than CHECKPOINT_INTERVAL
            // bytes of WAL since last checkpoint.
            {
                let timelines = self.timelines.lock().unwrap();
                for (_timelineid, timeline) in timelines.iter() {
                    STORAGE_TIME
                        .with_label_values(&["checkpoint_timed"])
                        .observe_closure_duration(|| timeline.checkpoint_internal(false))?
                }
                // release lock on 'timelines'
            }

            // Garbage collect old files that are not needed for PITR anymore
            if conf.gc_horizon > 0 {
                self.gc_iteration(None, conf.gc_horizon, false).unwrap();
            }
        }
    }

    /// Save timeline metadata to file
    fn save_metadata(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        data: &TimelineMetadata,
    ) -> Result<()> {
        let path = conf.timeline_path(&timelineid, &tenantid).join("metadata");
        let mut file = File::create(&path)?;

        info!("saving metadata {}", path.display());

        file.write_all(&TimelineMetadata::ser(data)?)?;

        Ok(())
    }

    fn load_metadata(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
    ) -> Result<TimelineMetadata> {
        let path = conf.timeline_path(&timelineid, &tenantid).join("metadata");
        let data = std::fs::read(&path)?;

        let data = TimelineMetadata::des(&data)?;
        assert!(data.disk_consistent_lsn.is_aligned());

        Ok(data)
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
    // 1. Grab a mutex to prevent new timelines from being created
    // 2. Scan all timelines, and on each timeline, make note of the
    //    all the points where other timelines have been branched off.
    //    We will refrain from removing page versions at those LSNs.
    // 3. For each timeline, scan all layer files on the timeline.
    //    Remove all files for which a newer file exists and which
    //    don't cover any branch point LSNs.
    //
    // TODO:
    // - if a relation has been modified on a child branch, then we
    //   don't need to keep that in the parent anymore. But currently
    //   we do.
    fn gc_iteration_internal(
        &self,
        target_timelineid: Option<ZTimelineId>,
        horizon: u64,
        compact: bool,
    ) -> Result<GcResult> {
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        // grab mutex to prevent new timelines from being created here.
        // TODO: We will hold it for a long time
        let mut timelines = self.timelines.lock().unwrap();

        // Scan all timelines. For each timeline, remember the timeline ID and
        // the branch point where it was created.
        //
        // We scan the directory, not the in-memory hash table, because the hash
        // table only contains entries for timelines that have been accessed. We
        // need to take all timelines into account, not only the active ones.
        let mut timelineids: Vec<ZTimelineId> = Vec::new();
        let mut all_branchpoints: BTreeSet<(ZTimelineId, Lsn)> = BTreeSet::new();
        let timelines_path = self.conf.timelines_path(&self.tenantid);
        for direntry in fs::read_dir(timelines_path)? {
            let direntry = direntry?;
            if let Some(fname) = direntry.file_name().to_str() {
                if let Ok(timelineid) = fname.parse::<ZTimelineId>() {
                    timelineids.push(timelineid);

                    // Read the metadata of this timeline to get its parent timeline.
                    //
                    // We read the ancestor information directly from the file, instead
                    // of calling get_timeline(). We don't want to load the timeline
                    // into memory just for GC.
                    //
                    // FIXME: we open the timeline in the loop below with
                    // get_timeline_locked() anyway, so maybe we should just do it
                    // here, too.
                    let metadata = Self::load_metadata(self.conf, timelineid, self.tenantid)?;
                    if let Some(ancestor_timeline) = metadata.ancestor_timeline {
                        all_branchpoints.insert((ancestor_timeline, metadata.ancestor_lsn));
                    }
                }
            }
        }

        // Ok, we now know all the branch points. Iterate through them.
        for timelineid in timelineids {
            // If a target timeline was specified, leave the other timelines alone.
            // This is a bit inefficient, because we still collect the information for
            // all the timelines above.
            if let Some(x) = target_timelineid {
                if x != timelineid {
                    continue;
                }
            }

            let branchpoints: Vec<Lsn> = all_branchpoints
                .range((
                    Included((timelineid, Lsn(0))),
                    Included((timelineid, Lsn(u64::MAX))),
                ))
                .map(|&x| x.1)
                .collect();

            let timeline = self.get_timeline_locked(timelineid, &mut *timelines)?;
            let last_lsn = timeline.get_last_record_lsn();

            if let Some(cutoff) = last_lsn.checked_sub(horizon) {
                // If GC was explicitly requested by the admin, force flush all in-memory
                // layers to disk first, so that they too can be garbage collected. That's
                // used in tests, so we want as deterministic results as possible.
                if compact {
                    timeline.checkpoint()?;
                }

                let result = timeline.gc_timeline(branchpoints, cutoff)?;

                totals += result;
            }
        }

        totals.elapsed = now.elapsed();
        Ok(totals)
    }

    fn assert_size_calculation_matches(incremental: usize, timeline: &LayeredTimeline) {
        match timeline.get_current_logical_size_non_incremental(timeline.get_last_record_lsn()) {
            Ok(non_incremental) => {
                if incremental != non_incremental {
                    error!("timeline size calculation diverged, incremental doesn't match non incremental. incremental={} non_incremental={}", incremental, non_incremental);
                }
            }
            Err(e) => error!("failed to calculate non incremental timeline size: {}", e),
        }
    }

    fn assert_size_calculation_matches_offloaded(timeline: Arc<LayeredTimeline>) {
        let incremental = timeline.get_current_logical_size();
        thread::spawn(move || {
            Self::assert_size_calculation_matches(incremental, &timeline);
        });
    }
}

/// Metadata stored on disk for each timeline
///
/// The fields correspond to the values we hold in memory, in LayeredTimeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineMetadata {
    disk_consistent_lsn: Lsn,

    // This is only set if we know it. We track it in memory when the page
    // server is running, but we only track the value corresponding to
    // 'last_record_lsn', not 'disk_consistent_lsn' which can lag behind by a
    // lot. We only store it in the metadata file when we flush *all* the
    // in-memory data so that 'last_record_lsn' is the same as
    // 'disk_consistent_lsn'.  That's OK, because after page server restart, as
    // soon as we reprocess at least one record, we will have a valid
    // 'prev_record_lsn' value in memory again. This is only really needed when
    // doing a clean shutdown, so that there is no more WAL beyond
    // 'disk_consistent_lsn'
    prev_record_lsn: Option<Lsn>,

    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

pub struct LayeredTimeline {
    conf: &'static PageServerConf,

    tenantid: ZTenantId,
    timelineid: ZTimelineId,

    layers: Mutex<LayerMap>,

    // WAL redo manager
    walredo_mgr: Arc<dyn WalRedoManager + Sync + Send>,

    // What page versions do we hold in the repository? If we get a
    // request > last_record_lsn, we need to wait until we receive all
    // the WAL up to the request. The SeqWait provides functions for
    // that. TODO: If we get a request for an old LSN, such that the
    // versions have already been garbage collected away, we should
    // throw an error, but we don't track that currently.
    //
    // last_record_lsn.load().last points to the end of last processed WAL record.
    //
    // We also remember the starting point of the previous record in
    // 'last_record_lsn.load().prev'. It's used to set the xl_prev pointer of the
    // first WAL record when the node is started up. But here, we just
    // keep track of it.
    last_record_lsn: SeqWait<RecordLsn, Lsn>,

    // All WAL records have been processed and stored durably on files on
    // local disk, up to this LSN. On crash and restart, we need to re-process
    // the WAL starting from this point.
    //
    // Some later WAL records might have been processed and also flushed to disk
    // already, so don't be surprised to see some, but there's no guarantee on
    // them yet.
    disk_consistent_lsn: AtomicLsn,

    // Parent timeline that this timeline was branched from, and the LSN
    // of the branch point.
    ancestor_timeline: Option<Arc<LayeredTimeline>>,
    ancestor_lsn: Lsn,

    // this variable indicates how much space is used from user's point of view,
    // e.g. we do not account here for multiple versions of data and so on.
    // this is counted incrementally based on physical relishes (excluding FileNodeMap)
    // current_logical_size is not stored no disk and initialized on timeline creation using
    // get_current_logical_size_non_incremental in init_current_logical_size
    // this is needed because when we save it in metadata it can become out of sync
    // because current_logical_size is consistent on last_record_lsn, not ondisk_consistent_lsn
    // NOTE: current_logical_size also includes size of the ancestor
    current_logical_size: AtomicUsize, // bytes
}

/// Public interface functions
impl Timeline for LayeredTimeline {
    /// Wait until WAL has been received up to the given LSN.
    fn wait_lsn(&self, lsn: Lsn) -> Result<()> {
        // This should never be called from the WAL receiver thread, because that could lead
        // to a deadlock. FIXME: Is there a less hacky way to check that?
        assert_ne!(thread::current().name(), Some("WAL receiver thread"));

        self.last_record_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive",
                    lsn
                )
            })?;

        Ok(())
    }

    /// Look up given page version.
    fn get_page_at_lsn(&self, rel: RelishTag, blknum: u32, lsn: Lsn) -> Result<Bytes> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }
        debug_assert!(lsn <= self.get_last_record_lsn());

        let seg = SegmentTag::from_blknum(rel, blknum);

        if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
            RECONSTRUCT_TIME
                .observe_closure_duration(|| self.materialize_page(seg, blknum, lsn, &*layer))
        } else {
            bail!("relish {} not found at {}", rel, lsn);
        }
    }

    fn get_relish_size(&self, rel: RelishTag, lsn: Lsn) -> Result<Option<u32>> {
        if !rel.is_blocky() {
            bail!(
                "invalid get_relish_size request for non-blocky relish {}",
                rel
            );
        }
        debug_assert!(lsn <= self.get_last_record_lsn());

        let mut segno = 0;
        loop {
            let seg = SegmentTag { rel, segno };

            let segsize;
            if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
                segsize = layer.get_seg_size(lsn)?;
                trace!(
                    "get_seg_size: {} at {}/{} -> {}",
                    seg,
                    self.timelineid,
                    lsn,
                    segsize
                );
            } else {
                if segno == 0 {
                    return Ok(None);
                }
                segsize = 0;
            }

            if segsize != RELISH_SEG_SIZE {
                let result = segno * RELISH_SEG_SIZE + segsize;
                return Ok(Some(result));
            }
            segno += 1;
        }
    }

    fn get_rel_exists(&self, rel: RelishTag, lsn: Lsn) -> Result<bool> {
        debug_assert!(lsn <= self.get_last_record_lsn());

        let seg = SegmentTag { rel, segno: 0 };

        let result;
        if let Some((layer, lsn)) = self.get_layer_for_read(seg, lsn)? {
            result = layer.get_seg_exists(lsn)?;
        } else {
            result = false;
        }

        trace!("get_rel_exists: {} at {} -> {}", rel, lsn, result);
        Ok(result)
    }

    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        let request_tag = RelTag {
            spcnode: spcnode,
            dbnode: dbnode,
            relnode: 0,
            forknum: 0,
        };

        self.list_relishes(Some(request_tag), lsn)
    }

    fn list_nonrels(&self, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        info!("list_nonrels called at {}", lsn);

        self.list_relishes(None, lsn)
    }

    fn list_relishes(&self, tag: Option<RelTag>, lsn: Lsn) -> Result<HashSet<RelishTag>> {
        trace!("list_relishes called at {}", lsn);
        debug_assert!(lsn <= self.get_last_record_lsn());

        // List of all relishes along with a flag that marks if they exist at the given lsn.
        let mut all_relishes_map: HashMap<RelishTag, bool> = HashMap::new();
        let mut result = HashSet::new();
        let mut timeline = self;

        // Iterate through layers back in time and find the most
        // recent state of the relish. Don't add relish to the list
        // if newer version is already there.
        //
        // This most recent version can represent dropped or existing relish.
        // We will filter dropped relishes below.
        //
        loop {
            let rels = timeline.layers.lock().unwrap().list_relishes(tag, lsn)?;

            for (&new_relish, &new_relish_exists) in rels.iter() {
                match all_relishes_map.entry(new_relish) {
                    Entry::Occupied(o) => {
                        trace!(
                            "Newer version of the object {} is already found: exists {}",
                            new_relish,
                            o.get(),
                        );
                    }
                    Entry::Vacant(v) => {
                        v.insert(new_relish_exists);
                        trace!(
                            "Newer version of the object {} NOT found. Insert NEW: exists {}",
                            new_relish,
                            new_relish_exists
                        );
                    }
                }
            }

            if let Some(ancestor) = timeline.ancestor_timeline.as_ref() {
                timeline = ancestor;
                continue;
            } else {
                break;
            }
        }

        // Filter out dropped relishes
        for (&new_relish, &new_relish_exists) in all_relishes_map.iter() {
            if new_relish_exists {
                result.insert(new_relish);
                trace!("List object {}", new_relish);
            } else {
                trace!("Filter out droped object {}", new_relish);
            }
        }

        Ok(result)
    }

    fn put_wal_record(&self, rel: RelishTag, blknum: u32, rec: WALRecord) -> Result<()> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }

        let seg = SegmentTag::from_blknum(rel, blknum);
        let layer = self.get_layer_for_write(seg, rec.lsn)?;
        self.increase_current_logical_size(layer.put_wal_record(blknum, rec)? * BLCKSZ as u32);
        Ok(())
    }

    fn put_truncation(&self, rel: RelishTag, lsn: Lsn, relsize: u32) -> anyhow::Result<()> {
        if !rel.is_blocky() {
            bail!("invalid truncation for non-blocky relish {}", rel);
        }

        debug!("put_truncation: {} to {} blocks at {}", rel, relsize, lsn);

        let oldsize = self
            .get_relish_size(rel, self.get_last_record_lsn())?
            .ok_or_else(|| {
                anyhow!(
                    "attempted to truncate non-existent relish {} at {}",
                    rel,
                    lsn
                )
            })?;

        if oldsize <= relsize {
            return Ok(());
        }
        let old_last_seg = (oldsize - 1) / RELISH_SEG_SIZE;

        let last_remain_seg = if relsize == 0 {
            0
        } else {
            (relsize - 1) / RELISH_SEG_SIZE
        };

        // Drop segments beyond the last remaining segment.
        for remove_segno in (last_remain_seg + 1)..=old_last_seg {
            let seg = SegmentTag {
                rel,
                segno: remove_segno,
            };
            let layer = self.get_layer_for_write(seg, lsn)?;
            layer.drop_segment(lsn)?;
        }

        // Truncate the last remaining segment to the specified size
        if relsize == 0 || relsize % RELISH_SEG_SIZE != 0 {
            let seg = SegmentTag {
                rel,
                segno: last_remain_seg,
            };
            let layer = self.get_layer_for_write(seg, lsn)?;
            layer.put_truncation(lsn, relsize % RELISH_SEG_SIZE)?;
        }
        self.decrease_current_logical_size((oldsize - relsize) * BLCKSZ as u32);
        Ok(())
    }

    fn drop_relish(&self, rel: RelishTag, lsn: Lsn) -> Result<()> {
        trace!("drop_segment: {} at {}", rel, lsn);

        if rel.is_blocky() {
            if let Some(oldsize) = self.get_relish_size(rel, self.get_last_record_lsn())? {
                let old_last_seg = if oldsize == 0 {
                    0
                } else {
                    (oldsize - 1) / RELISH_SEG_SIZE
                };

                // Drop all segments of the relish
                for remove_segno in 0..=old_last_seg {
                    let seg = SegmentTag {
                        rel,
                        segno: remove_segno,
                    };
                    let layer = self.get_layer_for_write(seg, lsn)?;
                    layer.drop_segment(lsn)?;
                }
                self.decrease_current_logical_size(oldsize * BLCKSZ as u32);
            } else {
                warn!(
                    "drop_segment called on non-existent relish {} at {}",
                    rel, lsn
                );
            }
        } else {
            // TODO handle TwoPhase relishes
            let seg = SegmentTag::from_blknum(rel, 0);
            let layer = self.get_layer_for_write(seg, lsn)?;
            layer.drop_segment(lsn)?;
        }

        Ok(())
    }

    fn put_page_image(&self, rel: RelishTag, blknum: u32, lsn: Lsn, img: Bytes) -> Result<()> {
        if !rel.is_blocky() && blknum != 0 {
            bail!(
                "invalid request for block {} for non-blocky relish {}",
                blknum,
                rel
            );
        }

        let seg = SegmentTag::from_blknum(rel, blknum);

        let layer = self.get_layer_for_write(seg, lsn)?;
        self.increase_current_logical_size(layer.put_page_image(blknum, lsn, img)? * BLCKSZ as u32);
        Ok(())
    }

    /// Public entry point for checkpoint(). All the logic is in the private
    /// checkpoint_internal function, this public facade just wraps it for
    /// metrics collection.
    fn checkpoint(&self) -> Result<()> {
        STORAGE_TIME
            .with_label_values(&["checkpoint_force"])
            .observe_closure_duration(|| self.checkpoint_internal(true))
    }

    ///
    /// Remember the (end of) last valid WAL record remembered in the timeline.
    ///
    fn advance_last_record_lsn(&self, new_lsn: Lsn) {
        assert!(new_lsn.is_aligned());

        self.last_record_lsn.advance(new_lsn);
    }

    fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().last
    }

    fn get_prev_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().prev
    }

    fn get_last_record_rlsn(&self) -> RecordLsn {
        self.last_record_lsn.load()
    }

    fn get_start_lsn(&self) -> Lsn {
        if let Some(ancestor) = self.ancestor_timeline.as_ref() {
            ancestor.get_start_lsn()
        } else {
            self.ancestor_lsn
        }
    }

    fn get_current_logical_size(&self) -> usize {
        self.current_logical_size.load(Ordering::Acquire) as usize
    }

    fn get_current_logical_size_non_incremental(&self, lsn: Lsn) -> Result<usize> {
        let mut total_blocks: usize = 0;

        // list of all relations in this timeline, including ancestor timelines
        let all_rels = self.list_rels(0, 0, lsn)?;

        for rel in all_rels {
            if let Some(size) = self.get_relish_size(rel, lsn)? {
                total_blocks += size as usize;
            }
        }

        let non_rels = self.list_nonrels(lsn)?;
        for non_rel in non_rels {
            // TODO support TwoPhase
            if matches!(non_rel, RelishTag::Slru { slru: _, segno: _ }) {
                if let Some(size) = self.get_relish_size(non_rel, lsn)? {
                    total_blocks += size as usize;
                }
            }
        }

        Ok(total_blocks * BLCKSZ as usize)
    }
}

impl LayeredTimeline {
    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory, but not the layer map.
    fn new(
        conf: &'static PageServerConf,
        metadata: TimelineMetadata,
        ancestor: Option<Arc<LayeredTimeline>>,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        current_logical_size: usize,
    ) -> Result<LayeredTimeline> {
        let timeline = LayeredTimeline {
            conf,
            timelineid,
            tenantid,
            layers: Mutex::new(LayerMap::default()),

            walredo_mgr,

            // initialize in-memory 'last_record_lsn' from 'disk_consistent_lsn'.
            last_record_lsn: SeqWait::new(RecordLsn {
                last: metadata.disk_consistent_lsn,
                prev: metadata.prev_record_lsn.unwrap_or(Lsn(0)),
            }),
            disk_consistent_lsn: AtomicLsn::new(metadata.disk_consistent_lsn.0),

            ancestor_timeline: ancestor,
            ancestor_lsn: metadata.ancestor_lsn,
            current_logical_size: AtomicUsize::new(current_logical_size),
        };
        Ok(timeline)
    }

    ///
    /// Scan the timeline directory to populate the layer map
    ///
    fn load_layer_map(&self) -> anyhow::Result<()> {
        info!(
            "loading layer map for timeline {} into memory",
            self.timelineid
        );
        let mut layers = self.layers.lock().unwrap();
        let (imgfilenames, mut deltafilenames) =
            filename::list_files(self.conf, self.timelineid, self.tenantid)?;

        // First create ImageLayer structs for each image file.
        for filename in imgfilenames.iter() {
            let layer = ImageLayer::new(self.conf, self.timelineid, self.tenantid, filename);

            info!(
                "found layer {} {} on timeline {}",
                layer.get_seg_tag(),
                layer.get_start_lsn(),
                self.timelineid
            );
            layers.insert_historic(Arc::new(layer));
        }

        // Then for the Delta files. The delta files are created in order starting
        // from the oldest file, because each DeltaLayer needs a reference to its
        // predecessor.
        deltafilenames.sort();

        for filename in deltafilenames.iter() {
            let predecessor = layers.get(&filename.seg, filename.start_lsn);

            let predecessor_str: String = if let Some(prec) = &predecessor {
                prec.filename().display().to_string()
            } else {
                "none".to_string()
            };

            let layer = DeltaLayer::new(
                self.conf,
                self.timelineid,
                self.tenantid,
                filename,
                predecessor,
            );

            info!(
                "found layer {} on timeline {}, predecessor: {}",
                layer.filename().display(),
                self.timelineid,
                predecessor_str,
            );
            layers.insert_historic(Arc::new(layer));
        }

        Ok(())
    }

    ///
    /// Used to init current logical size on startup
    ///
    fn init_current_logical_size(&mut self) -> Result<()> {
        if self.current_logical_size.load(Ordering::Relaxed) != 0 {
            bail!("cannot init already initialized current logical size")
        };
        let lsn = self.get_last_record_lsn();
        self.current_logical_size =
            AtomicUsize::new(self.get_current_logical_size_non_incremental(lsn)?);
        trace!(
            "current_logical_size initialized to {}",
            self.current_logical_size.load(Ordering::Relaxed)
        );
        Ok(())
    }

    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned Layer might be from an ancestor timeline, if the
    /// segment hasn't been updated on this timeline yet.
    ///
    fn get_layer_for_read(
        &self,
        seg: SegmentTag,
        lsn: Lsn,
    ) -> Result<Option<(Arc<dyn Layer>, Lsn)>> {
        let self_layers = self.layers.lock().unwrap();
        self.get_layer_for_read_locked(seg, lsn, &self_layers)
    }

    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned Layer might be from an ancestor timeline, if the
    /// segment hasn't been updated on this timeline yet.
    ///
    /// This function takes the current timeline's locked LayerMap as an argument,
    /// so callers can avoid potential race conditions.
    fn get_layer_for_read_locked(
        &self,
        seg: SegmentTag,
        lsn: Lsn,
        self_layers: &MutexGuard<LayerMap>,
    ) -> Result<Option<(Arc<dyn Layer>, Lsn)>> {
        trace!(
            "get_layer_for_read called for {} at {}/{}",
            seg,
            self.timelineid,
            lsn
        );

        // If you requested a page at an older LSN, before the branch point, dig into
        // the right ancestor timeline. This can only happen if you launch a read-only
        // node with an old LSN, a primary always uses a recent LSN in its requests.
        let mut timeline = self;
        let mut lsn = lsn;

        while lsn < timeline.ancestor_lsn {
            trace!("going into ancestor {} ", timeline.ancestor_lsn);
            timeline = timeline.ancestor_timeline.as_ref().unwrap();
        }

        // Now we have the right starting timeline for our search.
        loop {
            let layers_owned: MutexGuard<LayerMap>;
            let layers = if self as *const LayeredTimeline != timeline as *const LayeredTimeline {
                layers_owned = timeline.layers.lock().unwrap();
                &layers_owned
            } else {
                self_layers
            };

            //
            // FIXME: If the relation has been dropped, does this return the right
            // thing? The compute node should not normally request dropped relations,
            // but if OID wraparound happens the same relfilenode might get reused
            // for an unrelated relation.
            //

            // Do we have a layer on this timeline?
            if let Some(layer) = layers.get(&seg, lsn) {
                trace!(
                    "found layer in cache: {} {}-{}",
                    timeline.timelineid,
                    layer.get_start_lsn(),
                    layer.get_end_lsn()
                );

                assert!(layer.get_start_lsn() <= lsn);

                if layer.is_dropped() && layer.get_end_lsn() <= lsn {
                    return Ok(None);
                }

                return Ok(Some((layer.clone(), lsn)));
            }

            // If not, check if there's a layer on the ancestor timeline
            if let Some(ancestor) = &timeline.ancestor_timeline {
                lsn = timeline.ancestor_lsn;
                timeline = ancestor.as_ref();
                trace!("recursing into ancestor at {}/{}", timeline.timelineid, lsn);
                continue;
            }
            return Ok(None);
        }
    }

    ///
    /// Get a handle to the latest layer for appending.
    ///
    fn get_layer_for_write(&self, seg: SegmentTag, lsn: Lsn) -> Result<Arc<InMemoryLayer>> {
        let mut layers = self.layers.lock().unwrap();

        assert!(lsn.is_aligned());

        let last_record_lsn = self.get_last_record_lsn();
        if lsn <= last_record_lsn {
            panic!(
                "cannot modify relation after advancing last_record_lsn (incoming_lsn={}, last_record_lsn={})",
                lsn,
                last_record_lsn
            );
        }

        // Do we have a layer open for writing already?
        if let Some(layer) = layers.get_open(&seg) {
            if layer.get_start_lsn() > lsn {
                bail!("unexpected open layer in the future");
            }
            return Ok(layer);
        }

        // No (writeable) layer for this relation yet. Create one.
        //
        // Is this a completely new relation? Or the first modification after branching?
        //

        let layer;
        if let Some((prev_layer, _prev_lsn)) = self.get_layer_for_read_locked(seg, lsn, &layers)? {
            // Create new entry after the previous one.
            let start_lsn;
            if prev_layer.get_timeline_id() != self.timelineid {
                // First modification on this timeline
                start_lsn = self.ancestor_lsn;
                trace!(
                    "creating file for write for {} at branch point {}/{}",
                    seg,
                    self.timelineid,
                    start_lsn
                );
            } else {
                start_lsn = prev_layer.get_end_lsn();
                trace!(
                    "creating file for write for {} after previous layer {}/{}",
                    seg,
                    self.timelineid,
                    start_lsn
                );
            }
            trace!(
                "prev layer is at {}/{} - {}",
                prev_layer.get_timeline_id(),
                prev_layer.get_start_lsn(),
                prev_layer.get_end_lsn()
            );
            layer = InMemoryLayer::create_successor_layer(
                self.conf,
                prev_layer,
                self.timelineid,
                self.tenantid,
                start_lsn,
                lsn,
            )?;
        } else {
            // New relation.
            trace!(
                "creating layer for write for new rel {} at {}/{}",
                seg,
                self.timelineid,
                lsn
            );

            layer =
                InMemoryLayer::create(self.conf, self.timelineid, self.tenantid, seg, lsn, lsn)?;
        }

        let layer_rc: Arc<InMemoryLayer> = Arc::new(layer);
        layers.insert_open(Arc::clone(&layer_rc));

        Ok(layer_rc)
    }

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint_internal(&self, force: bool) -> Result<()> {
        // Grab lock on the layer map.
        //
        // TODO: We hold it locked throughout the checkpoint operation. That's bad,
        // the checkpointing could take many seconds, and any incoming get_page_at_lsn()
        // requests will block.
        let mut layers = self.layers.lock().unwrap();

        // Bump the generation number in the layer map, so that we can distinguish
        // entries inserted after the checkpoint started
        let current_generation = layers.increment_generation();

        // Read 'last_record_lsn'. That becomes the cutoff LSN for frozen layers.
        let RecordLsn {
            last: last_record_lsn,
            prev: prev_record_lsn,
        } = self.last_record_lsn.load();

        trace!(
            "checkpointing timeline {} at {}",
            self.timelineid,
            last_record_lsn
        );

        // Take the in-memory layer with the oldest WAL record. If it's older
        // than the threshold, write it out to disk as a new image and delta file.
        // Repeat until all remaining in-memory layers are within the threshold.
        //
        // That's necessary to limit the amount of WAL that needs to be kept
        // in the safekeepers, and that needs to be reprocessed on page server
        // crash. TODO: It's not a great policy for keeping memory usage in
        // check, though. We should also aim at flushing layers that consume
        // a lot of memory and/or aren't receiving much updates anymore.
        let mut disk_consistent_lsn = last_record_lsn;

        while let Some((oldest_layer, oldest_generation)) = layers.peek_oldest_open() {
            let oldest_pending_lsn = oldest_layer.get_oldest_pending_lsn();

            // Does this layer need freezing?
            //
            // Write out all in-memory layers that contain WAL older than OLDEST_INMEM_DISTANCE.
            // Or if 'force' is true, write out all of them. If we reach a layer with the same
            // generation number, we know that we have cycled through all layers that were open
            // when we started. We don't want to process layers inserted after we started, to
            // avoid getting into an infinite loop trying to process again entries that we
            // inserted ourselves.
            let distance = last_record_lsn.widening_sub(oldest_pending_lsn);
            if distance < 0
                || (!force && distance < OLDEST_INMEM_DISTANCE)
                || oldest_generation == current_generation
            {
                info!(
                    "the oldest layer is now {} which is {} bytes behind last_record_lsn",
                    oldest_layer.filename().display(),
                    distance
                );
                disk_consistent_lsn = oldest_pending_lsn;
                break;
            }

            // freeze it
            let FreezeLayers {
                frozen,
                open: maybe_new_open,
            } = oldest_layer.freeze(last_record_lsn)?;

            let new_historics = frozen.write_to_disk(self)?;

            if let Some(last_historic) = new_historics.last() {
                if let Some(new_open) = &maybe_new_open {
                    new_open.update_predecessor(Arc::clone(last_historic));
                }
            }

            // replace this layer with the new layers that 'freeze' returned
            layers.pop_oldest_open();
            if let Some(n) = maybe_new_open {
                layers.insert_open(n);
            }
            for n in new_historics {
                layers.insert_historic(n);
            }
        }

        // Call unload() on all frozen layers, to release memory.
        // This shouldn't be much memory, as only metadata is slurped
        // into memory.
        for layer in layers.iter_historic_layers() {
            layer.unload()?;
        }

        // Save the metadata, with updated 'disk_consistent_lsn', to a
        // file in the timeline dir. After crash, we will restart WAL
        // streaming and processing from that point.

        // We can only save a valid 'prev_record_lsn' value on disk if we
        // flushed *all* in-memory changes to disk. We only track
        // 'prev_record_lsn' in memory for the latest processed record, so we
        // don't remember what the correct value that corresponds to some old
        // LSN is. But if we flush everything, then the value corresponding
        // current 'last_record_lsn' is correct and we can store it on disk.
        let ondisk_prev_record_lsn = {
            if disk_consistent_lsn == last_record_lsn {
                Some(prev_record_lsn)
            } else {
                None
            }
        };

        let ancestor_timelineid = self.ancestor_timeline.as_ref().map(|x| x.timelineid);

        let metadata = TimelineMetadata {
            disk_consistent_lsn,
            prev_record_lsn: ondisk_prev_record_lsn,
            ancestor_timeline: ancestor_timelineid,
            ancestor_lsn: self.ancestor_lsn,
        };
        LayeredRepository::save_metadata(self.conf, self.timelineid, self.tenantid, &metadata)?;

        // Also update the in-memory copy
        self.disk_consistent_lsn.store(disk_consistent_lsn);

        Ok(())
    }

    ///
    /// Garbage collect layer files on a timeline that are no longer needed.
    ///
    /// The caller specifies how much history is needed with the two arguments:
    ///
    /// retain_lsns: keep a version of each page at these LSNs
    /// cutoff: also keep everything newer than this LSN
    ///
    /// The 'retain_lsns' list is currently used to prevent removing files that
    /// are needed by child timelines. In the future, the user might be able to
    /// name additional points in time to retain. The caller is responsible for
    /// collecting that information.
    ///
    /// The 'cutoff' point is used to retain recent versions that might still be
    /// needed by read-only nodes. (As of this writing, the caller just passes
    /// the latest LSN subtracted by a constant, and doesn't do anything smart
    /// to figure out what read-only nodes might actually need.)
    ///
    /// Currently, we don't make any attempt at removing unneeded page versions
    /// within a layer file. We can only remove the whole file if it's fully
    /// obsolete.
    ///
    pub fn gc_timeline(&self, retain_lsns: Vec<Lsn>, cutoff: Lsn) -> Result<GcResult> {
        let now = Instant::now();
        let mut result: GcResult = Default::default();

        info!(
            "running GC on timeline {}, cutoff {}",
            self.timelineid, cutoff
        );

        let mut layers_to_remove: Vec<Arc<dyn Layer>> = Vec::new();

        // Scan all layer files in the directory. For each file, if a newer file
        // exists, we can remove the old one.
        //
        // Determine for each file if it needs to be retained
        // FIXME: also scan open in-memory layers. Normally we cannot remove the
        // latest layer of any seg, but if it was dropped it's possible
        let mut layers = self.layers.lock().unwrap();
        'outer: for l in layers.iter_historic_layers() {
            let seg = l.get_seg_tag();

            if seg.rel.is_relation() {
                result.ondisk_relfiles_total += 1;
            } else {
                result.ondisk_nonrelfiles_total += 1;
            }

            // Is it newer than cutoff point?
            if l.get_end_lsn() > cutoff {
                info!(
                    "keeping {} {}-{} because it's newer than cutoff {}",
                    seg,
                    l.get_start_lsn(),
                    l.get_end_lsn(),
                    cutoff
                );
                if seg.rel.is_relation() {
                    result.ondisk_relfiles_needed_by_cutoff += 1;
                } else {
                    result.ondisk_nonrelfiles_needed_by_cutoff += 1;
                }
                continue 'outer;
            }

            // Is it needed by a child branch?
            for retain_lsn in &retain_lsns {
                // FIXME: are the bounds inclusive or exclusive?
                if l.get_start_lsn() <= *retain_lsn && *retain_lsn <= l.get_end_lsn() {
                    info!(
                        "keeping {} {}-{} because it's needed by branch point {}",
                        seg,
                        l.get_start_lsn(),
                        l.get_end_lsn(),
                        *retain_lsn
                    );
                    if seg.rel.is_relation() {
                        result.ondisk_relfiles_needed_by_branches += 1;
                    } else {
                        result.ondisk_nonrelfiles_needed_by_branches += 1;
                    }
                    continue 'outer;
                }
            }

            // Unless the relation was dropped, is there a later image file for this relation?
            if !l.is_dropped() && !layers.newer_image_layer_exists(l.get_seg_tag(), l.get_end_lsn())
            {
                if seg.rel.is_relation() {
                    result.ondisk_relfiles_not_updated += 1;
                } else {
                    result.ondisk_nonrelfiles_not_updated += 1;
                }
                continue 'outer;
            }

            // We didn't find any reason to keep this file, so remove it.
            info!(
                "garbage collecting {} {}-{} {}",
                l.get_seg_tag(),
                l.get_start_lsn(),
                l.get_end_lsn(),
                l.is_dropped()
            );
            layers_to_remove.push(Arc::clone(&l));
        }

        // Actually delete the layers from disk and remove them from the map.
        // (couldn't do this in the loop above, because you cannot modify a collection
        // while iterating it. BTreeMap::retain() would be another option)
        for doomed_layer in layers_to_remove {
            doomed_layer.delete()?;
            layers.remove_historic(&*doomed_layer);

            match (
                doomed_layer.is_dropped(),
                doomed_layer.get_seg_tag().rel.is_relation(),
            ) {
                (true, true) => result.ondisk_relfiles_dropped += 1,
                (true, false) => result.ondisk_nonrelfiles_dropped += 1,
                (false, true) => result.ondisk_relfiles_removed += 1,
                (false, false) => result.ondisk_nonrelfiles_removed += 1,
            }
        }

        result.elapsed = now.elapsed();
        Ok(result)
    }

    ///
    /// Reconstruct a page version from given Layer
    ///
    fn materialize_page(
        &self,
        seg: SegmentTag,
        blknum: u32,
        lsn: Lsn,
        layer: &dyn Layer,
    ) -> Result<Bytes> {
        let mut data = PageReconstructData {
            records: Vec::new(),
            page_img: None,
        };

        // Holds an Arc reference to 'layer_ref' when iterating in the loop below.
        let mut layer_arc: Arc<dyn Layer>;

        // Call the layer's get_page_reconstruct_data function to get the base image
        // and WAL records needed to materialize the page. If it returns 'Continue',
        // call it again on the predecessor layer until we have all the required data.
        let mut layer_ref = layer;
        let mut curr_lsn = lsn;
        loop {
            match layer_ref.get_page_reconstruct_data(blknum, curr_lsn, &mut data)? {
                PageReconstructResult::Complete => break,
                PageReconstructResult::Continue(cont_lsn, cont_layer) => {
                    // Fetch base image / more WAL from the returned predecessor layer
                    layer_arc = cont_layer;
                    layer_ref = &*layer_arc;
                    curr_lsn = cont_lsn;
                    continue;
                }
                PageReconstructResult::Missing(lsn) => {
                    // Oops, we could not reconstruct the page.
                    if data.records.is_empty() {
                        // no records, and no base image. This can happen if PostgreSQL extends a relation
                        // but never writes the page.
                        //
                        // Would be nice to detect that situation better.
                        warn!("Page {} blk {} at {} not found", seg.rel, blknum, lsn);
                        return Ok(ZERO_PAGE.clone());
                    }
                    bail!(
                        "No base image found for page {} blk {} at {}/{}",
                        seg.rel,
                        blknum,
                        self.timelineid,
                        lsn,
                    );
                }
            }
        }

        self.reconstruct_page(seg.rel, blknum, lsn, data)
    }

    ///
    /// Reconstruct a page version, using the given base image and WAL records in 'data'.
    ///
    fn reconstruct_page(
        &self,
        rel: RelishTag,
        blknum: u32,
        request_lsn: Lsn,
        mut data: PageReconstructData,
    ) -> Result<Bytes> {
        // Perform WAL redo if needed
        data.records.reverse();

        // If we have a page image, and no WAL, we're all set
        if data.records.is_empty() {
            if let Some(img) = &data.page_img {
                trace!(
                    "found page image for blk {} in {} at {}/{}, no WAL redo required",
                    blknum,
                    rel,
                    self.timelineid,
                    request_lsn
                );
                Ok(img.clone())
            } else {
                // FIXME: this ought to be an error?
                warn!("Page {} blk {} at {} not found", rel, blknum, request_lsn);
                Ok(ZERO_PAGE.clone())
            }
        } else {
            // We need to do WAL redo.
            //
            // If we don't have a base image, then the oldest WAL record better initialize
            // the page
            if data.page_img.is_none() && !data.records.first().unwrap().will_init {
                // FIXME: this ought to be an error?
                warn!(
                    "Base image for page {}/{} at {} not found, but got {} WAL records",
                    rel,
                    blknum,
                    request_lsn,
                    data.records.len()
                );
                Ok(ZERO_PAGE.clone())
            } else {
                if data.page_img.is_some() {
                    trace!("found {} WAL records and a base image for blk {} in {} at {}/{}, performing WAL redo", data.records.len(), blknum, rel, self.timelineid, request_lsn);
                } else {
                    trace!("found {} WAL records that will init the page for blk {} in {} at {}/{}, performing WAL redo", data.records.len(), blknum, rel, self.timelineid, request_lsn);
                }
                let img = self.walredo_mgr.request_redo(
                    rel,
                    blknum,
                    request_lsn,
                    data.page_img.clone(),
                    data.records,
                )?;

                Ok(img)
            }
        }
    }

    ///
    /// This is a helper function to increase current_total_relation_size
    ///
    fn increase_current_logical_size(&self, diff: u32) {
        let val = self
            .current_logical_size
            .fetch_add(diff as usize, Ordering::SeqCst);
        trace!(
            "increase_current_logical_size: {} + {} = {}",
            val,
            diff,
            val + diff as usize,
        );
        LOGICAL_TIMELINE_SIZE
            .with_label_values(&[&self.tenantid.to_string(), &self.timelineid.to_string()])
            .set(val as i64 + diff as i64)
    }

    ///
    /// This is a helper function to decrease current_total_relation_size
    ///
    fn decrease_current_logical_size(&self, diff: u32) {
        let val = self
            .current_logical_size
            .fetch_sub(diff as usize, Ordering::SeqCst);
        trace!(
            "decrease_current_logical_size: {} - {} = {}",
            val,
            diff,
            val - diff as usize,
        );
        LOGICAL_TIMELINE_SIZE
            .with_label_values(&[&self.tenantid.to_string(), &self.timelineid.to_string()])
            .set(val as i64 - diff as i64)
    }
}

/// Dump contents of a layer file to stdout.
pub fn dump_layerfile_from_path(path: &Path) -> Result<()> {
    let fname = path.file_name().unwrap().to_str().unwrap();

    let dummy_tenantid = ZTenantId::from_str("00000000000000000000000000000000")?;
    let dummy_timelineid = ZTimelineId::from_str("00000000000000000000000000000000")?;

    if let Some(deltafilename) = DeltaFileName::from_str(fname) {
        DeltaLayer::new_for_path(path, dummy_timelineid, dummy_tenantid, &deltafilename).dump()?;
    } else if let Some(imgfilename) = ImageFileName::from_str(fname) {
        ImageLayer::new_for_path(path, dummy_timelineid, dummy_tenantid, &imgfilename).dump()?;
    } else {
        bail!("unrecognized layer file name : {}", fname);
    }

    Ok(())
}
