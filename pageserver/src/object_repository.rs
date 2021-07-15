//!
//! Implementation of the Repository/Timeline traits, using a key-value store
//! (ObjectStore) to for the actual storage.
//!
//! This maps the relation-oriented operations in the Timeline interface into
//! objects stored in an ObjectStore. Relation size is stored as a separate object
//! in the key-value store. If a page is written beyond the current end-of-file,
//! we also insert the new size as a new "page version" in the key-value store.
//!
//! Also, this implements Copy-on-Write forking of timelines. For each timeline,
//! we store the parent timeline in the object store, in a little metadata blob.
//! When we need to find a version of a page, we walk the timeline history backwards
//! until we find the page we're looking for, making a separate lookup into the
//! key-value store for each timeline.

use crate::object_key::*;
use crate::object_store::ObjectStore;
use crate::repository::*;
use crate::restore_local_repo::import_timeline_wal;
use crate::walredo::WalRedoManager;
use crate::{PageServerConf, ZTimelineId};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::*;
use postgres_ffi::pg_constants;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryInto;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
///
pub struct ObjectRepository {
    obj_store: Arc<dyn ObjectStore>,
    conf: &'static PageServerConf,
    timelines: Mutex<HashMap<ZTimelineId, Arc<ObjectTimeline>>>,
    walredo_mgr: Arc<dyn WalRedoManager>,
}

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(600);

impl ObjectRepository {
    pub fn new(
        conf: &'static PageServerConf,
        obj_store: Arc<dyn ObjectStore>,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> ObjectRepository {
        ObjectRepository {
            conf,
            obj_store,
            timelines: Mutex::new(HashMap::new()),
            walredo_mgr,
        }
    }
}

impl Repository for ObjectRepository {
    /// Get Timeline handle for given zenith timeline ID.
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let timeline = ObjectTimeline::open(
                    Arc::clone(&self.obj_store),
                    timelineid,
                    self.walredo_mgr.clone(),
                )?;

                // Load any new WAL after the last checkpoint into the repository.
                info!(
                    "Loading WAL for timeline {} starting at {}",
                    timelineid,
                    timeline.get_last_record_lsn()
                );
                let wal_dir = self.conf.timeline_path(timelineid).join("wal");
                import_timeline_wal(&wal_dir, &timeline, timeline.get_last_record_lsn())?;

                let timeline_rc = Arc::new(timeline);

                if self.conf.gc_horizon != 0 {
                    ObjectTimeline::launch_gc_thread(self.conf, timeline_rc.clone());
                }

                timelines.insert(timelineid, timeline_rc.clone());

                Ok(timeline_rc)
            }
        }
    }

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        start_lsn: Lsn,
    ) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        // Write initial metadata key.
        let metadata = MetadataEntry {
            last_valid_lsn: start_lsn,
            last_record_lsn: start_lsn,
            prev_record_lsn: Lsn(0),
            ancestor_timeline: None,
            ancestor_lsn: start_lsn,
        };
        let val = ObjectValue::TimelineMetadata(metadata);
        self.obj_store.put(
            &timeline_metadata_key(timelineid),
            Lsn(0),
            &ObjectValue::ser(&val)?,
        )?;

        info!("Created empty timeline {}", timelineid);

        let timeline = ObjectTimeline::open(
            Arc::clone(&self.obj_store),
            timelineid,
            self.walredo_mgr.clone(),
        )?;

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());

        // don't start the garbage collector for unit tests, either.

        Ok(timeline_rc)
    }

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, at_lsn: Lsn) -> Result<()> {
        let src_timeline = self.get_timeline(src)?;

        // Write a metadata key, noting the ancestor of th new timeline. There is initially
        // no data in it, but all the read-calls know to look into the ancestor.
        let metadata = MetadataEntry {
            last_valid_lsn: at_lsn,
            last_record_lsn: at_lsn,
            prev_record_lsn: src_timeline.get_prev_record_lsn(),
            ancestor_timeline: Some(src),
            ancestor_lsn: at_lsn,
        };
        let val = ObjectValue::TimelineMetadata(metadata);
        self.obj_store.put(
            &timeline_metadata_key(dst),
            Lsn(0),
            &ObjectValue::ser(&val)?,
        )?;

        // Copy non-rel objects
        for tag in src_timeline.list_nonrels(at_lsn)? {
            match tag {
                ObjectTag::TimelineMetadataTag => {} // skip it
                _ => {
                    let img = src_timeline.get_page_at_lsn_nowait(tag, at_lsn)?;
                    let val = ObjectValue::Page(PageEntry::Page(img));
                    let key = ObjectKey { timeline: dst, tag };
                    self.obj_store.put(&key, at_lsn, &ObjectValue::ser(&val)?)?;
                }
            }
        }
        Ok(())
    }
}

///
/// Relation metadata (currently only size) is stored in separate record
/// in object store and is update by each put operation: we need to check if
/// new block size is greater or equal than existed relation size and if so: update
/// the relation metadata. So it requires extra object storage lookup at each
/// put operation. To avoid this lookup we maintain in-memory cache of relation metadata.
/// To prevent memory overflow metadata only of the most recent version of relation is cached.
/// If page server needs to access some older version, then object storage has to be accessed.
///
struct RelMetadata {
    size: u32,         // size of relation
    last_updated: Lsn, // lsn of last metadata update (used to determine when cache value can be used)
}

///
/// A handle to a specific timeline in the repository. This is the API
/// that's exposed to the rest of the system.
///
pub struct ObjectTimeline {
    timelineid: ZTimelineId,

    // Backing key-value store
    obj_store: Arc<dyn ObjectStore>,

    // WAL redo manager, for reconstructing page versions from WAL records.
    walredo_mgr: Arc<dyn WalRedoManager>,

    // What page versions do we hold in the cache? If we get a request > last_valid_lsn,
    // we need to wait until we receive all the WAL up to the request. The SeqWait
    // provides functions for that. TODO: If we get a request for an old LSN, such that
    // the versions have already been garbage collected away, we should throw an error,
    // but we don't track that currently.
    //
    // last_record_lsn points to the end of last processed WAL record.
    // It can lag behind last_valid_lsn, if the WAL receiver has received some WAL
    // after the end of last record, but not the whole next record yet. In the
    // page cache, we care about last_valid_lsn, but if the WAL receiver needs to
    // restart the streaming, it needs to restart at the end of last record, so
    // we track them separately. last_record_lsn should perhaps be in
    // walreceiver.rs instead of here, but it seems convenient to keep all three
    // values together.
    //
    last_valid_lsn: SeqWait<Lsn>,
    last_record_lsn: AtomicLsn,
    prev_record_lsn: AtomicLsn,

    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,

    rel_meta: RwLock<BTreeMap<RelTag, RelMetadata>>,
}

impl ObjectTimeline {
    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory.
    fn open(
        obj_store: Arc<dyn ObjectStore>,
        timelineid: ZTimelineId,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> Result<ObjectTimeline> {
        // Load metadata into memory
        let v = obj_store
            .get(&timeline_metadata_key(timelineid), Lsn(0))
            .with_context(|| "timeline not found in repository")?;
        let metadata = ObjectValue::des_timeline_metadata(&v)?;

        let timeline = ObjectTimeline {
            timelineid,
            obj_store,
            walredo_mgr,
            last_valid_lsn: SeqWait::new(metadata.last_valid_lsn),
            last_record_lsn: AtomicLsn::new(metadata.last_record_lsn.0),
            prev_record_lsn: AtomicLsn::new(metadata.prev_record_lsn.0),
            ancestor_timeline: metadata.ancestor_timeline,
            ancestor_lsn: metadata.ancestor_lsn,
            rel_meta: RwLock::new(BTreeMap::new()),
        };
        Ok(timeline)
    }
}

impl Timeline for ObjectTimeline {
    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, tag: ObjectTag, req_lsn: Lsn) -> Result<Bytes> {
        let lsn = self.wait_lsn(req_lsn)?;

        self.get_page_at_lsn_nowait(tag, lsn)
    }

    fn get_page_at_lsn_nowait(&self, tag: ObjectTag, req_lsn: Lsn) -> Result<Bytes> {
        const ZERO_PAGE: [u8; 8192] = [0u8; 8192];
        // Look up the page entry. If it's a page image, return that. If it's a WAL record,
        // ask the WAL redo service to reconstruct the page image from the WAL records.
        let searchkey = ObjectKey {
            timeline: self.timelineid,
            tag,
        };
        let mut iter = self.object_versions(&*self.obj_store, &searchkey, req_lsn)?;

        if let Some((lsn, value)) = iter.next().transpose()? {
            let page_img: Bytes;

            match ObjectValue::des(&value)? {
                ObjectValue::Page(PageEntry::Page(img)) => {
                    page_img = img;
                }
                ObjectValue::Page(PageEntry::WALRecord(_rec)) => {
                    // Request the WAL redo manager to apply the WAL records for us.
                    let (base_img, records) = self.collect_records_for_apply(tag, lsn)?;
                    page_img = self.walredo_mgr.request_redo(tag, lsn, base_img, records)?;

                    // Garbage collection assumes that we remember the materialized page
                    // version. Otherwise we could opt to not do it, with the downside that
                    // the next GetPage@LSN call of the same page version would have to
                    // redo the WAL again.
                    self.put_page_image(tag, lsn, page_img.clone(), false)?;
                }
                ObjectValue::SLRUTruncate => page_img = Bytes::from_static(&ZERO_PAGE),
                _ => bail!("Invalid object kind, expected a page entry or SLRU truncate"),
            }
            // FIXME: assumes little-endian. Only used for the debugging log though
            let page_lsn_hi = u32::from_le_bytes(page_img.get(0..4).unwrap().try_into().unwrap());
            let page_lsn_lo = u32::from_le_bytes(page_img.get(4..8).unwrap().try_into().unwrap());
            trace!(
                "Returning page with LSN {:X}/{:X} for {:?} from {} (request {})",
                page_lsn_hi,
                page_lsn_lo,
                tag,
                lsn,
                req_lsn
            );
            return Ok(page_img);
        }
        trace!("page {:?} at {} not found", tag, req_lsn);
        Ok(Bytes::from_static(&ZERO_PAGE))
        /* return Err("could not find page image")?; */
    }

    /// Get size of relation
    fn get_rel_size(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        let lsn = self.wait_lsn(lsn)?;

        match self.relsize_get_nowait(rel, lsn)? {
            Some(nblocks) => Ok(nblocks),
            None => bail!("relation {} not found at {}", rel, lsn),
        }
    }

    /// Does relation exist at given LSN?
    fn get_rel_exists(&self, rel: RelTag, req_lsn: Lsn) -> Result<bool> {
        let lsn = self.wait_lsn(req_lsn)?;
        {
            let rel_meta = self.rel_meta.read().unwrap();
            if let Some(meta) = rel_meta.get(&rel) {
                if meta.last_updated <= lsn {
                    return Ok(true);
                }
            }
        }
        let key = relation_size_key(self.timelineid, rel);
        let mut iter = self.object_versions(&*self.obj_store, &key, lsn)?;
        if let Some((_key, _val)) = iter.next().transpose()? {
            debug!("Relation {} exists at {}", rel, lsn);
            return Ok(true);
        }
        debug!("Relation {} doesn't exist at {}", rel, lsn);
        Ok(false)
    }

    /// Get a list of non-relational objects
    fn list_nonrels<'a>(&'a self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = ObjectTag> + 'a>> {
        self.obj_store.list_objects(self.timelineid, true, lsn)
    }

    // Get total size (in bytes) of all relations in the timeline
    fn get_timeline_size(&self, lsn: Lsn) -> Result<u64>
    {
        // list of all relations in this timeline, including ancestor timelines
        let all_rels = self.list_rels(0, 0, lsn)?;

        let mut total_blocks: u64 = 0;

        // This calculation is quite slow,
        // but now we do it only once at compute node start.
        for rel in all_rels {
            match self.relsize_get_nowait(rel, lsn)? {
                Some(nblocks) => { total_blocks += nblocks as u64 },
                None => {},
            }
        }

        Ok(total_blocks * pg_constants::BLCKSZ as u64)
    }

    /// Get a list of all distinct relations in given tablespace and database.
    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>> {
        // List all relations in this timeline.
        let mut all_rels = self
            .obj_store
            .list_rels(self.timelineid, spcnode, dbnode, lsn)?;

        // Also list all relations in ancestor timelines. If a relation hasn't been modified
        // after the fork, there will be no trace of it in the object store with the current
        // timeline id.
        let mut prev_timeline: Option<ZTimelineId> = self.ancestor_timeline;
        let mut lsn = self.ancestor_lsn;
        while let Some(timeline) = prev_timeline {
            let this_rels = self.obj_store.list_rels(timeline, spcnode, dbnode, lsn)?;

            for rel in this_rels {
                all_rels.insert(rel);
            }

            // Load ancestor metadata.
            let v = self
                .obj_store
                .get(&timeline_metadata_key(timeline), Lsn(0))
                .with_context(|| "timeline not found in repository")?;
            let metadata = ObjectValue::des_timeline_metadata(&v)?;

            prev_timeline = metadata.ancestor_timeline;
            lsn = metadata.ancestor_lsn;
        }

        Ok(all_rels)
    }

    //------------------------------------------------------------------------------
    // Public PUT functions, to update the repository with new page versions.
    //
    // These are called by the WAL receiver to digest WAL records.
    //------------------------------------------------------------------------------

    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    fn put_wal_record(&self, tag: ObjectTag, rec: WALRecord) -> Result<()> {
        let lsn = rec.lsn;

        self.put_page_entry(&tag, lsn, PageEntry::WALRecord(rec))?;
        debug!("put_wal_record {:?} at {}", tag, lsn);

        if let ObjectTag::RelationBuffer(tag) = tag {
            // Also check if this created or extended the file
            let old_nblocks = self.relsize_get_nowait(tag.rel, lsn)?.unwrap_or(0);

            if tag.blknum >= old_nblocks {
                let new_nblocks = tag.blknum + 1;

                trace!(
                    "Extended relation {} from {} to {} blocks at {}",
                    tag.rel,
                    old_nblocks,
                    new_nblocks,
                    lsn
                );

                self.put_relsize_entry(&tag.rel, lsn, RelationSizeEntry::Size(new_nblocks))?;
                let mut rel_meta = self.rel_meta.write().unwrap();
                rel_meta.insert(
                    tag.rel,
                    RelMetadata {
                        size: new_nblocks,
                        last_updated: lsn,
                    },
                );
            }
        }
        Ok(())
    }

    /// Unlink relation. This method is used for marking dropped relations.
    fn put_unlink(&self, rel_tag: RelTag, lsn: Lsn) -> Result<()> {
        self.put_relsize_entry(&rel_tag, lsn, RelationSizeEntry::Unlink)?;

        Ok(())
    }

    /// Truncate SLRU segment
    fn put_slru_truncate(&self, tag: ObjectTag, lsn: Lsn) -> Result<()> {
        let key = ObjectKey {
            timeline: self.timelineid,
            tag,
        };
        let val = ObjectValue::SLRUTruncate;
        self.obj_store.put(&key, lsn, &ObjectValue::ser(&val)?)?;
        Ok(())
    }

    fn get_next_tag(&self, tag: ObjectTag) -> Result<Option<ObjectTag>> {
        let key = ObjectKey {
            timeline: self.timelineid,
            tag,
        };
        if let Some(key) = self.obj_store.get_next_key(&key)? {
            Ok(Some(key.tag))
        } else {
            Ok(None)
        }
    }

    fn put_raw_data(&self, tag: ObjectTag, lsn: Lsn, data: &[u8]) -> Result<()> {
        let key = ObjectKey {
            timeline: self.timelineid,
            tag,
        };
        self.obj_store.put(&key, lsn, data)?;
        Ok(())
    }

    ///
    /// Memorize a full image of a page version
    ///
    fn put_page_image(
        &self,
        tag: ObjectTag,
        lsn: Lsn,
        img: Bytes,
        update_meta: bool,
    ) -> Result<()> {
        self.put_page_entry(&tag, lsn, PageEntry::Page(img))?;

        debug!("put_page_image rel {:?} at {}", tag, lsn);

        if !update_meta {
            return Ok(());
        }
        if let ObjectTag::RelationBuffer(tag) = tag {
            // Also check if this created or extended the file
            let old_nblocks = self.relsize_get_nowait(tag.rel, lsn)?.unwrap_or(0);

            if tag.blknum >= old_nblocks {
                let new_nblocks = tag.blknum + 1;

                trace!(
                    "Extended relation {} from {} to {} blocks at {}",
                    tag.rel,
                    old_nblocks,
                    new_nblocks,
                    lsn
                );

                self.put_relsize_entry(&tag.rel, lsn, RelationSizeEntry::Size(new_nblocks))?;
                let mut rel_meta = self.rel_meta.write().unwrap();
                rel_meta.insert(
                    tag.rel,
                    RelMetadata {
                        size: new_nblocks,
                        last_updated: lsn,
                    },
                );
            }
        }
        Ok(())
    }

    ///
    /// Adds a relation-wide WAL record (like truncate) to the repository,
    /// associating it with all pages started with specified block number
    ///
    fn put_truncation(&self, rel: RelTag, lsn: Lsn, nblocks: u32) -> Result<()> {
        info!("Truncate relation {} to {} blocks at {}", rel, nblocks, lsn);

        self.put_relsize_entry(&rel, lsn, RelationSizeEntry::Size(nblocks))?;
        let mut rel_meta = self.rel_meta.write().unwrap();
        rel_meta.insert(
            rel,
            RelMetadata {
                size: nblocks,
                last_updated: lsn,
            },
        );

        Ok(())
    }

    /// Remember the all WAL before the given LSN has been processed.
    ///
    /// The WAL receiver calls this after the put_* functions, to indicate that
    /// all WAL before this point has been digested. Before that, if you call
    /// GET on an earlier LSN, it will block.
    fn advance_last_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);

        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last valid LSN backwards (was {}, new {})",
                old, lsn
            );
        }
    }

    fn get_last_valid_lsn(&self) -> Lsn {
        self.last_valid_lsn.load()
    }

    fn init_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);
        assert!(old == Lsn(0));
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old == Lsn(0));
        self.prev_record_lsn.store(Lsn(0));
    }

    /// Like `advance_last_valid_lsn`, but this always points to the end of
    /// a WAL record, not in the middle of one.
    ///
    /// This must be <= last valid LSN. This is tracked separately from last
    /// valid LSN, so that the WAL receiver knows where to restart streaming.
    ///
    /// NOTE: this updates last_valid_lsn as well.
    fn advance_last_record_lsn(&self, lsn: Lsn) {
        // Can't move backwards.
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old <= lsn);

        // Use old value of last_record_lsn as prev_record_lsn
        self.prev_record_lsn.fetch_max(old);

        // Also advance last_valid_lsn
        let old = self.last_valid_lsn.advance(lsn);
        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last record LSN backwards (was {}, new {})",
                old, lsn
            );
        }
    }
    fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load()
    }

    fn get_prev_record_lsn(&self) -> Lsn {
        self.prev_record_lsn.load()
    }

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.

    // Flush all the changes written so far with PUT functions to disk.
    // RocksDB writes out things as we go (?), so we don't need to do much here. We just
    // write out the last valid and record LSNs.
    fn checkpoint(&self) -> Result<()> {
        let metadata = MetadataEntry {
            last_valid_lsn: self.last_valid_lsn.load(),
            last_record_lsn: self.last_record_lsn.load(),
            prev_record_lsn: self.prev_record_lsn.load(),
            ancestor_timeline: self.ancestor_timeline,
            ancestor_lsn: self.ancestor_lsn,
        };
        trace!("checkpoint at {}", metadata.last_valid_lsn);

        self.put_timeline_metadata_entry(metadata)?;

        Ok(())
    }

    fn history<'a>(&'a self) -> Result<Box<dyn History + 'a>> {
        let lsn = self.last_valid_lsn.load();
        let iter = self.obj_store.objects(self.timelineid, lsn)?;
        Ok(Box::new(ObjectHistory {
            lsn,
            iter,
            last_relation_size: None,
        }))
    }

    fn gc_iteration(&self, horizon: u64) -> Result<GcResult> {
        let last_lsn = self.get_last_valid_lsn();
        let mut result: GcResult = Default::default();

        // checked_sub() returns None on overflow.
        if let Some(horizon) = last_lsn.checked_sub(horizon) {
            // WAL is large enough to perform GC
            let now = Instant::now();
            let mut prepared_horizon = Lsn(u64::MAX);
            // Iterate through all objects in timeline
            for obj in self
                .obj_store
                .list_objects(self.timelineid, false, last_lsn)?
            {
                result.inspected += 1;
                match obj {
                    // Prepared transactions
                    ObjectTag::TwoPhase(prepare) => {
                        let key = ObjectKey {
                            timeline: self.timelineid,
                            tag: obj,
                        };
                        for vers in self.obj_store.object_versions(&key, horizon)? {
                            let lsn = vers.0;
                            prepared_horizon = Lsn::min(lsn, prepared_horizon);
                            if self.get_tx_status(prepare.xid, horizon)?
                                != pg_constants::TRANSACTION_STATUS_IN_PROGRESS
                            {
                                self.obj_store.unlink(&key, lsn)?;
                                result.prep_deleted += 1;
                            }
                        }
                    }
                    ObjectTag::RelationMetadata(_) => {
                        // Do not need to reconstruct page images,
                        // just delete all old versions over horizon
                        let mut last_version = true;
                        let key = ObjectKey {
                            timeline: self.timelineid,
                            tag: obj,
                        };
                        for vers in self.obj_store.object_versions(&key, horizon)? {
                            let lsn = vers.0;
                            if last_version {
                                let content = vers.1;
                                match ObjectValue::des(&content[..])? {
                                    ObjectValue::RelationSize(RelationSizeEntry::Unlink) => {
                                        self.obj_store.unlink(&key, lsn)?;
                                        result.deleted += 1;
                                        result.dropped += 1;
                                    }
                                    _ => (), // preserve last version
                                }
                                last_version = false;
                                result.truncated += 1;
                                result.n_relations += 1;
                            } else {
                                self.obj_store.unlink(&key, lsn)?;
                                result.deleted += 1;
                            }
                        }
                    }
                    ObjectTag::RelationBuffer(tag) => {
                        // Reconstruct page at horizon unless relation was dropped
                        // and delete all older versions over horizon
                        let mut last_version = true;
                        let key = ObjectKey {
                            timeline: self.timelineid,
                            tag: obj,
                        };
                        for vers in self.obj_store.object_versions(&key, horizon)? {
                            let lsn = vers.0;
                            if last_version {
                                result.truncated += 1;
                                last_version = false;
                                if let Some(rel_size) =
                                    self.relsize_get_nowait(tag.rel, last_lsn)?
                                {
                                    if rel_size > tag.blknum {
                                        // preserve and materialize last version before deleting all preceeding
                                        self.get_page_at_lsn_nowait(obj, lsn)?;
                                        continue;
                                    }
                                    debug!("Drop last block {} of relation {:?} at {} because it is beyond relation size {}", tag.blknum, tag.rel, lsn, rel_size);
                                } else {
                                    if let Some(rel_size) =
                                        self.relsize_get_nowait(tag.rel, last_lsn)?
                                    {
                                        debug!("Preserve block {} of relation {:?} at {} because relation has size {} at {}", tag.rel, tag, lsn, rel_size, last_lsn);
                                        continue;
                                    }
                                    debug!("Relation {:?} was dropped at {}", tag.rel, lsn);
                                }
                                // relation was dropped or truncated so this block can be removed
                            }
                            self.obj_store.unlink(&key, lsn)?;
                            result.deleted += 1;
                        }
                    }
                    // SLRU-s
                    ObjectTag::Clog(_)
                    | ObjectTag::MultiXactOffsets(_)
                    | ObjectTag::MultiXactMembers(_) => {
                        // Remove old versions over horizon
                        let mut last_version = true;
                        let key = ObjectKey {
                            timeline: self.timelineid,
                            tag: obj,
                        };
                        for vers in self
                            .obj_store
                            .object_versions(&key, Lsn::min(prepared_horizon, horizon))?
                        {
                            let lsn = vers.0;
                            if last_version {
                                let content = vers.1;
                                match ObjectValue::des(&content[..])? {
                                    ObjectValue::SLRUTruncate => {
                                        self.obj_store.unlink(&key, lsn)?;
                                        result.slru_deleted += 1;
                                    }
                                    ObjectValue::Page(PageEntry::WALRecord(_)) => {
                                        // preserve and materialize last version before deleting all preceeding
                                        self.get_page_at_lsn_nowait(obj, lsn)?;
                                    }
                                    _ => {} // do nothing if already materialized
                                }
                                last_version = false;
                            } else {
                                // delete deteriorated version
                                self.obj_store.unlink(&key, lsn)?;
                                result.slru_deleted += 1;
                            }
                        }
                    }
                    // versioned always materialized objects: no need to reconstruct pages
                    ObjectTag::Checkpoint | ObjectTag::ControlFile => {
                        // Remove old versions over horizon
                        let mut last_version = true;
                        let key = ObjectKey {
                            timeline: self.timelineid,
                            tag: obj,
                        };
                        for vers in self.obj_store.object_versions(&key, horizon)? {
                            let lsn = vers.0;
                            if last_version {
                                // preserve last version
                                last_version = false;
                            } else {
                                // delete deteriorated version
                                self.obj_store.unlink(&key, lsn)?;
                                result.chkp_deleted += 1;
                            }
                        }
                    }
                    _ => (), // do nothing
                }
            }
            result.elapsed = now.elapsed();
            info!("Garbage collection completed in {:?}: {} relations inspected, {} object inspected, {} version histories truncated, {} versions deleted, {} relations dropped",
                  result.elapsed, result.n_relations, result.inspected, result.truncated, result.deleted, result.dropped);
            self.obj_store.compact();
        }
        Ok(result)
    }
}

impl ObjectTimeline {
    ///
    /// Internal function to get relation size at given LSN.
    ///
    /// The caller must ensure that WAL has been received up to 'lsn'.
    ///
    fn relsize_get_nowait(&self, rel: RelTag, lsn: Lsn) -> Result<Option<u32>> {
        {
            let rel_meta = self.rel_meta.read().unwrap();
            if let Some(meta) = rel_meta.get(&rel) {
                if meta.last_updated <= lsn {
                    return Ok(Some(meta.size));
                }
            }
        }
        let key = relation_size_key(self.timelineid, rel);
        let mut iter = self.object_versions(&*self.obj_store, &key, lsn)?;

        if let Some((version_lsn, value)) = iter.next().transpose()? {
            match ObjectValue::des_relsize(&value)? {
                RelationSizeEntry::Size(nblocks) => {
                    trace!(
                        "relation {} has size {} at {} (request {})",
                        rel,
                        nblocks,
                        version_lsn,
                        lsn
                    );
                    Ok(Some(nblocks))
                }
                RelationSizeEntry::Unlink => {
                    trace!(
                        "relation {} not found; it was dropped at lsn {}",
                        rel,
                        version_lsn
                    );
                    Ok(None)
                }
            }
        } else {
            info!("relation {} not found at {}", rel, lsn);
            Ok(None)
        }
    }

    ///
    /// Collect all the WAL records that are needed to reconstruct a page
    /// image for the given cache entry.
    ///
    /// Returns an old page image (if any), and a vector of WAL records to apply
    /// over it.
    ///
    fn collect_records_for_apply(
        &self,
        tag: ObjectTag,
        lsn: Lsn,
    ) -> Result<(Option<Bytes>, Vec<WALRecord>)> {
        let mut base_img: Option<Bytes> = None;
        let mut records: Vec<WALRecord> = Vec::new();

        // Scan backwards, collecting the WAL records, until we hit an
        // old page image.
        let searchkey = ObjectKey {
            timeline: self.timelineid,
            tag,
        };
        let mut iter = self.object_versions(&*self.obj_store, &searchkey, lsn)?;
        while let Some((_key, value)) = iter.next().transpose()? {
            match ObjectValue::des_page(&value)? {
                PageEntry::Page(img) => {
                    // We have a base image. No need to dig deeper into the list of
                    // records
                    base_img = Some(img);
                    break;
                }
                PageEntry::WALRecord(rec) => {
                    records.push(rec.clone());
                    // If this WAL record initializes the page, no need to dig deeper.
                    if rec.will_init {
                        break;
                    }
                }
            }
        }
        records.reverse();
        Ok((base_img, records))
    }

    fn launch_gc_thread(conf: &'static PageServerConf, timeline_rc: Arc<ObjectTimeline>) {
        let _gc_thread = thread::Builder::new()
            .name("Garbage collection thread".into())
            .spawn(move || {
                // FIXME
                timeline_rc.gc_loop(conf).expect("GC thread died");
            })
            .unwrap();
    }

    fn gc_loop(&self, conf: &'static PageServerConf) -> Result<()> {
        loop {
            thread::sleep(conf.gc_period);
            self.gc_iteration(conf.gc_horizon)?;
        }
    }

    //
    // Wait until WAL has been received up to the given LSN.
    //
    fn wait_lsn(&self, mut lsn: Lsn) -> Result<Lsn> {
        // When invalid LSN is requested, it means "don't wait, return latest version of the page"
        // This is necessary for bootstrap.
        if lsn == Lsn(0) {
            let last_valid_lsn = self.last_valid_lsn.load();
            trace!(
                "walreceiver doesn't work yet last_valid_lsn {}, requested {}",
                last_valid_lsn,
                lsn
            );
            lsn = last_valid_lsn;
        }
        trace!(
            "Start waiting for LSN {}, valid LSN is {}",
            lsn,
            self.last_valid_lsn.load()
        );
        self.last_valid_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive. valid LSN in {}",
                    lsn,
                    self.last_valid_lsn.load(),
                )
            })?;
        //trace!("Stop waiting for LSN {}, valid LSN is {}", lsn,  self.last_valid_lsn.load());

        Ok(lsn)
    }

    ///
    /// Iterate through object versions with given key, in reverse LSN order.
    ///
    /// This implements following the timeline history over the plain
    /// ObjectStore::object_versions function, which doesn't know
    /// about the relationships between timeline.
    ///
    fn object_versions<'a>(
        &self,
        obj_store: &'a dyn ObjectStore,
        key: &ObjectKey,
        lsn: Lsn,
    ) -> Result<ObjectVersionIter<'a>> {
        let current_iter = obj_store.object_versions(key, lsn)?;

        Ok(ObjectVersionIter {
            obj_store,
            object_tag: key.tag,
            current_iter,
            ancestor_timeline: self.ancestor_timeline,
            ancestor_lsn: self.ancestor_lsn,
        })
    }

    //
    // Helper functions to store different kinds of objects to the underlying ObjectStore
    //
    fn put_page_entry(&self, tag: &ObjectTag, lsn: Lsn, val: PageEntry) -> Result<()> {
        let key = ObjectKey {
            timeline: self.timelineid,
            tag: *tag,
        };
        let val = ObjectValue::Page(val);

        self.obj_store.put(&key, lsn, &ObjectValue::ser(&val)?)
    }

    fn put_relsize_entry(&self, tag: &RelTag, lsn: Lsn, val: RelationSizeEntry) -> Result<()> {
        let key = relation_size_key(self.timelineid, *tag);
        let val = ObjectValue::RelationSize(val);

        self.obj_store.put(&key, lsn, &ObjectValue::ser(&val)?)
    }

    fn put_timeline_metadata_entry(&self, val: MetadataEntry) -> Result<()> {
        let key = timeline_metadata_key(self.timelineid);
        let val = ObjectValue::TimelineMetadata(val);

        self.obj_store.put(&key, Lsn(0), &ObjectValue::ser(&val)?)
    }
}

struct ObjectHistory<'a> {
    iter: Box<dyn Iterator<Item = Result<(ObjectTag, Lsn, Vec<u8>)>> + 'a>,
    lsn: Lsn,
    last_relation_size: Option<(RelTag, u32)>,
}

impl<'a> Iterator for ObjectHistory<'a> {
    type Item = Result<RelationUpdate>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_result().transpose()
    }
}

impl<'a> History for ObjectHistory<'a> {
    fn lsn(&self) -> Lsn {
        self.lsn
    }
}

impl<'a> ObjectHistory<'a> {
    fn handle_relation_size(
        &mut self,
        rel_tag: RelTag,
        entry: RelationSizeEntry,
    ) -> Option<Update> {
        match entry {
            RelationSizeEntry::Size(size) => {
                // we only want to output truncations, expansions are filtered out
                let last_relation_size = self.last_relation_size.replace((rel_tag, size));

                match last_relation_size {
                    Some((last_buf, last_size)) if last_buf != rel_tag || size < last_size => {
                        Some(Update::Truncate { n_blocks: size })
                    }
                    _ => None,
                }
            }
            RelationSizeEntry::Unlink => Some(Update::Unlink),
        }
    }

    fn handle_page(&mut self, buf_tag: BufferTag, entry: PageEntry) -> Update {
        match entry {
            PageEntry::Page(img) => Update::Page {
                blknum: buf_tag.blknum,
                img,
            },
            PageEntry::WALRecord(rec) => Update::WALRecord {
                blknum: buf_tag.blknum,
                rec,
            },
        }
    }

    fn next_result(&mut self) -> Result<Option<RelationUpdate>> {
        while let Some((object_tag, lsn, value)) = self.iter.next().transpose()? {
            let (rel_tag, update) = match object_tag {
                ObjectTag::RelationMetadata(rel_tag) => {
                    let entry = ObjectValue::des_relsize(&value)?;
                    match self.handle_relation_size(rel_tag, entry) {
                        Some(relation_update) => (rel_tag, relation_update),
                        None => continue,
                    }
                }
                ObjectTag::RelationBuffer(buf_tag) => {
                    let entry = ObjectValue::des_page(&value)?;
                    let update = self.handle_page(buf_tag, entry);

                    (buf_tag.rel, update)
                }
                _ => continue,
            };

            return Ok(Some(RelationUpdate {
                rel: rel_tag,
                lsn,
                update,
            }));
        }

        Ok(None)
    }
}

///
/// We store several kinds of objects in the repository.
/// We have per-page, per-relation and per-timeline entries.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ObjectValue {
    Page(PageEntry),
    RelationSize(RelationSizeEntry),
    TimelineMetadata(MetadataEntry),
    SLRUTruncate,
}

///
/// This is what we store for each page in the object store. Use
/// ObjectTag::RelationBuffer as key.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
enum PageEntry {
    /// Ready-made image of the block
    Page(Bytes),

    /// WAL record, to be applied on top of the "previous" entry
    ///
    /// Some WAL records will initialize the page from scratch. For such records,
    /// the 'will_init' flag is set. They don't need the previous page image before
    /// applying. The 'will_init' flag is set for records containing a full-page image,
    /// and for records with the BKPBLOCK_WILL_INIT flag. These differ from Page images
    /// stored directly in the repository in that you still need to run the WAL redo
    /// routine to generate the page image.
    WALRecord(WALRecord),
}

///
/// In addition to page versions, we store relation size as a separate, versioned,
/// object. That way we can answer nblocks requests faster, and we also use it to
/// support relation truncation without having to add a tombstone page version for
/// each block that is truncated away.
///
/// Use ObjectTag::RelationMetadata as the key.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
enum RelationSizeEntry {
    Size(u32),

    /// Tombstone for a dropped relation.
    Unlink,
}

const fn relation_size_key(timelineid: ZTimelineId, rel: RelTag) -> ObjectKey {
    ObjectKey {
        timeline: timelineid,
        tag: ObjectTag::RelationMetadata(rel),
    }
}

///
/// In addition to the per-page and per-relation entries, we also store
/// a little metadata blob for each timeline. This is not versioned, use
/// ObjectTag::TimelineMetadataTag with constant Lsn(0) as the key.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataEntry {
    last_valid_lsn: Lsn,
    last_record_lsn: Lsn,
    prev_record_lsn: Lsn,
    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

const fn timeline_metadata_key(timelineid: ZTimelineId) -> ObjectKey {
    ObjectKey {
        timeline: timelineid,
        tag: ObjectTag::TimelineMetadataTag,
    }
}

///
/// Helper functions to deserialize ObjectValue, when the caller knows what kind of
/// a value it should be.
///
/// There are no matching helper functions for serializing. Instead, there are
/// `put_page_entry`, `put_relsize_entry`, and `put_timeline_metadata_entry` helper
/// functions in ObjectTimeline that both construct the right kind of key and
/// serialize the value in the same call.
///
impl ObjectValue {
    fn des_page(v: &[u8]) -> Result<PageEntry> {
        match ObjectValue::des(&v)? {
            ObjectValue::Page(p) => Ok(p),
            _ => {
                bail!("Invalid object kind, expected a page entry");
            }
        }
    }

    fn des_relsize(v: &[u8]) -> Result<RelationSizeEntry> {
        match ObjectValue::des(&v)? {
            ObjectValue::RelationSize(rs) => Ok(rs),
            _ => {
                bail!("Invalid object kind, expected a relation size entry");
            }
        }
    }

    fn des_timeline_metadata(v: &[u8]) -> Result<MetadataEntry> {
        match ObjectValue::des(&v)? {
            ObjectValue::TimelineMetadata(t) => Ok(t),
            _ => {
                bail!("Invalid object kind, expected a timeline metadata entry");
            }
        }
    }
}

///
/// Iterator for `object_versions`. Returns all page versions of a given block, in
/// reverse LSN order. This implements the traversal of ancestor timelines. If
/// a page isn't found in the most recent timeline, this iterates to the parent,
/// until a page version is found.
///
struct ObjectVersionIter<'a> {
    obj_store: &'a dyn ObjectStore,

    object_tag: ObjectTag,

    /// Iterator on the current timeline.
    current_iter: Box<dyn Iterator<Item = (Lsn, Vec<u8>)> + 'a>,

    /// Ancestor of the current timeline being iterated.
    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

impl<'a> Iterator for ObjectVersionIter<'a> {
    type Item = Result<(Lsn, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_result().transpose()
    }
}

impl<'a> ObjectVersionIter<'a> {
    ///
    /// "transposed" version of the standard Iterator::next function.
    ///
    /// The rust standard Iterator::next function returns an
    /// Option of a Result, but it's more convenient to work with
    /// Result of a Option so that you can use ? to check for errors.
    ///
    fn next_result(&mut self) -> Result<Option<(Lsn, Vec<u8>)>> {
        loop {
            // If there is another entry on the current timeline, return it.
            if let Some(result) = self.current_iter.next() {
                return Ok(Some(result));
            }

            // Out of entries on this timeline. Move to the ancestor, if any.
            if let Some(ancestor_timeline) = self.ancestor_timeline {
                let searchkey = ObjectKey {
                    timeline: ancestor_timeline,
                    tag: self.object_tag,
                };
                let ancestor_iter = self
                    .obj_store
                    .object_versions(&searchkey, self.ancestor_lsn)?;

                // Load the parent timeline's metadata. (We don't
                // actually need it yet, only if we need to follow to
                // the grandparent timeline)
                let v = self
                    .obj_store
                    .get(&timeline_metadata_key(ancestor_timeline), Lsn(0))
                    .with_context(|| "timeline not found in repository")?;
                let ancestor_metadata = ObjectValue::des_timeline_metadata(&v)?;
                self.ancestor_timeline = ancestor_metadata.ancestor_timeline;
                self.ancestor_lsn = ancestor_metadata.ancestor_lsn;
                self.current_iter = ancestor_iter;
            } else {
                return Ok(None);
            }
        }
    }
}
