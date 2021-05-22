//
// A Repository holds all the different page versions and WAL records
//
// This implementation uses RocksDB to store WAL wal records and
// full page images, keyed by the RelFileNode, blocknumber, and the
// LSN.

use crate::repository::{BufferTag, RelTag, Repository, Timeline, WALRecord};
use crate::restore_local_repo::restore_timeline;
use crate::waldecoder::{Oid, TransactionId};
use crate::walredo::WalRedoManager;
use crate::PageServerConf;
use crate::ZTimelineId;
// use crate::PageServerConf;
// use crate::branches;
use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use postgres_ffi::nonrelfile_utils::transaction_id_get_status;
use postgres_ffi::*;
use std::cmp::min;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use zenith_utils::lsn::{AtomicLsn, Lsn};
use zenith_utils::seqwait::SeqWait;

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

pub struct RocksRepository {
    conf: &'static PageServerConf,
    timelines: Mutex<HashMap<ZTimelineId, Arc<RocksTimeline>>>,

    walredo_mgr: Arc<dyn WalRedoManager>,
}

pub struct RocksTimeline {
    // RocksDB handle
    db: rocksdb::DB,

    // WAL redo manager
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

    // Counters, for metrics collection.
    pub num_entries: AtomicU64,
    pub num_page_images: AtomicU64,
    pub num_wal_records: AtomicU64,
    pub num_getpage_requests: AtomicU64,
}

//
// We store two kinds of entries in the repository:
//
// 1. Ready-made images of the block
// 2. WAL records, to be applied on top of the "previous" entry
//
// Some WAL records will initialize the page from scratch. For such records,
// the 'will_init' flag is set. They don't need the previous page image before
// applying. The 'will_init' flag is set for records containing a full-page image,
// and for records with the BKPBLOCK_WILL_INIT flag. These differ from PageImages
// stored directly in the cache entry in that you still need to run the WAL redo
// routine to generate the page image.
//
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct CacheKey {
    pub tag: BufferTag,
    pub lsn: Lsn,
}

impl CacheKey {
    fn pack(&self, buf: &mut BytesMut) {
        self.tag.pack(buf);
        buf.put_u64(self.lsn.0);
    }
    fn unpack(buf: &mut Bytes) -> CacheKey {
        CacheKey {
            tag: BufferTag::unpack(buf),
            lsn: Lsn::from(buf.get_u64()),
        }
    }

    fn from_slice(slice: &[u8]) -> Self {
        let mut buf = Bytes::copy_from_slice(slice);
        Self::unpack(&mut buf)
    }

    fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        self.pack(&mut buf);
        buf
    }
}

enum CacheEntryContent {
    PageImage(Bytes),
    WALRecord(WALRecord),
    Truncation,
}

// The serialized representation of a CacheEntryContent begins with
// single byte that indicates what kind of entry it is. There is also
// an UNUSED_VERSION_FLAG that is not represented in the CacheEntryContent
// at all, you must peek into the first byte of the serialized representation
// to read it.
const CONTENT_PAGE_IMAGE: u8 = 1u8;
const CONTENT_WAL_RECORD: u8 = 2u8;
const CONTENT_TRUNCATION: u8 = 3u8;

const CONTENT_KIND_MASK: u8 = 3u8; // bitmask that covers the above

const UNUSED_VERSION_FLAG: u8 = 4u8;

impl CacheEntryContent {
    pub fn pack(&self, buf: &mut BytesMut) {
        match self {
            CacheEntryContent::PageImage(image) => {
                buf.put_u8(CONTENT_PAGE_IMAGE);
                buf.put_u16(image.len() as u16);
                buf.put_slice(&image[..]);
            }
            CacheEntryContent::WALRecord(rec) => {
                buf.put_u8(CONTENT_WAL_RECORD);
                rec.pack(buf);
            }
            CacheEntryContent::Truncation => {
                buf.put_u8(CONTENT_TRUNCATION);
            }
        }
    }
    pub fn unpack(buf: &mut Bytes) -> CacheEntryContent {
        let kind = buf.get_u8() & CONTENT_KIND_MASK;

        match kind {
            CONTENT_PAGE_IMAGE => {
                let len = buf.get_u16() as usize;
                let mut dst = vec![0u8; len];
                buf.copy_to_slice(&mut dst);
                CacheEntryContent::PageImage(Bytes::from(dst))
            }
            CONTENT_WAL_RECORD => CacheEntryContent::WALRecord(WALRecord::unpack(buf)),
            CONTENT_TRUNCATION => CacheEntryContent::Truncation,
            _ => unreachable!(),
        }
    }

    fn from_slice(slice: &[u8]) -> Self {
        let mut buf = Bytes::copy_from_slice(slice);
        Self::unpack(&mut buf)
    }

    fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        self.pack(&mut buf);
        buf
    }
}

impl RocksRepository {
    pub fn new(
        conf: &'static PageServerConf,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> RocksRepository {
        RocksRepository {
            conf,
            timelines: Mutex::new(HashMap::new()),
            walredo_mgr,
        }
    }
}

// Get handle to a given timeline. It is assumed to already exist.
impl Repository for RocksRepository {
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => bail!("timeline not found"),
        }
    }

    fn get_or_restore_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        match timelines.get(&timelineid) {
            Some(timeline) => Ok(timeline.clone()),
            None => {
                let timeline = RocksTimeline::new(self.conf, timelineid, self.walredo_mgr.clone());

                restore_timeline(self.conf, &timeline, timelineid)?;

                let timeline_rc = Arc::new(timeline);

                timelines.insert(timelineid, timeline_rc.clone());

                if self.conf.gc_horizon != 0 {
                    let timeline_rc_copy = timeline_rc.clone();
                    let conf = self.conf;
                    let _gc_thread = thread::Builder::new()
                        .name("Garbage collection thread".into())
                        .spawn(move || {
                            // FIXME
                            timeline_rc_copy.do_gc(conf).expect("GC thread died");
                        })
                        .unwrap();
                }
                Ok(timeline_rc)
            }
        }
    }

    #[cfg(test)]
    fn create_empty_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>> {
        let mut timelines = self.timelines.lock().unwrap();

        let timeline = RocksTimeline::new(&self.conf, timelineid, self.walredo_mgr.clone());

        let timeline_rc = Arc::new(timeline);
        let r = timelines.insert(timelineid, timeline_rc.clone());
        assert!(r.is_none());

        // don't start the garbage collector for unit tests, either.

        Ok(timeline_rc)
    }
}

impl RocksTimeline {
    fn open_rocksdb(conf: &PageServerConf, timelineid: ZTimelineId) -> rocksdb::DB {
        let path = conf.timeline_path(timelineid);
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_use_fsync(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_compaction_filter("ttl", move |_level: u32, _key: &[u8], val: &[u8]| {
            if (val[0] & UNUSED_VERSION_FLAG) != 0 {
                rocksdb::compaction_filter::Decision::Remove
            } else {
                rocksdb::compaction_filter::Decision::Keep
            }
        });
        rocksdb::DB::open(&opts, &path).unwrap()
    }

    fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        walredo_mgr: Arc<dyn WalRedoManager>,
    ) -> RocksTimeline {
        RocksTimeline {
            db: RocksTimeline::open_rocksdb(conf, timelineid),

            walredo_mgr,

            last_valid_lsn: SeqWait::new(Lsn(0)),
            last_record_lsn: AtomicLsn::new(0),

            num_entries: AtomicU64::new(0),
            num_page_images: AtomicU64::new(0),
            num_wal_records: AtomicU64::new(0),
            num_getpage_requests: AtomicU64::new(0),
        }
    }
}

impl RocksTimeline {
    ///
    /// Collect all the WAL records that are needed to reconstruct a page
    /// image for the given cache entry.
    ///
    /// Returns an old page image (if any), and a vector of WAL records to apply
    /// over it.
    ///
    fn collect_records_for_apply(
        &self,
        tag: BufferTag,
        lsn: Lsn,
    ) -> (Option<Bytes>, Vec<WALRecord>) {
        let key = CacheKey { tag, lsn };
        let mut base_img: Option<Bytes> = None;
        let mut records: Vec<WALRecord> = Vec::new();

        let mut iter = self.db.raw_iterator();
        iter.seek_for_prev(key.to_bytes());

        // Scan backwards, collecting the WAL records, until we hit an
        // old page image.
        while iter.valid() {
            let key = CacheKey::from_slice(iter.key().unwrap());
            if key.tag != tag {
                break;
            }
            let content = CacheEntryContent::from_slice(iter.value().unwrap());
            if let CacheEntryContent::PageImage(img) = content {
                // We have a base image. No need to dig deeper into the list of
                // records
                base_img = Some(img);
                break;
            } else if let CacheEntryContent::WALRecord(rec) = content {
                records.push(rec.clone());
                // If this WAL record initializes the page, no need to dig deeper.
                if rec.will_init {
                    break;
                }
            } else {
                panic!("no base image and no WAL record on cache entry");
            }
            iter.prev();
        }
        records.reverse();
        (base_img, records)
    }

    // Internal functions

    //
    // Internal function to get relation size at given LSN.
    //
    // The caller must ensure that WAL has been received up to 'lsn'.
    //
    fn relsize_get_nowait(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        assert!(lsn <= self.last_valid_lsn.load());

        let mut key = CacheKey {
            tag: BufferTag {
                rel,
                blknum: u32::MAX,
            },
            lsn,
        };
        let mut iter = self.db.raw_iterator();
        loop {
            iter.seek_for_prev(key.to_bytes());
            if iter.valid() {
                let thiskey = CacheKey::from_slice(iter.key().unwrap());
                if thiskey.tag.rel == rel {
                    let content = CacheEntryContent::from_slice(iter.value().unwrap());
                    if let CacheEntryContent::Truncation = content {
                        if thiskey.tag.blknum > 0 {
                            key.tag.blknum = thiskey.tag.blknum - 1;
                            continue;
                        }
                        break;
                    }
                    let relsize = thiskey.tag.blknum + 1;
                    debug!("Size of relation {} at {} is {}", rel, lsn, relsize);
                    return Ok(relsize);
                }
            }
            break;
        }
        debug!("Size of relation {} at {} is zero", rel, lsn);
        Ok(0)
    }

    fn do_gc(&self, conf: &'static PageServerConf) -> Result<Bytes> {
        loop {
            thread::sleep(conf.gc_period);
            let last_lsn = self.get_last_valid_lsn();

            // checked_sub() returns None on overflow.
            if let Some(horizon) = last_lsn.checked_sub(conf.gc_horizon) {
                let mut maxkey = CacheKey {
                    tag: BufferTag {
                        rel: RelTag {
                            spcnode: u32::MAX,
                            dbnode: u32::MAX,
                            relnode: u32::MAX,
                            forknum: u8::MAX,
                        },
                        blknum: u32::MAX,
                    },
                    lsn: Lsn::MAX,
                };
                let now = Instant::now();
                let mut reconstructed = 0u64;
                let mut truncated = 0u64;
                let mut inspected = 0u64;
                let mut deleted = 0u64;
                loop {
                    let mut iter = self.db.raw_iterator();
                    iter.seek_for_prev(maxkey.to_bytes());
                    if iter.valid() {
                        let key = CacheKey::from_slice(iter.key().unwrap());
                        let v = iter.value().unwrap();

                        inspected += 1;

                        // Construct boundaries for old records cleanup
                        maxkey.tag = key.tag;
                        let last_lsn = key.lsn;
                        maxkey.lsn = min(horizon, last_lsn); // do not remove last version

                        let mut minkey = maxkey.clone();
                        minkey.lsn = Lsn(0); // first version

                        // Special handling of delete of PREPARE WAL record
                        if last_lsn < horizon
                            && key.tag.rel.forknum == pg_constants::PG_TWOPHASE_FORKNUM
                        {
                            if (v[0] & UNUSED_VERSION_FLAG) == 0 {
                                let mut v = v.to_owned();
                                v[0] |= UNUSED_VERSION_FLAG;
                                self.db.put(key.to_bytes(), &v[..])?;
                                deleted += 1;
                            }
                            maxkey = minkey;
                            continue;
                        }
                        // reconstruct most recent page version
                        if (v[0] & CONTENT_KIND_MASK) == CONTENT_WAL_RECORD {
                            // force reconstruction of most recent page version
                            let (base_img, records) =
                                self.collect_records_for_apply(key.tag, key.lsn);

                            trace!(
                                "Reconstruct most recent page {} blk {} at {} from {} records",
                                key.tag.rel,
                                key.tag.blknum,
                                key.lsn,
                                records.len()
                            );

                            let new_img = self
                                .walredo_mgr
                                .request_redo(key.tag, key.lsn, base_img, records)?;
                            self.put_page_image(key.tag, key.lsn, new_img.clone());

                            reconstructed += 1;
                        }

                        iter.seek_for_prev(maxkey.to_bytes());
                        if iter.valid() {
                            // do not remove last version
                            if last_lsn > horizon {
                                // locate most recent record before horizon
                                let key = CacheKey::from_slice(iter.key().unwrap());
                                if key.tag == maxkey.tag {
                                    let v = iter.value().unwrap();
                                    if (v[0] & CONTENT_KIND_MASK) == CONTENT_WAL_RECORD {
                                        let (base_img, records) =
                                            self.collect_records_for_apply(key.tag, key.lsn);
                                        trace!("Reconstruct horizon page {} blk {} at {} from {} records",
                                              key.tag.rel, key.tag.blknum, key.lsn, records.len());
                                        let new_img = self
                                            .walredo_mgr
                                            .request_redo(key.tag, key.lsn, base_img, records)?;
                                        self.put_page_image(key.tag, key.lsn, new_img.clone());

                                        truncated += 1;
                                    } else {
                                        trace!(
                                            "Keeping horizon page {} blk {} at {}",
                                            key.tag.rel,
                                            key.tag.blknum,
                                            key.lsn
                                        );
                                    }
                                }
                            } else {
                                trace!(
                                    "Last page {} blk {} at {}, horizon {}",
                                    key.tag.rel,
                                    key.tag.blknum,
                                    key.lsn,
                                    horizon
                                );
                            }
                            // remove records prior to horizon
                            loop {
                                iter.prev();
                                if !iter.valid() {
                                    break;
                                }
                                let key = CacheKey::from_slice(iter.key().unwrap());
                                if key.tag != maxkey.tag {
                                    break;
                                }
                                let v = iter.value().unwrap();
                                if (v[0] & UNUSED_VERSION_FLAG) == 0 {
                                    let mut v = v.to_owned();
                                    v[0] |= UNUSED_VERSION_FLAG;
                                    self.db.put(key.to_bytes(), &v[..])?;
                                    deleted += 1;
                                    trace!(
                                        "deleted: {} blk {} at {}",
                                        key.tag.rel,
                                        key.tag.blknum,
                                        key.lsn
                                    );
                                } else {
                                    break;
                                }
                            }
                        }
                        maxkey = minkey;
                    } else {
                        break;
                    }
                }
                info!("Garbage collection completed in {:?}:\n{} version chains inspected, {} pages reconstructed, {} version histories truncated, {} versions deleted",
					  now.elapsed(), inspected, reconstructed, truncated, deleted);
            }
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
        //trace!("Start waiting for LSN {}, valid LSN is {}", lsn,  self.last_valid_lsn.load());
        self.last_valid_lsn
            .wait_for_timeout(lsn, TIMEOUT)
            .with_context(|| {
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive",
                    lsn
                )
            })?;
        //trace!("Stop waiting for LSN {}, valid LSN is {}", lsn,  self.last_valid_lsn.load());

        Ok(lsn)
    }
}

impl Timeline for RocksTimeline {
    // Public GET interface functions

    ///
    /// GetPage@LSN
    ///
    /// Returns an 8k page image
    ///
    fn get_page_at_lsn(&self, tag: BufferTag, req_lsn: Lsn) -> Result<Bytes> {
        self.num_getpage_requests.fetch_add(1, Ordering::Relaxed);

        let lsn = self.wait_lsn(req_lsn)?;

        // Look up cache entry. If it's a page image, return that. If it's a WAL record,
        // ask the WAL redo service to reconstruct the page image from the WAL records.
        let key = CacheKey { tag, lsn };

        let mut iter = self.db.raw_iterator();
        iter.seek_for_prev(key.to_bytes());

        if iter.valid() {
            let key = CacheKey::from_slice(iter.key().unwrap());
            if key.tag == tag {
                let content = CacheEntryContent::from_slice(iter.value().unwrap());
                let page_img: Bytes;
                if let CacheEntryContent::PageImage(img) = content {
                    page_img = img;
                } else if let CacheEntryContent::WALRecord(_rec) = content {
                    // Request the WAL redo manager to apply the WAL records for us.
                    let (base_img, records) = self.collect_records_for_apply(tag, lsn);
                    page_img = self.walredo_mgr.request_redo(tag, lsn, base_img, records)?;

                    self.put_page_image(tag, lsn, page_img.clone());
                } else {
                    // No base image, and no WAL record. Huh?
                    bail!("no page image or WAL record for requested page");
                }
                // FIXME: assumes little-endian. Only used for the debugging log though
                let page_lsn_hi =
                    u32::from_le_bytes(page_img.get(0..4).unwrap().try_into().unwrap());
                let page_lsn_lo =
                    u32::from_le_bytes(page_img.get(4..8).unwrap().try_into().unwrap());
                debug!(
                    "Returning page with LSN {:X}/{:X} for {} blk {}",
                    page_lsn_hi, page_lsn_lo, tag.rel, tag.blknum
                );
                return Ok(page_img);
            }
        }
        static ZERO_PAGE: [u8; 8192] = [0u8; 8192];
        debug!(
            "Page {} blk {} at {}({}) not found",
            tag.rel, tag.blknum, req_lsn, lsn
        );
        Ok(Bytes::from_static(&ZERO_PAGE))
        /* return Err("could not find page image")?; */
    }

    ///
    /// Get size of relation at given LSN.
    ///
    fn get_relsize(&self, rel: RelTag, lsn: Lsn) -> Result<u32> {
        let lsn = self.wait_lsn(lsn)?;
        self.relsize_get_nowait(rel, lsn)
    }

    /// Get vector of prepared twophase transactions
    fn get_twophase(&self, lsn: Lsn) -> Result<Vec<TransactionId>> {
        let key = CacheKey {
            // minimal key
            tag: BufferTag {
                rel: RelTag {
                    forknum: pg_constants::PG_TWOPHASE_FORKNUM,
                    spcnode: 0,
                    dbnode: 0,
                    relnode: 0,
                },
                blknum: 0,
            },
            lsn: Lsn(0),
        };
        let mut gxacts = Vec::new();

        let mut iter = self.db.raw_iterator();
        iter.seek(key.to_bytes());
        while iter.valid() {
            let key = CacheKey::from_slice(iter.key().unwrap());
            if key.tag.rel.forknum != pg_constants::PG_TWOPHASE_FORKNUM {
                break; // we are done with this fork
            }
            if key.lsn <= lsn {
                let xid = key.tag.blknum;
                let tag = BufferTag {
                    rel: RelTag {
                        forknum: pg_constants::PG_XACT_FORKNUM,
                        spcnode: 0,
                        dbnode: 0,
                        relnode: 0,
                    },
                    blknum: xid / pg_constants::CLOG_XACTS_PER_PAGE,
                };
                let clog_page = self.get_page_at_lsn(tag, lsn)?;
                let status = transaction_id_get_status(xid, &clog_page[..]);
                if status == pg_constants::TRANSACTION_STATUS_IN_PROGRESS {
                    gxacts.push(xid);
                }
            }
            iter.next();
        }
        Ok(gxacts)
    }

    /// Get databases. This function is used to local pg_filenode.map files
    fn get_databases(&self, lsn: Lsn) -> Result<Vec<RelTag>> {
        let key = CacheKey {
            // minimal key
            tag: BufferTag {
                rel: RelTag {
                    forknum: pg_constants::PG_FILENODEMAP_FORKNUM,
                    spcnode: 0,
                    dbnode: 0,
                    relnode: 0,
                },
                blknum: 0,
            },
            lsn: Lsn(0),
        };
        let mut dbs = Vec::new();

        let mut iter = self.db.raw_iterator();
        iter.seek(key.to_bytes());
        let mut prev_tag = key.tag.rel;
        while iter.valid() {
            let key = CacheKey::from_slice(iter.key().unwrap());
            if key.tag.rel.forknum != pg_constants::PG_FILENODEMAP_FORKNUM {
                break; // we are done with this fork
            }
            if key.tag.rel != prev_tag && key.lsn <= lsn {
                prev_tag = key.tag.rel;
                dbs.push(prev_tag); // collect unique tags
            }
            iter.next();
        }
        Ok(dbs)
    }

    /// Get range [begin,end) of stored blocks. Used mostly for SMGR pseudorelations
    /// but can be also applied to normal relations.
    fn get_range(&self, rel: RelTag, lsn: Lsn) -> Result<(u32, u32)> {
        let _lsn = self.wait_lsn(lsn)?;
        let mut key = CacheKey {
            // minimal key to start with
            tag: BufferTag { rel, blknum: 0 },
            lsn: Lsn(0),
        };
        let mut iter = self.db.raw_iterator();
        iter.seek(key.to_bytes()); // locate first entry
        if iter.valid() {
            let thiskey = CacheKey::from_slice(iter.key().unwrap());
            let tag = thiskey.tag;
            if tag.rel == rel {
                // still trversing this relation
                let first_blknum = tag.blknum;
                key.tag.blknum = u32::MAX; // maximal key
                let mut iter = self.db.raw_iterator();
                iter.seek_for_prev(key.to_bytes()); // localte last entry
                if iter.valid() {
                    let thiskey = CacheKey::from_slice(iter.key().unwrap());
                    let last_blknum = thiskey.tag.blknum;
                    return Ok((first_blknum, last_blknum + 1)); // upper boundary is exclusive
                }
            }
        }
        Ok((0, 0)) // empty range
    }

    ///
    /// Does relation exist at given LSN?
    ///
    /// FIXME: this actually returns true, if the relation exists at *any* LSN
    fn get_relsize_exists(&self, rel: RelTag, req_lsn: Lsn) -> Result<bool> {
        let lsn = self.wait_lsn(req_lsn)?;

        let key = CacheKey {
            tag: BufferTag {
                rel,
                blknum: u32::MAX,
            },
            lsn,
        };
        let mut iter = self.db.raw_iterator();
        iter.seek_for_prev(key.to_bytes());
        if iter.valid() {
            let key = CacheKey::from_slice(iter.key().unwrap());
            if key.tag.rel == rel {
                debug!("Relation {} exists at {}", rel, lsn);
                return Ok(true);
            }
        }
        debug!("Relation {} doesn't exist at {}", rel, lsn);
        Ok(false)
    }

    // Other public functions, for updating the repository.
    // These are used by the WAL receiver and WAL redo.

    ///
    /// Adds a WAL record to the repository
    ///
    fn put_wal_record(&self, tag: BufferTag, rec: WALRecord) {
        let lsn = rec.lsn;
        let key = CacheKey { tag, lsn };

        let content = CacheEntryContent::WALRecord(rec);

        let _res = self.db.put(key.to_bytes(), content.to_bytes());
        trace!(
            "put_wal_record rel {} blk {} at {}",
            tag.rel,
            tag.blknum,
            lsn
        );

        self.num_entries.fetch_add(1, Ordering::Relaxed);
        self.num_wal_records.fetch_add(1, Ordering::Relaxed);
    }

    ///
    /// Adds a relation-wide WAL record (like truncate) to the repository,
    /// associating it with all pages started with specified block number
    ///
    fn put_truncation(&self, rel: RelTag, lsn: Lsn, nblocks: u32) -> Result<()> {
        // What was the size of the relation before this record?
        let last_lsn = self.last_valid_lsn.load();
        let old_rel_size = self.relsize_get_nowait(rel, last_lsn)?;

        let content = CacheEntryContent::Truncation;
        // set new relation size
        trace!("Truncate relation {} to {} blocks at {}", rel, nblocks, lsn);

        for blknum in nblocks..old_rel_size {
            let key = CacheKey {
                tag: BufferTag { rel, blknum },
                lsn,
            };
            trace!("put_wal_record lsn: {}", key.lsn);
            let _res = self.db.put(key.to_bytes(), content.to_bytes());
        }
        let n = (old_rel_size - nblocks) as u64;
        self.num_entries.fetch_add(n, Ordering::Relaxed);
        self.num_wal_records.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }

    ///
    /// Get page image at particular LSN
    ///
    fn get_page_image(&self, tag: BufferTag, lsn: Lsn) -> Result<Option<Bytes>> {
        let key = CacheKey { tag, lsn };
        if let Some(bytes) = self.db.get(key.to_bytes())? {
            let content = CacheEntryContent::from_slice(&bytes);
            if let CacheEntryContent::PageImage(img) = content {
                return Ok(Some(img));
            }
        }
        Ok(None)
    }

    ///
    /// Memorize a full image of a page version
    ///
    fn put_page_image(&self, tag: BufferTag, lsn: Lsn, img: Bytes) {
        let img_len = img.len();
        let key = CacheKey { tag, lsn };
        let content = CacheEntryContent::PageImage(img);

        let mut val_buf = content.to_bytes();

        // Zero size of page image indicates that page can be removed
        if img_len == 0 {
            if (val_buf[0] & UNUSED_VERSION_FLAG) != 0 {
                // records already marked for deletion
                return;
            } else {
                // delete truncated multixact page
                val_buf[0] |= UNUSED_VERSION_FLAG;
            }
        }

        trace!("put_wal_record lsn: {}", key.lsn);
        let _res = self.db.put(key.to_bytes(), content.to_bytes());

        trace!(
            "put_page_image rel {} blk {} at {}",
            tag.rel,
            tag.blknum,
            lsn
        );
        self.num_page_images.fetch_add(1, Ordering::Relaxed);
    }

    fn put_create_database(
        &self,
        lsn: Lsn,
        db_id: Oid,
        tablespace_id: Oid,
        src_db_id: Oid,
        src_tablespace_id: Oid,
    ) -> Result<()> {
        let mut n = 0;
        for forknum in &[
            pg_constants::MAIN_FORKNUM,
            pg_constants::FSM_FORKNUM,
            pg_constants::VISIBILITYMAP_FORKNUM,
            pg_constants::INIT_FORKNUM,
            pg_constants::PG_FILENODEMAP_FORKNUM,
        ] {
            let key = CacheKey {
                tag: BufferTag {
                    rel: RelTag {
                        spcnode: src_tablespace_id,
                        dbnode: src_db_id,
                        relnode: 0,
                        forknum: *forknum,
                    },
                    blknum: 0,
                },
                lsn: Lsn(0),
            };
            let mut iter = self.db.raw_iterator();
            iter.seek(key.to_bytes());
            while iter.valid() {
                let mut key = CacheKey::from_slice(iter.key().unwrap());
                if key.tag.rel.spcnode != src_tablespace_id || key.tag.rel.dbnode != src_db_id {
                    break;
                }
                key.tag.rel.spcnode = tablespace_id;
                key.tag.rel.dbnode = db_id;
                key.lsn = lsn;

                let v = iter.value().unwrap();
                self.db.put(key.to_bytes(), v)?;
                n += 1;
                iter.next();
            }
        }
        info!(
            "Create database {}/{}, copy {} entries",
            tablespace_id, db_id, n
        );
        Ok(())
    }

    /// Remember that WAL has been received and added to the timeline up to the given LSN
    fn advance_last_valid_lsn(&self, lsn: Lsn) {
        let lsn = Lsn((lsn.0 + 7) & !7); // align position on 8 bytes
        let old = self.last_valid_lsn.advance(lsn);

        // Can't move backwards.
        if lsn < old {
            warn!(
                "attempted to move last valid LSN backwards (was {}, new {})",
                old, lsn
            );
        }
    }

    ///
    /// Remember the (end of) last valid WAL record remembered for the timeline.
    ///
    /// NOTE: this updates last_valid_lsn as well.
    ///
    fn advance_last_record_lsn(&self, lsn: Lsn) {
        let lsn = Lsn((lsn.0 + 7) & !7); // align position on 8 bytes
                                         // Can't move backwards.
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old <= lsn);

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

    fn init_valid_lsn(&self, lsn: Lsn) {
        let old = self.last_valid_lsn.advance(lsn);
        assert!(old == Lsn(0));
        let old = self.last_record_lsn.fetch_max(lsn);
        assert!(old == Lsn(0));
    }

    fn get_last_valid_lsn(&self) -> Lsn {
        self.last_valid_lsn.load()
    }

    //
    // Get statistics to be displayed in the user interface.
    //
    // FIXME
    /*
    fn get_stats(&self) -> TimelineStats {
        TimelineStats {
            num_entries: self.num_entries.load(Ordering::Relaxed),
            num_page_images: self.num_page_images.load(Ordering::Relaxed),
            num_wal_records: self.num_wal_records.load(Ordering::Relaxed),
            num_getpage_requests: self.num_getpage_requests.load(Ordering::Relaxed),
        }
    }
    */
}
