//
// Page Cache holds all the different page versions and WAL records
//
// The Page Cache is a BTreeMap, keyed by the RelFileNode an blocknumber, and the LSN.
// The BTreeMap is protected by a Mutex, and each cache entry is protected by another
// per-entry mutex.
//

use crate::restore_local_repo::restore_timeline;
use crate::ZTimelineId;
use crate::{walredo, PageServerConf};
use anyhow::bail;
use bytes::Bytes;
use core::ops::Bound::Included;
use crossbeam_channel::unbounded;
use crossbeam_channel::{Receiver, Sender};
use lazy_static::lazy_static;
use log::*;
use rand::Rng;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
use std::{convert::TryInto, ops::AddAssign};

// Timeout when waiting or WAL receiver to catch up to an LSN given in a GetPage@LSN call.
static TIMEOUT: Duration = Duration::from_secs(60);

pub struct PageCache {
    shared: Mutex<PageCacheShared>,

    // Channel for communicating with the WAL redo process here.
    pub walredo_sender: Sender<Arc<CacheEntry>>,
    pub walredo_receiver: Receiver<Arc<CacheEntry>>,

    valid_lsn_condvar: Condvar,

    // Counters, for metrics collection.
    pub num_entries: AtomicU64,
    pub num_page_images: AtomicU64,
    pub num_wal_records: AtomicU64,
    pub num_getpage_requests: AtomicU64,

    // copies of shared.first/last_valid_lsn fields (copied here so
    // that they can be read without acquiring the mutex).
    pub first_valid_lsn: AtomicU64,
    pub last_valid_lsn: AtomicU64,
    pub last_record_lsn: AtomicU64,
}

#[derive(Clone)]
pub struct PageCacheStats {
    pub num_entries: u64,
    pub num_page_images: u64,
    pub num_wal_records: u64,
    pub num_getpage_requests: u64,
    pub first_valid_lsn: u64,
    pub last_valid_lsn: u64,
    pub last_record_lsn: u64,
}

impl AddAssign for PageCacheStats {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            num_entries: self.num_entries + other.num_entries,
            num_page_images: self.num_page_images + other.num_page_images,
            num_wal_records: self.num_wal_records + other.num_wal_records,
            num_getpage_requests: self.num_getpage_requests + other.num_getpage_requests,
            first_valid_lsn: self.first_valid_lsn + other.first_valid_lsn,
            last_valid_lsn: self.last_valid_lsn + other.last_valid_lsn,
            last_record_lsn: self.last_record_lsn + other.last_record_lsn,
        }
    }
}

//
// Shared data structure, holding page cache and related auxiliary information
//
struct PageCacheShared {
    // The actual page cache
    pagecache: BTreeMap<CacheKey, Arc<CacheEntry>>,

    // Relation n_blocks cache
    //
    // This hashtable should be updated together with the pagecache. Now it is
    // accessed unreasonably often through the smgr_nblocks(). It is better to just
    // cache it in postgres smgr and ask only on restart.
    relsize_cache: HashMap<RelTag, u32>,

    // What page versions do we hold in the cache? If we get GetPage with
    // LSN < first_valid_lsn, that's an error because we (no longer) hold that
    // page version. If we get a request > last_valid_lsn, we need to wait until
    // we receive all the WAL up to the request.
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
    first_valid_lsn: u64,
    last_valid_lsn: u64,
    last_record_lsn: u64,
    walreceiver_works: bool,
}

lazy_static! {
    pub static ref PAGECACHES: Mutex<HashMap<ZTimelineId, Arc<PageCache>>> =
        Mutex::new(HashMap::new());
}

// Get Page Cache for given timeline. It is assumed to already exist.
pub fn get_pagecache(_conf: &PageServerConf, timelineid: ZTimelineId) -> Option<Arc<PageCache>> {
    let pcaches = PAGECACHES.lock().unwrap();

    match pcaches.get(&timelineid) {
        Some(pcache) => Some(pcache.clone()),
        None => None,
    }
}

pub fn get_or_restore_pagecache(
    conf: &PageServerConf,
    timelineid: ZTimelineId,
) -> anyhow::Result<Arc<PageCache>> {
    let mut pcaches = PAGECACHES.lock().unwrap();

    match pcaches.get(&timelineid) {
        Some(pcache) => Ok(pcache.clone()),
        None => {
            let pcache = init_page_cache();

            restore_timeline(conf, &pcache, timelineid)?;

            let result = Arc::new(pcache);

            pcaches.insert(timelineid, result.clone());

            // Initialize the WAL redo thread
            //
            // Now join_handle is not saved any where and we won'try restart tharead
            // if it is dead. We may later stop that treads after some inactivity period
            // and restart them on demand.
            let conf_copy = conf.clone();
            let _walredo_thread = thread::Builder::new()
                .name("WAL redo thread".into())
                .spawn(move || {
                    walredo::wal_redo_main(&conf_copy, timelineid);
                })
                .unwrap();

            return Ok(result);
        }
    }
}

fn init_page_cache() -> PageCache {
    // Initialize the channel between the page cache and the WAL applicator
    let (s, r) = unbounded();

    PageCache {
        shared: Mutex::new(PageCacheShared {
            pagecache: BTreeMap::new(),
            relsize_cache: HashMap::new(),
            first_valid_lsn: 0,
            last_valid_lsn: 0,
            last_record_lsn: 0,
            walreceiver_works: false,
        }),
        valid_lsn_condvar: Condvar::new(),

        walredo_sender: s,
        walredo_receiver: r,

        num_entries: AtomicU64::new(0),
        num_page_images: AtomicU64::new(0),
        num_wal_records: AtomicU64::new(0),
        num_getpage_requests: AtomicU64::new(0),

        first_valid_lsn: AtomicU64::new(0),
        last_valid_lsn: AtomicU64::new(0),
        last_record_lsn: AtomicU64::new(0),
    }
}

//
// We store two kinds of entries in the page cache:
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
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct CacheKey {
    pub tag: BufferTag,
    pub lsn: u64,
}

pub struct CacheEntry {
    pub key: CacheKey,

    pub content: Mutex<CacheEntryContent>,

    // Condition variable used by the WAL redo service, to wake up
    // requester.
    //
    // FIXME: this takes quite a lot of space. Consider using parking_lot::Condvar
    // or something else.
    pub walredo_condvar: Condvar,
}

pub struct CacheEntryContent {
    pub page_image: Option<Bytes>,
    pub wal_record: Option<WALRecord>,
    pub apply_pending: bool,
}

impl CacheEntry {
    fn new(key: CacheKey) -> CacheEntry {
        CacheEntry {
            key,
            content: Mutex::new(CacheEntryContent {
                page_image: None,
                wal_record: None,
                apply_pending: false,
            }),
            walredo_condvar: Condvar::new(),
        }
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub struct RelTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u8,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub struct BufferTag {
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
    pub forknum: u8,
    pub blknum: u32,
}

#[derive(Clone)]
pub struct WALRecord {
    pub lsn: u64, // LSN at the *end* of the record
    pub will_init: bool,
    pub rec: Bytes,
    // Remember the offset of main_data in rec,
    // so that we don't have to parse the record again.
    // If record has no main_data, this offset equals rec.len().
    pub main_data_offset: usize,
}

// Public interface functions

impl PageCache {
    //
    // GetPage@LSN
    //
    // Returns an 8k page image
    //
    pub fn get_page_at_lsn(&self, tag: BufferTag, lsn: u64) -> anyhow::Result<Bytes> {
        self.num_getpage_requests.fetch_add(1, Ordering::Relaxed);

        // Look up cache entry. If it's a page image, return that. If it's a WAL record,
        // ask the WAL redo service to reconstruct the page image from the WAL records.
        let minkey = CacheKey { tag, lsn: 0 };
        let maxkey = CacheKey { tag, lsn };

        let entry_rc: Arc<CacheEntry>;
        {
            let mut shared = self.shared.lock().unwrap();
            let mut waited = false;

            // There is a a race at postgres instance start
            // when we request a page before walsender established connection
            // and was able to stream the page. Just don't wait and return what we have.
            // TODO is there any corner case when this is incorrect?
            if !shared.walreceiver_works {
                trace!(
                    " walreceiver doesn't work yet last_valid_lsn {}, requested {}",
                    shared.last_valid_lsn,
                    lsn
                );
            }

            if shared.walreceiver_works {
                while lsn > shared.last_valid_lsn {
                    // TODO: Wait for the WAL receiver to catch up
                    waited = true;
                    trace!(
                        "not caught up yet: {}, requested {}",
                        shared.last_valid_lsn,
                        lsn
                    );
                    let wait_result = self
                        .valid_lsn_condvar
                        .wait_timeout(shared, TIMEOUT)
                        .unwrap();

                    shared = wait_result.0;
                    if wait_result.1.timed_out() {
                        bail!(
                            "Timed out while waiting for WAL record at LSN {:X}/{:X} to arrive",
                            lsn >> 32,
                            lsn & 0xffff_ffff
                        );
                    }
                }
            }
            if waited {
                trace!("caught up now, continuing");
            }

            if lsn < shared.first_valid_lsn {
                bail!(
                    "LSN {:X}/{:X} has already been removed",
                    lsn >> 32,
                    lsn & 0xffff_ffff
                );
            }

            let pagecache = &shared.pagecache;

            let mut entries = pagecache.range((Included(&minkey), Included(&maxkey)));

            let entry_opt = entries.next_back();

            if entry_opt.is_none() {
                static ZERO_PAGE: [u8; 8192] = [0u8; 8192];
                return Ok(Bytes::from_static(&ZERO_PAGE));
                /* return Err("could not find page image")?; */
            }
            let (_key, entry) = entry_opt.unwrap();
            entry_rc = entry.clone();

            // Now that we have a reference to the cache entry, drop the lock on the map.
            // It's important to do this before waiting on the condition variable below,
            // and better to do it as soon as possible to maximize concurrency.
        }

        // Lock the cache entry and dig the page image out of it.
        let page_img: Bytes;
        {
            let mut entry_content = entry_rc.content.lock().unwrap();

            if let Some(img) = &entry_content.page_image {
                assert!(!entry_content.apply_pending);
                page_img = img.clone();
            } else if entry_content.wal_record.is_some() {
                //
                // If this page needs to be reconstructed by applying some WAL,
                // send a request to the WAL redo thread.
                //
                if !entry_content.apply_pending {
                    assert!(!entry_content.apply_pending);
                    entry_content.apply_pending = true;

                    let s = &self.walredo_sender;
                    s.send(entry_rc.clone())?;
                }

                while entry_content.apply_pending {
                    entry_content = entry_rc.walredo_condvar.wait(entry_content).unwrap();
                }

                // We should now have a page image. If we don't, it means that WAL redo
                // failed to reconstruct it. WAL redo should've logged that error already.
                page_img = match &entry_content.page_image {
                    Some(p) => p.clone(),
                    None => {
                        error!(
                            "could not apply WAL to reconstruct page image for GetPage@LSN request"
                        );
                        bail!("could not apply WAL to reconstruct page image");
                    }
                };
            } else {
                // No base image, and no WAL record. Huh?
                bail!("no page image or WAL record for requested page");
            }
        }

        // FIXME: assumes little-endian. Only used for the debugging log though
        let page_lsn_hi = u32::from_le_bytes(page_img.get(0..4).unwrap().try_into().unwrap());
        let page_lsn_lo = u32::from_le_bytes(page_img.get(4..8).unwrap().try_into().unwrap());
        trace!(
            "Returning page with LSN {:X}/{:X} for {}/{}/{}.{} blk {}",
            page_lsn_hi,
            page_lsn_lo,
            tag.spcnode,
            tag.dbnode,
            tag.relnode,
            tag.forknum,
            tag.blknum
        );

        return Ok(page_img);
    }

    //
    // Collect all the WAL records that are needed to reconstruct a page
    // image for the given cache entry.
    //
    // Returns an old page image (if any), and a vector of WAL records to apply
    // over it.
    //
    pub fn collect_records_for_apply(&self, entry: &CacheEntry) -> (Option<Bytes>, Vec<WALRecord>) {
        // Scan the BTreeMap backwards, starting from the given entry.
        let shared = self.shared.lock().unwrap();
        let pagecache = &shared.pagecache;

        let minkey = CacheKey {
            tag: entry.key.tag,
            lsn: 0,
        };
        let maxkey = CacheKey {
            tag: entry.key.tag,
            lsn: entry.key.lsn,
        };
        let entries = pagecache.range((Included(&minkey), Included(&maxkey)));

        // the last entry in the range should be the CacheEntry we were given
        //let _last_entry = entries.next_back();
        //assert!(last_entry == entry);

        let mut base_img: Option<Bytes> = None;
        let mut records: Vec<WALRecord> = Vec::new();

        // Scan backwards, collecting the WAL records, until we hit an
        // old page image.
        for (_key, e) in entries.rev() {
            let e = e.content.lock().unwrap();

            if let Some(img) = &e.page_image {
                // We have a base image. No need to dig deeper into the list of
                // records
                base_img = Some(img.clone());
                break;
            } else if let Some(rec) = &e.wal_record {
                records.push(rec.clone());

                // If this WAL record initializes the page, no need to dig deeper.
                if rec.will_init {
                    break;
                }
            } else {
                panic!("no base image and no WAL record on cache entry");
            }
        }

        records.reverse();
        return (base_img, records);
    }

    //
    // Adds a WAL record to the page cache
    //
    pub fn put_wal_record(&self, tag: BufferTag, rec: WALRecord) {
        let lsn = rec.lsn;
        let key = CacheKey { tag, lsn };

        let entry = CacheEntry::new(key.clone());
        entry.content.lock().unwrap().wal_record = Some(rec);

        let mut shared = self.shared.lock().unwrap();

        let rel_tag = RelTag {
            spcnode: tag.spcnode,
            dbnode: tag.dbnode,
            relnode: tag.relnode,
            forknum: tag.forknum,
        };
        let rel_entry = shared.relsize_cache.entry(rel_tag).or_insert(0);
        if tag.blknum >= *rel_entry {
            *rel_entry = tag.blknum + 1;
        }

        //trace!("put_wal_record lsn: {}", lsn);

        let oldentry = shared.pagecache.insert(key, Arc::new(entry));
        self.num_entries.fetch_add(1, Ordering::Relaxed);

        if !oldentry.is_none() {
            error!(
                "overwriting WAL record with LSN {:X}/{:X} in page cache",
                lsn >> 32,
                lsn & 0xffffffff
            );
        }

        self.num_wal_records.fetch_add(1, Ordering::Relaxed);
    }

    //
    // Memorize a full image of a page version
    //
    pub fn put_page_image(&self, tag: BufferTag, lsn: u64, img: Bytes) {
        let key = CacheKey { tag, lsn };

        let entry = CacheEntry::new(key.clone());
        entry.content.lock().unwrap().page_image = Some(img);

        let mut shared = self.shared.lock().unwrap();
        let pagecache = &mut shared.pagecache;

        let oldentry = pagecache.insert(key, Arc::new(entry));
        self.num_entries.fetch_add(1, Ordering::Relaxed);
        assert!(oldentry.is_none());

        //debug!("inserted page image for {}/{}/{}_{} blk {} at {}",
        //        tag.spcnode, tag.dbnode, tag.relnode, tag.forknum, tag.blknum, lsn);

        self.num_page_images.fetch_add(1, Ordering::Relaxed);
    }

    //
    pub fn advance_last_valid_lsn(&self, lsn: u64, from_walreceiver: bool) {
        let mut shared = self.shared.lock().unwrap();

        // Can't move backwards.
        let oldlsn = shared.last_valid_lsn;
        if lsn >= oldlsn {
            // Now we receive entries from walreceiver and should wait
            if from_walreceiver {
                shared.walreceiver_works = true;
            }

            shared.last_valid_lsn = lsn;
            self.valid_lsn_condvar.notify_all();

            self.last_valid_lsn.store(lsn, Ordering::Relaxed);
        } else {
            warn!(
                "attempted to move last valid LSN backwards (was {:X}/{:X}, new {:X}/{:X})",
                oldlsn >> 32,
                oldlsn & 0xffffffff,
                lsn >> 32,
                lsn & 0xffffffff
            );
        }
    }

    //
    // NOTE: this updates last_valid_lsn as well.
    //
    pub fn advance_last_record_lsn(&self, lsn: u64) {
        let mut shared = self.shared.lock().unwrap();

        // Can't move backwards.
        assert!(lsn >= shared.last_valid_lsn);
        assert!(lsn >= shared.last_record_lsn);

        shared.last_valid_lsn = lsn;
        shared.last_record_lsn = lsn;
        self.valid_lsn_condvar.notify_all();

        self.last_valid_lsn.store(lsn, Ordering::Relaxed);
        self.last_record_lsn.store(lsn, Ordering::Relaxed);
    }

    //
    pub fn _advance_first_valid_lsn(&self, lsn: u64) {
        let mut shared = self.shared.lock().unwrap();

        // Can't move backwards.
        assert!(lsn >= shared.first_valid_lsn);

        // Can't overtake last_valid_lsn (except when we're
        // initializing the system and last_valid_lsn hasn't been set yet.
        assert!(shared.last_valid_lsn == 0 || lsn < shared.last_valid_lsn);

        shared.first_valid_lsn = lsn;
        self.first_valid_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn init_valid_lsn(&self, lsn: u64) {
        let mut shared = self.shared.lock().unwrap();

        assert!(shared.first_valid_lsn == 0);
        assert!(shared.last_valid_lsn == 0);
        assert!(shared.last_record_lsn == 0);

        shared.first_valid_lsn = lsn;
        shared.last_valid_lsn = lsn;
        shared.last_record_lsn = lsn;

        self.first_valid_lsn.store(lsn, Ordering::Relaxed);
        self.last_valid_lsn.store(lsn, Ordering::Relaxed);
        self.last_record_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn get_last_valid_lsn(&self) -> u64 {
        let shared = self.shared.lock().unwrap();

        return shared.last_record_lsn;
    }

    //
    // Simple test function for the WAL redo code:
    //
    // 1. Pick a page from the page cache at random.
    // 2. Request that page with GetPage@LSN, using Max LSN (i.e. get the latest page version)
    //
    //
    pub fn _test_get_page_at_lsn(&self) {
        // for quick testing of the get_page_at_lsn() funcion.
        //
        // Get a random page from the page cache. Apply all its WAL, by requesting
        // that page at the highest lsn.

        let mut tag: Option<BufferTag> = None;

        {
            let shared = self.shared.lock().unwrap();
            let pagecache = &shared.pagecache;

            if pagecache.is_empty() {
                info!("page cache is empty");
                return;
            }

            // Find nth entry in the map, where n is picked at random
            let n = rand::thread_rng().gen_range(0..pagecache.len());
            let mut i = 0;
            for (key, _e) in pagecache.iter() {
                if i == n {
                    tag = Some(key.tag);
                    break;
                }
                i += 1;
            }
        }

        info!("testing GetPage@LSN for block {}", tag.unwrap().blknum);
        match self.get_page_at_lsn(tag.unwrap(), 0xffff_ffff_ffff_eeee) {
            Ok(_img) => {
                // This prints out the whole page image.
                //println!("{:X?}", img);
            }
            Err(error) => {
                error!("GetPage@LSN failed: {}", error);
            }
        }
    }

    /// Remember a relation's size in blocks.
    ///
    /// If 'to' is larger than the previously remembered size, the remembered size is increased to 'to'.
    /// But if it's smaller, there is no change.
    pub fn relsize_inc(&self, rel: &RelTag, to: u32) {
        // FIXME: Shouldn't relation size also be tracked with an LSN?
        // If a replica is lagging behind, it needs to get the size as it was on
        // the replica's current replay LSN.
        let mut shared = self.shared.lock().unwrap();
        let entry = shared.relsize_cache.entry(*rel).or_insert(0);

        if to >= *entry {
            *entry = to;
        }
    }

    pub fn relsize_get(&self, rel: &RelTag) -> u32 {
        let mut shared = self.shared.lock().unwrap();
        let entry = shared.relsize_cache.entry(*rel).or_insert(0);
        *entry
    }

    pub fn relsize_exist(&self, rel: &RelTag) -> bool {
        let shared = self.shared.lock().unwrap();
        let relsize_cache = &shared.relsize_cache;
        relsize_cache.contains_key(rel)
    }

    pub fn get_stats(&self) -> PageCacheStats {
        PageCacheStats {
            num_entries: self.num_entries.load(Ordering::Relaxed),
            num_page_images: self.num_page_images.load(Ordering::Relaxed),
            num_wal_records: self.num_wal_records.load(Ordering::Relaxed),
            num_getpage_requests: self.num_getpage_requests.load(Ordering::Relaxed),
            first_valid_lsn: self.first_valid_lsn.load(Ordering::Relaxed),
            last_valid_lsn: self.last_valid_lsn.load(Ordering::Relaxed),
            last_record_lsn: self.last_record_lsn.load(Ordering::Relaxed),
        }
    }
}

pub fn get_stats() -> PageCacheStats {
    let pcaches = PAGECACHES.lock().unwrap();

    let mut stats = PageCacheStats {
        num_entries: 0,
        num_page_images: 0,
        num_wal_records: 0,
        num_getpage_requests: 0,
        first_valid_lsn: 0,
        last_valid_lsn: 0,
        last_record_lsn: 0,
    };

    pcaches.iter().for_each(|(_sys_id, pcache)| {
        stats += pcache.get_stats();
    });
    stats
}
