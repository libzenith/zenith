//!
//! An in-memory layer stores recently received page versions in memory. The page versions
//! are held in a BTreeMap, and there's another BTreeMap to track the size of the relation.
//!
use crate::layered_repository::filename::DeltaFileName;
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, PageVersion, SegmentTag, RELISH_SEG_SIZE,
};
use crate::layered_repository::LayeredTimeline;
use crate::layered_repository::ZERO_PAGE;
use crate::layered_repository::{DeltaLayer, ImageLayer};
use crate::repository::WALRecord;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use std::cmp::min;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use zenith_utils::accum::Accum;
use zenith_utils::lsn::Lsn;

pub struct InMemoryLayer {
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    seg: SegmentTag,

    ///
    /// This layer contains all the changes from 'start_lsn'. The
    /// start is inclusive.
    ///
    start_lsn: Lsn,

    /// Frozen in-memory layers have an inclusive end LSN.
    end_lsn: Option<Lsn>,

    /// LSN of the oldest page version stored in this layer
    oldest_pending_lsn: Lsn,

    /// The above fields never change. The parts that do change are in 'inner',
    /// and protected by mutex.
    inner: Mutex<InMemoryLayerInner>,
}

pub struct InMemoryLayerInner {
    /// If this relation was dropped, remember when that happened.
    drop_lsn: Option<Lsn>,

    ///
    /// All versions of all pages in the layer are are kept here.
    /// Indexed by block number and LSN.
    ///
    page_versions: BTreeMap<(u32, Lsn), PageVersion>,

    ///
    /// `segsizes` tracks the size of the segment at different points in time.
    ///
    segsizes: BTreeMap<Lsn, u32>,

    /// Writes are only allowed when true.
    /// Set to false when this layer is in the process of being replaced.
    writeable: bool,

    /// Predecessor layer
    predecessor: Option<Arc<dyn Layer>>,
}

impl InMemoryLayerInner {
    fn check_writeable(&self) -> WriteResult<()> {
        if self.writeable {
            Ok(())
        } else {
            Err(NonWriteableError)
        }
    }

    fn get_seg_size(&self, lsn: Lsn) -> u32 {
        // Scan the BTreeMap backwards, starting from the given entry.
        let mut iter = self.segsizes.range((Included(&Lsn(0)), Included(&lsn)));

        if let Some((_entry_lsn, entry)) = iter.next_back() {
            *entry
        } else {
            0
        }
    }
}

impl Layer for InMemoryLayer {
    // An in-memory layer doesn't really have a filename as it's not stored on disk,
    // but we construct a filename as if it was a delta layer
    fn filename(&self) -> PathBuf {
        let inner = self.inner.lock().unwrap();

        let end_lsn;
        let dropped;
        if let Some(drop_lsn) = inner.drop_lsn {
            end_lsn = drop_lsn;
            dropped = true;
        } else {
            end_lsn = Lsn(u64::MAX);
            dropped = false;
        }

        let delta_filename = DeltaFileName {
            seg: self.seg,
            start_lsn: self.start_lsn,
            end_lsn,
            dropped,
        }
        .to_string();

        PathBuf::from(format!("inmem-{}", delta_filename))
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_seg_tag(&self) -> SegmentTag {
        self.seg
    }

    fn get_start_lsn(&self) -> Lsn {
        self.start_lsn
    }

    fn get_end_lsn(&self) -> Lsn {
        if let Some(end_lsn) = self.end_lsn {
            return Lsn(end_lsn.0 + 1);
        }

        let inner = self.inner.lock().unwrap();

        if let Some(drop_lsn) = inner.drop_lsn {
            drop_lsn
        } else {
            Lsn(u64::MAX)
        }
    }

    fn is_dropped(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.drop_lsn.is_some()
    }

    /// Look up given page in the cache.
    fn get_page_reconstruct_data(
        &self,
        blknum: u32,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<PageReconstructResult> {
        let mut need_image = true;

        assert!(self.seg.blknum_in_seg(blknum));

        let predecessor: Option<Arc<dyn Layer>>;

        {
            let inner = self.inner.lock().unwrap();

            // Scan the BTreeMap backwards, starting from reconstruct_data.lsn.
            let minkey = (blknum, Lsn(0));
            let maxkey = (blknum, lsn);
            let mut iter = inner
                .page_versions
                .range((Included(&minkey), Included(&maxkey)));
            while let Some(((_blknum, _entry_lsn), entry)) = iter.next_back() {
                if let Some(img) = &entry.page_image {
                    reconstruct_data.page_img = Some(img.clone());
                    need_image = false;
                    break;
                } else if let Some(rec) = &entry.record {
                    reconstruct_data.records.push(rec.clone());
                    if rec.will_init {
                        // This WAL record initializes the page, so no need to go further back
                        need_image = false;
                        break;
                    }
                } else {
                    // No base image, and no WAL record. Huh?
                    bail!("no page image or WAL record for requested page");
                }
            }

            predecessor = inner.predecessor.clone();
            // release lock on 'inner'
        }

        // If an older page image is needed to reconstruct the page, let the
        // caller know about the predecessor layer.
        if need_image {
            if let Some(cont_layer) = predecessor {
                Ok(PageReconstructResult::Continue(self.start_lsn, cont_layer))
            } else {
                Ok(PageReconstructResult::Missing(self.start_lsn))
            }
        } else {
            Ok(PageReconstructResult::Complete)
        }
    }

    /// Get size of the relation at given LSN
    fn get_seg_size(&self, lsn: Lsn) -> Result<u32> {
        assert!(lsn >= self.start_lsn);

        let inner = self.inner.lock().unwrap();
        Ok(inner.get_seg_size(lsn))
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool> {
        let inner = self.inner.lock().unwrap();

        // If the segment created after requested LSN,
        // it doesn't exist in the layer. But we shouldn't
        // have requested it in the first place.
        assert!(lsn >= self.start_lsn);

        // Is the requested LSN after the segment was dropped?
        if let Some(drop_lsn) = inner.drop_lsn {
            if lsn >= drop_lsn {
                return Ok(false);
            }
        }

        // Otherwise, it exists
        Ok(true)
    }

    /// Cannot unload anything in an in-memory layer, since there's no backing
    /// store. To release memory used by an in-memory layer, use 'freeze' to turn
    /// it into an on-disk layer.
    fn unload(&self) -> Result<()> {
        Ok(())
    }

    /// Nothing to do here. When you drop the last reference to the layer, it will
    /// be deallocated.
    fn delete(&self) -> Result<()> {
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.predecessor.is_some()
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();

        let end_str = inner
            .drop_lsn
            .as_ref()
            .map(|drop_lsn| drop_lsn.to_string())
            .unwrap_or_default();

        println!(
            "----- in-memory layer for tli {} seg {} {}-{} ----",
            self.timelineid, self.seg, self.start_lsn, end_str
        );

        for (k, v) in inner.segsizes.iter() {
            println!("segsizes {}: {}", k, v);
        }

        for (k, v) in inner.page_versions.iter() {
            println!(
                "blk {} at {}: {}/{}\n",
                k.0,
                k.1,
                v.page_image.is_some(),
                v.record.is_some()
            );
        }

        Ok(())
    }
}

/// Write failed because the layer is in process of being replaced.
/// See [`LayeredTimeline::perform_write_op`] for how to handle this error.
#[derive(Debug)]
pub struct NonWriteableError;

pub type WriteResult<T> = std::result::Result<T, NonWriteableError>;

/// Helper struct to cleanup `InMemoryLayer::freeze` return signature.
pub struct FreezeLayers {
    /// Replacement layer for the layer which freeze was called on.
    pub frozen: Arc<InMemoryLayer>,
    /// New open layer containing leftover data.
    pub open: Option<Arc<InMemoryLayer>>,
}

// TODO kb docs
pub struct LayersOnDisk {
    pub delta_layers: Vec<DeltaLayer>,
    pub image_layer: Option<ImageLayer>,
}

impl InMemoryLayer {
    fn assert_not_frozen(&self) {
        assert!(self.end_lsn.is_none());
    }

    /// Return the oldest page version that's stored in this layer
    pub fn get_oldest_pending_lsn(&self) -> Lsn {
        self.oldest_pending_lsn
    }

    ///
    /// Create a new, empty, in-memory layer
    ///
    pub fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        start_lsn: Lsn,
        oldest_pending_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!(
            "initializing new empty InMemoryLayer for writing {} on timeline {} at {}",
            seg,
            timelineid,
            start_lsn
        );

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            seg,
            start_lsn,
            end_lsn: None,
            oldest_pending_lsn,
            inner: Mutex::new(InMemoryLayerInner {
                drop_lsn: None,
                page_versions: BTreeMap::new(),
                segsizes: BTreeMap::new(),
                writeable: true,
                predecessor: None,
            }),
        })
    }

    // Write operations

    /// Remember new page version, as a WAL record over previous version
    pub fn put_wal_record(&self, blknum: u32, rec: WALRecord) -> WriteResult<u32> {
        self.put_page_version(
            blknum,
            rec.lsn,
            PageVersion {
                page_image: None,
                record: Some(rec),
            },
        )
    }

    /// Remember new page version, as a full page image
    pub fn put_page_image(&self, blknum: u32, lsn: Lsn, img: Bytes) -> WriteResult<u32> {
        self.put_page_version(
            blknum,
            lsn,
            PageVersion {
                page_image: Some(img),
                record: None,
            },
        )
    }

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    pub fn put_page_version(&self, blknum: u32, lsn: Lsn, pv: PageVersion) -> WriteResult<u32> {
        self.assert_not_frozen();
        assert!(self.seg.blknum_in_seg(blknum));

        trace!(
            "put_page_version blk {} of {} at {}/{}",
            blknum,
            self.seg.rel,
            self.timelineid,
            lsn
        );
        let mut inner = self.inner.lock().unwrap();

        inner.check_writeable()?;

        let old = inner.page_versions.insert((blknum, lsn), pv);

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!(
                "Page version of rel {} blk {} at {} already exists",
                self.seg.rel, blknum, lsn
            );
        }

        // Also update the relation size, if this extended the relation.
        if self.seg.rel.is_blocky() {
            let newsize = blknum - self.seg.segno * RELISH_SEG_SIZE + 1;

            // use inner get_seg_size, since calling self.get_seg_size will try to acquire self.inner.lock
            // which we've just acquired above
            let oldsize = inner.get_seg_size(lsn);
            if newsize > oldsize {
                trace!(
                    "enlarging segment {} from {} to {} blocks at {}",
                    self.seg,
                    oldsize,
                    newsize,
                    lsn
                );

                // If we are extending the relation by more than one page, initialize the "gap"
                // with zeros
                //
                // XXX: What if the caller initializes the gap with subsequent call with same LSN?
                // I don't think that can happen currently, but that is highly dependent on how
                // PostgreSQL writes its WAL records and there's no guarantee of it. If it does
                // happen, we would hit the "page version already exists" warning above on the
                // subsequent call to initialize the gap page.
                let gapstart = self.seg.segno * RELISH_SEG_SIZE + oldsize;
                for gapblknum in gapstart..blknum {
                    let zeropv = PageVersion {
                        page_image: Some(ZERO_PAGE.clone()),
                        record: None,
                    };
                    trace!(
                        "filling gap blk {} with zeros for write of {}",
                        gapblknum,
                        blknum
                    );
                    let old = inner.page_versions.insert((gapblknum, lsn), zeropv);
                    // We already had an entry for this LSN. That's odd..

                    if old.is_some() {
                        warn!(
                            "Page version of rel {} blk {} at {} already exists",
                            self.seg.rel, blknum, lsn
                        );
                    }
                }

                inner.segsizes.insert(lsn, newsize);
                return Ok(newsize - oldsize);
            }
        }
        Ok(0)
    }

    /// Remember that the relation was truncated at given LSN
    pub fn put_truncation(&self, lsn: Lsn, segsize: u32) -> WriteResult<()> {
        self.assert_not_frozen();

        let mut inner = self.inner.lock().unwrap();
        inner.check_writeable()?;

        // check that this we truncate to a smaller size than segment was before the truncation
        let oldsize = inner.get_seg_size(lsn);
        assert!(segsize < oldsize);

        let old = inner.segsizes.insert(lsn, segsize);

        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Inserting truncation, but had an entry for the LSN already");
        }

        Ok(())
    }

    /// Remember that the segment was dropped at given LSN
    pub fn drop_segment(&self, lsn: Lsn) -> WriteResult<()> {
        self.assert_not_frozen();

        let mut inner = self.inner.lock().unwrap();

        inner.check_writeable()?;

        assert!(inner.drop_lsn.is_none());
        inner.drop_lsn = Some(lsn);
        inner.writeable = false;

        trace!("dropped segment {} at {}", self.seg, lsn);

        Ok(())
    }

    ///
    /// Initialize a new InMemoryLayer for, by copying the state at the given
    /// point in time from given existing layer.
    ///
    pub fn create_successor_layer(
        conf: &'static PageServerConf,
        src: Arc<dyn Layer>,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        start_lsn: Lsn,
        oldest_pending_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        let seg = src.get_seg_tag();

        assert!(oldest_pending_lsn.is_aligned());
        assert!(oldest_pending_lsn >= start_lsn);

        trace!(
            "initializing new InMemoryLayer for writing {} on timeline {} at {}",
            seg,
            timelineid,
            start_lsn,
        );

        // For convenience, copy the segment size from the predecessor layer
        let mut segsizes = BTreeMap::new();
        if seg.rel.is_blocky() {
            let size = src.get_seg_size(start_lsn)?;
            segsizes.insert(start_lsn, size);
        }

        Ok(InMemoryLayer {
            conf,
            timelineid,
            tenantid,
            seg,
            start_lsn,
            end_lsn: None,
            oldest_pending_lsn,
            inner: Mutex::new(InMemoryLayerInner {
                drop_lsn: None,
                page_versions: BTreeMap::new(),
                segsizes,
                writeable: true,
                predecessor: Some(src),
            }),
        })
    }

    pub fn is_writeable(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.writeable
    }

    /// Splits `self` into two InMemoryLayers: `frozen` and `open`.
    /// All data up to and including `cutoff_lsn`
    /// is copied to `frozen`, while the remaining data is copied to `open`.
    /// After completion, self is non-writeable, but not frozen.
    pub fn freeze(self: Arc<Self>, cutoff_lsn: Lsn) -> Result<FreezeLayers> {
        info!(
            "freezing in memory layer {} on timeline {} at {} (oldest {})",
            self.filename().display(),
            self.timelineid,
            cutoff_lsn,
            self.oldest_pending_lsn
        );

        self.assert_not_frozen();

        let self_ref = self.clone();
        let mut inner = self_ref.inner.lock().unwrap();
        // Dropped layers don't need any special freeze actions,
        // they are marked as non-writeable at drop and just
        // written out to disk by checkpointer.
        if inner.drop_lsn.is_some() {
            assert!(!inner.writeable);
            info!(
                "freezing in memory layer for {} on timeline {} is dropped at {}",
                self.seg,
                self.timelineid,
                inner.drop_lsn.unwrap()
            );

            // There should be no newer layer that refers this non-writeable layer,
            // because layer that is created after dropped one represents a new rel.
            return Ok(FreezeLayers {
                frozen: self,
                open: None,
            });
        }
        assert!(inner.writeable);
        inner.writeable = false;

        // Divide all the page versions into old and new
        // at the 'cutoff_lsn' point.
        let mut before_segsizes = BTreeMap::new();
        let mut after_segsizes = BTreeMap::new();
        let mut after_oldest_lsn: Accum<Lsn> = Accum(None);
        for (lsn, size) in inner.segsizes.iter() {
            if *lsn > cutoff_lsn {
                after_segsizes.insert(*lsn, *size);
                after_oldest_lsn.accum(min, *lsn);
            } else {
                before_segsizes.insert(*lsn, *size);
            }
        }

        let mut before_page_versions = BTreeMap::new();
        let mut after_page_versions = BTreeMap::new();
        for ((blknum, lsn), pv) in inner.page_versions.iter() {
            if *lsn > cutoff_lsn {
                after_page_versions.insert((*blknum, *lsn), pv.clone());
                after_oldest_lsn.accum(min, *lsn);
            } else {
                before_page_versions.insert((*blknum, *lsn), pv.clone());
            }
        }

        let frozen = Arc::new(InMemoryLayer {
            conf: self.conf,
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            seg: self.seg,
            start_lsn: self.start_lsn,
            end_lsn: Some(cutoff_lsn),
            oldest_pending_lsn: self.start_lsn,
            inner: Mutex::new(InMemoryLayerInner {
                drop_lsn: inner.drop_lsn,
                page_versions: before_page_versions,
                segsizes: before_segsizes,
                writeable: false,
                predecessor: inner.predecessor.clone(),
            }),
        });

        let open = if !after_segsizes.is_empty() || !after_page_versions.is_empty() {
            let mut new_open = Self::create_successor_layer(
                self.conf,
                frozen.clone(),
                self.timelineid,
                self.tenantid,
                cutoff_lsn + 1,
                after_oldest_lsn.0.unwrap(),
            )?;

            let new_inner = new_open.inner.get_mut().unwrap();
            new_inner.page_versions.append(&mut after_page_versions);
            new_inner.segsizes.append(&mut after_segsizes);

            Some(Arc::new(new_open))
        } else {
            None
        };

        Ok(FreezeLayers { frozen, open })
    }

    /// Write the this frozen in-memory layer to disk.
    ///
    /// Returns new layers that replace this one.
    /// If not dropped, returns a new image layer containing the page versions
    /// at the `end_lsn`. Can also return a DeltaLayer that includes all the
    /// WAL records between start and end LSN. (The delta layer is not needed
    /// when a new relish is created with a single LSN, so that the start and
    /// end LSN are the same.)
    pub fn write_to_disk(&self, timeline: &LayeredTimeline) -> Result<LayersOnDisk> {
        trace!(
            "write_to_disk {} end_lsn is {} get_end_lsn is {}",
            self.filename().display(),
            self.end_lsn.unwrap_or(Lsn(0)),
            self.get_end_lsn()
        );

        let inner = self.inner.lock().unwrap();
        let drop_lsn = inner.drop_lsn;

        assert!(!inner.writeable);

        let end_lsn = match drop_lsn {
            Some(dlsn) => dlsn,
            None => self.end_lsn.unwrap(),
        };

        let predecessor = inner.predecessor.clone();

        let mut before_page_versions;
        let mut before_segsizes;
        if inner.drop_lsn.is_none() {
            before_segsizes = BTreeMap::new();
            for (lsn, size) in inner.segsizes.iter() {
                if *lsn <= end_lsn {
                    before_segsizes.insert(*lsn, *size);
                }
            }

            before_page_versions = BTreeMap::new();
            for ((blknum, lsn), pv) in inner.page_versions.iter() {
                if *lsn < end_lsn {
                    before_page_versions.insert((*blknum, *lsn), pv.clone());
                }
            }
        } else {
            before_page_versions = inner.page_versions.clone();
            before_segsizes = inner.segsizes.clone();
        }

        drop(inner);

        let mut delta_layers = Vec::new();
        if self.start_lsn != end_lsn {
            // Write the page versions before the cutoff to disk.
            let delta_layer = DeltaLayer::create(
                self.conf,
                self.timelineid,
                self.tenantid,
                self.seg,
                self.start_lsn,
                end_lsn,
                drop_lsn.is_some(),
                predecessor,
                before_page_versions,
                before_segsizes,
            )?;
            delta_layers.push(delta_layer);
            trace!(
                "freeze: created delta layer {} {}-{}",
                self.seg,
                self.start_lsn,
                end_lsn
            );
        } else {
            assert!(before_page_versions.is_empty());
        }

        let image_layer = if drop_lsn.is_none() {
            // Write a new base image layer at the cutoff point
            let image_layer = ImageLayer::create_from_src(self.conf, timeline, self, end_lsn)?;
            trace!("freeze: created image layer {} at {}", self.seg, end_lsn);
            Some(image_layer)
        } else {
            None
        };

        Ok(LayersOnDisk {
            delta_layers,
            image_layer,
        })
    }

    pub fn update_predecessor(&self, predecessor: Arc<dyn Layer>) -> Option<Arc<dyn Layer>> {
        let mut inner = self.inner.lock().unwrap();
        inner.predecessor.replace(predecessor)
    }
}
