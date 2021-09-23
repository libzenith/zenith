//!
//! A DeltaLayer represents a collection of WAL records or page images in a range of
//! LSNs, for one segment. It is stored on a file on disk.
//!
//! Usually a delta layer only contains differences - in the form of WAL records against
//! a base LSN. However, if a segment is newly created, by creating a new relation or
//! extending an old one, there might be no base image. In that case, all the entries in
//! the delta layer must be page images or WAL records with the 'will_init' flag set, so
//! that they can be replayed without referring to an older page version. Also in some
//! circumstances, the predecessor layer might actually be another delta layer. That
//! can happen when you create a new branch in the middle of a delta layer, and the WAL
//! records on the new branch are put in a new delta layer.
//!
//! When a delta file needs to be accessed, we slurp the metadata and relsize chapters
//! into memory, into the DeltaLayerInner struct. See load() and unload() functions.
//! To access a page/WAL record, we search `page_version_metas` for the block # and LSN.
//! The byte ranges in the metadata can be used to find the page/WAL record in
//! PAGE_VERSIONS_CHAPTER.
//!
//! On disk, the delta files are stored in timelines/<timelineid> directory.
//! Currently, there are no subdirectories, and each delta file is named like this:
//!
//!    <spcnode>_<dbnode>_<relnode>_<forknum>_<segno>_<start LSN>_<end LSN>
//!
//! For example:
//!
//!    1663_13990_2609_0_5_000000000169C348_000000000169C349
//!
//! If a relation is dropped, we add a '_DROPPED' to the end of the filename to indicate that.
//! So the above example would become:
//!
//!    1663_13990_2609_0_5_000000000169C348_000000000169C349_DROPPED
//!
//! The end LSN indicates when it was dropped in that case, we don't store it in the
//! file contents in any way.
//!
//! A detlta file is constructed using the 'bookfile' crate. Each file consists of two
//! parts: the page versions and the relation sizes. They are stored as separate chapters.
//!
use crate::layered_repository::blob::BlobWriter;
use crate::layered_repository::filename::{DeltaFileName, PathOrConf};
use crate::layered_repository::storage_layer::{
    Layer, PageReconstructData, PageReconstructResult, PageVersion, SegmentTag,
};
use crate::repository::WALRecord;
use crate::waldecoder;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Result};
use bytes::Bytes;
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
// avoid binding to Write (conflicts with std::io::Write)
// while being able to use std::fmt::Write's methods
use std::fmt::Write as _;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Bound::Included;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use bookfile::{Book, BookWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

use super::blob::{read_blob, BlobRange};

// Magic constant to identify a Zenith delta file
pub const DELTA_FILE_MAGIC: u32 = 0x5A616E01;

/// Mapping from (block #, lsn) -> page/WAL record
/// byte ranges in PAGE_VERSIONS_CHAPTER
static PAGE_VERSION_METAS_CHAPTER: u64 = 1;
/// Page/WAL bytes - cannot be interpreted
/// without PAGE_VERSION_METAS_CHAPTER
static PAGE_VERSIONS_CHAPTER: u64 = 2;
static REL_SIZES_CHAPTER: u64 = 3;

/// Contains the [`Summary`] struct
static SUMMARY_CHAPTER: u64 = 4;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    seg: SegmentTag,

    start_lsn: Lsn,
    end_lsn: Lsn,

    dropped: bool,
}

impl From<&DeltaLayer> for Summary {
    fn from(layer: &DeltaLayer) -> Self {
        Self {
            tenantid: layer.tenantid,
            timelineid: layer.timelineid,
            seg: layer.seg,

            start_lsn: layer.start_lsn,
            end_lsn: layer.end_lsn,

            dropped: layer.dropped,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct PageVersionMeta {
    page_image_range: Option<BlobRange>,
    record_range: Option<BlobRange>,
}

///
/// DeltaLayer is the in-memory data structure associated with an
/// on-disk delta file.  We keep a DeltaLayer in memory for each
/// file, in the LayerMap. If a layer is in "loaded" state, we have a
/// copy of the file in memory, in 'inner'. Otherwise the struct is
/// just a placeholder for a file that exists on disk, and it needs to
/// be loaded before using it in queries.
///
pub struct DeltaLayer {
    path_or_conf: PathOrConf,

    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub seg: SegmentTag,

    //
    // This entry contains all the changes from 'start_lsn' to 'end_lsn'. The
    // start is inclusive, and end is exclusive.
    //
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,

    dropped: bool,

    /// Predecessor layer
    predecessor: Option<Arc<dyn Layer>>,

    inner: Mutex<DeltaLayerInner>,
}

pub struct DeltaLayerInner {
    /// If false, the 'page_version_metas' and 'relsizes' have not been
    /// loaded into memory yet.
    loaded: bool,

    /// All versions of all pages in the file are are kept here.
    /// Indexed by block number and LSN.
    page_version_metas: BTreeMap<(u32, Lsn), PageVersionMeta>,

    /// `relsizes` tracks the size of the relation at different points in time.
    relsizes: BTreeMap<Lsn, u32>,
}

impl Layer for DeltaLayer {
    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_seg_tag(&self) -> SegmentTag {
        self.seg
    }

    fn is_dropped(&self) -> bool {
        self.dropped
    }

    fn get_start_lsn(&self) -> Lsn {
        self.start_lsn
    }

    fn get_end_lsn(&self) -> Lsn {
        self.end_lsn
    }

    fn filename(&self) -> PathBuf {
        PathBuf::from(self.layer_name().to_string())
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

        {
            // Open the file and lock the metadata in memory
            // TODO: avoid opening the file for each read
            let (_path, book) = self.open_book()?;
            let page_version_reader = book.chapter_reader(PAGE_VERSIONS_CHAPTER)?;
            let inner = self.load()?;

            // Scan the metadata BTreeMap backwards, starting from the given entry.
            let minkey = (blknum, Lsn(0));
            let maxkey = (blknum, lsn);
            let mut iter = inner
                .page_version_metas
                .range((Included(&minkey), Included(&maxkey)));
            while let Some(((_blknum, _entry_lsn), entry)) = iter.next_back() {
                if let Some(img_range) = &entry.page_image_range {
                    // Found a page image, return it
                    let img = Bytes::from(read_blob(&page_version_reader, img_range)?);
                    reconstruct_data.page_img = Some(img);
                    need_image = false;
                    break;
                } else if let Some(rec_range) = &entry.record_range {
                    let rec = WALRecord::des(&read_blob(&page_version_reader, rec_range)?)?;
                    let will_init = rec.will_init;
                    reconstruct_data.records.push(rec);
                    if will_init {
                        // This WAL record initializes the page, so no need to go further back
                        need_image = false;
                        break;
                    }
                } else {
                    // No base image, and no WAL record. Huh?
                    bail!("no page image or WAL record for requested page");
                }
            }

            // release metadata lock and close the file
        }

        // If an older page image is needed to reconstruct the page, let the
        // caller know about the predecessor layer.
        if need_image {
            if let Some(cont_layer) = &self.predecessor {
                Ok(PageReconstructResult::Continue(
                    self.start_lsn,
                    Arc::clone(cont_layer),
                ))
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

        // Scan the BTreeMap backwards, starting from the given entry.
        let inner = self.load()?;
        let mut iter = inner.relsizes.range((Included(&Lsn(0)), Included(&lsn)));

        let result;
        if let Some((_entry_lsn, entry)) = iter.next_back() {
            result = *entry;
        // Use the base image if needed
        } else if let Some(predecessor) = &self.predecessor {
            result = predecessor.get_seg_size(lsn)?;
        } else {
            result = 0;
        }
        Ok(result)
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, lsn: Lsn) -> Result<bool> {
        // Is the requested LSN after the rel was dropped?
        if self.dropped && lsn >= self.end_lsn {
            return Ok(false);
        }

        // Otherwise, it exists.
        Ok(true)
    }

    ///
    /// Release most of the memory used by this layer. If it's accessed again later,
    /// it will need to be loaded back.
    ///
    fn unload(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.page_version_metas = BTreeMap::new();
        inner.relsizes = BTreeMap::new();
        inner.loaded = false;
        Ok(())
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        true
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        println!(
            "----- delta layer for ten {} tli {} seg {} {}-{} ----",
            self.tenantid, self.timelineid, self.seg, self.start_lsn, self.end_lsn
        );

        println!("--- relsizes ---");
        let inner = self.load()?;
        for (k, v) in inner.relsizes.iter() {
            println!("  {}: {}", k, v);
        }
        println!("--- page versions ---");
        let (_path, book) = self.open_book()?;
        let chapter = book.chapter_reader(PAGE_VERSIONS_CHAPTER)?;
        for (k, v) in inner.page_version_metas.iter() {
            let mut desc = String::new();

            if let Some(page_image_range) = v.page_image_range.as_ref() {
                let image = read_blob(&chapter, page_image_range)?;
                write!(&mut desc, " img {} bytes", image.len())?;
            }
            if let Some(record_range) = v.record_range.as_ref() {
                let record_bytes = read_blob(&chapter, record_range)?;
                let rec = WALRecord::des(&record_bytes)?;
                let wal_desc = waldecoder::describe_wal_record(&rec.rec);
                write!(
                    &mut desc,
                    " rec {} bytes will_init: {} {}",
                    rec.rec.len(),
                    rec.will_init,
                    wal_desc
                )?;
            }
            println!("  blk {} at {}: {}", k.0, k.1, desc);
        }

        Ok(())
    }
}

impl DeltaLayer {
    pub fn path_for(
        path_or_conf: &PathOrConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &DeltaFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.clone(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(&timelineid, &tenantid)
                .join(fname.to_string()),
        }
    }

    /// Create a new delta file, using the given btreemaps containing the page versions and
    /// relsizes.
    ///
    /// This is used to write the in-memory layer to disk. The in-memory layer uses the same
    /// data structure with two btreemaps as we do, so passing the btreemaps is currently
    /// expedient.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        start_lsn: Lsn,
        end_lsn: Lsn,
        dropped: bool,
        predecessor: Option<Arc<dyn Layer>>,
        page_versions: BTreeMap<(u32, Lsn), PageVersion>,
        relsizes: BTreeMap<Lsn, u32>,
    ) -> Result<DeltaLayer> {
        let delta_layer = DeltaLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            seg,
            start_lsn,
            end_lsn,
            dropped,
            inner: Mutex::new(DeltaLayerInner {
                loaded: true,
                page_version_metas: BTreeMap::new(),
                relsizes,
            }),
            predecessor,
        };
        let mut inner = delta_layer.inner.lock().unwrap();

        // Write the in-memory btreemaps into a file
        let path = delta_layer.path();

        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let file = File::create(&path)?;
        let buf_writer = BufWriter::new(file);
        let book = BookWriter::new(buf_writer, DELTA_FILE_MAGIC)?;

        let mut page_version_writer = BlobWriter::new(book, PAGE_VERSIONS_CHAPTER);

        for (key, page_version) in page_versions {
            let page_image_range = page_version
                .page_image
                .map(|page_image| page_version_writer.write_blob(page_image.as_ref()))
                .transpose()?;

            let record_range = page_version
                .record
                .map(|record| {
                    let buf = WALRecord::ser(&record)?;
                    page_version_writer.write_blob(&buf)
                })
                .transpose()?;

            let old = inner.page_version_metas.insert(
                key,
                PageVersionMeta {
                    page_image_range,
                    record_range,
                },
            );

            assert!(old.is_none());
        }

        let book = page_version_writer.close()?;

        // Write out page versions
        let mut chapter = book.new_chapter(PAGE_VERSION_METAS_CHAPTER);
        let buf = BTreeMap::ser(&inner.page_version_metas)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        // and relsizes to separate chapter
        let mut chapter = book.new_chapter(REL_SIZES_CHAPTER);
        let buf = BTreeMap::ser(&inner.relsizes)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        let mut chapter = book.new_chapter(SUMMARY_CHAPTER);
        let summary = Summary {
            tenantid,
            timelineid,
            seg,

            start_lsn,
            end_lsn,

            dropped,
        };
        Summary::ser_into(&summary, &mut chapter)?;
        let book = chapter.close()?;

        // This flushes the underlying 'buf_writer'.
        book.close()?;

        trace!("saved {}", &path.display());

        drop(inner);

        Ok(delta_layer)
    }

    fn open_book(&self) -> Result<(PathBuf, Book<File>)> {
        let path = self.path();

        let file = File::open(&path)?;
        let book = Book::new(file)?;

        Ok((path, book))
    }

    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<MutexGuard<DeltaLayerInner>> {
        // quick exit if already loaded
        let mut inner = self.inner.lock().unwrap();

        if inner.loaded {
            return Ok(inner);
        }

        let (path, book) = self.open_book()?;

        match &self.path_or_conf {
            PathOrConf::Conf(_) => {
                let chapter = book.read_chapter(SUMMARY_CHAPTER)?;
                let actual_summary = Summary::des(&chapter)?;

                let expected_summary = Summary::from(self);

                if actual_summary != expected_summary {
                    bail!("in-file summary does not match expected summary. actual = {:?} expected = {:?}", actual_summary, expected_summary);
                }
            }
            PathOrConf::Path(path) => {
                let actual_filename = Path::new(path.file_name().unwrap());
                let expected_filename = self.filename();

                if actual_filename != expected_filename {
                    println!(
                        "warning: filename does not match what is expected from in-file summary"
                    );
                    println!("actual: {:?}", actual_filename);
                    println!("expected: {:?}", expected_filename);
                }
            }
        }

        let chapter = book.read_chapter(PAGE_VERSION_METAS_CHAPTER)?;
        let page_version_metas = BTreeMap::des(&chapter)?;

        let chapter = book.read_chapter(REL_SIZES_CHAPTER)?;
        let relsizes = BTreeMap::des(&chapter)?;

        debug!("loaded from {}", &path.display());

        *inner = DeltaLayerInner {
            loaded: true,
            page_version_metas,
            relsizes,
        };

        Ok(inner)
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &DeltaFileName,
        predecessor: Option<Arc<dyn Layer>>,
    ) -> DeltaLayer {
        DeltaLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            seg: filename.seg,
            start_lsn: filename.start_lsn,
            end_lsn: filename.end_lsn,
            dropped: filename.dropped,
            inner: Mutex::new(DeltaLayerInner {
                loaded: false,
                page_version_metas: BTreeMap::new(),
                relsizes: BTreeMap::new(),
            }),
            predecessor,
        }
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'dump_layerfile' binary.
    pub fn new_for_path(path: &Path, book: &Book<File>) -> Result<Self> {
        let chapter = book.read_chapter(SUMMARY_CHAPTER)?;
        let summary = Summary::des(&chapter)?;

        Ok(DeltaLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            timelineid: summary.timelineid,
            tenantid: summary.tenantid,
            seg: summary.seg,
            start_lsn: summary.start_lsn,
            end_lsn: summary.end_lsn,
            dropped: summary.dropped,
            inner: Mutex::new(DeltaLayerInner {
                loaded: false,
                page_version_metas: BTreeMap::new(),
                relsizes: BTreeMap::new(),
            }),
            predecessor: None,
        })
    }

    pub fn layer_name(&self) -> DeltaFileName {
        DeltaFileName {
            seg: self.seg,
            start_lsn: self.start_lsn,
            end_lsn: self.end_lsn,
            dropped: self.dropped,
        }
    }

    fn path(&self) -> PathBuf {
        Self::path_for(
            &self.path_or_conf,
            self.timelineid,
            self.tenantid,
            &self.layer_name(),
        )
    }
}
