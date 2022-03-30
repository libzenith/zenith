//! An ImageLayer represents an image or a snapshot of a key-range at
//! one particular LSN. It contains an image of all key-value pairs
//! in its key-range. Any key that falls into the image layer's range
//! but does not exist in the layer, does not exist.
//!
//! An image layer is stored in a file on disk. The file is stored in
//! timelines/<timelineid> directory.  Currently, there are no
//! subdirectories, and each image layer file is named like this:
//!
//!    <key start>-<key end>__<LSN>
//!
//! For example:
//!
//!    000000067F000032BE0000400000000070B6-000000067F000032BE0000400000000080B6__00000000346BC568
//!
//! An image file is constructed using the 'bookfile' crate.
//!
//! Only metadata is loaded into memory by the load function.
//! When images are needed, they are read directly from disk.
//!
use crate::config::PageServerConf;
use crate::layered_repository::filename::{ImageFileName, PathOrConf};
use crate::layered_repository::storage_layer::{
    BlobRef, Layer, ValueReconstructResult, ValueReconstructState,
};
use crate::repository::{Key, Value};
use crate::virtual_file::VirtualFile;
use crate::IMAGE_FILE_MAGIC;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, TryLockError};

use bookfile::{Book, BookWriter, ChapterWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

/// Mapping from (key, lsn) -> page/WAL record
/// byte ranges in VALUES_CHAPTER
static INDEX_CHAPTER: u64 = 1;

/// Contains each block in block # order
const VALUES_CHAPTER: u64 = 2;

/// Contains the [`Summary`] struct
const SUMMARY_CHAPTER: u64 = 3;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    key_range: Range<Key>,

    lsn: Lsn,
}

impl From<&ImageLayer> for Summary {
    fn from(layer: &ImageLayer) -> Self {
        Self {
            tenantid: layer.tenantid,
            timelineid: layer.timelineid,
            key_range: layer.key_range.clone(),

            lsn: layer.lsn,
        }
    }
}

///
/// ImageLayer is the in-memory data structure associated with an on-disk image
/// file.  We keep an ImageLayer in memory for each file, in the LayerMap. If a
/// layer is in "loaded" state, we have a copy of the index in memory, in 'inner'.
/// Otherwise the struct is just a placeholder for a file that exists on disk,
/// and it needs to be loaded before using it in queries.
///
pub struct ImageLayer {
    path_or_conf: PathOrConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub key_range: Range<Key>,

    // This entry contains an image of all pages as of this LSN
    pub lsn: Lsn,

    inner: RwLock<ImageLayerInner>,
}

pub struct ImageLayerInner {
    /// If false, the 'index' has not been loaded into memory yet.
    loaded: bool,

    /// The underlying (virtual) file handle. None if the layer hasn't been loaded
    /// yet.
    book: Option<Book<VirtualFile>>,

    /// offset of each value
    index: HashMap<Key, BlobRef>,
}

impl Layer for ImageLayer {
    fn filename(&self) -> PathBuf {
        PathBuf::from(self.layer_name().to_string())
    }

    fn get_tenant_id(&self) -> ZTenantId {
        self.tenantid
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        // End-bound is exclusive
        self.lsn..(self.lsn + 1)
    }

    /// Look up given page in the file
    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
    ) -> anyhow::Result<ValueReconstructResult> {
        assert!(self.key_range.contains(&key));
        assert!(lsn_range.end >= self.lsn);

        let inner = self.load()?;

        if let Some(blob_ref) = inner.index.get(&key) {
            let chapter = inner
                .book
                .as_ref()
                .unwrap()
                .chapter_reader(VALUES_CHAPTER)?;

            let mut blob = vec![0; blob_ref.size()];
            chapter
                .read_exact_at(&mut blob, blob_ref.pos())
                .with_context(|| {
                    format!(
                        "failed to read {} bytes from data file {} at offset {}",
                        blob_ref.size(),
                        self.filename().display(),
                        blob_ref.pos()
                    )
                })?;
            let value = Bytes::from(blob);

            reconstruct_state.img = Some((self.lsn, value));
            Ok(ValueReconstructResult::Complete)
        } else {
            Ok(ValueReconstructResult::Missing)
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>>> {
        todo!();
    }

    fn unload(&self) -> Result<()> {
        // Unload the index.
        //
        // TODO: we should access the index directly from pages on the disk,
        // using the buffer cache. This load/unload mechanism is really ad hoc.

        // FIXME: In debug mode, loading and unloading the index slows
        // things down so much that you get timeout errors. At least
        // with the test_parallel_copy test. So as an even more ad hoc
        // stopgap fix for that, only unload every on average 10
        // checkpoint cycles.
        use rand::RngCore;
        if rand::thread_rng().next_u32() > (u32::MAX / 10) {
            return Ok(());
        }

        let mut inner = match self.inner.try_write() {
            Ok(inner) => inner,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(TryLockError::Poisoned(_)) => panic!("ImageLayer lock was poisoned"),
        };
        inner.index = HashMap::default();
        inner.loaded = false;

        Ok(())
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        false
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        println!(
            "----- image layer for ten {} tli {} key {}-{} at {} ----",
            self.tenantid, self.timelineid, self.key_range.start, self.key_range.end, self.lsn
        );

        let inner = self.load()?;

        let mut index_vec: Vec<(&Key, &BlobRef)> = inner.index.iter().collect();
        index_vec.sort_by_key(|x| x.1.pos());

        for (key, blob_ref) in index_vec {
            println!(
                "key: {} size {} offset {}",
                key,
                blob_ref.size(),
                blob_ref.pos()
            );
        }

        Ok(())
    }
}

impl ImageLayer {
    fn path_for(
        path_or_conf: &PathOrConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &ImageFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.to_path_buf(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(&timelineid, &tenantid)
                .join(fname.to_string()),
        }
    }

    ///
    /// Open the underlying file and read the metadata into memory, if it's
    /// not loaded already.
    ///
    fn load(&self) -> Result<RwLockReadGuard<ImageLayerInner>> {
        loop {
            // Quick exit if already loaded
            let inner = self.inner.read().unwrap();
            if inner.loaded {
                return Ok(inner);
            }

            // Need to open the file and load the metadata. Upgrade our lock to
            // a write lock. (Or rather, release and re-lock in write mode.)
            drop(inner);
            let mut inner = self.inner.write().unwrap();
            if !inner.loaded {
                self.load_inner(&mut inner)?;
            } else {
                // Another thread loaded it while we were not holding the lock.
            }

            // We now have the file open and loaded. There's no function to do
            // that in the std library RwLock, so we have to release and re-lock
            // in read mode. (To be precise, the lock guard was moved in the
            // above call to `load_inner`, so it's already been released). And
            // while we do that, another thread could unload again, so we have
            // to re-check and retry if that happens.
        }
    }

    fn load_inner(&self, inner: &mut ImageLayerInner) -> Result<()> {
        let path = self.path();

        // Open the file if it's not open already.
        if inner.book.is_none() {
            let file = VirtualFile::open(&path)
                .with_context(|| format!("Failed to open file '{}'", path.display()))?;
            inner.book = Some(Book::new(file).with_context(|| {
                format!("Failed to open file '{}' as a bookfile", path.display())
            })?);
        }
        let book = inner.book.as_ref().unwrap();

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

        let chapter = book.read_chapter(INDEX_CHAPTER)?;
        let index = HashMap::des(&chapter)?;

        info!("loaded from {}", &path.display());

        inner.index = index;
        inner.loaded = true;

        Ok(())
    }

    /// Create an ImageLayer struct representing an existing file on disk
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &ImageFileName,
    ) -> ImageLayer {
        ImageLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            key_range: filename.key_range.clone(),
            lsn: filename.lsn,
            inner: RwLock::new(ImageLayerInner {
                book: None,
                index: HashMap::new(),
                loaded: false,
            }),
        }
    }

    /// Create an ImageLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'dump_layerfile' binary.
    pub fn new_for_path<F>(path: &Path, book: &Book<F>) -> Result<ImageLayer>
    where
        F: std::os::unix::prelude::FileExt,
    {
        let chapter = book.read_chapter(SUMMARY_CHAPTER)?;
        let summary = Summary::des(&chapter)?;

        Ok(ImageLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            timelineid: summary.timelineid,
            tenantid: summary.tenantid,
            key_range: summary.key_range,
            lsn: summary.lsn,
            inner: RwLock::new(ImageLayerInner {
                book: None,
                index: HashMap::new(),
                loaded: false,
            }),
        })
    }

    fn layer_name(&self) -> ImageFileName {
        ImageFileName {
            key_range: self.key_range.clone(),
            lsn: self.lsn,
        }
    }

    /// Path to the layer file in pageserver workdir.
    pub fn path(&self) -> PathBuf {
        Self::path_for(
            &self.path_or_conf,
            self.timelineid,
            self.tenantid,
            &self.layer_name(),
        )
    }
}

/// A builder object for constructing a new image layer.
///
/// Usage:
///
/// 1. Create the ImageLayerWriter by calling ImageLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_page_image` for every page
///    in the segment.
///
/// 3. Call `finish`.
///
pub struct ImageLayerWriter {
    conf: &'static PageServerConf,
    path: PathBuf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
    key_range: Range<Key>,
    lsn: Lsn,

    values_writer: Option<ChapterWriter<BufWriter<VirtualFile>>>,
    end_offset: u64,

    index: HashMap<Key, BlobRef>,

    finished: bool,
}

impl ImageLayerWriter {
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        key_range: &Range<Key>,
        lsn: Lsn,
    ) -> anyhow::Result<ImageLayerWriter> {
        // Create the file
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path = ImageLayer::path_for(
            &PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            &ImageFileName {
                key_range: key_range.clone(),
                lsn,
            },
        );
        info!("new image layer {}", path.display());
        let file = VirtualFile::create(&path)?;
        let buf_writer = BufWriter::new(file);
        let book = BookWriter::new(buf_writer, IMAGE_FILE_MAGIC)?;

        // Open the page-images chapter for writing. The calls to
        // `put_image` will use this to write the contents.
        let chapter = book.new_chapter(VALUES_CHAPTER);

        let writer = ImageLayerWriter {
            conf,
            path,
            timelineid,
            tenantid,
            key_range: key_range.clone(),
            lsn,
            values_writer: Some(chapter),
            index: HashMap::new(),
            end_offset: 0,
            finished: false,
        };

        Ok(writer)
    }

    ///
    /// Write next value to the file.
    ///
    /// The page versions must be appended in blknum order.
    ///
    pub fn put_image(&mut self, key: Key, img: &[u8]) -> Result<()> {
        ensure!(self.key_range.contains(&key));
        let off = self.end_offset;

        if let Some(writer) = &mut self.values_writer {
            let len = img.len();
            writer.write_all(img)?;
            self.end_offset += len as u64;

            let old = self.index.insert(key, BlobRef::new(off, len, true));
            assert!(old.is_none());
        } else {
            panic!()
        }

        Ok(())
    }

    pub fn finish(&mut self) -> anyhow::Result<ImageLayer> {
        // Close the values chapter
        let book = self.values_writer.take().unwrap().close()?;

        // Write out the index
        let mut chapter = book.new_chapter(INDEX_CHAPTER);
        let buf = HashMap::ser(&self.index)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        // Write out the summary chapter
        let mut chapter = book.new_chapter(SUMMARY_CHAPTER);
        let summary = Summary {
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            key_range: self.key_range.clone(),
            lsn: self.lsn,
        };
        Summary::ser_into(&summary, &mut chapter)?;
        let book = chapter.close()?;

        // This flushes the underlying 'buf_writer'.
        book.close()?;

        // Note: Because we open the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.book here. The first read will have to re-open it.
        let layer = ImageLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            timelineid: self.timelineid,
            tenantid: self.tenantid,
            key_range: self.key_range.clone(),
            lsn: self.lsn,
            inner: RwLock::new(ImageLayerInner {
                book: None,
                loaded: false,
                index: HashMap::new(),
            }),
        };
        trace!("created image layer {}", layer.path().display());

        self.finished = true;

        Ok(layer)
    }
}

impl Drop for ImageLayerWriter {
    fn drop(&mut self) {
        if let Some(page_image_writer) = self.values_writer.take() {
            if let Ok(book) = page_image_writer.close() {
                let _ = book.close();
            }
        }
        if !self.finished {
            let _ = fs::remove_file(&self.path);
        }
    }
}
