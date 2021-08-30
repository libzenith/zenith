//! An ImageLayer represents an image or a snapshot of a segment at one particular LSN.
//! It is stored in a file on disk.
//!
//! On disk, the image files are stored in timelines/<timelineid> directory.
//! Currently, there are no subdirectories, and each snapshot file is named like this:
//!
//!    <spcnode>_<dbnode>_<relnode>_<forknum>_<segno>_<LSN>
//!
//! For example:
//!
//!    1663_13990_2609_0_5_000000000169C348
//!
//! An image file is constructed using the 'bookfile' crate.
//!
//! Only metadata is loaded into memory by the load function.
//! When images are needed, they are read directly from disk.
//!
//! For blocky segments, the images are stored in BLOCKY_IMAGES_CHAPTER.
//! All the images are required to be BLOCK_SIZE, which allows for random access.
//!
//! For non-blocky segments, the image can be found in NONBLOCKY_IMAGE_CHAPTER.
//!
use crate::layered_repository::filename::ImageFileName;
use crate::layered_repository::storage_layer::{Layer, PageReconstructData, SegmentTag};
use crate::layered_repository::LayeredTimeline;
use crate::layered_repository::RELISH_SEG_SIZE;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{anyhow, bail, ensure, Result};
use bytes::Bytes;
use log::*;
use std::convert::TryInto;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use bookfile::{Book, BookWriter};

use zenith_utils::lsn::Lsn;

// Magic constant to identify a Zenith segment image file
const IMAGE_FILE_MAGIC: u32 = 0x5A616E01 + 1;

/// Contains each block in block # order
const BLOCKY_IMAGES_CHAPTER: u64 = 1;
const NONBLOCKY_IMAGE_CHAPTER: u64 = 2;

const BLOCK_SIZE: usize = 8192;

///
/// ImageLayer is the in-memory data structure associated with an on-disk image
/// file.  We keep an ImageLayer in memory for each file, in the LayerMap. If a
/// layer is in "loaded" state, we have a copy of the file in memory, in 'inner'.
/// Otherwise the struct is just a placeholder for a file that exists on disk,
/// and it needs to be loaded before using it in queries.
///
pub struct ImageLayer {
    conf: &'static PageServerConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub seg: SegmentTag,

    // This entry contains an image of all pages as of this LSN
    pub lsn: Lsn,

    inner: Mutex<ImageLayerInner>,
}

#[derive(Clone)]
enum ImageType {
    Blocky { num_blocks: u32 },
    NonBlocky,
}

pub struct ImageLayerInner {
    /// If false, the 'image_type' has not been
    /// loaded into memory yet.
    loaded: bool,

    /// Derived from filename and bookfile chapter metadata
    image_type: ImageType,
}

impl Layer for ImageLayer {
    fn filename(&self) -> PathBuf {
        PathBuf::from(
            ImageFileName {
                seg: self.seg,
                lsn: self.lsn,
            }
            .to_string(),
        )
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        return self.timelineid;
    }

    fn get_seg_tag(&self) -> SegmentTag {
        return self.seg;
    }

    fn is_dropped(&self) -> bool {
        return false;
    }

    fn get_start_lsn(&self) -> Lsn {
        return self.lsn;
    }

    fn get_end_lsn(&self) -> Lsn {
        return self.lsn;
    }

    /// Look up given page in the file
    fn get_page_reconstruct_data(
        &self,
        blknum: u32,
        lsn: Lsn,
        reconstruct_data: &mut PageReconstructData,
    ) -> Result<Option<Lsn>> {
        assert!(lsn >= self.lsn);

        let inner = self.load()?;

        let base_blknum = blknum % RELISH_SEG_SIZE;

        let (_path, book) = self.open_book()?;

        let buf = match &inner.image_type {
            ImageType::Blocky { num_blocks } => {
                if base_blknum >= *num_blocks {
                    bail!(
                        "no base img found for {} at blk {} at LSN {}",
                        self.seg,
                        base_blknum,
                        lsn
                    );
                }

                let mut buf = vec![0u8; BLOCK_SIZE];
                let offset = BLOCK_SIZE as u64 * base_blknum as u64;

                let chapter = book.chapter_reader(BLOCKY_IMAGES_CHAPTER)?;
                chapter.read_exact_at(&mut buf, offset)?;

                buf
            }
            ImageType::NonBlocky => {
                ensure!(base_blknum == 0);
                book.read_chapter(NONBLOCKY_IMAGE_CHAPTER)?.into_vec()
            }
        };

        reconstruct_data.page_img = Some(Bytes::from(buf));
        Ok(None)
    }

    /// Get size of the segment
    fn get_seg_size(&self, _lsn: Lsn) -> Result<u32> {
        let inner = self.load()?;
        match inner.image_type {
            ImageType::Blocky { num_blocks } => Ok(num_blocks),
            ImageType::NonBlocky => Err(anyhow!("get_seg_size called for non-blocky segment")),
        }
    }

    /// Does this segment exist at given LSN?
    fn get_seg_exists(&self, _lsn: Lsn) -> Result<bool> {
        Ok(true)
    }

    ///
    /// Release most of the memory used by this layer. If it's accessed again later,
    /// it will need to be loaded back.
    ///
    fn unload(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.image_type = ImageType::Blocky { num_blocks: 0 };
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
}

impl ImageLayer {
    fn path(&self) -> PathBuf {
        Self::path_for(
            self.conf,
            self.timelineid,
            self.tenantid,
            &ImageFileName {
                seg: self.seg,
                lsn: self.lsn,
            },
        )
    }

    fn path_for(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &ImageFileName,
    ) -> PathBuf {
        conf.timeline_path(&timelineid, &tenantid)
            .join(fname.to_string())
    }

    /// Create a new image file, using the given array of pages.
    fn create(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        seg: SegmentTag,
        lsn: Lsn,
        base_images: Vec<Bytes>,
    ) -> Result<ImageLayer> {
        let image_type = if seg.rel.is_blocky() {
            let num_blocks: u32 = base_images.len().try_into()?;
            ImageType::Blocky { num_blocks }
        } else {
            assert_eq!(base_images.len(), 1);
            ImageType::NonBlocky
        };

        let layer = ImageLayer {
            conf: conf,
            timelineid: timelineid,
            tenantid: tenantid,
            seg: seg,
            lsn: lsn,
            inner: Mutex::new(ImageLayerInner {
                loaded: true,
                image_type: image_type.clone(),
            }),
        };
        let inner = layer.inner.lock().unwrap();

        // Write the images into a file
        let path = layer.path();

        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let file = File::create(&path)?;
        let book = BookWriter::new(file, IMAGE_FILE_MAGIC)?;

        let book = match &image_type {
            ImageType::Blocky { .. } => {
                let mut chapter = book.new_chapter(BLOCKY_IMAGES_CHAPTER);
                for block_bytes in base_images {
                    assert_eq!(block_bytes.len(), BLOCK_SIZE);
                    chapter.write_all(&block_bytes)?;
                }
                chapter.close()?
            }
            ImageType::NonBlocky => {
                let mut chapter = book.new_chapter(NONBLOCKY_IMAGE_CHAPTER);
                chapter.write_all(&base_images[0])?;
                chapter.close()?
            }
        };

        book.close()?;

        trace!("saved {}", &path.display());

        drop(inner);

        Ok(layer)
    }

    // Create a new image file by materializing every page in a source layer
    // at given LSN.
    pub fn create_from_src(
        conf: &'static PageServerConf,
        timeline: &LayeredTimeline,
        src: &dyn Layer,
        lsn: Lsn,
    ) -> Result<ImageLayer> {
        let seg = src.get_seg_tag();
        let timelineid = timeline.timelineid;

        let startblk;
        let size;
        if seg.rel.is_blocky() {
            size = src.get_seg_size(lsn)?;
            startblk = seg.segno * RELISH_SEG_SIZE;
        } else {
            size = 1;
            startblk = 0;
        }

        trace!(
            "creating new ImageLayer for {} on timeline {} at {}",
            seg,
            timelineid,
            lsn,
        );

        let mut base_images: Vec<Bytes> = Vec::new();
        for blknum in startblk..(startblk + size) {
            let img = timeline.materialize_page(seg, blknum, lsn, &*src)?;

            base_images.push(img);
        }

        Self::create(conf, timelineid, timeline.tenantid, seg, lsn, base_images)
    }

    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<MutexGuard<ImageLayerInner>> {
        // quick exit if already loaded
        let mut inner = self.inner.lock().unwrap();

        if inner.loaded {
            return Ok(inner);
        }

        let (path, book) = self.open_book()?;

        let image_type = if self.seg.rel.is_blocky() {
            let chapter = book.chapter_reader(BLOCKY_IMAGES_CHAPTER)?;
            let images_len = chapter.len();
            ensure!(images_len % BLOCK_SIZE as u64 == 0);
            let num_blocks: u32 = (images_len / BLOCK_SIZE as u64).try_into()?;
            ImageType::Blocky { num_blocks }
        } else {
            let _chapter = book.chapter_reader(NONBLOCKY_IMAGE_CHAPTER)?;
            ImageType::NonBlocky
        };

        debug!("loaded from {}", &path.display());

        *inner = ImageLayerInner {
            loaded: true,
            image_type,
        };

        Ok(inner)
    }

    fn open_book(&self) -> Result<(PathBuf, Book<File>)> {
        let path = Self::path_for(
            self.conf,
            self.timelineid,
            self.tenantid,
            &ImageFileName {
                seg: self.seg,
                lsn: self.lsn,
            },
        );

        let file = File::open(&path)?;
        let book = Book::new(file)?;

        Ok((path, book))
    }

    /// Create an ImageLayer struct representing an existing file on disk
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &ImageFileName,
    ) -> ImageLayer {
        ImageLayer {
            conf,
            timelineid,
            tenantid,
            seg: filename.seg,
            lsn: filename.lsn,
            inner: Mutex::new(ImageLayerInner {
                loaded: false,
                image_type: ImageType::Blocky { num_blocks: 0 },
            }),
        }
    }

    /// debugging function to print out the contents of the layer
    #[allow(unused)]
    pub fn dump(&self) -> String {
        let mut result = format!("----- image layer for {} at {} ----\n", self.seg, self.lsn);

        //let inner = self.inner.lock().unwrap();

        //for (k, v) in inner.page_versions.iter() {
        //    result += &format!("blk {} at {}: {}/{}\n", k.0, k.1, v.page_image.is_some(), v.record.is_some());
        //}

        result
    }
}
