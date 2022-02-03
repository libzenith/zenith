//!
//! Data structure to ingest incoming WAL into an append-only file.
//!
//! - The file is considered temporary, and will be discarded on crash
//! - based on a B-tree
//!

use std::os::unix::fs::FileExt;
use std::{collections::HashMap, ops::RangeBounds, slice};

use anyhow::Result;

use std::cmp::min;
use std::io::Seek;

use zenith_utils::{lsn::Lsn, vec_map::VecMap};

use super::storage_layer::PageVersion;
use crate::layered_repository::ephemeral_file::EphemeralFile;

use zenith_utils::bin_ser::BeSer;

const EMPTY_SLICE: &[(Lsn, u64)] = &[];

pub struct PageVersions {
    map: HashMap<u32, VecMap<Lsn, u64>>,

    /// The PageVersion structs are stored in a serialized format in this file.
    /// Each serialized PageVersion is preceded by a 'u32' length field.
    /// The 'map' stores offsets into this file.
    file: EphemeralFile,
}

impl PageVersions {
    pub fn new(file: EphemeralFile) -> PageVersions {
        PageVersions {
            map: HashMap::new(),
            file,
        }
    }

    pub fn append_or_update_last(
        &mut self,
        blknum: u32,
        lsn: Lsn,
        page_version: PageVersion,
    ) -> Result<Option<u64>> {
        // remember starting position
        let pos = self.file.stream_position()?;

        // make room for the 'length' field by writing zeros as a placeholder.
        self.file.seek(std::io::SeekFrom::Start(pos + 4)).unwrap();

        page_version.ser_into(&mut self.file).unwrap();

        // write the 'length' field.
        let len = self.file.stream_position()? - pos - 4;
        let lenbuf = u32::to_ne_bytes(len as u32);
        self.file.write_all_at(&lenbuf, pos)?;

        let map = self.map.entry(blknum).or_insert_with(VecMap::default);
        Ok(map.append_or_update_last(lsn, pos as u64).unwrap().0)
    }

    /// Get all [`PageVersion`]s in a block
    fn get_block_slice(&self, blknum: u32) -> &[(Lsn, u64)] {
        self.map
            .get(&blknum)
            .map(VecMap::as_slice)
            .unwrap_or(EMPTY_SLICE)
    }

    /// Get a range of [`PageVersions`] in a block
    pub fn get_block_lsn_range<R: RangeBounds<Lsn>>(&self, blknum: u32, range: R) -> &[(Lsn, u64)] {
        self.map
            .get(&blknum)
            .map(|vec_map| vec_map.slice_range(range))
            .unwrap_or(EMPTY_SLICE)
    }

    /// Iterate through [`PageVersion`]s in (block, lsn) order.
    /// If a [`cutoff_lsn`] is set, only show versions with `lsn < cutoff_lsn`
    pub fn ordered_page_version_iter(&self, cutoff_lsn: Option<Lsn>) -> OrderedPageVersionIter<'_> {
        let mut ordered_blocks: Vec<u32> = self.map.keys().cloned().collect();
        ordered_blocks.sort_unstable();

        let slice = ordered_blocks
            .first()
            .map(|&blknum| self.get_block_slice(blknum))
            .unwrap_or(EMPTY_SLICE);

        OrderedPageVersionIter {
            page_versions: self,
            ordered_blocks,
            cur_block_idx: 0,
            cutoff_lsn,
            cur_slice_iter: slice.iter(),
        }
    }

    ///
    /// Read a page version.
    ///
    pub fn read_pv(&self, off: u64) -> Result<PageVersion> {
        let mut buf = Vec::new();
        self.read_pv_bytes(off, &mut buf)?;
        Ok(PageVersion::des(&buf)?)
    }

    ///
    /// Read a page version, as raw bytes, at the given offset. The bytes
    /// are read into 'buf', which is expanded if necessary. Returns the
    /// size of the page version.
    ///
    pub fn read_pv_bytes(&self, off: u64, buf: &mut Vec<u8>) -> Result<usize> {
        // read length
        let mut lenbuf = [0u8; 4];
        self.file.read_exact_at(&mut lenbuf, off)?;
        let len = u32::from_ne_bytes(lenbuf) as usize;

        // Resize the buffer to fit the data, if needed.
        //
        // We don't shrink the buffer if it's larger than necessary. That avoids
        // repeatedly shrinking and expanding when you reuse the same buffer to
        // read multiple page versions. Expanding a Vec requires initializing the
        // new bytes, which is a waste of time because we're immediately overwriting
        // it, but there's no way to avoid it without resorting to unsafe code.
        if buf.len() < len {
            buf.resize(len, 0);
        }
        self.file.read_exact_at(&mut buf[0..len], off + 4)?;

        Ok(len)
    }
}

pub struct PageVersionReader<'a> {
    file: &'a EphemeralFile,
    pos: u64,
    end_pos: u64,
}

impl<'a> std::io::Read for PageVersionReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let len = min(buf.len(), (self.end_pos - self.pos) as usize);
        let n = self.file.read_at(&mut buf[..len], self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

pub struct OrderedPageVersionIter<'a> {
    page_versions: &'a PageVersions,

    ordered_blocks: Vec<u32>,
    cur_block_idx: usize,

    cutoff_lsn: Option<Lsn>,

    cur_slice_iter: slice::Iter<'a, (Lsn, u64)>,
}

impl OrderedPageVersionIter<'_> {
    fn is_lsn_before_cutoff(&self, lsn: &Lsn) -> bool {
        if let Some(cutoff_lsn) = self.cutoff_lsn.as_ref() {
            lsn < cutoff_lsn
        } else {
            true
        }
    }
}

impl<'a> Iterator for OrderedPageVersionIter<'a> {
    type Item = (u32, Lsn, u64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((lsn, pos)) = self.cur_slice_iter.next() {
                if self.is_lsn_before_cutoff(lsn) {
                    let blknum = self.ordered_blocks[self.cur_block_idx];
                    return Some((blknum, *lsn, *pos));
                }
            }

            let next_block_idx = self.cur_block_idx + 1;
            let blknum: u32 = *self.ordered_blocks.get(next_block_idx)?;
            self.cur_block_idx = next_block_idx;
            self.cur_slice_iter = self.page_versions.get_block_slice(blknum).iter();
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::config::PageServerConf;
    use std::fs;
    use std::str::FromStr;
    use zenith_utils::zid::{ZTenantId, ZTimelineId};

    fn repo_harness(test_name: &str) -> Result<(&'static PageServerConf, ZTenantId, ZTimelineId)> {
        let repo_dir = PageServerConf::test_repo_dir(test_name);
        let _ = fs::remove_dir_all(&repo_dir);
        let conf = PageServerConf::dummy_conf(repo_dir);
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));

        let tenantid = ZTenantId::from_str("11000000000000000000000000000000").unwrap();
        let timelineid = ZTimelineId::from_str("22000000000000000000000000000000").unwrap();
        fs::create_dir_all(conf.timeline_path(&timelineid, &tenantid))?;

        Ok((conf, tenantid, timelineid))
    }

    #[test]
    fn test_ordered_iter() -> Result<()> {
        let (conf, tenantid, timelineid) = repo_harness("test_ordered_iter")?;

        let file = EphemeralFile::create(conf, tenantid, timelineid)?;

        let mut page_versions = PageVersions::new(file);

        const BLOCKS: u32 = 1000;
        const LSNS: u64 = 50;

        let empty_page = Bytes::from_static(&[0u8; 8192]);
        let empty_page_version = PageVersion::Page(empty_page);

        for blknum in 0..BLOCKS {
            for lsn in 0..LSNS {
                let old = page_versions.append_or_update_last(
                    blknum,
                    Lsn(lsn),
                    empty_page_version.clone(),
                )?;
                assert!(old.is_none());
            }
        }

        let mut iter = page_versions.ordered_page_version_iter(None);
        for blknum in 0..BLOCKS {
            for lsn in 0..LSNS {
                let (actual_blknum, actual_lsn, _pv) = iter.next().unwrap();
                assert_eq!(actual_blknum, blknum);
                assert_eq!(Lsn(lsn), actual_lsn);
            }
        }
        assert!(iter.next().is_none());
        assert!(iter.next().is_none()); // should be robust against excessive next() calls

        const CUTOFF_LSN: Lsn = Lsn(30);
        let mut iter = page_versions.ordered_page_version_iter(Some(CUTOFF_LSN));
        for blknum in 0..BLOCKS {
            for lsn in 0..CUTOFF_LSN.0 {
                let (actual_blknum, actual_lsn, _pv) = iter.next().unwrap();
                assert_eq!(actual_blknum, blknum);
                assert_eq!(Lsn(lsn), actual_lsn);
            }
        }
        assert!(iter.next().is_none());
        assert!(iter.next().is_none()); // should be robust against excessive next() calls

        Ok(())
    }
}
