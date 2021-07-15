//!
//! Generate a tarball with files needed to bootstrap ComputeNode.
//!
//! TODO: this module has nothing to do with PostgreSQL pg_basebackup.
//! It could use a better name.
//!
//! Stateless Postgres compute node is launched by sending tarball which contains non-relational data (multixacts, clog, filenodemaps, twophase files)
//! and generate pg_control and dummy segment of WAL. This module is responsible for creation of such tarball from snapshot directory and
//! data stored in object storage.
//!
use crate::ZTimelineId;
use bytes::{BufMut, BytesMut};
use log::*;
use std::io::Write;
use std::sync::Arc;
use std::time::SystemTime;
use tar::{Builder, Header};
use walkdir::WalkDir;

use crate::object_key::*;
use crate::repository::Timeline;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::*;
use zenith_utils::lsn::Lsn;

/// This is short-living object only for the time of tarball creation,
/// created mostly to avoid passing a lot of parameters between various functions
/// used for constructing tarball.
pub struct Basebackup<'a> {
    ar: Builder<&'a mut dyn Write>,
    timeline: &'a Arc<dyn Timeline>,
    lsn: Lsn,
    prev_record_lsn: Lsn,
    snappath: String,
    slru_buf: [u8; pg_constants::SLRU_SEG_SIZE],
    slru_segno: u32,
    slru_path: &'static str,
}

impl<'a> Basebackup<'a> {
    pub fn new(
        write: &'a mut dyn Write,
        timelineid: ZTimelineId,
        timeline: &'a Arc<dyn Timeline>,
        lsn: Lsn,
        prev_record_lsn: Lsn,
        snapshot_lsn: Lsn,
    ) -> Basebackup<'a> {
        Basebackup {
            ar: Builder::new(write),
            timeline,
            lsn,
            prev_record_lsn,
            snappath: format!("timelines/{}/snapshots/{:016X}", timelineid, snapshot_lsn.0),
            slru_path: "",
            slru_segno: u32::MAX,
            slru_buf: [0u8; pg_constants::SLRU_SEG_SIZE],
        }
    }

    pub fn send_tarball(&mut self) -> anyhow::Result<()> {
        debug!("sending tarball of snapshot in {}", self.snappath);
        for entry in WalkDir::new(&self.snappath) {
            let entry = entry?;
            let fullpath = entry.path();
            let relpath = entry.path().strip_prefix(&self.snappath).unwrap();

            if relpath.to_str().unwrap() == "" {
                continue;
            }

            if entry.file_type().is_dir() {
                trace!(
                    "sending dir {} as {}",
                    fullpath.display(),
                    relpath.display()
                );
                self.ar.append_dir(relpath, fullpath)?;
            } else if entry.file_type().is_symlink() {
                error!("ignoring symlink in snapshot dir");
            } else if entry.file_type().is_file() {
                if !is_rel_file_path(relpath.to_str().unwrap()) {
                    if entry.file_name() != "pg_filenode.map" // this files will be generated from object storage
                        && !relpath.starts_with("pg_xact/")
                        && !relpath.starts_with("pg_multixact/")
                    {
                        trace!("sending {}", relpath.display());
                        self.ar.append_path_with_name(fullpath, relpath)?;
                    }
                } else {
                    // relation pages are loaded on demand and should not be included in tarball
                    trace!("not sending {}", relpath.display());
                }
            } else {
                error!("unknown file type: {}", fullpath.display());
            }
        }

        // Generate non-relational files.
        // Iteration is sorted order: all objects of the same time are grouped and traversed
        // in key ascending order. For example all pg_xact records precede pg_multixact records and are sorted by block number.
        // It allows to easily construct SLRU segments (32 blocks).
        for obj in self.timeline.list_nonrels(self.lsn)? {
            match obj {
                ObjectTag::Clog(slru) => self.add_slru_segment("pg_xact", &obj, slru.blknum)?,
                ObjectTag::MultiXactMembers(slru) => {
                    self.add_slru_segment("pg_multixact/members", &obj, slru.blknum)?
                }
                ObjectTag::MultiXactOffsets(slru) => {
                    self.add_slru_segment("pg_multixact/offsets", &obj, slru.blknum)?
                }
                ObjectTag::FileNodeMap(db) => self.add_relmap_file(&obj, &db)?,
                ObjectTag::TwoPhase(prepare) => self.add_twophase_file(&obj, prepare.xid)?,
                _ => {}
            }
        }
        self.finish_slru_segment()?; // write last non-completed SLRU segment (if any)
        self.add_pgcontrol_file()?;
        self.ar.finish()?;
        debug!("all tarred up!");
        Ok(())
    }

    //
    // Generate SLRU segment files from repository. Path identifies SLRU kind (pg_xact, pg_multixact/members, ...).
    // Intially pass an empty string.
    //
    fn add_slru_segment(
        &mut self,
        path: &'static str,
        tag: &ObjectTag,
        page: u32,
    ) -> anyhow::Result<()> {
        let img = self.timeline.get_page_at_lsn_nowait(*tag, self.lsn)?;
        // Zero length image indicates truncated segment: just skip it
        if !img.is_empty() {
            assert!(img.len() == pg_constants::BLCKSZ as usize);
            let segno = page / pg_constants::SLRU_PAGES_PER_SEGMENT;
            if self.slru_path != "" && (self.slru_segno != segno || self.slru_path != path) {
                // Switch to new segment: save old one
                let segname = format!("{}/{:>04X}", self.slru_path, self.slru_segno);
                let header = new_tar_header(&segname, pg_constants::SLRU_SEG_SIZE as u64)?;
                self.ar.append(&header, &self.slru_buf[..])?;
                self.slru_buf = [0u8; pg_constants::SLRU_SEG_SIZE]; // reinitialize segment buffer
            }
            self.slru_segno = segno;
            self.slru_path = path;
            let offs_start = (page % pg_constants::SLRU_PAGES_PER_SEGMENT) as usize
                * pg_constants::BLCKSZ as usize;
            let offs_end = offs_start + pg_constants::BLCKSZ as usize;
            self.slru_buf[offs_start..offs_end].copy_from_slice(&img);
        }
        Ok(())
    }

    //
    // We flush SLRU segments to the tarball once them are completed.
    // This method is used to flush last (may be incompleted) segment.
    //
    fn finish_slru_segment(&mut self) -> anyhow::Result<()> {
        if self.slru_path != "" {
            // is there is some incompleted segment
            let segname = format!("{}/{:>04X}", self.slru_path, self.slru_segno);
            let header = new_tar_header(&segname, pg_constants::SLRU_SEG_SIZE as u64)?;
            self.ar.append(&header, &self.slru_buf[..])?;
        }
        Ok(())
    }

    //
    // Extract pg_filenode.map files from repository
    //
    fn add_relmap_file(&mut self, tag: &ObjectTag, db: &DatabaseTag) -> anyhow::Result<()> {
        let img = self.timeline.get_page_at_lsn_nowait(*tag, self.lsn)?;
        info!("add_relmap_file {:?}", db);
        let path = if db.spcnode == pg_constants::GLOBALTABLESPACE_OID {
            String::from("global/pg_filenode.map") // filenode map for global tablespace
        } else {
            // User defined tablespaces are not supported
            assert!(db.spcnode == pg_constants::DEFAULTTABLESPACE_OID);
            let src_path = format!("{}/base/1/PG_VERSION", self.snappath);
            let dst_path = format!("base/{}/PG_VERSION", db.dbnode);
            self.ar.append_path_with_name(&src_path, &dst_path)?;
            format!("base/{}/pg_filenode.map", db.dbnode)
        };
        assert!(img.len() == 512);
        let header = new_tar_header(&path, img.len() as u64)?;
        self.ar.append(&header, &img[..])?;
        Ok(())
    }

    //
    // Extract twophase state files
    //
    fn add_twophase_file(&mut self, tag: &ObjectTag, xid: TransactionId) -> anyhow::Result<()> {
        // Include in tarball two-phase files only of in-progress transactions
        if self.timeline.get_tx_status(xid, self.lsn)?
            == pg_constants::TRANSACTION_STATUS_IN_PROGRESS
        {
            let img = self.timeline.get_page_at_lsn_nowait(*tag, self.lsn)?;
            let mut buf = BytesMut::new();
            buf.extend_from_slice(&img[..]);
            let crc = crc32c::crc32c(&img[..]);
            buf.put_u32_le(crc);
            let path = format!("pg_twophase/{:>08X}", xid);
            let header = new_tar_header(&path, buf.len() as u64)?;
            self.ar.append(&header, &buf[..])?;
        }
        Ok(())
    }

    //
    // Add generated pg_control file
    //
    fn add_pgcontrol_file(&mut self) -> anyhow::Result<()> {
        let checkpoint_bytes = self
            .timeline
            .get_page_at_lsn_nowait(ObjectTag::Checkpoint, self.lsn)?;
        let pg_control_bytes = self
            .timeline
            .get_page_at_lsn_nowait(ObjectTag::ControlFile, self.lsn)?;
        let mut pg_control = ControlFileData::decode(&pg_control_bytes)?;
        let mut checkpoint = CheckPoint::decode(&checkpoint_bytes)?;

        // Generate new pg_control and WAL needed for bootstrap
        let checkpoint_segno = self.lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE);
        let checkpoint_lsn = XLogSegNoOffsetToRecPtr(
            checkpoint_segno,
            XLOG_SIZE_OF_XLOG_LONG_PHD as u32,
            pg_constants::WAL_SEGMENT_SIZE,
        );
        checkpoint.redo = self.lsn.0 + self.lsn.calc_padding(8u32);

        //reset some fields we don't want to preserve
        checkpoint.oldestActiveXid = 0;

        //save new values in pg_control
        pg_control.checkPoint = checkpoint_lsn;
        pg_control.checkPointCopy = checkpoint;
        info!("pg_control.state = {}", pg_control.state);
        pg_control.state = pg_constants::DB_SHUTDOWNED;

        // add zenith.signal file
        self.ar.append(
            &new_tar_header("zenith.signal", 8)?,
            &self.prev_record_lsn.0.to_le_bytes()[..],
        )?;

        // send timeline size to enforce size quota
        // TODO don't introduce new file. Add it to zenith.signal?
        let last_lsn = self.timeline.get_last_valid_lsn();
        let timeline_size = &self.timeline.get_timeline_size(last_lsn).unwrap();
        info!("send timeline_size {}", timeline_size);

        self.ar.append(
            &new_tar_header("zenith.size", 8)?,
            &timeline_size.to_le_bytes()[..],
        )?;

        //send pg_control
        let pg_control_bytes = pg_control.encode();
        let header = new_tar_header("global/pg_control", pg_control_bytes.len() as u64)?;
        self.ar.append(&header, &pg_control_bytes[..])?;

        //send wal segment
        let wal_file_name = XLogFileName(
            1, // FIXME: always use Postgres timeline 1
            checkpoint_segno,
            pg_constants::WAL_SEGMENT_SIZE,
        );
        let wal_file_path = format!("pg_wal/{}", wal_file_name);
        let header = new_tar_header(&wal_file_path, pg_constants::WAL_SEGMENT_SIZE as u64)?;
        let wal_seg = generate_wal_segment(&pg_control);
        self.ar.append(&header, &wal_seg[..])?;
        Ok(())
    }
}

///
/// Parse a path, relative to the root of PostgreSQL data directory, as
/// a PostgreSQL relation data file.
///
fn parse_rel_file_path(path: &str) -> Result<(), FilePathError> {
    /*
     * Relation data files can be in one of the following directories:
     *
     * global/
     *		shared relations
     *
     * base/<db oid>/
     *		regular relations, default tablespace
     *
     * pg_tblspc/<tblspc oid>/<tblspc version>/
     *		within a non-default tablespace (the name of the directory
     *		depends on version)
     *
     * And the relation data files themselves have a filename like:
     *
     * <oid>.<segment number>
     */
    if let Some(fname) = path.strip_prefix("global/") {
        let (_relnode, _forknum, _segno) = parse_relfilename(fname)?;

        Ok(())
    } else if let Some(dbpath) = path.strip_prefix("base/") {
        let mut s = dbpath.split('/');
        let dbnode_str = s.next().ok_or(FilePathError::InvalidFileName)?;
        let _dbnode = dbnode_str.parse::<u32>()?;
        let fname = s.next().ok_or(FilePathError::InvalidFileName)?;
        if s.next().is_some() {
            return Err(FilePathError::InvalidFileName);
        };

        let (_relnode, _forknum, _segno) = parse_relfilename(fname)?;

        Ok(())
    } else if path.strip_prefix("pg_tblspc/").is_some() {
        // TODO
        error!("tablespaces not implemented yet");
        Err(FilePathError::InvalidFileName)
    } else {
        Err(FilePathError::InvalidFileName)
    }
}

//
// Check if it is relational file
//
fn is_rel_file_path(path: &str) -> bool {
    parse_rel_file_path(path).is_ok()
}

//
// Create new tarball entry header
//
fn new_tar_header(path: &str, size: u64) -> anyhow::Result<Header> {
    let mut header = Header::new_gnu();
    header.set_size(size);
    header.set_path(path)?;
    header.set_mode(0b110000000); // -rw-------
    header.set_mtime(
        // use currenttime as last modified time
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );
    header.set_cksum();
    Ok(header)
}
