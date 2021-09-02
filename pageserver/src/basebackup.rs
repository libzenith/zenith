//!
//! Generate a tarball with files needed to bootstrap ComputeNode.
//!
//! TODO: this module has nothing to do with PostgreSQL pg_basebackup.
//! It could use a better name.
//!
//! Stateless Postgres compute node is launched by sending a tarball
//! which contains non-relational data (multixacts, clog, filenodemaps, twophase files),
//! generated pg_control and dummy segment of WAL.
//! This module is responsible for creation of such tarball
//! from data stored in object storage.
//!
use bytes::{BufMut, BytesMut};
use log::*;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::time::SystemTime;
use tar::{Builder, EntryType, Header};

use crate::relish::*;
use crate::repository::Timeline;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::*;
use zenith_utils::lsn::{Lsn, RecordLsn};

/// This is short-living object only for the time of tarball creation,
/// created mostly to avoid passing a lot of parameters between various functions
/// used for constructing tarball.
pub struct Basebackup<'a> {
    ar: Builder<&'a mut dyn Write>,
    timeline: &'a Arc<dyn Timeline>,
    lsn: Lsn,
    prev_record_lsn: Lsn,
}

// Create basebackup with non-rel data in it. Omit relational data.
//
// Currently we use empty lsn in two cases:
//  * During the basebackup right after timeline creation
//  * When working without safekeepers. In this situation it is important to match the lsn
//    we are taking basebackup on with the lsn that is used in pageserver's walreceiver
//    to start the replication.
impl<'a> Basebackup<'a> {
    pub fn new(
        write: &'a mut dyn Write,
        timeline: &'a Arc<dyn Timeline>,
        req_lsn: Option<Lsn>,
    ) -> Basebackup<'a> {
        let RecordLsn {
            last: lsn,
            prev: prev_record_lsn,
        } = if let Some(lsn) = req_lsn {
            // FIXME: that wouldn't work since we don't know prev for old LSN's.
            // Probably it is better to avoid using prev in compute node start
            // at all and accept the fact that first WAL record in the timeline would
            // have zero as prev. https://github.com/zenithdb/zenith/issues/506
            RecordLsn {
                last: lsn,
                prev: lsn,
            }
        } else {
            // Atomically get last and prev LSN's
            timeline.get_last_record_rlsn()
        };

        Basebackup {
            ar: Builder::new(write),
            timeline,
            lsn,
            prev_record_lsn,
        }
    }

    pub fn send_tarball(&mut self) -> anyhow::Result<()> {
        // Create pgdata subdirs structure
        for dir in pg_constants::PGDATA_SUBDIRS.iter() {
            info!("send subdir {:?}", *dir);
            let header = new_tar_header_dir(*dir)?;
            self.ar.append(&header, &mut io::empty())?;
        }

        // Send empty config files.
        for filepath in pg_constants::PGDATA_SPECIAL_FILES.iter() {
            if *filepath == "pg_hba.conf" {
                let data = pg_constants::PG_HBA.as_bytes();
                let header = new_tar_header(&filepath, data.len() as u64)?;
                self.ar.append(&header, &data[..])?;
            } else {
                let header = new_tar_header(&filepath, 0)?;
                self.ar.append(&header, &mut io::empty())?;
            }
        }

        // Gather non-relational files from object storage pages.
        for obj in self.timeline.list_nonrels(self.lsn)? {
            match obj {
                RelishTag::Slru { slru, segno } => {
                    self.add_slru_segment(slru, segno)?;
                }
                RelishTag::FileNodeMap { spcnode, dbnode } => {
                    self.add_relmap_file(spcnode, dbnode)?;
                }
                RelishTag::TwoPhase { xid } => {
                    self.add_twophase_file(xid)?;
                }
                _ => {}
            }
        }

        // Generate pg_control and bootstrap WAL segment.
        self.add_pgcontrol_file()?;
        self.ar.finish()?;
        debug!("all tarred up!");
        Ok(())
    }

    //
    // Generate SLRU segment files from repository.
    //
    fn add_slru_segment(&mut self, slru: SlruKind, segno: u32) -> anyhow::Result<()> {
        let seg_size = self
            .timeline
            .get_relish_size(RelishTag::Slru { slru, segno }, self.lsn)?;

        if seg_size == None {
            trace!(
                "SLRU segment {}/{:>04X} was truncated",
                slru.to_str(),
                segno
            );
            return Ok(());
        }

        let nblocks = seg_size.unwrap();

        let mut slru_buf: Vec<u8> =
            Vec::with_capacity(nblocks as usize * pg_constants::BLCKSZ as usize);
        for blknum in 0..nblocks {
            let img = self.timeline.get_page_at_lsn_nowait(
                RelishTag::Slru { slru, segno },
                blknum,
                self.lsn,
            )?;
            assert!(img.len() == pg_constants::BLCKSZ as usize);

            slru_buf.extend_from_slice(&img);
        }

        let segname = format!("{}/{:>04X}", slru.to_str(), segno);
        let header = new_tar_header(&segname, slru_buf.len() as u64)?;
        self.ar.append(&header, slru_buf.as_slice())?;

        trace!("Added to basebackup slru {} relsize {}", segname, nblocks);
        Ok(())
    }

    //
    // Extract pg_filenode.map files from repository
    // Along with them also send PG_VERSION for each database.
    //
    fn add_relmap_file(&mut self, spcnode: u32, dbnode: u32) -> anyhow::Result<()> {
        let img = self.timeline.get_page_at_lsn_nowait(
            RelishTag::FileNodeMap { spcnode, dbnode },
            0,
            self.lsn,
        )?;
        let path = if spcnode == pg_constants::GLOBALTABLESPACE_OID {
            let dst_path = "PG_VERSION";
            let version_bytes = pg_constants::PG_MAJORVERSION.as_bytes();
            let header = new_tar_header(&dst_path, version_bytes.len() as u64)?;
            self.ar.append(&header, &version_bytes[..])?;

            let dst_path = format!("global/PG_VERSION");
            let header = new_tar_header(&dst_path, version_bytes.len() as u64)?;
            self.ar.append(&header, &version_bytes[..])?;

            String::from("global/pg_filenode.map") // filenode map for global tablespace
        } else {
            // User defined tablespaces are not supported
            assert!(spcnode == pg_constants::DEFAULTTABLESPACE_OID);

            // Append dir path for each database
            let path = format!("base/{}", dbnode);
            let header = new_tar_header_dir(&path)?;
            self.ar.append(&header, &mut io::empty())?;

            let dst_path = format!("base/{}/PG_VERSION", dbnode);
            let version_bytes = pg_constants::PG_MAJORVERSION.as_bytes();
            let header = new_tar_header(&dst_path, version_bytes.len() as u64)?;
            self.ar.append(&header, &version_bytes[..])?;

            format!("base/{}/pg_filenode.map", dbnode)
        };
        assert!(img.len() == 512);
        let header = new_tar_header(&path, img.len() as u64)?;
        self.ar.append(&header, &img[..])?;
        Ok(())
    }

    //
    // Extract twophase state files
    //
    fn add_twophase_file(&mut self, xid: TransactionId) -> anyhow::Result<()> {
        let img = self
            .timeline
            .get_page_at_lsn_nowait(RelishTag::TwoPhase { xid }, 0, self.lsn)?;

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&img[..]);
        let crc = crc32c::crc32c(&img[..]);
        buf.put_u32_le(crc);
        let path = format!("pg_twophase/{:>08X}", xid);
        let header = new_tar_header(&path, buf.len() as u64)?;
        self.ar.append(&header, &buf[..])?;

        Ok(())
    }

    //
    // Add generated pg_control file and bootstrap WAL segment.
    // Also send zenith.signal file with extra bootstrap data.
    //
    fn add_pgcontrol_file(&mut self) -> anyhow::Result<()> {
        let checkpoint_bytes =
            self.timeline
                .get_page_at_lsn_nowait(RelishTag::Checkpoint, 0, self.lsn)?;
        let pg_control_bytes =
            self.timeline
                .get_page_at_lsn_nowait(RelishTag::ControlFile, 0, self.lsn)?;
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
        //TODO Check this.
        //We may need to determine the value from twophase data.
        checkpoint.oldestActiveXid = 0;

        //save new values in pg_control
        pg_control.checkPoint = checkpoint_lsn;
        pg_control.checkPointCopy = checkpoint;
        pg_control.state = pg_constants::DB_SHUTDOWNED;

        // add zenith.signal file
        self.ar.append(
            &new_tar_header("zenith.signal", 8)?,
            &self.prev_record_lsn.0.to_le_bytes()[..],
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
        assert!(wal_seg.len() == pg_constants::WAL_SEGMENT_SIZE);
        self.ar.append(&header, &wal_seg[..])?;
        Ok(())
    }
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

fn new_tar_header_dir(path: &str) -> anyhow::Result<Header> {
    let mut header = Header::new_gnu();
    header.set_size(0);
    header.set_path(path)?;
    header.set_mode(0o755); // -rw-------
    header.set_entry_type(EntryType::dir());
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
