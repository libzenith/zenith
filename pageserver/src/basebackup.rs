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
use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::{BufMut, BytesMut};
use fail::fail_point;
use itertools::Itertools;
use std::fmt::Write as FmtWrite;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::time::SystemTime;
use tar::{Builder, EntryType, Header};
use tracing::*;

use crate::layered_repository::Timeline;
use crate::reltag::{RelTag, SlruKind};

use postgres_ffi::v14::pg_constants;
use postgres_ffi::v14::xlog_utils::{generate_wal_segment, normalize_lsn, XLogFileName};
use postgres_ffi::v14::{CheckPoint, ControlFileData};
use postgres_ffi::TransactionId;
use postgres_ffi::PG_TLI;
use postgres_ffi::{BLCKSZ, RELSEG_SIZE};
use utils::lsn::Lsn;

/// This is short-living object only for the time of tarball creation,
/// created mostly to avoid passing a lot of parameters between various functions
/// used for constructing tarball.
pub struct Basebackup<'a, W>
where
    W: Write,
{
    ar: Builder<AbortableWrite<W>>,
    timeline: &'a Arc<Timeline>,
    pub lsn: Lsn,
    prev_record_lsn: Lsn,
    full_backup: bool,
    finished: bool,
}

// Create basebackup with non-rel data in it.
// Only include relational data if 'full_backup' is true.
//
// Currently we use empty lsn in two cases:
//  * During the basebackup right after timeline creation
//  * When working without safekeepers. In this situation it is important to match the lsn
//    we are taking basebackup on with the lsn that is used in pageserver's walreceiver
//    to start the replication.
impl<'a, W> Basebackup<'a, W>
where
    W: Write,
{
    pub fn new(
        write: W,
        timeline: &'a Arc<Timeline>,
        req_lsn: Option<Lsn>,
        prev_lsn: Option<Lsn>,
        full_backup: bool,
    ) -> Result<Basebackup<'a, W>> {
        // Compute postgres doesn't have any previous WAL files, but the first
        // record that it's going to write needs to include the LSN of the
        // previous record (xl_prev). We include prev_record_lsn in the
        // "zenith.signal" file, so that postgres can read it during startup.
        //
        // We don't keep full history of record boundaries in the page server,
        // however, only the predecessor of the latest record on each
        // timeline. So we can only provide prev_record_lsn when you take a
        // base backup at the end of the timeline, i.e. at last_record_lsn.
        // Even at the end of the timeline, we sometimes don't have a valid
        // prev_lsn value; that happens if the timeline was just branched from
        // an old LSN and it doesn't have any WAL of its own yet. We will set
        // prev_lsn to Lsn(0) if we cannot provide the correct value.
        let (backup_prev, backup_lsn) = if let Some(req_lsn) = req_lsn {
            // Backup was requested at a particular LSN. Wait for it to arrive.
            info!("waiting for {}", req_lsn);
            timeline.wait_lsn(req_lsn)?;

            // If the requested point is the end of the timeline, we can
            // provide prev_lsn. (get_last_record_rlsn() might return it as
            // zero, though, if no WAL has been generated on this timeline
            // yet.)
            let end_of_timeline = timeline.get_last_record_rlsn();
            if req_lsn == end_of_timeline.last {
                (end_of_timeline.prev, req_lsn)
            } else {
                (Lsn(0), req_lsn)
            }
        } else {
            // Backup was requested at end of the timeline.
            let end_of_timeline = timeline.get_last_record_rlsn();
            (end_of_timeline.prev, end_of_timeline.last)
        };

        // Consolidate the derived and the provided prev_lsn values
        let prev_lsn = if let Some(provided_prev_lsn) = prev_lsn {
            if backup_prev != Lsn(0) {
                ensure!(backup_prev == provided_prev_lsn)
            }
            provided_prev_lsn
        } else {
            backup_prev
        };

        info!(
            "taking basebackup lsn={}, prev_lsn={} (full_backup={})",
            backup_lsn, prev_lsn, full_backup
        );

        Ok(Basebackup {
            ar: Builder::new(AbortableWrite::new(write)),
            timeline,
            lsn: backup_lsn,
            prev_record_lsn: prev_lsn,
            full_backup,
            finished: false,
        })
    }

    pub fn send_tarball(mut self) -> anyhow::Result<()> {
        // TODO include checksum

        // Create pgdata subdirs structure
        for dir in pg_constants::PGDATA_SUBDIRS.iter() {
            let header = new_tar_header_dir(*dir)?;
            self.ar.append(&header, &mut io::empty())?;
        }

        // Send empty config files.
        for filepath in pg_constants::PGDATA_SPECIAL_FILES.iter() {
            if *filepath == "pg_hba.conf" {
                let data = pg_constants::PG_HBA.as_bytes();
                let header = new_tar_header(filepath, data.len() as u64)?;
                self.ar.append(&header, data)?;
            } else {
                let header = new_tar_header(filepath, 0)?;
                self.ar.append(&header, &mut io::empty())?;
            }
        }

        // Gather non-relational files from object storage pages.
        for kind in [
            SlruKind::Clog,
            SlruKind::MultiXactOffsets,
            SlruKind::MultiXactMembers,
        ] {
            for segno in self.timeline.list_slru_segments(kind, self.lsn)? {
                self.add_slru_segment(kind, segno)?;
            }
        }

        // Create tablespace directories
        for ((spcnode, dbnode), has_relmap_file) in self.timeline.list_dbdirs(self.lsn)? {
            self.add_dbdir(spcnode, dbnode, has_relmap_file)?;

            // Gather and send relational files in each database if full backup is requested.
            if self.full_backup {
                for rel in self.timeline.list_rels(spcnode, dbnode, self.lsn)? {
                    self.add_rel(rel)?;
                }
            }
        }
        for xid in self.timeline.list_twophase_files(self.lsn)? {
            self.add_twophase_file(xid)?;
        }

        fail_point!("basebackup-before-control-file", |_| {
            bail!("failpoint basebackup-before-control-file")
        });

        // Generate pg_control and bootstrap WAL segment.
        self.add_pgcontrol_file()?;
        self.ar.finish()?;
        self.finished = true;
        debug!("all tarred up!");
        Ok(())
    }

    fn add_rel(&mut self, tag: RelTag) -> anyhow::Result<()> {
        let nblocks = self.timeline.get_rel_size(tag, self.lsn, false)?;

        // Function that adds relation segment data to archive
        let mut add_file = |segment_index, data: &Vec<u8>| -> anyhow::Result<()> {
            let file_name = tag.to_segfile_name(segment_index as u32);
            let header = new_tar_header(&file_name, data.len() as u64)?;
            self.ar.append(&header, data.as_slice())?;
            Ok(())
        };

        // If the relation is empty, create an empty file
        if nblocks == 0 {
            add_file(0, &vec![])?;
            return Ok(());
        }

        // Add a file for each chunk of blocks (aka segment)
        let chunks = (0..nblocks).chunks(RELSEG_SIZE as usize);
        for (seg, blocks) in chunks.into_iter().enumerate() {
            let mut segment_data: Vec<u8> = vec![];
            for blknum in blocks {
                let img = self
                    .timeline
                    .get_rel_page_at_lsn(tag, blknum, self.lsn, false)?;
                segment_data.extend_from_slice(&img[..]);
            }

            add_file(seg, &segment_data)?;
        }

        Ok(())
    }

    //
    // Generate SLRU segment files from repository.
    //
    fn add_slru_segment(&mut self, slru: SlruKind, segno: u32) -> anyhow::Result<()> {
        let nblocks = self.timeline.get_slru_segment_size(slru, segno, self.lsn)?;

        let mut slru_buf: Vec<u8> = Vec::with_capacity(nblocks as usize * BLCKSZ as usize);
        for blknum in 0..nblocks {
            let img = self
                .timeline
                .get_slru_page_at_lsn(slru, segno, blknum, self.lsn)?;

            if slru == SlruKind::Clog {
                ensure!(img.len() == BLCKSZ as usize || img.len() == BLCKSZ as usize + 8);
            } else {
                ensure!(img.len() == BLCKSZ as usize);
            }

            slru_buf.extend_from_slice(&img[..BLCKSZ as usize]);
        }

        let segname = format!("{}/{:>04X}", slru.to_str(), segno);
        let header = new_tar_header(&segname, slru_buf.len() as u64)?;
        self.ar.append(&header, slru_buf.as_slice())?;

        trace!("Added to basebackup slru {} relsize {}", segname, nblocks);
        Ok(())
    }

    //
    // Include database/tablespace directories.
    //
    // Each directory contains a PG_VERSION file, and the default database
    // directories also contain pg_filenode.map files.
    //
    fn add_dbdir(
        &mut self,
        spcnode: u32,
        dbnode: u32,
        has_relmap_file: bool,
    ) -> anyhow::Result<()> {
        let relmap_img = if has_relmap_file {
            let img = self.timeline.get_relmap_file(spcnode, dbnode, self.lsn)?;
            ensure!(img.len() == 512);
            Some(img)
        } else {
            None
        };

        if spcnode == pg_constants::GLOBALTABLESPACE_OID {
            let version_bytes = pg_constants::PG_MAJORVERSION.as_bytes();
            let header = new_tar_header("PG_VERSION", version_bytes.len() as u64)?;
            self.ar.append(&header, version_bytes)?;

            let header = new_tar_header("global/PG_VERSION", version_bytes.len() as u64)?;
            self.ar.append(&header, version_bytes)?;

            if let Some(img) = relmap_img {
                // filenode map for global tablespace
                let header = new_tar_header("global/pg_filenode.map", img.len() as u64)?;
                self.ar.append(&header, &img[..])?;
            } else {
                warn!("global/pg_filenode.map is missing");
            }
        } else {
            // User defined tablespaces are not supported. However, as
            // a special case, if a tablespace/db directory is
            // completely empty, we can leave it out altogether. This
            // makes taking a base backup after the 'tablespace'
            // regression test pass, because the test drops the
            // created tablespaces after the tests.
            //
            // FIXME: this wouldn't be necessary, if we handled
            // XLOG_TBLSPC_DROP records. But we probably should just
            // throw an error on CREATE TABLESPACE in the first place.
            if !has_relmap_file
                && self
                    .timeline
                    .list_rels(spcnode, dbnode, self.lsn)?
                    .is_empty()
            {
                return Ok(());
            }
            // User defined tablespaces are not supported
            ensure!(spcnode == pg_constants::DEFAULTTABLESPACE_OID);

            // Append dir path for each database
            let path = format!("base/{}", dbnode);
            let header = new_tar_header_dir(&path)?;
            self.ar.append(&header, &mut io::empty())?;

            if let Some(img) = relmap_img {
                let dst_path = format!("base/{}/PG_VERSION", dbnode);
                let version_bytes = pg_constants::PG_MAJORVERSION.as_bytes();
                let header = new_tar_header(&dst_path, version_bytes.len() as u64)?;
                self.ar.append(&header, version_bytes)?;

                let relmap_path = format!("base/{}/pg_filenode.map", dbnode);
                let header = new_tar_header(&relmap_path, img.len() as u64)?;
                self.ar.append(&header, &img[..])?;
            }
        };
        Ok(())
    }

    //
    // Extract twophase state files
    //
    fn add_twophase_file(&mut self, xid: TransactionId) -> anyhow::Result<()> {
        let img = self.timeline.get_twophase_file(xid, self.lsn)?;

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
        let checkpoint_bytes = self
            .timeline
            .get_checkpoint(self.lsn)
            .context("failed to get checkpoint bytes")?;
        let pg_control_bytes = self
            .timeline
            .get_control_file(self.lsn)
            .context("failed get control bytes")?;
        let mut pg_control = ControlFileData::decode(&pg_control_bytes)?;
        let mut checkpoint = CheckPoint::decode(&checkpoint_bytes)?;

        // Generate new pg_control needed for bootstrap
        checkpoint.redo = normalize_lsn(self.lsn, pg_constants::WAL_SEGMENT_SIZE).0;

        //reset some fields we don't want to preserve
        //TODO Check this.
        //We may need to determine the value from twophase data.
        checkpoint.oldestActiveXid = 0;

        //save new values in pg_control
        pg_control.checkPoint = 0;
        pg_control.checkPointCopy = checkpoint;
        pg_control.state = pg_constants::DB_SHUTDOWNED;

        // add zenith.signal file
        let mut zenith_signal = String::new();
        if self.prev_record_lsn == Lsn(0) {
            if self.lsn == self.timeline.get_ancestor_lsn() {
                write!(zenith_signal, "PREV LSN: none")?;
            } else {
                write!(zenith_signal, "PREV LSN: invalid")?;
            }
        } else {
            write!(zenith_signal, "PREV LSN: {}", self.prev_record_lsn)?;
        }
        self.ar.append(
            &new_tar_header("zenith.signal", zenith_signal.len() as u64)?,
            zenith_signal.as_bytes(),
        )?;

        //send pg_control
        let pg_control_bytes = pg_control.encode();
        let header = new_tar_header("global/pg_control", pg_control_bytes.len() as u64)?;
        self.ar.append(&header, &pg_control_bytes[..])?;

        //send wal segment
        let segno = self.lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE);
        let wal_file_name = XLogFileName(PG_TLI, segno, pg_constants::WAL_SEGMENT_SIZE);
        let wal_file_path = format!("pg_wal/{}", wal_file_name);
        let header = new_tar_header(&wal_file_path, pg_constants::WAL_SEGMENT_SIZE as u64)?;
        let wal_seg = generate_wal_segment(segno, pg_control.system_identifier)
            .map_err(|e| anyhow!(e).context("Failed generating wal segment"))?;
        ensure!(wal_seg.len() == pg_constants::WAL_SEGMENT_SIZE);
        self.ar.append(&header, &wal_seg[..])?;
        Ok(())
    }
}

impl<'a, W> Drop for Basebackup<'a, W>
where
    W: Write,
{
    /// If the basebackup was not finished, prevent the Archive::drop() from
    /// writing the end-of-archive marker.
    fn drop(&mut self) {
        if !self.finished {
            self.ar.get_mut().abort();
        }
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

/// A wrapper that passes through all data to the underlying Write,
/// until abort() is called.
///
/// tar::Builder has an annoying habit of finishing the archive with
/// a valid tar end-of-archive marker (two 512-byte sectors of zeros),
/// even if an error occurs and we don't finish building the archive.
/// We'd rather abort writing the tarball immediately than construct
/// a seemingly valid but incomplete archive. This wrapper allows us
/// to swallow the end-of-archive marker that Builder::drop() emits,
/// without writing it to the underlying sink.
///
struct AbortableWrite<W> {
    w: W,
    aborted: bool,
}

impl<W> AbortableWrite<W> {
    pub fn new(w: W) -> Self {
        AbortableWrite { w, aborted: false }
    }

    pub fn abort(&mut self) {
        self.aborted = true;
    }
}

impl<W> Write for AbortableWrite<W>
where
    W: Write,
{
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.aborted {
            Ok(data.len())
        } else {
            self.w.write(data)
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.aborted {
            Ok(())
        } else {
            self.w.flush()
        }
    }
}
