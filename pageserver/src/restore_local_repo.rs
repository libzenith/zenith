//!
//! Import data and WAL from a PostgreSQL data directory and WAL segments into
//! zenith Timeline.
//!
use log::*;
use std::cmp::min;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use anyhow::Result;
use bytes::{Buf, Bytes};

use crate::relish::*;
use crate::repository::*;
use crate::waldecoder::*;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::{pg_constants, CheckPoint, ControlFileData};
use postgres_ffi::{Oid, TransactionId};
use zenith_utils::lsn::Lsn;

const MAX_MBR_BLKNO: u32 =
    pg_constants::MAX_MULTIXACT_ID / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;

const ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

///
/// Import all relation data pages from local disk into the repository.
///
pub fn import_timeline_from_postgres_datadir(
    path: &Path,
    timeline: &dyn Timeline,
    lsn: Lsn,
) -> Result<()> {
    // Scan 'global'
    for direntry in fs::read_dir(path.join("global"))? {
        let direntry = direntry?;
        match direntry.file_name().to_str() {
            None => continue,

            // These special files appear in the snapshot, but are not needed by the page server
            Some("pg_control") => {
                import_nonrel_file(timeline, lsn, RelishTag::ControlFile, &direntry.path())?;
                // Extract checkpoint record from pg_control and store is as separate object
                let pg_control_bytes =
                    timeline.get_page_at_lsn_nowait(RelishTag::ControlFile, 0, lsn)?;
                let pg_control = ControlFileData::decode(&pg_control_bytes)?;
                let checkpoint_bytes = pg_control.checkPointCopy.encode();
                timeline.put_page_image(RelishTag::Checkpoint, 0, lsn, checkpoint_bytes, false)?;
            }
            Some("pg_filenode.map") => import_nonrel_file(
                timeline,
                lsn,
                RelishTag::FileNodeMap {
                    spcnode: pg_constants::GLOBALTABLESPACE_OID,
                    dbnode: 0,
                },
                &direntry.path(),
            )?,

            // Load any relation files into the page server
            _ => import_relfile(
                &direntry.path(),
                timeline,
                lsn,
                pg_constants::GLOBALTABLESPACE_OID,
                0,
            )?,
        }
    }

    // Scan 'base'. It contains database dirs, the database OID is the filename.
    // E.g. 'base/12345', where 12345 is the database OID.
    for direntry in fs::read_dir(path.join("base"))? {
        let direntry = direntry?;

        //skip all temporary files
        if direntry.file_name().to_str().unwrap() == "pgsql_tmp" {
            continue;
        }

        let dboid = direntry.file_name().to_str().unwrap().parse::<u32>()?;

        for direntry in fs::read_dir(direntry.path())? {
            let direntry = direntry?;
            match direntry.file_name().to_str() {
                None => continue,

                // These special files appear in the snapshot, but are not needed by the page server
                Some("PG_VERSION") => continue,
                Some("pg_filenode.map") => import_nonrel_file(
                    timeline,
                    lsn,
                    RelishTag::FileNodeMap {
                        spcnode: pg_constants::DEFAULTTABLESPACE_OID,
                        dbnode: dboid,
                    },
                    &direntry.path(),
                )?,

                // Load any relation files into the page server
                _ => import_relfile(
                    &direntry.path(),
                    timeline,
                    lsn,
                    pg_constants::DEFAULTTABLESPACE_OID,
                    dboid,
                )?,
            }
        }
    }
    for entry in fs::read_dir(path.join("pg_xact"))? {
        let entry = entry?;
        import_slru_file(timeline, lsn, SlruKind::Clog, &entry.path())?;
    }
    for entry in fs::read_dir(path.join("pg_multixact").join("members"))? {
        let entry = entry?;
        import_slru_file(timeline, lsn, SlruKind::MultiXactMembers, &entry.path())?;
    }
    for entry in fs::read_dir(path.join("pg_multixact").join("offsets"))? {
        let entry = entry?;
        import_slru_file(timeline, lsn, SlruKind::MultiXactOffsets, &entry.path())?;
    }
    for entry in fs::read_dir(path.join("pg_twophase"))? {
        let entry = entry?;
        let xid = u32::from_str_radix(&entry.path().to_str().unwrap(), 16)?;
        import_nonrel_file(timeline, lsn, RelishTag::TwoPhase { xid }, &entry.path())?;
    }
    // TODO: Scan pg_tblspc

    timeline.advance_last_valid_lsn(lsn);
    timeline.checkpoint()?;

    Ok(())
}

// subroutine of import_timeline_from_postgres_datadir(), to load one relation file.
fn import_relfile(
    path: &Path,
    timeline: &dyn Timeline,
    lsn: Lsn,
    spcoid: Oid,
    dboid: Oid,
) -> Result<()> {
    // Does it look like a relation file?

    let p = parse_relfilename(path.file_name().unwrap().to_str().unwrap());
    if let Err(e) = p {
        warn!("unrecognized file in snapshot: {:?} ({})", path, e);
        return Err(e.into());
    }
    let (relnode, forknum, segno) = p.unwrap();

    let mut file = File::open(path)?;
    let mut buf: [u8; 8192] = [0u8; 8192];

    let mut blknum: u32 = segno * (1024 * 1024 * 1024 / pg_constants::BLCKSZ as u32);
    loop {
        let r = file.read_exact(&mut buf);
        match r {
            Ok(_) => {
                let rel = RelTag {
                    spcnode: spcoid,
                    dbnode: dboid,
                    relnode,
                    forknum,
                };
                let tag = RelishTag::Relation(rel);
                timeline.put_page_image(tag, blknum, lsn, Bytes::copy_from_slice(&buf), true)?;
            }

            // TODO: UnexpectedEof is expected
            Err(e) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    // FIXME: maybe check that we read the full length of the file?
                    break;
                }
                _ => {
                    error!("error reading file: {:?} ({})", path, e);
                    break;
                }
            },
        };
        blknum += 1;
    }

    Ok(())
}

///
/// Import a "non-blocky" file into the repository
///
/// This is used for small files like the control file, twophase files etc. that
/// are just slurped into the repository as one blob.
///
fn import_nonrel_file(
    timeline: &dyn Timeline,
    lsn: Lsn,
    tag: RelishTag,
    path: &Path,
) -> Result<()> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    // read the whole file
    file.read_to_end(&mut buffer)?;

    info!("importing non-rel file {}", path.display());

    timeline.put_page_image(tag, 0, lsn, Bytes::copy_from_slice(&buffer[..]), false)?;
    Ok(())
}

///
/// Import an SLRU segment file
///
fn import_slru_file(timeline: &dyn Timeline, lsn: Lsn, slru: SlruKind, path: &Path) -> Result<()> {
    // Does it look like an SLRU file?
    let mut file = File::open(path)?;
    let mut buf: [u8; 8192] = [0u8; 8192];
    let segno = u32::from_str_radix(path.file_name().unwrap().to_str().unwrap(), 16)?;

    info!("importing slru file {}", path.display());

    let mut rpageno = 0;
    loop {
        let r = file.read_exact(&mut buf);
        match r {
            Ok(_) => {
                timeline.put_page_image(
                    RelishTag::Slru { slru, segno },
                    rpageno,
                    lsn,
                    Bytes::copy_from_slice(&buf),
                    true,
                )?;
            }

            // TODO: UnexpectedEof is expected
            Err(e) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    // FIXME: maybe check that we read the full length of the file?
                    break;
                }
                _ => {
                    error!("error reading file: {:?} ({})", path, e);
                    break;
                }
            },
        };
        rpageno += 1;

        // TODO: Check that the file isn't unexpectedly large, not larger than SLRU_PAGES_PER_SEGMENT pages
    }

    Ok(())
}

/// Scan PostgreSQL WAL files in given directory
/// and load all records >= 'startpoint' into the repository.
pub fn import_timeline_wal(walpath: &Path, timeline: &dyn Timeline, startpoint: Lsn) -> Result<()> {
    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let mut segno = startpoint.segment_number(pg_constants::WAL_SEGMENT_SIZE);
    let mut offset = startpoint.segment_offset(pg_constants::WAL_SEGMENT_SIZE);
    let mut last_lsn = startpoint;

    let checkpoint_bytes = timeline.get_page_at_lsn_nowait(RelishTag::Checkpoint, 0, startpoint)?;
    let mut checkpoint = CheckPoint::decode(&checkpoint_bytes)?;

    loop {
        // FIXME: assume postgresql tli 1 for now
        let filename = XLogFileName(1, segno, pg_constants::WAL_SEGMENT_SIZE);
        let mut buf = Vec::new();

        //Read local file
        let mut path = walpath.join(&filename);

        // It could be as .partial
        if !PathBuf::from(&path).exists() {
            path = walpath.join(filename + ".partial");
        }

        // Slurp the WAL file
        let open_result = File::open(&path);
        if let Err(e) = &open_result {
            if e.kind() == std::io::ErrorKind::NotFound {
                break;
            }
        }
        let mut file = open_result?;

        if offset > 0 {
            file.seek(SeekFrom::Start(offset as u64))?;
        }

        let nread = file.read_to_end(&mut buf)?;
        if nread != pg_constants::WAL_SEGMENT_SIZE - offset as usize {
            // Maybe allow this for .partial files?
            error!("read only {} bytes from WAL file", nread);
        }

        waldecoder.feed_bytes(&buf);

        let mut nrecords = 0;
        loop {
            let rec = waldecoder.poll_decode();
            if rec.is_err() {
                // Assume that an error means we've reached the end of
                // a partial WAL record. So that's ok.
                trace!("WAL decoder error {:?}", rec);
                break;
            }
            if let Some((lsn, recdata)) = rec.unwrap() {
                let decoded = decode_wal_record(recdata.clone());
                save_decoded_record(&mut checkpoint, timeline, &decoded, recdata, lsn)?;
                last_lsn = lsn;
            } else {
                break;
            }
            nrecords += 1;
        }

        info!("imported {} records up to {}", nrecords, last_lsn);

        segno += 1;
        offset = 0;
    }

    info!("reached end of WAL at {}", last_lsn);
    let checkpoint_bytes = checkpoint.encode();
    timeline.put_page_image(RelishTag::Checkpoint, 0, last_lsn, checkpoint_bytes, false)?;

    timeline.advance_last_valid_lsn(last_lsn);
    timeline.checkpoint()?;
    Ok(())
}

///
/// Helper function to parse a WAL record and call the Timeline's PUT functions for all the
/// relations/pages that the record affects.
///
pub fn save_decoded_record(
    checkpoint: &mut CheckPoint,
    timeline: &dyn Timeline,
    decoded: &DecodedWALRecord,
    recdata: Bytes,
    lsn: Lsn,
) -> Result<()> {
    checkpoint.update_next_xid(decoded.xl_xid);

    // Iterate through all the blocks that the record modifies, and
    // "put" a separate copy of the record for each block.
    for blk in decoded.blocks.iter() {
        let tag = RelishTag::Relation(RelTag {
            spcnode: blk.rnode_spcnode,
            dbnode: blk.rnode_dbnode,
            relnode: blk.rnode_relnode,
            forknum: blk.forknum as u8,
        });

        let rec = WALRecord {
            lsn,
            will_init: blk.will_init || blk.apply_image,
            rec: recdata.clone(),
            main_data_offset: decoded.main_data_offset as u32,
        };

        timeline.put_wal_record(tag, blk.blkno, rec)?;
    }

    let mut buf = decoded.record.clone();
    buf.advance(decoded.main_data_offset);

    // Handle a few special record types
    if decoded.xl_rmid == pg_constants::RM_SMGR_ID
        && (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK) == pg_constants::XLOG_SMGR_TRUNCATE
    {
        let truncate = XlSmgrTruncate::decode(&mut buf);
        save_xlog_smgr_truncate(timeline, lsn, &truncate)?;
    } else if decoded.xl_rmid == pg_constants::RM_DBASE_ID {
        if (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK) == pg_constants::XLOG_DBASE_CREATE {
            let createdb = XlCreateDatabase::decode(&mut buf);
            save_xlog_dbase_create(timeline, lsn, &createdb)?;
        } else {
            // TODO
            trace!("XLOG_DBASE_DROP is not handled yet");
        }
    } else if decoded.xl_rmid == pg_constants::RM_TBLSPC_ID {
        trace!("XLOG_TBLSPC_CREATE/DROP is not handled yet");
    } else if decoded.xl_rmid == pg_constants::RM_CLOG_ID {
        let info = decoded.xl_info & !pg_constants::XLR_INFO_MASK;
        if info == pg_constants::CLOG_ZEROPAGE {
            let pageno = buf.get_u32_le();
            let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
            let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
            timeline.put_page_image(
                RelishTag::Slru {
                    slru: SlruKind::Clog,
                    segno,
                },
                rpageno,
                lsn,
                ZERO_PAGE,
                true,
            )?;
        } else {
            assert!(info == pg_constants::CLOG_TRUNCATE);
            let xlrec = XlClogTruncate::decode(&mut buf);
            save_clog_truncate_record(checkpoint, timeline, lsn, &xlrec)?;
        }
    } else if decoded.xl_rmid == pg_constants::RM_XACT_ID {
        let info = decoded.xl_info & pg_constants::XLOG_XACT_OPMASK;
        if info == pg_constants::XLOG_XACT_COMMIT || info == pg_constants::XLOG_XACT_ABORT {
            let parsed_xact = XlXactParsedRecord::decode(&mut buf, decoded.xl_xid, decoded.xl_info);
            save_xact_record(timeline, lsn, &parsed_xact, decoded)?;
        } else if info == pg_constants::XLOG_XACT_COMMIT_PREPARED
            || info == pg_constants::XLOG_XACT_ABORT_PREPARED
        {
            let parsed_xact = XlXactParsedRecord::decode(&mut buf, decoded.xl_xid, decoded.xl_info);
            save_xact_record(timeline, lsn, &parsed_xact, decoded)?;
            // Remove twophase file. see RemoveTwoPhaseFile() in postgres code
            trace!(
                "unlink twophaseFile for xid {} parsed_xact.xid {} here at {}",
                decoded.xl_xid, parsed_xact.xid, lsn
            );
            timeline.put_unlink(
                RelishTag::TwoPhase {
                    xid: parsed_xact.xid,
                },
                lsn,
            )?;
        } else if info == pg_constants::XLOG_XACT_PREPARE {
            let mut buf = decoded.record.clone();
            buf.advance(decoded.main_data_offset);

            timeline.put_page_image(
                RelishTag::TwoPhase {
                    xid: decoded.xl_xid,
                },
                0,
                lsn,
                Bytes::copy_from_slice(&buf[..]),
                true,
            )?;
        }
    } else if decoded.xl_rmid == pg_constants::RM_MULTIXACT_ID {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

        if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE {
            let pageno = buf.get_u32_le();
            let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
            let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
            timeline.put_page_image(
                RelishTag::Slru {
                    slru: SlruKind::MultiXactOffsets,
                    segno,
                },
                rpageno,
                lsn,
                ZERO_PAGE,
                true,
            )?;
        } else if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE {
            let pageno = buf.get_u32_le();
            let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
            let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
            timeline.put_page_image(
                RelishTag::Slru {
                    slru: SlruKind::MultiXactMembers,
                    segno,
                },
                rpageno,
                lsn,
                ZERO_PAGE,
                true,
            )?;
        } else if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
            let xlrec = XlMultiXactCreate::decode(&mut buf);
            save_multixact_create_record(checkpoint, timeline, lsn, &xlrec, decoded)?;
        } else if info == pg_constants::XLOG_MULTIXACT_TRUNCATE_ID {
            let xlrec = XlMultiXactTruncate::decode(&mut buf);
            save_multixact_truncate_record(checkpoint, timeline, lsn, &xlrec)?;
        }
    } else if decoded.xl_rmid == pg_constants::RM_RELMAP_ID {
        let xlrec = XlRelmapUpdate::decode(&mut buf);
        save_relmap_record(timeline, lsn, &xlrec, decoded)?;
    } else if decoded.xl_rmid == pg_constants::RM_XLOG_ID {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_NEXTOID {
            let next_oid = buf.get_u32_le();
            checkpoint.nextOid = next_oid;
        } else if info == pg_constants::XLOG_CHECKPOINT_ONLINE
            || info == pg_constants::XLOG_CHECKPOINT_SHUTDOWN
        {
            let mut checkpoint_bytes = [0u8; SIZEOF_CHECKPOINT];
            let mut buf = decoded.record.clone();
            buf.advance(decoded.main_data_offset);
            buf.copy_to_slice(&mut checkpoint_bytes);
            let xlog_checkpoint = CheckPoint::decode(&checkpoint_bytes).unwrap();
            trace!(
                "xlog_checkpoint.oldestXid={}, checkpoint.oldestXid={}",
                xlog_checkpoint.oldestXid,
                checkpoint.oldestXid
            );
            if (checkpoint.oldestXid.wrapping_sub(xlog_checkpoint.oldestXid) as i32) < 0 {
                checkpoint.oldestXid = xlog_checkpoint.oldestXid;
            }
        }
    }
    // Now that this record has been handled, let the repository know that
    // it is up-to-date to this LSN
    timeline.advance_last_record_lsn(lsn);
    Ok(())
}

/// Subroutine of save_decoded_record(), to handle an XLOG_DBASE_CREATE record.
fn save_xlog_dbase_create(timeline: &dyn Timeline, lsn: Lsn, rec: &XlCreateDatabase) -> Result<()> {
    let db_id = rec.db_id;
    let tablespace_id = rec.tablespace_id;
    let src_db_id = rec.src_db_id;
    let src_tablespace_id = rec.src_tablespace_id;

    // Creating a database is implemented by copying the template (aka. source) database.
    // To copy all the relations, we need to ask for the state as of the same LSN, but we
    // cannot pass 'lsn' to the Timeline.get_* functions, or they will block waiting for
    // the last valid LSN to advance up to it. So we use the previous record's LSN in the
    // get calls instead.
    let req_lsn = min(timeline.get_last_record_lsn(), lsn);

    let rels = timeline.list_rels(src_tablespace_id, src_db_id, req_lsn)?;

    trace!("save_create_database: {} rels", rels.len());

    let mut num_rels_copied = 0;
    let mut num_blocks_copied = 0;
    for src_rel in rels {
        assert_eq!(src_rel.spcnode, src_tablespace_id);
        assert_eq!(src_rel.dbnode, src_db_id);

        let nblocks = timeline
            .get_relish_size(RelishTag::Relation(src_rel), req_lsn)?
            .unwrap_or(0);
        let dst_rel = RelTag {
            spcnode: tablespace_id,
            dbnode: db_id,
            relnode: src_rel.relnode,
            forknum: src_rel.forknum,
        };

        // Copy content
        for blknum in 0..nblocks {
            let content =
                timeline.get_page_at_lsn_nowait(RelishTag::Relation(src_rel), blknum, req_lsn)?;

            debug!("copying block {} from {} to {}", blknum, src_rel, dst_rel);

            timeline.put_page_image(RelishTag::Relation(dst_rel), blknum, lsn, content, true)?;
            num_blocks_copied += 1;
        }

        if nblocks == 0 {
            // make sure we have some trace of the relation, even if it's empty
            timeline.put_truncation(RelishTag::Relation(dst_rel), lsn, 0)?;
        }

        num_rels_copied += 1;
    }

    // Copy relfilemap
    // TODO This implementation is very inefficient -
    // it scans all non-rels only to find FileNodeMaps
    for tag in timeline.list_nonrels(req_lsn)? {
        match tag {
            RelishTag::FileNodeMap { spcnode, dbnode } => {
                if spcnode == src_tablespace_id && dbnode == src_db_id {
                    let img = timeline.get_page_at_lsn_nowait(tag, 0, req_lsn)?;
                    let new_tag = RelishTag::FileNodeMap {
                        spcnode: tablespace_id,
                        dbnode: db_id,
                    };
                    timeline.put_page_image(new_tag, 0, lsn, img, false)?;
                    break;
                }
            }
            _ => {} // do nothing
        }
    }
    info!(
        "Created database {}/{}, copied {} blocks in {} rels at {}",
        tablespace_id, db_id, num_blocks_copied, num_rels_copied, lsn
    );
    Ok(())
}

/// Subroutine of save_decoded_record(), to handle an XLOG_SMGR_TRUNCATE record.
///
/// This is the same logic as in PostgreSQL's smgr_redo() function.
fn save_xlog_smgr_truncate(timeline: &dyn Timeline, lsn: Lsn, rec: &XlSmgrTruncate) -> Result<()> {
    let spcnode = rec.rnode.spcnode;
    let dbnode = rec.rnode.dbnode;
    let relnode = rec.rnode.relnode;

    if (rec.flags & pg_constants::SMGR_TRUNCATE_HEAP) != 0 {
        let rel = RelTag {
            spcnode,
            dbnode,
            relnode,
            forknum: pg_constants::MAIN_FORKNUM,
        };
        timeline.put_truncation(RelishTag::Relation(rel), lsn, rec.blkno)?;
    }
    if (rec.flags & pg_constants::SMGR_TRUNCATE_FSM) != 0 {
        let rel = RelTag {
            spcnode,
            dbnode,
            relnode,
            forknum: pg_constants::FSM_FORKNUM,
        };

        // FIXME: 'blkno' stored in the WAL record is the new size of the
        // heap. The formula for calculating the new size of the FSM is
        // pretty complicated (see FreeSpaceMapPrepareTruncateRel() in
        // PostgreSQL), and we should also clear bits in the tail FSM block,
        // and update the upper level FSM pages. None of that has been
        // implemented. What we do instead, is always just truncate the FSM
        // to zero blocks. That's bad for performance, but safe. (The FSM
        // isn't needed for correctness, so we could also leave garbage in
        // it. Seems more tidy to zap it away.)
        if rec.blkno != 0 {
            info!("Partial truncation of FSM is not supported");
        }
        let num_fsm_blocks = 0;
        timeline.put_truncation(RelishTag::Relation(rel), lsn, num_fsm_blocks)?;
    }
    if (rec.flags & pg_constants::SMGR_TRUNCATE_VM) != 0 {
        let rel = RelTag {
            spcnode,
            dbnode,
            relnode,
            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
        };

        // FIXME: Like with the FSM above, the logic to truncate the VM
        // correctly has not been implemented. Just zap it away completely,
        // always. Unlike the FSM, the VM must never have bits incorrectly
        // set. From a correctness point of view, it's always OK to clear
        // bits or remove it altogether, though.
        if rec.blkno != 0 {
            info!("Partial truncation of VM is not supported");
        }
        let num_vm_blocks = 0;
        timeline.put_truncation(RelishTag::Relation(rel), lsn, num_vm_blocks)?;
    }
    Ok(())
}

/// Subroutine of save_decoded_record(), to handle an XLOG_XACT_* records.
///
/// We are currently only interested in the dropped relations.
fn save_xact_record(
    timeline: &dyn Timeline,
    lsn: Lsn,
    parsed: &XlXactParsedRecord,
    decoded: &DecodedWALRecord,
) -> Result<()> {
    // Record update of CLOG page
    let mut pageno = parsed.xid / pg_constants::CLOG_XACTS_PER_PAGE;

    let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
    let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
    let rec = WALRecord {
        lsn,
        will_init: false,
        rec: decoded.record.clone(),
        main_data_offset: decoded.main_data_offset as u32,
    };
    timeline.put_wal_record(
        RelishTag::Slru {
            slru: SlruKind::Clog,
            segno,
        },
        rpageno,
        rec.clone(),
    )?;

    for subxact in &parsed.subxacts {
        let subxact_pageno = subxact / pg_constants::CLOG_XACTS_PER_PAGE;
        if subxact_pageno != pageno {
            pageno = subxact_pageno;
            let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
            let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
            timeline.put_wal_record(
                RelishTag::Slru {
                    slru: SlruKind::Clog,
                    segno,
                },
                rpageno,
                rec.clone(),
            )?;
        }
    }
    for xnode in &parsed.xnodes {
        for forknum in pg_constants::MAIN_FORKNUM..=pg_constants::VISIBILITYMAP_FORKNUM {
            let rel = RelTag {
                forknum,
                spcnode: xnode.spcnode,
                dbnode: xnode.dbnode,
                relnode: xnode.relnode,
            };
            timeline.put_unlink(RelishTag::Relation(rel), lsn)?;
        }
    }
    Ok(())
}

// See TransactionIdPrecedes in transam.c
fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    /*
     * If either ID is a permanent XID then we can just do unsigned
     * comparison.  If both are normal, do a modulo-2^32 comparison.
     */

    if !(id1 >= pg_constants::FIRST_NORMAL_TRANSACTION_ID)
        || !(id2 >= pg_constants::FIRST_NORMAL_TRANSACTION_ID)
    {
        return id1 < id2;
    }

    let diff = id1.wrapping_sub(id2) as i32;
    return diff < 0;
}

// See CLOGPagePrecedes in clog.c
fn clogpage_precedes(page1: u32, page2: u32) -> bool {
    let mut xid1 = page1 * pg_constants::CLOG_XACTS_PER_PAGE;
    xid1 += pg_constants::FIRST_NORMAL_TRANSACTION_ID + 1;
    let mut xid2 = page2 * pg_constants::CLOG_XACTS_PER_PAGE;
    xid2 += pg_constants::FIRST_NORMAL_TRANSACTION_ID + 1;

    return transaction_id_precedes(xid1, xid2)
        && transaction_id_precedes(xid1, xid2 + pg_constants::CLOG_XACTS_PER_PAGE - 1);
}

// See SlruMayDeleteSegment() in slru.c
fn slru_may_delete_clogsegment(segpage: u32, cutoff_page: u32) -> bool {
    let seg_last_page = segpage + pg_constants::SLRU_PAGES_PER_SEGMENT - 1;

    assert_eq!(segpage % pg_constants::SLRU_PAGES_PER_SEGMENT, 0);

    return clogpage_precedes(segpage, cutoff_page)
        && clogpage_precedes(seg_last_page, cutoff_page);
}

fn save_clog_truncate_record(
    checkpoint: &mut CheckPoint,
    timeline: &dyn Timeline,
    lsn: Lsn,
    xlrec: &XlClogTruncate,
) -> Result<()> {
    info!(
        "RM_CLOG_ID truncate pageno {} oldestXid {} oldestXidDB {} lsn {}",
        xlrec.pageno, xlrec.oldest_xid, xlrec.oldest_xid_db, lsn
    );

    // Here we treat oldestXid and oldestXidDB
    // differently from postgres redo routines.
    // In postgres checkpoint.oldestXid lags behind xlrec.oldest_xid
    // until checkpoint happens and updates the value.
    // Here we can use the most recent value.
    // It's just an optimization, though and can be deleted.
    // TODO Figure out if there will be any issues with replica.
    checkpoint.oldestXid = xlrec.oldest_xid;
    checkpoint.oldestXidDB = xlrec.oldest_xid_db;

    // TODO Treat AdvanceOldestClogXid() or write a comment why we don't need it

    let latest_page_number = checkpoint.nextXid.value as u32 / pg_constants::CLOG_XACTS_PER_PAGE;

    // Now delete all segments containing pages between xlrec.pageno
    // and latest_page_number.

    // First, make an important safety check:
    // the current endpoint page must not be eligible for removal.
    // See SimpleLruTruncate() in slru.c
    if clogpage_precedes(latest_page_number, xlrec.pageno) {
        info!("could not truncate directory pg_xact apparent wraparound");
        return Ok(());
    }

    // Iterate via SLRU CLOG segments and unlink segments that we're ready to truncate
    // TODO This implementation is very inefficient -
    // it scans all non-rels only to find Clog
    //
    // We cannot pass 'lsn' to the Timeline.list_nonrels(), or it
    // will block waiting for the last valid LSN to advance up to
    // it. So we use the previous record's LSN in the get calls
    // instead.
    let req_lsn = min(timeline.get_last_record_lsn(), lsn);
    for obj in timeline.list_nonrels(req_lsn)? {
        match obj {
            RelishTag::Slru { slru, segno } => {
                if slru == SlruKind::Clog {
                    let segpage = segno * pg_constants::SLRU_PAGES_PER_SEGMENT;
                    if slru_may_delete_clogsegment(segpage, xlrec.pageno) {
                        timeline.put_unlink(RelishTag::Slru { slru, segno }, lsn)?;
                        trace!("unlink CLOG segment {:>04X} at lsn {}", segno, lsn);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn save_multixact_create_record(
    checkpoint: &mut CheckPoint,
    timeline: &dyn Timeline,
    lsn: Lsn,
    xlrec: &XlMultiXactCreate,
    decoded: &DecodedWALRecord,
) -> Result<()> {
    let rec = WALRecord {
        lsn,
        will_init: false,
        rec: decoded.record.clone(),
        main_data_offset: decoded.main_data_offset as u32,
    };
    let pageno = xlrec.mid / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
    let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
    let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
    timeline.put_wal_record(
        RelishTag::Slru {
            slru: SlruKind::MultiXactOffsets,
            segno,
        },
        rpageno,
        rec.clone(),
    )?;

    let first_mbr_pageno = xlrec.moff / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
    let last_mbr_pageno =
        (xlrec.moff + xlrec.nmembers - 1) / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
    // The members SLRU can, in contrast to the offsets one, be filled to almost
    // the full range at once. So we need to handle wraparound.
    let mut pageno = first_mbr_pageno;
    loop {
        // Update members page
        let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
        let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
        timeline.put_wal_record(
            RelishTag::Slru {
                slru: SlruKind::MultiXactMembers,
                segno,
            },
            rpageno,
            rec.clone(),
        )?;

        if pageno == last_mbr_pageno {
            // last block inclusive
            break;
        }

        // handle wraparound
        if pageno == MAX_MBR_BLKNO {
            pageno = 0;
        } else {
            pageno += 1;
        }
    }
    if xlrec.mid >= checkpoint.nextMulti {
        checkpoint.nextMulti = xlrec.mid + 1;
    }
    if xlrec.moff + xlrec.nmembers > checkpoint.nextMultiOffset {
        checkpoint.nextMultiOffset = xlrec.moff + xlrec.nmembers;
    }
    let max_mbr_xid = xlrec.members.iter().fold(0u32, |acc, mbr| {
        if mbr.xid.wrapping_sub(acc) as i32 > 0 {
            mbr.xid
        } else {
            acc
        }
    });

    checkpoint.update_next_xid(max_mbr_xid);
    Ok(())
}

#[allow(non_upper_case_globals)]
const MaxMultiXactOffset: u32 = 0xFFFFFFFF;

#[allow(non_snake_case)]
const fn MXOffsetToMemberPage(xid: u32) -> u32 {
    xid / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32
}
#[allow(non_snake_case)]
const fn MXOffsetToMemberSegment(xid: u32) -> i32 {
    (MXOffsetToMemberPage(xid) / pg_constants::SLRU_PAGES_PER_SEGMENT) as i32
}

fn save_multixact_truncate_record(
    checkpoint: &mut CheckPoint,
    timeline: &dyn Timeline,
    lsn: Lsn,
    xlrec: &XlMultiXactTruncate,
) -> Result<()> {
    checkpoint.oldestMulti = xlrec.end_trunc_off;
    checkpoint.oldestMultiDB = xlrec.oldest_multi_db;

    // PerformMembersTruncation
    let maxsegment: i32 = MXOffsetToMemberSegment(MaxMultiXactOffset);
    let startsegment: i32 = MXOffsetToMemberSegment(xlrec.start_trunc_memb);
    let endsegment: i32 = MXOffsetToMemberSegment(xlrec.end_trunc_memb);
    let mut segment: i32 = startsegment;

    // Delete all the segments except the last one. The last segment can still
    // contain, possibly partially, valid data.
    while segment != endsegment {
        timeline.put_unlink(
            RelishTag::Slru {
                slru: SlruKind::MultiXactMembers,
                segno: segment as u32,
            },
            lsn,
        )?;

        /* move to next segment, handling wraparound correctly */
        if segment == maxsegment {
            segment = 0;
        } else {
            segment += 1;
        }
    }

    // Truncate offsets
    // FIXME: this did not handle wraparound correctly

    Ok(())
}

fn save_relmap_record(
    timeline: &dyn Timeline,
    lsn: Lsn,
    xlrec: &XlRelmapUpdate,
    decoded: &DecodedWALRecord,
) -> Result<()> {
    let rec = WALRecord {
        lsn,
        will_init: true,
        rec: decoded.record.clone(),
        main_data_offset: decoded.main_data_offset as u32,
    };
    let tag = RelishTag::FileNodeMap {
        spcnode: xlrec.tsid,
        dbnode: xlrec.dbid,
    };
    timeline.put_wal_record(tag, 0, rec)?;
    Ok(())
}
