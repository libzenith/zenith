//!
//! Import data and WAL from a PostgreSQL data directory and WAL segments into
//! zenith Timeline.
//!
use log::*;
use std::cmp::{max, min};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use anyhow::Result;
use bytes::{Buf, Bytes};

use crate::object_key::*;
use crate::repository::*;
use crate::waldecoder::*;
use crate::PageServerConf;
use crate::ZTenantId;
use crate::ZTimelineId;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::{pg_constants, CheckPoint, ControlFileData};
use zenith_utils::lsn::Lsn;

const MAX_MBR_BLKNO: u32 =
    pg_constants::MAX_MULTIXACT_ID / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;

///
/// Find latest snapshot in a timeline's 'snapshots' directory
///
pub fn find_latest_snapshot(
    conf: &PageServerConf,
    timelineid: &ZTimelineId,
    tenantid: &ZTenantId,
) -> Result<Lsn> {
    let snapshotspath = conf.snapshots_path(timelineid, tenantid);
    let mut last_snapshot_lsn = Lsn(0);
    for direntry in fs::read_dir(&snapshotspath).unwrap() {
        let filename = direntry.unwrap().file_name();

        if let Ok(lsn) = Lsn::from_filename(&filename) {
            last_snapshot_lsn = max(lsn, last_snapshot_lsn);
        } else {
            error!("unrecognized file in snapshots directory: {:?}", filename);
        }
    }

    if last_snapshot_lsn == Lsn(0) {
        error!(
            "could not find valid snapshot in {}",
            snapshotspath.display()
        );
        // TODO return error?
    }
    Ok(last_snapshot_lsn)
}

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
                import_nonrel_file(timeline, lsn, ObjectTag::ControlFile, &direntry.path())?;
                // Extract checkpoint record from pg_control and store is as separate object
                let pg_control_bytes =
                    timeline.get_page_at_lsn_nowait(ObjectTag::ControlFile, lsn)?;
                let pg_control = ControlFileData::decode(&pg_control_bytes)?;
                let checkpoint_bytes = pg_control.checkPointCopy.encode();
                timeline.put_page_image(ObjectTag::Checkpoint, lsn, checkpoint_bytes, false)?;
            }
            Some("pg_filenode.map") => import_nonrel_file(
                timeline,
                lsn,
                ObjectTag::FileNodeMap(DatabaseTag {
                    spcnode: pg_constants::GLOBALTABLESPACE_OID,
                    dbnode: 0,
                }),
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
                    ObjectTag::FileNodeMap(DatabaseTag {
                        spcnode: pg_constants::DEFAULTTABLESPACE_OID,
                        dbnode: dboid,
                    }),
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
        import_slru_file(
            timeline,
            lsn,
            |blknum| ObjectTag::Clog(SlruBufferTag { blknum }),
            &entry.path(),
        )?;
    }
    for entry in fs::read_dir(path.join("pg_multixact").join("members"))? {
        let entry = entry?;
        import_slru_file(
            timeline,
            lsn,
            |blknum| ObjectTag::MultiXactMembers(SlruBufferTag { blknum }),
            &entry.path(),
        )?;
    }
    for entry in fs::read_dir(path.join("pg_multixact").join("offsets"))? {
        let entry = entry?;
        import_slru_file(
            timeline,
            lsn,
            |blknum| ObjectTag::MultiXactOffsets(SlruBufferTag { blknum }),
            &entry.path(),
        )?;
    }
    for entry in fs::read_dir(path.join("pg_twophase"))? {
        let entry = entry?;
        let xid = u32::from_str_radix(&entry.path().to_str().unwrap(), 16)?;
        import_nonrel_file(
            timeline,
            lsn,
            ObjectTag::TwoPhase(PrepareTag { xid }),
            &entry.path(),
        )?;
    }
    // TODO: Scan pg_tblspc

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
                let tag = ObjectTag::RelationBuffer(BufferTag {
                    rel: RelTag {
                        spcnode: spcoid,
                        dbnode: dboid,
                        relnode,
                        forknum,
                    },
                    blknum,
                });
                timeline.put_page_image(tag, lsn, Bytes::copy_from_slice(&buf), true)?;
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

fn import_nonrel_file(
    timeline: &dyn Timeline,
    lsn: Lsn,
    tag: ObjectTag,
    path: &Path,
) -> Result<()> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    // read the whole file
    file.read_to_end(&mut buffer)?;

    timeline.put_page_image(tag, lsn, Bytes::copy_from_slice(&buffer[..]), false)?;
    Ok(())
}

fn import_slru_file(
    timeline: &dyn Timeline,
    lsn: Lsn,
    gen_tag: fn(blknum: u32) -> ObjectTag,
    path: &Path,
) -> Result<()> {
    // Does it look like a relation file?

    let mut file = File::open(path)?;
    let mut buf: [u8; 8192] = [0u8; 8192];
    let segno = u32::from_str_radix(path.file_name().unwrap().to_str().unwrap(), 16)?;
    let mut blknum: u32 = segno * pg_constants::SLRU_PAGES_PER_SEGMENT;
    loop {
        let r = file.read_exact(&mut buf);
        match r {
            Ok(_) => {
                timeline.put_page_image(
                    gen_tag(blknum),
                    lsn,
                    Bytes::copy_from_slice(&buf),
                    false,
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
        blknum += 1;
    }

    Ok(())
}

/// Scan PostgreSQL WAL files in given directory, and load all records >= 'startpoint' into
/// the repository.
pub fn import_timeline_wal(walpath: &Path, timeline: &dyn Timeline, startpoint: Lsn) -> Result<()> {
    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let mut segno = startpoint.segment_number(pg_constants::WAL_SEGMENT_SIZE);
    let mut offset = startpoint.segment_offset(pg_constants::WAL_SEGMENT_SIZE);
    let mut last_lsn = startpoint;

    let checkpoint_bytes = timeline.get_page_at_lsn_nowait(ObjectTag::Checkpoint, startpoint)?;
    let mut checkpoint = CheckPoint::decode(&checkpoint_bytes)?;

    loop {
        // FIXME: assume postgresql tli 1 for now
        let filename = XLogFileName(1, segno, pg_constants::WAL_SEGMENT_SIZE);
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

        let mut buf = Vec::new();
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

        info!(
            "imported {} records from WAL file {} up to {}",
            nrecords,
            path.display(),
            last_lsn
        );

        segno += 1;
        offset = 0;
    }
    info!("reached end of WAL at {}", last_lsn);
    let checkpoint_bytes = checkpoint.encode();
    timeline.put_page_image(ObjectTag::Checkpoint, last_lsn, checkpoint_bytes, false)?;
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
        let tag = ObjectTag::RelationBuffer(BufferTag {
            rel: RelTag {
                spcnode: blk.rnode_spcnode,
                dbnode: blk.rnode_dbnode,
                relnode: blk.rnode_relnode,
                forknum: blk.forknum as u8,
            },
            blknum: blk.blkno,
        });

        let rec = WALRecord {
            lsn,
            will_init: blk.will_init || blk.apply_image,
            rec: recdata.clone(),
            main_data_offset: decoded.main_data_offset as u32,
        };

        timeline.put_wal_record(tag, rec)?;
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
        let blknum = buf.get_u32_le();
        let info = decoded.xl_info & !pg_constants::XLR_INFO_MASK;
        let tag = ObjectTag::Clog(SlruBufferTag { blknum });
        if info == pg_constants::CLOG_ZEROPAGE {
            let rec = WALRecord {
                lsn,
                will_init: true,
                rec: recdata.clone(),
                main_data_offset: decoded.main_data_offset as u32,
            };
            timeline.put_wal_record(tag, rec)?;
        } else {
            assert!(info == pg_constants::CLOG_TRUNCATE);
            checkpoint.oldestXid = buf.get_u32_le();
            checkpoint.oldestXidDB = buf.get_u32_le();
            trace!(
                "RM_CLOG_ID truncate blkno {} oldestXid {} oldestXidDB {}",
                blknum,
                checkpoint.oldestXid,
                checkpoint.oldestXidDB
            );
            if let Some(ObjectTag::Clog(first_slru_tag)) =
                timeline.get_next_tag(ObjectTag::Clog(SlruBufferTag { blknum: 0 }))?
            {
                for trunc_blknum in first_slru_tag.blknum..=blknum {
                    let tag = ObjectTag::Clog(SlruBufferTag {
                        blknum: trunc_blknum,
                    });
                    timeline.put_slru_truncate(tag, lsn)?;
                }
            }
        }
    } else if decoded.xl_rmid == pg_constants::RM_XACT_ID {
        let info = decoded.xl_info & pg_constants::XLOG_XACT_OPMASK;
        if info == pg_constants::XLOG_XACT_COMMIT
            || info == pg_constants::XLOG_XACT_COMMIT_PREPARED
            || info == pg_constants::XLOG_XACT_ABORT
            || info == pg_constants::XLOG_XACT_ABORT_PREPARED
        {
            let parsed_xact = XlXactParsedRecord::decode(&mut buf, decoded.xl_xid, decoded.xl_info);
            save_xact_record(timeline, lsn, &parsed_xact, decoded)?;
        } else if info == pg_constants::XLOG_XACT_PREPARE {
            let rec = WALRecord {
                lsn,
                will_init: true,
                rec: recdata.clone(),
                main_data_offset: decoded.main_data_offset as u32,
            };
            timeline.put_wal_record(
                ObjectTag::TwoPhase(PrepareTag {
                    xid: decoded.xl_xid,
                }),
                rec,
            )?;
        }
    } else if decoded.xl_rmid == pg_constants::RM_MULTIXACT_ID {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE
            || info == pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE
        {
            let blknum = buf.get_u32_le();
            let rec = WALRecord {
                lsn,
                will_init: true,
                rec: recdata.clone(),
                main_data_offset: decoded.main_data_offset as u32,
            };
            let tag = if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE {
                ObjectTag::MultiXactOffsets(SlruBufferTag { blknum })
            } else {
                ObjectTag::MultiXactMembers(SlruBufferTag { blknum })
            };
            timeline.put_wal_record(tag, rec)?;
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

        let nblocks = timeline.get_rel_size(src_rel, req_lsn)?;
        let dst_rel = RelTag {
            spcnode: tablespace_id,
            dbnode: db_id,
            relnode: src_rel.relnode,
            forknum: src_rel.forknum,
        };

        // Copy content
        for blknum in 0..nblocks {
            let src_key = ObjectTag::RelationBuffer(BufferTag {
                rel: src_rel,
                blknum,
            });
            let dst_key = ObjectTag::RelationBuffer(BufferTag {
                rel: dst_rel,
                blknum,
            });

            let content = timeline.get_page_at_lsn_nowait(src_key, req_lsn)?;

            debug!("copying block {:?} to {:?}", src_key, dst_key);

            timeline.put_page_image(dst_key, lsn, content, true)?;
            num_blocks_copied += 1;
        }

        if nblocks == 0 {
            // make sure we have some trace of the relation, even if it's empty
            timeline.put_truncation(dst_rel, lsn, 0)?;
        }

        num_rels_copied += 1;
    }
    // Copy relfilemap
    for tag in timeline.list_nonrels(req_lsn)? {
        match tag {
            ObjectTag::FileNodeMap(db) => {
                if db.spcnode == src_tablespace_id && db.dbnode == src_db_id {
                    let img = timeline.get_page_at_lsn_nowait(tag, req_lsn)?;
                    let new_tag = ObjectTag::FileNodeMap(DatabaseTag {
                        spcnode: tablespace_id,
                        dbnode: db_id,
                    });
                    timeline.put_page_image(new_tag, lsn, img, false)?;
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
        timeline.put_truncation(rel, lsn, rec.blkno)?;
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
        timeline.put_truncation(rel, lsn, num_fsm_blocks)?;
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
        timeline.put_truncation(rel, lsn, num_vm_blocks)?;
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
    let mut blknum = parsed.xid / pg_constants::CLOG_XACTS_PER_PAGE;
    let tag = ObjectTag::Clog(SlruBufferTag { blknum });
    let rec = WALRecord {
        lsn,
        will_init: false,
        rec: decoded.record.clone(),
        main_data_offset: decoded.main_data_offset as u32,
    };
    timeline.put_wal_record(tag, rec.clone())?;

    for subxact in &parsed.subxacts {
        let subxact_blknum = subxact / pg_constants::CLOG_XACTS_PER_PAGE;
        if subxact_blknum != blknum {
            blknum = subxact_blknum;
            let tag = ObjectTag::Clog(SlruBufferTag { blknum });
            timeline.put_wal_record(tag, rec.clone())?;
        }
    }
    for xnode in &parsed.xnodes {
        for forknum in pg_constants::MAIN_FORKNUM..=pg_constants::VISIBILITYMAP_FORKNUM {
            let rel_tag = RelTag {
                forknum,
                spcnode: xnode.spcnode,
                dbnode: xnode.dbnode,
                relnode: xnode.relnode,
            };
            timeline.put_unlink(rel_tag, lsn)?;
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
    let blknum = xlrec.mid / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
    let tag = ObjectTag::MultiXactOffsets(SlruBufferTag { blknum });
    timeline.put_wal_record(tag, rec.clone())?;

    let first_mbr_blkno = xlrec.moff / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
    let last_mbr_blkno =
        (xlrec.moff + xlrec.nmembers - 1) / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
    // The members SLRU can, in contrast to the offsets one, be filled to almost
    // the full range at once. So we need to handle wraparound.
    let mut blknum = first_mbr_blkno;
    loop {
        // Update members page
        let tag = ObjectTag::MultiXactMembers(SlruBufferTag { blknum });
        timeline.put_wal_record(tag, rec.clone())?;

        if blknum == last_mbr_blkno {
            // last block inclusive
            break;
        }

        // handle wraparound
        if blknum == MAX_MBR_BLKNO {
            blknum = 0;
        } else {
            blknum += 1;
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

fn save_multixact_truncate_record(
    checkpoint: &mut CheckPoint,
    timeline: &dyn Timeline,
    lsn: Lsn,
    xlrec: &XlMultiXactTruncate,
) -> Result<()> {
    checkpoint.oldestMulti = xlrec.end_trunc_off;
    checkpoint.oldestMultiDB = xlrec.oldest_multi_db;
    let first_off_blkno = xlrec.start_trunc_off / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
    let last_off_blkno = xlrec.end_trunc_off / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
    // Delete all the segments except the last one. The last segment can still
    // contain, possibly partially, valid data.
    for blknum in first_off_blkno..last_off_blkno {
        let tag = ObjectTag::MultiXactOffsets(SlruBufferTag { blknum });
        timeline.put_slru_truncate(tag, lsn)?;
    }
    let first_mbr_blkno = xlrec.start_trunc_memb / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
    let last_mbr_blkno = xlrec.end_trunc_memb / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
    // The members SLRU can, in contrast to the offsets one, be filled to almost
    // the full range at once. So we need to handle wraparound.
    let mut blknum = first_mbr_blkno;
    // Delete all the segments but the last one. The last segment can still
    // contain, possibly partially, valid data.
    while blknum != last_mbr_blkno {
        let tag = ObjectTag::MultiXactMembers(SlruBufferTag { blknum });
        timeline.put_slru_truncate(tag, lsn)?;
        // handle wraparound
        if blknum == MAX_MBR_BLKNO {
            blknum = 0;
        } else {
            blknum += 1;
        }
    }
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
    let tag = ObjectTag::FileNodeMap(DatabaseTag {
        spcnode: xlrec.tsid,
        dbnode: xlrec.dbid,
    });
    timeline.put_wal_record(tag, rec)?;
    Ok(())
}
