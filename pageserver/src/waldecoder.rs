use crate::pg_constants;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use std::cmp::min;
use std::str;
use thiserror::Error;
use zenith_utils::lsn::Lsn;

const XLOG_BLCKSZ: u32 = 8192;

// FIXME: this is configurable in PostgreSQL, 16 MB is the default
const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

// From PostgreSQL headers

#[repr(C)]
#[derive(Debug)]
pub struct XLogPageHeaderData {
    xlp_magic: u16,    /* magic value for correctness checks */
    xlp_info: u16,     /* flag bits, see below */
    xlp_tli: u32,      /* TimeLineID of first record on page */
    xlp_pageaddr: u64, /* XLOG address of this page */
    xlp_rem_len: u32,  /* total len of remaining data for record */
}

// FIXME: this assumes MAXIMUM_ALIGNOF 8. There are 4 padding bytes at end
#[allow(non_upper_case_globals)]
const SizeOfXLogShortPHD: usize = 2 + 2 + 4 + 8 + 4 + 4;

#[repr(C)]
#[derive(Debug)]
pub struct XLogLongPageHeaderData {
    std: XLogPageHeaderData, /* standard header fields */
    xlp_sysid: u64,          /* system identifier from pg_control */
    xlp_seg_size: u32,       /* just as a cross-check */
    xlp_xlog_blcksz: u32,    /* just as a cross-check */
}

// FIXME: this assumes MAXIMUM_ALIGNOF 8.
#[allow(non_upper_case_globals)]
const SizeOfXLogLongPHD: usize = (2 + 2 + 4 + 8 + 4) + 4 + 8 + 4 + 4;

#[allow(dead_code)]
pub struct WalStreamDecoder {
    lsn: Lsn,

    startlsn: Lsn, // LSN where this record starts
    contlen: u32,
    padlen: u32,

    inputbuf: BytesMut,

    recordbuf: BytesMut,
}

#[derive(Error, Debug, Clone)]
#[error("{msg} at {lsn}")]
pub struct WalDecodeError {
    msg: String,
    lsn: Lsn,
}

//
// WalRecordStream is a Stream that returns a stream of WAL records
// FIXME: This isn't a proper rust stream
//
impl WalStreamDecoder {
    pub fn new(lsn: Lsn) -> WalStreamDecoder {
        WalStreamDecoder {
            lsn,

            startlsn: Lsn(0),
            contlen: 0,
            padlen: 0,

            inputbuf: BytesMut::new(),
            recordbuf: BytesMut::new(),
        }
    }

    pub fn feed_bytes(&mut self, buf: &[u8]) {
        self.inputbuf.extend_from_slice(buf);
    }

    /// Attempt to decode another WAL record from the input that has been fed to the
    /// decoder so far.
    ///
    /// Returns one of the following:
    ///     Ok((u64, Bytes)): a tuple containing the LSN of next record, and the record itself
    ///     Ok(None): there is not enough data in the input buffer. Feed more by calling the `feed_bytes` function
    ///     Err(WalDecodeError): an error occured while decoding, meaning the input was invalid.
    ///
    pub fn poll_decode(&mut self) -> Result<Option<(Lsn, Bytes)>, WalDecodeError> {
        loop {
            // parse and verify page boundaries as we go
            if self.lsn.segment_offset(WAL_SEGMENT_SIZE) == 0 {
                // parse long header

                if self.inputbuf.remaining() < SizeOfXLogLongPHD {
                    return Ok(None);
                }

                let hdr = self.decode_XLogLongPageHeaderData();
                if hdr.std.xlp_pageaddr != self.lsn.0 {
                    return Err(WalDecodeError {
                        msg: "invalid xlog segment header".into(),
                        lsn: self.lsn,
                    });
                }
                // TODO: verify the remaining fields in the header

                self.lsn += SizeOfXLogLongPHD as u64;
                continue;
            } else if self.lsn.0 % (XLOG_BLCKSZ as u64) == 0 {
                // FIXME: make this a member of Lsn, but what should it be called?
                // parse page header

                if self.inputbuf.remaining() < SizeOfXLogShortPHD {
                    return Ok(None);
                }

                let hdr = self.decode_XLogPageHeaderData();
                if hdr.xlp_pageaddr != self.lsn.0 {
                    return Err(WalDecodeError {
                        msg: "invalid xlog page header".into(),
                        lsn: self.lsn,
                    });
                }
                // TODO: verify the remaining fields in the header

                self.lsn += SizeOfXLogShortPHD as u64;
                continue;
            } else if self.padlen > 0 {
                if self.inputbuf.remaining() < self.padlen as usize {
                    return Ok(None);
                }

                // skip padding
                self.inputbuf.advance(self.padlen as usize);
                self.lsn += self.padlen as u64;
                self.padlen = 0;
            } else if self.contlen == 0 {
                // need to have at least the xl_tot_len field

                if self.inputbuf.remaining() < 4 {
                    return Ok(None);
                }

                // read xl_tot_len FIXME: assumes little-endian
                self.startlsn = self.lsn;
                let xl_tot_len = self.inputbuf.get_u32_le();
                if xl_tot_len < SizeOfXLogRecord {
                    return Err(WalDecodeError {
                        msg: format!("invalid xl_tot_len {}", xl_tot_len),
                        lsn: self.lsn,
                    });
                }
                self.lsn += 4;

                self.recordbuf.clear();
                self.recordbuf.reserve(xl_tot_len as usize);
                self.recordbuf.put_u32_le(xl_tot_len);

                self.contlen = xl_tot_len - 4;
                continue;
            } else {
                // we're continuing a record, possibly from previous page.
                // FIXME: Should any of this math be captured into Lsn or a related type?
                let pageleft: u32 = XLOG_BLCKSZ - (self.lsn.0 % (XLOG_BLCKSZ as u64)) as u32;

                // read the rest of the record, or as much as fits on this page.
                let n = min(self.contlen, pageleft) as usize;

                if self.inputbuf.remaining() < n {
                    return Ok(None);
                }

                self.recordbuf.put(self.inputbuf.split_to(n));
                self.lsn += n as u64;
                self.contlen -= n as u32;

                if self.contlen == 0 {
                    let recordbuf = std::mem::replace(&mut self.recordbuf, BytesMut::new());

                    let recordbuf = recordbuf.freeze();

                    // XLOG_SWITCH records are special. If we see one, we need to skip
                    // to the next WAL segment.
                    if is_xlog_switch_record(&recordbuf) {
                        trace!("saw xlog switch record at {}", self.lsn);
                        self.padlen = (WAL_SEGMENT_SIZE - (self.lsn.0 % WAL_SEGMENT_SIZE)) as u32;
                    }

                    // FIXME: what does this code do?
                    if self.lsn.0 % 8 != 0 {
                        self.padlen = 8 - (self.lsn.0 % 8) as u32;
                    }

                    let result = (self.lsn, recordbuf);
                    return Ok(Some(result));
                }
                continue;
            }
        }
        // check record boundaries

        // deal with continuation records

        // deal with xlog_switch records
    }

    #[allow(non_snake_case)]
    fn decode_XLogPageHeaderData(&mut self) -> XLogPageHeaderData {
        let buf = &mut self.inputbuf;

        // FIXME: Assume little-endian

        let hdr: XLogPageHeaderData = XLogPageHeaderData {
            xlp_magic: buf.get_u16_le(),
            xlp_info: buf.get_u16_le(),
            xlp_tli: buf.get_u32_le(),
            xlp_pageaddr: buf.get_u64_le(),
            xlp_rem_len: buf.get_u32_le(),
        };
        // 4 bytes of padding, on 64-bit systems
        buf.advance(4);

        // FIXME: check that hdr.xlp_rem_len matches self.contlen
        //println!("next xlog page (xlp_rem_len: {})", hdr.xlp_rem_len);

        hdr
    }

    #[allow(non_snake_case)]
    fn decode_XLogLongPageHeaderData(&mut self) -> XLogLongPageHeaderData {
        let hdr: XLogLongPageHeaderData = XLogLongPageHeaderData {
            std: self.decode_XLogPageHeaderData(),
            xlp_sysid: self.inputbuf.get_u64_le(),
            xlp_seg_size: self.inputbuf.get_u32_le(),
            xlp_xlog_blcksz: self.inputbuf.get_u32_le(),
        };

        hdr
    }
}

// FIXME:
const BLCKSZ: u16 = 8192;

//
// Constants from xlogrecord.h
//

const XLR_MAX_BLOCK_ID: u8 = 32;

const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

const BKPBLOCK_FORK_MASK: u8 = 0x0F;
const _BKPBLOCK_FLAG_MASK: u8 = 0xF0;
const BKPBLOCK_HAS_IMAGE: u8 = 0x10; /* block data is an XLogRecordBlockImage */
const BKPBLOCK_HAS_DATA: u8 = 0x20;
const BKPBLOCK_WILL_INIT: u8 = 0x40; /* redo will re-init the page */
const BKPBLOCK_SAME_REL: u8 = 0x80; /* RelFileNode omitted, same as previous */

/* Information stored in bimg_info */
const BKPIMAGE_HAS_HOLE: u8 = 0x01; /* page image has "hole" */
const BKPIMAGE_IS_COMPRESSED: u8 = 0x02; /* page image is compressed */
const BKPIMAGE_APPLY: u8 = 0x04; /* page image should be restored during replay */

#[allow(dead_code)]
pub struct DecodedBkpBlock {
    /* Is this block ref in use? */
    //in_use: bool,

    /* Identify the block this refers to */
    pub rnode_spcnode: u32,
    pub rnode_dbnode: u32,
    pub rnode_relnode: u32,
    // Note that we have a few special forknum values for non-rel files.
    pub forknum: u8,
    pub blkno: u32,

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    flags: u8,

    /* Information on full-page image, if any */
    has_image: bool,       /* has image, even for consistency checking */
    pub apply_image: bool, /* has image that should be restored */
    pub will_init: bool,
    //char	   *bkp_image;
    hole_offset: u16,
    hole_length: u16,
    bimg_len: u16,
    bimg_info: u8,

    /* Buffer holding the rmgr-specific data associated with this block */
    has_data: bool,
    data_len: u16,
}

impl DecodedBkpBlock {
    pub fn new() -> DecodedBkpBlock {
        DecodedBkpBlock {
            rnode_spcnode: 0,
            rnode_dbnode: 0,
            rnode_relnode: 0,
            forknum: 0,
            blkno: 0,

            flags: 0,
            has_image: false,
            apply_image: false,
            will_init: false,
            hole_offset: 0,
            hole_length: 0,
            bimg_len: 0,
            bimg_info: 0,

            has_data: false,
            data_len: 0,
        }
    }
}

#[allow(non_upper_case_globals)]
const SizeOfXLogRecord: u32 = 24;

pub struct DecodedWALRecord {
    pub xl_info: u8,
    pub xl_rmid: u8,
    pub record: Bytes, // raw XLogRecord

    pub blocks: Vec<DecodedBkpBlock>,
    pub main_data_offset: usize,
}

// Is this record an XLOG_SWITCH record? They need some special processing,
// so we need to check for that before the rest of the parsing.
//
// FIXME: refactor this and decode_wal_record() below to avoid the duplication.
fn is_xlog_switch_record(rec: &Bytes) -> bool {
    let mut buf = rec.clone();

    // FIXME: assume little-endian here
    let _xl_tot_len = buf.get_u32_le();
    let _xl_xid = buf.get_u32_le();
    let _xl_prev = buf.get_u64_le();
    let xl_info = buf.get_u8();
    let xl_rmid = buf.get_u8();
    buf.advance(2); // 2 bytes of padding
    let _xl_crc = buf.get_u32_le();

    xl_info == pg_constants::XLOG_SWITCH && xl_rmid == pg_constants::RM_XLOG_ID
}

pub type Oid = u32;
pub type BlockNumber = u32;

pub const MAIN_FORKNUM: u8 = 0;
pub const SMGR_TRUNCATE_HEAP: u32 = 0x0001;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RelFileNode {
    pub spcnode: Oid, /* tablespace */
    pub dbnode: Oid,  /* database */
    pub relnode: Oid, /* relation */
}

#[repr(C)]
#[derive(Debug)]
pub struct XlSmgrTruncate {
    pub blkno: BlockNumber,
    pub rnode: RelFileNode,
    pub flags: u32,
}

impl XlSmgrTruncate {
    pub fn decode(decoded: &DecodedWALRecord) -> XlSmgrTruncate {
        let mut buf = decoded.record.clone();
        buf.advance((SizeOfXLogRecord + 2) as usize);
        XlSmgrTruncate {
            blkno: buf.get_u32_le(),
            rnode: RelFileNode {
                spcnode: buf.get_u32_le(), /* tablespace */
                dbnode: buf.get_u32_le(),  /* database */
                relnode: buf.get_u32_le(), /* relation */
            },
            flags: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlCreateDatabase {
    pub db_id: Oid,
    pub tablespace_id: Oid,
    pub src_db_id: Oid,
    pub src_tablespace_id: Oid,
}

impl XlCreateDatabase {
    pub fn decode(decoded: &DecodedWALRecord) -> XlCreateDatabase {
        let mut buf = decoded.record.clone();
        buf.advance((SizeOfXLogRecord + 2) as usize);
        XlCreateDatabase {
            db_id: buf.get_u32_le(),
            tablespace_id: buf.get_u32_le(),
            src_db_id: buf.get_u32_le(),
            src_tablespace_id: buf.get_u32_le(),
        }
    }
}

//
// Routines to decode a WAL record and figure out which blocks are modified
//
// See xlogrecord.h for details
// The overall layout of an XLOG record is:
//		Fixed-size header (XLogRecord struct)
//      XLogRecordBlockHeader struct
//          If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows
//	           If BKPIMAGE_HAS_HOLE and BKPIMAGE_IS_COMPRESSED, an
//	           XLogRecordBlockCompressHeader struct follows.
//          If BKPBLOCK_SAME_REL is not set, a RelFileNode follows
//          BlockNumber follows
//      XLogRecordBlockHeader struct
//      ...
//      XLogRecordDataHeader[Short|Long] struct
//      block data
//      block data
//      ...
//      main data
pub fn decode_wal_record(record: Bytes) -> DecodedWALRecord {
    let mut rnode_spcnode: u32 = 0;
    let mut rnode_dbnode: u32 = 0;
    let mut rnode_relnode: u32 = 0;
    let mut got_rnode = false;

    let mut buf = record.clone();

    // 1. Parse XLogRecord struct

    // FIXME: assume little-endian here
    let xl_tot_len = buf.get_u32_le();
    let xl_xid = buf.get_u32_le();
    let xl_prev = buf.get_u64_le();
    let xl_info = buf.get_u8();
    let xl_rmid = buf.get_u8();
    buf.advance(2); // 2 bytes of padding
    let _xl_crc = buf.get_u32_le();

    trace!(
        "decode_wal_record xl_rmid = {} xl_info = {}",
        xl_rmid,
        xl_info
    );

    let remaining = xl_tot_len - SizeOfXLogRecord;

    if buf.remaining() != remaining as usize {
        //TODO error
    }

    let mut max_block_id = 0;
    let mut blocks_total_len: u32 = 0;
    let mut main_data_len = 0;
    let mut datatotal: u32 = 0;
    let mut blocks: Vec<DecodedBkpBlock> = Vec::new();

    // 2. Decode the headers.
    // XLogRecordBlockHeaders if any,
    // XLogRecordDataHeader[Short|Long]
    while buf.remaining() > datatotal as usize {
        let block_id = buf.get_u8();

        match block_id {
            XLR_BLOCK_ID_DATA_SHORT => {
                /* XLogRecordDataHeaderShort */
                main_data_len = buf.get_u8() as u32;
                datatotal += main_data_len;
            }

            XLR_BLOCK_ID_DATA_LONG => {
                /* XLogRecordDataHeaderLong */
                main_data_len = buf.get_u32_le();
                datatotal += main_data_len;
            }

            XLR_BLOCK_ID_ORIGIN => {
                // RepOriginId is uint16
                buf.advance(2);
            }

            XLR_BLOCK_ID_TOPLEVEL_XID => {
                // TransactionId is uint32
                buf.advance(4);
            }

            0..=XLR_MAX_BLOCK_ID => {
                /* XLogRecordBlockHeader */
                let mut blk = DecodedBkpBlock::new();
                let fork_flags: u8;

                if block_id <= max_block_id {
                    // TODO
                    //report_invalid_record(state,
                    //			  "out-of-order block_id %u at %X/%X",
                    //			  block_id,
                    //			  (uint32) (state->ReadRecPtr >> 32),
                    //			  (uint32) state->ReadRecPtr);
                    //    goto err;
                }
                max_block_id = block_id;

                fork_flags = buf.get_u8();
                blk.forknum = fork_flags & BKPBLOCK_FORK_MASK;
                blk.flags = fork_flags;
                blk.has_image = (fork_flags & BKPBLOCK_HAS_IMAGE) != 0;
                blk.has_data = (fork_flags & BKPBLOCK_HAS_DATA) != 0;
                blk.will_init = (fork_flags & BKPBLOCK_WILL_INIT) != 0;
                blk.data_len = buf.get_u16_le();

                /* TODO cross-check that the HAS_DATA flag is set iff data_length > 0 */

                datatotal += blk.data_len as u32;
                blocks_total_len += blk.data_len as u32;

                if blk.has_image {
                    blk.bimg_len = buf.get_u16_le();
                    blk.hole_offset = buf.get_u16_le();
                    blk.bimg_info = buf.get_u8();

                    blk.apply_image = (blk.bimg_info & BKPIMAGE_APPLY) != 0;

                    if blk.bimg_info & BKPIMAGE_IS_COMPRESSED != 0 {
                        if blk.bimg_info & BKPIMAGE_HAS_HOLE != 0 {
                            blk.hole_length = buf.get_u16_le();
                        } else {
                            blk.hole_length = 0;
                        }
                    } else {
                        blk.hole_length = BLCKSZ - blk.bimg_len;
                    }
                    datatotal += blk.bimg_len as u32;
                    blocks_total_len += blk.bimg_len as u32;

                    /*
                     * cross-check that hole_offset > 0, hole_length > 0 and
                     * bimg_len < BLCKSZ if the HAS_HOLE flag is set.
                     */
                    if blk.bimg_info & BKPIMAGE_HAS_HOLE != 0
                        && (blk.hole_offset == 0 || blk.hole_length == 0 || blk.bimg_len == BLCKSZ)
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "BKPIMAGE_HAS_HOLE set, but hole offset %u length %u block image length %u at %X/%X",
                                      (unsigned int) blk->hole_offset,
                                      (unsigned int) blk->hole_length,
                                      (unsigned int) blk->bimg_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that hole_offset == 0 and hole_length == 0 if
                     * the HAS_HOLE flag is not set.
                     */
                    if blk.bimg_info & BKPIMAGE_HAS_HOLE == 0
                        && (blk.hole_offset != 0 || blk.hole_length != 0)
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "BKPIMAGE_HAS_HOLE not set, but hole offset %u length %u at %X/%X",
                                      (unsigned int) blk->hole_offset,
                                      (unsigned int) blk->hole_length,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that bimg_len < BLCKSZ if the IS_COMPRESSED
                     * flag is set.
                     */
                    if (blk.bimg_info & BKPIMAGE_IS_COMPRESSED == 0) && blk.bimg_len == BLCKSZ {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "BKPIMAGE_IS_COMPRESSED set, but block image length %u at %X/%X",
                                      (unsigned int) blk->bimg_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that bimg_len = BLCKSZ if neither HAS_HOLE nor
                     * IS_COMPRESSED flag is set.
                     */
                    if blk.bimg_info & BKPIMAGE_HAS_HOLE == 0
                        && blk.bimg_info & BKPIMAGE_IS_COMPRESSED == 0
                        && blk.bimg_len != BLCKSZ
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_IS_COMPRESSED set, but block image length is %u at %X/%X",
                                      (unsigned int) blk->data_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }
                }
                if fork_flags & BKPBLOCK_SAME_REL == 0 {
                    rnode_spcnode = buf.get_u32_le();
                    rnode_dbnode = buf.get_u32_le();
                    rnode_relnode = buf.get_u32_le();
                    got_rnode = true;
                } else if !got_rnode {
                    // TODO
                    /*
                    report_invalid_record(state,
                                    "BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
                                    (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                    goto err;           */
                }

                blk.rnode_spcnode = rnode_spcnode;
                blk.rnode_dbnode = rnode_dbnode;
                blk.rnode_relnode = rnode_relnode;

                blk.blkno = buf.get_u32_le();
                trace!(
                    "this record affects {}/{}/{} blk {}",
                    rnode_spcnode,
                    rnode_dbnode,
                    rnode_relnode,
                    blk.blkno
                );

                blocks.push(blk);
            }

            _ => {
                // TODO: invalid block_id
            }
        }
    }

    // 3. Decode blocks.
    // We don't need them, so just skip blocks_total_len bytes
    buf.advance(blocks_total_len as usize);

    let main_data_offset = (xl_tot_len - main_data_len) as usize;

    // 4. Decode main_data
    if main_data_len > 0 {
        assert_eq!(buf.remaining(), main_data_len as usize);
    }

    //5. Handle special CLOG and XACT records
    if xl_rmid == pg_constants::RM_CLOG_ID {
        let mut blk = DecodedBkpBlock::new();
        blk.forknum = pg_constants::PG_XACT_FORKNUM as u8;
        blk.blkno = buf.get_i32_le() as u32;
        trace!("RM_CLOG_ID updates block {}", blk.blkno);
        blocks.push(blk);
    } else if xl_rmid == pg_constants::RM_XACT_ID {
        let info = xl_info & pg_constants::XLOG_XACT_OPMASK;
        if info == pg_constants::XLOG_XACT_COMMIT {
            let mut blk = DecodedBkpBlock::new();
            blk.forknum = pg_constants::PG_XACT_FORKNUM as u8;
            blk.blkno = xl_xid / pg_constants::CLOG_XACTS_PER_PAGE;
            trace!(
                "XLOG_XACT_COMMIT xl_prev {:X}/{:X}  xid {} updates block {}",
                (xl_prev >> 32),
                xl_prev & 0xffffffff,
                xl_xid,
                blk.blkno
            );
            blocks.push(blk);
            //TODO parse commit record to extract subtrans entries
        } else if info == pg_constants::XLOG_XACT_ABORT {
            let mut blk = DecodedBkpBlock::new();
            blk.forknum = pg_constants::PG_XACT_FORKNUM as u8;
            blk.blkno = xl_xid / pg_constants::CLOG_XACTS_PER_PAGE;
            trace!(
                "XLOG_XACT_ABORT xl_prev {:X}/{:X} xid {} updates block {}",
                (xl_prev >> 32),
                xl_prev & 0xffffffff,
                xl_xid,
                blk.blkno
            );
            blocks.push(blk);
            //TODO parse abort record to extract subtrans entries
        }
    } else if xl_rmid == pg_constants::RM_DBASE_ID {
        let info = xl_info & !pg_constants::XLR_INFO_MASK;
        if info == pg_constants::XLOG_DBASE_CREATE {
            //buf points to main_data
            let db_id = buf.get_u32_le();
            let tablespace_id = buf.get_u32_le();
            let src_db_id = buf.get_u32_le();
            let src_tablespace_id = buf.get_u32_le();
            trace!(
                "XLOG_DBASE_CREATE tablespace_id/db_id {}/{} src_db_id {}/{}",
                tablespace_id,
                db_id,
                src_tablespace_id,
                src_db_id
            );
            // in postgres it is implemented as copydir
            // we need to copy all pages in page_cache
        } else {
            trace!("XLOG_DBASE_DROP is not handled yet");
        }
    } else if xl_rmid == pg_constants::RM_TBLSPC_ID {
        let info = xl_info & !pg_constants::XLR_INFO_MASK;
        if info == pg_constants::XLOG_TBLSPC_CREATE {
            //buf points to main_data
            let ts_id = buf.get_u32_le();
            let ts_path = str::from_utf8(&buf).unwrap();
            trace!("XLOG_TBLSPC_CREATE ts_id {} ts_path {}", ts_id, ts_path);
        } else {
            trace!("XLOG_TBLSPC_DROP is not handled yet");
        }
    }

    DecodedWALRecord {
        xl_info,
        xl_rmid,
        record,
        blocks,
        main_data_offset,
    }
}
