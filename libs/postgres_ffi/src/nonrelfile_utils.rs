//!
//! Common utilities for dealing with PostgreSQL non-relation files.
//!
use super::{pg_constants, xlog_utils::XidCSN};
use crate::transaction_id_precedes;
use byteorder::{LittleEndian, ByteOrder};
use bytes::BytesMut;
use log::*;

use super::bindings::MultiXactId;

pub fn transaction_id_set_status(xid: u32, status: u8, page: &mut BytesMut) {
    trace!(
        "handle_apply_request for RM_XACT_ID-{} (1-commit, 2-abort, 3-sub_commit)",
        status
    );

    let byteno: usize = ((xid as u32 % pg_constants::CLOG_XACTS_PER_PAGE as u32)
        / pg_constants::CLOG_XACTS_PER_BYTE) as usize;

    let bshift: u8 =
        ((xid % pg_constants::CLOG_XACTS_PER_BYTE) * pg_constants::CLOG_BITS_PER_XACT as u32) as u8;

    page[byteno] =
        (page[byteno] & !(pg_constants::CLOG_XACT_BITMASK << bshift)) | (status << bshift);
}

pub fn transaction_id_get_status(xid: u32, page: &[u8]) -> u8 {
    let byteno: usize = ((xid as u32 % pg_constants::CLOG_XACTS_PER_PAGE as u32)
        / pg_constants::CLOG_XACTS_PER_BYTE) as usize;

    let bshift: u8 =
        ((xid % pg_constants::CLOG_XACTS_PER_BYTE) * pg_constants::CLOG_BITS_PER_XACT as u32) as u8;

    ((page[byteno] >> bshift) & pg_constants::CLOG_XACT_BITMASK) as u8
}

pub fn transaction_id_set_csn(xid: u32, csn: XidCSN, page: &mut BytesMut) {
    trace!("handle_apply_csn_request for RM_XACT_ID-{}", csn);

    let entryno: u32 = (xid as u32 % pg_constants::CSN_LOG_XACTS_PER_PAGE as u32) as u32;
    let csnsize: u32 = (std::mem::sizeof<XidCSN>()) as u32;
    let byteno: u32 = (entryno * csnsize) as u32;

    LittleEndian::write_u64(&mut page[byteno..byteno+csnsize], csn);
}

// See CLOGPagePrecedes in clog.c
pub const fn clogpage_precedes(page1: u32, page2: u32) -> bool {
    let mut xid1 = page1 * pg_constants::CLOG_XACTS_PER_PAGE;
    xid1 += pg_constants::FIRST_NORMAL_TRANSACTION_ID + 1;
    let mut xid2 = page2 * pg_constants::CLOG_XACTS_PER_PAGE;
    xid2 += pg_constants::FIRST_NORMAL_TRANSACTION_ID + 1;

    transaction_id_precedes(xid1, xid2)
        && transaction_id_precedes(xid1, xid2 + pg_constants::CLOG_XACTS_PER_PAGE - 1)
}

// See SlruMayDeleteSegment() in slru.c
pub fn slru_may_delete_segment(segpage: u32, cutoff_page: u32) -> bool {
    let seg_last_page = segpage + pg_constants::SLRU_PAGES_PER_SEGMENT - 1;

    assert_eq!(segpage % pg_constants::SLRU_PAGES_PER_SEGMENT, 0);

    clogpage_precedes(segpage, cutoff_page) && clogpage_precedes(seg_last_page, cutoff_page)
}

// Multixact utils

pub fn mx_offset_to_flags_offset(xid: MultiXactId) -> usize {
    ((xid / pg_constants::MULTIXACT_MEMBERS_PER_MEMBERGROUP as u32) as u16
        % pg_constants::MULTIXACT_MEMBERGROUPS_PER_PAGE
        * pg_constants::MULTIXACT_MEMBERGROUP_SIZE) as usize
}

pub fn mx_offset_to_flags_bitshift(xid: MultiXactId) -> u16 {
    (xid as u16) % pg_constants::MULTIXACT_MEMBERS_PER_MEMBERGROUP
        * pg_constants::MXACT_MEMBER_BITS_PER_XACT
}

/* Location (byte offset within page) of TransactionId of given member */
pub fn mx_offset_to_member_offset(xid: MultiXactId) -> usize {
    mx_offset_to_flags_offset(xid)
        + (pg_constants::MULTIXACT_FLAGBYTES_PER_GROUP
            + (xid as u16 % pg_constants::MULTIXACT_MEMBERS_PER_MEMBERGROUP) * 4) as usize
}

fn mx_offset_to_member_page(xid: u32) -> u32 {
    xid / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32
}

pub fn mx_offset_to_member_segment(xid: u32) -> i32 {
    (mx_offset_to_member_page(xid) / pg_constants::SLRU_PAGES_PER_SEGMENT) as i32
}

// See CSNLogPagePrecedes in csn_log.c
pub const fn csnlogpage_precedes(page1: u32, page2: u32) -> bool {
    let mut xid1: u32 = page1 * pg_constants::CSN_LOG_XACTS_PER_PAGE;
    xid1 += pg_constants::FirstNormalXidCSN + 1;
    let mut xid2: u32 = page2 * pg_constants::CSN_LOG_XACTS_PER_PAGE;
    xid2 += pg_constants::FirstNormalXidCSN + 1;

    transaction_id_precedes(xid1, xid2)
}