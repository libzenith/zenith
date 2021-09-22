//!
//! WAL receiver connects to the WAL safekeeper service, streams WAL,
//! decodes records and saves them in the repository for the correct
//! timeline.
//!
//! We keep one WAL receiver active per timeline.

use crate::relish::*;
use crate::restore_local_repo;
use crate::tenant_mgr;
use crate::waldecoder::*;
use crate::PageServerConf;
use anyhow::{Error, Result};
use lazy_static::lazy_static;
use log::*;
use postgres::fallible_iterator::FallibleIterator;
use postgres::replication::ReplicationIter;
use postgres::{Client, NoTls, SimpleQueryMessage, SimpleQueryRow};
use postgres_ffi::xlog_utils::*;
use postgres_ffi::*;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Mutex;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use zenith_utils::lsn::Lsn;
use zenith_utils::zid::ZTenantId;
use zenith_utils::zid::ZTimelineId;

//
// We keep one WAL Receiver active per timeline.
//
struct WalReceiverEntry {
    wal_producer_connstr: String,
}

lazy_static! {
    static ref WAL_RECEIVERS: Mutex<HashMap<ZTimelineId, WalReceiverEntry>> =
        Mutex::new(HashMap::new());
}

// Launch a new WAL receiver, or tell one that's running about change in connection string
pub fn launch_wal_receiver(
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    wal_producer_connstr: &str,
    tenantid: ZTenantId,
) {
    let mut receivers = WAL_RECEIVERS.lock().unwrap();

    match receivers.get_mut(&timelineid) {
        Some(receiver) => {
            receiver.wal_producer_connstr = wal_producer_connstr.into();
        }
        None => {
            let receiver = WalReceiverEntry {
                wal_producer_connstr: wal_producer_connstr.into(),
            };
            receivers.insert(timelineid, receiver);

            // Also launch a new thread to handle this connection
            //
            // NOTE: This thread name is checked in the assertion in wait_lsn. If you change
            // this, make sure you update the assertion too.
            let _walreceiver_thread = thread::Builder::new()
                .name("WAL receiver thread".into())
                .spawn(move || {
                    thread_main(conf, timelineid, tenantid);
                })
                .unwrap();
        }
    };
}

// Look up current WAL producer connection string in the hash table
fn get_wal_producer_connstr(timelineid: ZTimelineId) -> String {
    let receivers = WAL_RECEIVERS.lock().unwrap();

    receivers
        .get(&timelineid)
        .unwrap()
        .wal_producer_connstr
        .clone()
}

//
// This is the entry point for the WAL receiver thread.
//
fn thread_main(conf: &'static PageServerConf, timelineid: ZTimelineId, tenantid: ZTenantId) {
    info!(
        "WAL receiver thread started for timeline : '{}'",
        timelineid
    );

    //
    // Make a connection to the WAL safekeeper, or directly to the primary PostgreSQL server,
    // and start streaming WAL from it. If the connection is lost, keep retrying.
    //
    loop {
        // Look up the current WAL producer address
        let wal_producer_connstr = get_wal_producer_connstr(timelineid);

        let res = walreceiver_main(conf, timelineid, &wal_producer_connstr, tenantid);

        if let Err(e) = res {
            info!(
                "WAL streaming connection failed ({}), retrying in 1 second",
                e
            );
            sleep(Duration::from_secs(1));
        }
    }
}

fn walreceiver_main(
    conf: &PageServerConf,
    timelineid: ZTimelineId,
    wal_producer_connstr: &str,
    tenantid: ZTenantId,
) -> Result<(), Error> {
    // Connect to the database in replication mode.
    info!("connecting to {:?}", wal_producer_connstr);
    let connect_cfg = format!(
        "{} application_name=pageserver replication=true",
        wal_producer_connstr
    );

    let mut rclient = Client::connect(&connect_cfg, NoTls)?;
    info!("connected!");

    // Immediately increment the gauge, then create a job to decrement it on thread exit.
    // One of the pros of `defer!` is that this will *most probably*
    // get called, even in presence of panics.
    let gauge = crate::LIVE_CONNECTIONS_COUNT.with_label_values(&["wal_receiver"]);
    gauge.inc();
    scopeguard::defer! {
        gauge.dec();
    }

    let identify = identify_system(&mut rclient)?;
    info!("{:?}", identify);
    let end_of_wal = Lsn::from(u64::from(identify.xlogpos));
    let mut caught_up = false;

    let timeline = tenant_mgr::get_timeline_for_tenant(tenantid, timelineid)?;

    //
    // Start streaming the WAL, from where we left off previously.
    //
    // If we had previously received WAL up to some point in the middle of a WAL record, we
    // better start from the end of last full WAL record, not in the middle of one.
    let mut last_rec_lsn = timeline.get_last_record_lsn();
    let mut startpoint = last_rec_lsn;

    if startpoint == Lsn(0) {
        error!("No previous WAL position");
    }

    // There might be some padding after the last full record, skip it.
    startpoint += startpoint.calc_padding(8u32);

    info!(
        "last_record_lsn {} starting replication from {} for timeline {}, server is at {}...",
        last_rec_lsn, startpoint, timelineid, end_of_wal
    );

    let query = format!("START_REPLICATION PHYSICAL {}", startpoint);

    let copy_stream = rclient.copy_both_simple(&query)?;
    let mut physical_stream = ReplicationIter::new(copy_stream);

    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let checkpoint_bytes = timeline.get_page_at_lsn(RelishTag::Checkpoint, 0, startpoint)?;
    let mut checkpoint = CheckPoint::decode(&checkpoint_bytes)?;
    trace!("CheckPoint.nextXid = {}", checkpoint.nextXid.value);

    while let Some(replication_message) = physical_stream.next()? {
        let status_update = match replication_message {
            ReplicationMessage::XLogData(xlog_data) => {
                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                let data = xlog_data.data();
                let startlsn = Lsn::from(xlog_data.wal_start());
                let endlsn = startlsn + data.len() as u64;
                let prev_last_rec_lsn = last_rec_lsn;

                trace!("received XLogData between {} and {}", startlsn, endlsn);

                waldecoder.feed_bytes(data);

                while let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                    // Save old checkpoint value to compare with it after decoding WAL record
                    let old_checkpoint_bytes = checkpoint.encode();
                    let decoded = decode_wal_record(recdata.clone());

                    // It is important to deal with the aligned records as lsn in getPage@LSN is
                    // aligned and can be several bytes bigger. Without this alignment we are
                    // at risk of hittind a deadlock.
                    assert!(lsn.is_aligned());

                    restore_local_repo::save_decoded_record(
                        &mut checkpoint,
                        &*timeline,
                        &decoded,
                        recdata,
                        lsn,
                    )?;

                    let new_checkpoint_bytes = checkpoint.encode();
                    // Check if checkpoint data was updated by save_decoded_record
                    if new_checkpoint_bytes != old_checkpoint_bytes {
                        timeline.put_page_image(
                            RelishTag::Checkpoint,
                            0,
                            lsn,
                            new_checkpoint_bytes,
                        )?;
                    }

                    // Now that this record has been fully handled, including updating the
                    // checkpoint data, let the repository know that it is up-to-date to this LSN
                    timeline.advance_last_record_lsn(lsn);
                    last_rec_lsn = lsn;
                }

                // Somewhat arbitrarily, if we have at least 10 complete wal segments (16 MB each),
                // "checkpoint" the repository to flush all the changes from WAL we've processed
                // so far to disk. After this, we don't need the original WAL anymore, and it
                // can be removed. This is probably too aggressive for production, but it's useful
                // to expose bugs now.
                //
                // TODO: We don't actually dare to remove the WAL. It's useful for debugging,
                // and we might it for logical decoding other things in the future. Although
                // we should also be able to fetch it back from the WAL safekeepers or S3 if
                // needed.
                if prev_last_rec_lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE)
                    != last_rec_lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE)
                {
                    info!("switched segment {} to {}", prev_last_rec_lsn, last_rec_lsn);
                    let (oldest_segno, newest_segno) = find_wal_file_range(
                        conf,
                        &timelineid,
                        pg_constants::WAL_SEGMENT_SIZE,
                        last_rec_lsn,
                        &tenantid,
                    )?;

                    if newest_segno - oldest_segno >= 10 {
                        // TODO: This is where we could remove WAL older than last_rec_lsn.
                        //remove_wal_files(timelineid, pg_constants::WAL_SEGMENT_SIZE, last_rec_lsn)?;
                    }
                }

                if !caught_up && endlsn >= end_of_wal {
                    info!("caught up at LSN {}", endlsn);
                    caught_up = true;
                }

                Some(endlsn)
            }

            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = keepalive.wal_end();
                let timestamp = keepalive.timestamp();
                let reply_requested = keepalive.reply() != 0;

                trace!(
                    "received PrimaryKeepAlive(wal_end: {}, timestamp: {:?} reply: {})",
                    wal_end,
                    timestamp,
                    reply_requested,
                );

                if reply_requested {
                    Some(timeline.get_last_record_lsn())
                } else {
                    None
                }
            }

            _ => None,
        };

        if let Some(last_lsn) = status_update {
            // TODO: More thought should go into what values are sent here.
            let last_lsn = PgLsn::from(u64::from(last_lsn));
            let write_lsn = PgLsn::from(u64::from(timeline.get_disk_consistent_lsn()));
            let flush_lsn = last_lsn;
            let apply_lsn = PgLsn::from(0);
            let ts = SystemTime::now();
            const NO_REPLY: u8 = 0;
            physical_stream.standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, NO_REPLY)?;
        }
    }
    Ok(())
}

fn find_wal_file_range(
    conf: &PageServerConf,
    timeline: &ZTimelineId,
    wal_seg_size: usize,
    written_upto: Lsn,
    tenant: &ZTenantId,
) -> Result<(u64, u64)> {
    let written_upto_segno = written_upto.segment_number(wal_seg_size);

    let mut oldest_segno = written_upto_segno;
    let mut newest_segno = written_upto_segno;
    // Scan the wal directory, and count how many WAL filed we could remove
    let wal_dir = conf.wal_dir_path(timeline, tenant);
    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            continue;
        }

        let filename = path.file_name().unwrap().to_str().unwrap();

        if IsXLogFileName(filename) {
            let (segno, _tli) = XLogFromFileName(filename, wal_seg_size);

            if segno > written_upto_segno {
                // that's strange.
                warn!("there is a WAL file from future at {}", path.display());
                continue;
            }

            oldest_segno = min(oldest_segno, segno);
            newest_segno = max(newest_segno, segno);
        }
    }
    // FIXME: would be good to assert that there are no gaps in the WAL files

    Ok((oldest_segno, newest_segno))
}

/// Data returned from the postgres `IDENTIFY_SYSTEM` command
///
/// See the [postgres docs] for more details.
///
/// [postgres docs]: https://www.postgresql.org/docs/current/protocol-replication.html
#[derive(Debug)]
pub struct IdentifySystem {
    systemid: u64,
    timeline: u32,
    xlogpos: PgLsn,
    dbname: Option<String>,
}

/// There was a problem parsing the response to
/// a postgres IDENTIFY_SYSTEM command.
#[derive(Debug, thiserror::Error)]
#[error("IDENTIFY_SYSTEM parse error")]
pub struct IdentifyError;

/// Run the postgres `IDENTIFY_SYSTEM` command
pub fn identify_system(client: &mut Client) -> Result<IdentifySystem, Error> {
    let query_str = "IDENTIFY_SYSTEM";
    let response = client.simple_query(query_str)?;

    // get(N) from row, then parse it as some destination type.
    fn get_parse<T>(row: &SimpleQueryRow, idx: usize) -> Result<T, IdentifyError>
    where
        T: FromStr,
    {
        let val = row.get(idx).ok_or(IdentifyError)?;
        val.parse::<T>().or(Err(IdentifyError))
    }

    // extract the row contents into an IdentifySystem struct.
    // written as a closure so I can use ? for Option here.
    if let Some(SimpleQueryMessage::Row(first_row)) = response.get(0) {
        Ok(IdentifySystem {
            systemid: get_parse(first_row, 0)?,
            timeline: get_parse(first_row, 1)?,
            xlogpos: get_parse(first_row, 2)?,
            dbname: get_parse(first_row, 3).ok(),
        })
    } else {
        Err(IdentifyError.into())
    }
}
