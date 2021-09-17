//! This module implements the streaming side of replication protocol, starting
//! with the "START_REPLICATION" message.

use crate::send_wal::SendWalHandler;
use crate::timeline::{Timeline, TimelineTools};
use crate::WalAcceptorConf;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use log::*;
use postgres_ffi::xlog_utils::{
    get_current_timestamp, TimeLineID, TimestampTz, XLogFileName, MAX_SEND_SIZE,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::{str, thread};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage, XLogDataBody};
use zenith_utils::sock_split::ReadStream;

pub const END_REPLICATION_MARKER: Lsn = Lsn::MAX;

// See: https://www.postgresql.org/docs/13/protocol-replication.html
const HOT_STANDBY_FEEDBACK_TAG_BYTE: u8 = b'h';
const STANDBY_STATUS_UPDATE_TAG_BYTE: u8 = b'r';

type FullTransactionId = u64;

/// Hot standby feedback received from replica
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HotStandbyFeedback {
    pub ts: TimestampTz,
    pub xmin: FullTransactionId,
    pub catalog_xmin: FullTransactionId,
}

impl HotStandbyFeedback {
    pub fn empty() -> HotStandbyFeedback {
        HotStandbyFeedback {
            ts: 0,
            xmin: 0,
            catalog_xmin: 0,
        }
    }
}

/// A network connection that's speaking the typical postgres replication protocol.
pub struct StandardReplicationConn<'pg> {
    /// Postgres connection
    pgb: &'pg mut PostgresBackend,
}

struct StandardReplicationReciever(ReadStream);

/// Required functionality to implement the sending half of the replication protocol.
///
/// Implemented for [`StandardReplication`] and [`PushReplication`]
trait ReplicationSender {
    /// Sends XLogData over the connection
    fn send_xlogdata(&mut self, data: XLogDataBody<&[u8]>) -> Result<()>;
}

/// Required functionality to implement the receiving half of the replication protcol
trait ReplicationReceiver {
    /// Waits for the contents of the next CopyData message, or `None` if the connection
    /// has closed
    fn receive_copydata(&mut self) -> Result<Option<Bytes>>;
}

impl<'pg> ReplicationSender for StandardReplicationConn<'pg> {
    fn send_xlogdata(&mut self, data: XLogDataBody<&[u8]>) -> Result<()> {
        self.pgb.write_message(&BeMessage::XLogData(data))?;
        Ok(())
    }
}

impl ReplicationReceiver for StandardReplicationReciever {
    fn receive_copydata(&mut self) -> Result<Option<Bytes>> {
        // Repeatedly receive until we get a message we can do something with
        loop {
            let msg = FeMessage::read(&mut self.0)?
                .ok_or_else(|| anyhow!("connection ended without CopyDone"))?;

            match msg {
                FeMessage::CopyData(bytes) => return Ok(Some(bytes)),
                FeMessage::CopyDone => return Ok(None),
                FeMessage::CopyFail => return Err(anyhow!("Copy failed")),
                FeMessage::Sync => (),
                _ => {
                    // We only handle the above messages. Anything else is ignored.
                    info!("unexpected message {:?}", msg);
                }
            };
        }
    }
}

// TODO: move this to crate::timeline when there's more users
// TODO: design a proper Timeline mock api
trait HsFeedbackSubscriber {
    fn add_hs_feedback(&self, feedback: HotStandbyFeedback);
}

// Blanket implementation so that by-reference and by-value works for any function
// expecting `T: HsFeedbackSubscriber`
impl<'a, S: HsFeedbackSubscriber> HsFeedbackSubscriber for &'a S {
    fn add_hs_feedback(&self, feedback: HotStandbyFeedback) {
        (*self).add_hs_feedback(feedback)
    }
}

impl HsFeedbackSubscriber for Arc<Timeline> {
    #[inline(always)]
    fn add_hs_feedback(&self, feedback: HotStandbyFeedback) {
        Timeline::add_hs_feedback(self, feedback);
    }
}

/// Runs the replication data-sending loop over the provided connection
fn do_send_loop(
    conn: &mut impl ReplicationSender,
    conf: &WalAcceptorConf,
    mut start_pos: Lsn,
    stop_pos: Lsn,
    tli: TimeLineID,
    timeline: &Timeline,
    wal_seg_size: usize,
) -> Result<()> {
    let mut end_pos: Lsn;
    let mut wal_file: Option<File> = None;

    loop {
        /* Wait until we have some data to stream */
        if stop_pos != Lsn(0) {
            /* recovery mode: stream up to the specified LSN (VCL) */
            if start_pos >= stop_pos {
                /* recovery finished */
                break;
            }
            end_pos = stop_pos;
        } else {
            /* normal mode */
            end_pos = timeline.wait_for_lsn(start_pos);
        }
        if end_pos == END_REPLICATION_MARKER {
            break;
        }

        // Take the `File` from `wal_file`, or open a new file.
        let mut file = match wal_file.take() {
            Some(file) => file,
            None => {
                // Open a new file.
                let segno = start_pos.segment_number(wal_seg_size as usize);
                let wal_file_name = XLogFileName(tli, segno, wal_seg_size);
                let timeline_id = timeline.timelineid.to_string();
                let wal_file_path = conf.data_dir.join(timeline_id).join(wal_file_name);
                open_wal_file(&wal_file_path)?
            }
        };

        let xlogoff = start_pos.segment_offset(wal_seg_size as usize);

        // How much to read and send in message? We cannot cross the WAL file
        // boundary, and we don't want send more than MAX_SEND_SIZE.
        let send_size = end_pos.checked_sub(start_pos).unwrap().0 as usize;
        let send_size = min(send_size, wal_seg_size - xlogoff);
        let send_size = min(send_size, MAX_SEND_SIZE);

        // Read some data from the file.
        let mut file_buf = vec![0u8; send_size];
        file.seek(SeekFrom::Start(xlogoff as u64))
            .and_then(|_| file.read_exact(&mut file_buf))
            .context("Failed to read data from WAL file")?;

        conn.send_xlogdata(XLogDataBody {
            wal_start: start_pos.0,
            wal_end: end_pos.0,
            timestamp: get_current_timestamp(),
            data: &file_buf,
        })
        .context("Failed to send XLogData")?;

        start_pos += send_size as u64;

        debug!("sent WAL up to {}", end_pos);

        // Decide whether to reuse this file. If we don't set wal_file here
        // a new file will be opened next time.
        if start_pos.segment_offset(wal_seg_size) != 0 {
            wal_file = Some(file);
        }
    }

    Ok(())
}

fn do_receive_loop(
    conn: &mut impl ReplicationReceiver,
    subscriber: impl HsFeedbackSubscriber,
) -> Result<()> {
    while let Some(msg) = conn.receive_copydata()? {
        // There's two possible data messages that the client is supposed to send here,
        // inside of the CopyData: `HotStandbyFeedback` and `StandbyStatusUpdate`. We only
        // handle hot standby feedback.

        match msg.first().cloned() {
            Some(HOT_STANDBY_FEEDBACK_TAG_BYTE) => {
                // Note: deserializing is on m[1..] because we skip the tag byte.
                let feedback = HotStandbyFeedback::des(&msg[1..])
                    .context("failed to deserialize HotStandbyFeedback")?;
                subscriber.add_hs_feedback(feedback);
            }
            Some(STANDBY_STATUS_UPDATE_TAG_BYTE) => (),
            _ => warn!("unexpected message {:?}", msg),
        }
    }

    Ok(())
}

fn open_wal_file(wal_file_path: &Path) -> Result<File> {
    // First try to open the .partial file.
    let mut partial_path = wal_file_path.to_owned();
    partial_path.set_extension("partial");
    if let Ok(opened_file) = File::open(&partial_path) {
        return Ok(opened_file);
    }

    // If that failed, try it without the .partial extension.
    File::open(&wal_file_path)
        .with_context(|| format!("Failed to open WAL file {:?}", wal_file_path))
        .map_err(|e| {
            error!("{}", e);
            e
        })
}

impl<'pg> StandardReplicationConn<'pg> {
    /// Create a new `ReplicationConn`
    pub fn new(pgb: &'pg mut PostgresBackend) -> Self {
        StandardReplicationConn { pgb }
    }

    /// Helper function that parses a pair of LSNs.
    fn parse_start_stop(cmd: &[u8]) -> Result<(Lsn, Lsn)> {
        let re = Regex::new(r"([[:xdigit:]]+/[[:xdigit:]]+)").unwrap();
        let caps = re.captures_iter(str::from_utf8(cmd)?);
        let mut lsns = caps.map(|cap| cap[1].parse::<Lsn>());
        let start_pos = lsns
            .next()
            .ok_or_else(|| anyhow!("Failed to parse start LSN from command"))??;
        let stop_pos = lsns.next().transpose()?.unwrap_or(Lsn(0));
        Ok((start_pos, stop_pos))
    }

    ///
    /// Handle START_REPLICATION replication command
    ///
    pub fn run(&mut self, swh: &mut SendWalHandler, cmd: &Bytes) -> Result<()> {
        let timeline = swh.timeline.get();

        // spawn the background thread which receives HotStandbyFeedback messages.
        let bg_timeline = Arc::clone(timeline);
        let bg_stream_in = self.pgb.take_stream_in().unwrap();

        thread::spawn(|| {
            let mut recv = StandardReplicationReciever(bg_stream_in);

            if let Err(err) = do_receive_loop(&mut recv, bg_timeline) {
                error!("Replication background thread failed: {}", err);
            }
        });

        let (mut start_pos, mut stop_pos) = Self::parse_start_stop(cmd)?;

        let mut wal_seg_size: u32;
        loop {
            wal_seg_size = timeline.get_info().server.wal_seg_size;
            if wal_seg_size == 0 {
                error!("Cannot start replication before connecting to wal_proposer");
                sleep(Duration::from_secs(1));
            } else {
                break;
            }
        }
        let (wal_end, tli) = timeline.get_end_of_wal();
        if start_pos == Lsn(0) {
            start_pos = wal_end;
        }
        if stop_pos == Lsn(0) && swh.appname == Some("wal_proposer_recovery".to_string()) {
            stop_pos = wal_end;
        }
        info!("Start replication from {} till {}", start_pos, stop_pos);

        // switch to copy
        self.pgb.write_message(&BeMessage::CopyBothResponse)?;

        do_send_loop(
            self,
            &swh.conf,
            start_pos,
            stop_pos,
            tli,
            &timeline,
            wal_seg_size as usize,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // No-op impls for tests
    struct NoOpSubscriber;
    struct EmptyStream;

    impl HsFeedbackSubscriber for NoOpSubscriber {
        fn add_hs_feedback(&self, _feedback: HotStandbyFeedback) {}
    }

    impl ReplicationReceiver for EmptyStream {
        fn receive_copydata(&mut self) -> Result<Option<Bytes>> {
            Ok(None)
        }
    }

    #[test]
    fn test_replication_conn_background_thread_eof() {
        // Test that background_thread recognizes EOF
        do_receive_loop(&mut EmptyStream, NoOpSubscriber).unwrap();
    }
}
