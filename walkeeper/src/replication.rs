//! This module implements the streaming side of replication protocol, starting
//! with the "START_REPLICATION" message.

use crate::send_wal::SendWalHandler;
use crate::timeline::{Timeline, TimelineTools};
use crate::WalAcceptorConf;
use anyhow::{anyhow, Context, Result};
use bincode::Options;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use lazy_static::lazy_static;
use log::*;
use postgres_ffi::xlog_utils::{
    get_current_timestamp, TimeLineID, TimestampTz, XLogFileName, MAX_SEND_SIZE,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use std::{str, thread};
use tokio::runtime;
use tokio_postgres::{CopyBothSink, CopyBothStream, NoTls};
use zenith_utils::bin_ser::{be_coder, BeSer};
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage, XLogDataBody};
use zenith_utils::sock_split::ReadStream;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

pub const END_REPLICATION_MARKER: Lsn = Lsn::MAX;

// See: https://www.postgresql.org/docs/13/protocol-replication.html
const HOT_STANDBY_FEEDBACK_TAG_BYTE: u8 = b'h';
const STANDBY_STATUS_UPDATE_TAG_BYTE: u8 = b'r';
const XLOG_DATA_TAG_BYTE: u8 = b'w';
// Custom tag for push replication protocol. See: pageserver/src/walreceiver.rs.rs
const START_LSN_TAG_BYTE: u8 = b'i';

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

/// Network connection running the postgres replication protocol in the opposite direction to
/// usual
pub struct PushReplicationConn {
    // Cached runtime to execute on
    rt: tokio::runtime::Runtime,
    send_sink: CopyBothSink<Bytes>,
}

struct PushReplicationReceiver {
    // Cached runtime to execute on
    rt: tokio::runtime::Runtime,
    recv_stream: CopyBothStream,
}

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

impl ReplicationSender for PushReplicationConn {
    fn send_xlogdata(&mut self, data: XLogDataBody<&[u8]>) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u8(XLOG_DATA_TAG_BYTE);

        let mut w = buf.writer();
        // Serialization is taken
        be_coder()
            .serialize_into(&mut w, &data)
            .context("failed to serialize XLogData")
            .unwrap();

        self.rt
            .block_on(self.send_sink.send(w.into_inner().freeze()))?;
        Ok(())
    }
}

impl ReplicationReceiver for PushReplicationReceiver {
    fn receive_copydata(&mut self) -> Result<Option<Bytes>> {
        self.rt
            .block_on(self.recv_stream.next())
            // Go from Option<Result<_>> -> Result<Option<_>>
            .transpose()
            .map_err(|e| e.into())
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

// Helper function for deserializing a value with a particular leading tag byte
fn deserialize_tagged<T: BeSer>(bytes: &[u8], expected_tag: u8) -> Result<T> {
    let tag = bytes.get(0);
    if tag != Some(&expected_tag) {
        return Err(anyhow!("incorrect tag byte {:?}", tag));
    }

    T::des(&bytes[1..]).map_err(|e| e.into())
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

lazy_static! {
    static ref ACTIVE_PUSH_CONNS: Mutex<HashSet<ZTimelineId>> = Mutex::new(HashSet::new());
}

impl PushReplicationConn {
    /// Starts a push connection to the pageserver in another thread. Returns `Err(())` if
    /// we already have one running to the pageserver with the given timeline
    ///
    /// It's expected that `conf.pageserver_addr = Some(ps_addr)`, but not checked.
    // ^ This avoids "unnecessary" unwraps.
    //
    // TODO: Do we want to return an error of some kind if the connection immediately
    // fails? Would require some reworking, but easily possible to implement.
    pub(crate) fn start(
        ps_addr: String,
        conf: WalAcceptorConf,
        timeline: Arc<Timeline>,
        tenantid: ZTenantId,
        wal_seg_size: usize,
    ) -> Result<()> {
        debug_assert_eq!(Some(&ps_addr), conf.pageserver_addr.as_ref());

        // If we already have an active connection to the pageserver on that timeline,
        // don't create a new one.
        if ACTIVE_PUSH_CONNS
            .lock()
            .unwrap()
            .insert(timeline.timelineid)
        {
            info!(
                "already connected to page server at {} on timeline {} for tenant {}",
                ps_addr, timeline.timelineid, tenantid,
            );
            return Ok(());
        }

        info!(
            "connecting to page server at {} on timeline {} for tenant {}",
            ps_addr, timeline.timelineid, tenantid,
        );

        Self::start_new_connection(ps_addr, conf, timeline, tenantid, wal_seg_size)
    }

    /// Starts a fresh connection to the page server, without checking or updating
    /// `ACTIVE_PUSH_CONNS`
    ///
    /// We get the connection off the ground before returning, just to make sure that
    /// everything is ok and we're starting somewhere reasonable.
    fn start_new_connection(
        ps_addr: String,
        conf: WalAcceptorConf,
        timeline: Arc<Timeline>,
        tenantid: ZTenantId,
        wal_seg_size: usize,
    ) -> Result<()> {
        let rt = runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("failed to build tokio current-thread runtime");

        // Connect to the page server:
        let ps_connstr = format!(
            "postgresql://no_user:{}@{}/no_db",
            conf.pageserver_auth_token
                .as_ref()
                .unwrap_or(&String::new()),
            ps_addr,
        );

        let (client, _connection) = rt
            .block_on(tokio_postgres::connect(&ps_connstr, NoTls))
            .with_context(|| format!("Failed to connect to pageserver {}", ps_connstr))?;

        let (end_of_wal, tli) = timeline.get_end_of_wal();

        let query_str = format!(
            "START_PUSH_REPLICATION {} {} {}",
            tenantid, timeline.timelineid, end_of_wal,
        );

        let (send_sink, mut recv_stream) = rt
            .block_on(client.copy_both_simple::<Bytes>(&query_str))?
            .split();

        let stop_pos = Lsn(0);
        let start_pos = {
            let msg: Bytes = rt
                .block_on(recv_stream.next())
                .ok_or_else(|| anyhow!("failed to get replication start position"))?
                .context("failed to get replication start position")?;

            deserialize_tagged::<Lsn>(&msg, START_LSN_TAG_BYTE)
                .context("failed to deserialize start LSN")?
        };

        // Once everything's started up correctly, properly run the connection:
        let mut sender = PushReplicationConn { rt, send_sink };
        let mut receiver = PushReplicationReceiver {
            rt: runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .expect("failed to build tokio current-thread runtime"),
            recv_stream,
        };

        let bg_timeline = timeline.clone();
        thread::spawn(move || do_receive_loop(&mut receiver, bg_timeline));

        thread::spawn(move || {
            do_send_loop(
                &mut sender,
                &conf,
                start_pos,
                stop_pos,
                tli,
                &timeline,
                wal_seg_size,
            )
        });

        Ok(())
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
