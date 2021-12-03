//! This module implements the streaming side of replication protocol, starting
//! with the "START_REPLICATION" message.

use crate::send_wal::SendWalHandler;
use crate::timeline::{ReplicaState, Timeline, TimelineTools};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use log::*;
use postgres_ffi::xlog_utils::{
    get_current_timestamp, TimestampTz, XLogFileName, MAX_SEND_SIZE, PG_TLI,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::net::Shutdown;
use std::path::Path;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::{str, thread};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage, WalSndKeepAlive, XLogDataBody};
use zenith_utils::sock_split::ReadStream;

pub const END_REPLICATION_MARKER: Lsn = Lsn::MAX;

// See: https://www.postgresql.org/docs/13/protocol-replication.html
const HOT_STANDBY_FEEDBACK_TAG_BYTE: u8 = b'h';
const STANDBY_STATUS_UPDATE_TAG_BYTE: u8 = b'r';

type FullTransactionId = u64;

/// Hot standby feedback received from replica
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
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

/// Standby status update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StandbyReply {
    pub write_lsn: Lsn, // not used
    pub flush_lsn: Lsn, // not used
    pub apply_lsn: Lsn, // pageserver's disk consistent lSN
    pub reply_ts: TimestampTz,
    pub reply_requested: bool,
}

/// A network connection that's speaking the replication protocol.
pub struct ReplicationConn {
    /// This is an `Option` because we will spawn a background thread that will
    /// `take` it from us.
    stream_in: Option<ReadStream>,
}

/// Scope guard to unregister replication connection from timeline
struct ReplicationConnGuard {
    replica: usize, // replica internal ID assigned by timeline
    timeline: Arc<Timeline>,
}

impl Drop for ReplicationConnGuard {
    fn drop(&mut self) {
        self.timeline.update_replica_state(self.replica, None);
    }
}

impl ReplicationConn {
    /// Create a new `ReplicationConn`
    pub fn new(pgb: &mut PostgresBackend) -> Self {
        Self {
            stream_in: pgb.take_stream_in(),
        }
    }

    /// Handle incoming messages from the network.
    /// This is spawned into the background by `handle_start_replication`.
    fn background_thread(mut stream_in: ReadStream, timeline: Arc<Timeline>) -> Result<()> {
        let mut state = ReplicaState::new();
        let replica = timeline.add_replica(state);
        let _guard = ReplicationConnGuard {
            replica,
            timeline: timeline.clone(),
        };
        // Wait for replica's feedback.
        while let Some(msg) = FeMessage::read(&mut stream_in)? {
            match &msg {
                FeMessage::CopyData(m) => {
                    // There's two possible data messages that the client is supposed to send here:
                    // `HotStandbyFeedback` and `StandbyStatusUpdate`.

                    match m.first().cloned() {
                        Some(HOT_STANDBY_FEEDBACK_TAG_BYTE) => {
                            // Note: deserializing is on m[1..] because we skip the tag byte.
                            state.hs_feedback = HotStandbyFeedback::des(&m[1..])
                                .context("failed to deserialize HotStandbyFeedback")?;
                            timeline.update_replica_state(replica, Some(state));
                        }
                        Some(STANDBY_STATUS_UPDATE_TAG_BYTE) => {
                            let reply = StandbyReply::des(&m[1..])
                                .context("failed to deserialize StandbyReply")?;
                            state.disk_consistent_lsn = reply.apply_lsn;
                            timeline.update_replica_state(replica, Some(state));
                        }
                        _ => warn!("unexpected message {:?}", msg),
                    }
                }
                FeMessage::Sync => {}
                FeMessage::CopyFail => {
                    // Shutdown the connection, because rust-postgres client cannot be dropped
                    // when connection is alive.
                    let _ = stream_in.shutdown(Shutdown::Both);
                    return Err(anyhow!("Copy failed"));
                }
                _ => {
                    // We only handle `CopyData`, 'Sync', 'CopyFail' messages. Anything else is ignored.
                    info!("unexpected message {:?}", msg);
                }
            }
        }

        Ok(())
    }

    /// Helper function that parses a single LSN.
    fn parse_start(cmd: &[u8]) -> Result<Lsn> {
        let re = Regex::new(r"([[:xdigit:]]+/[[:xdigit:]]+)").unwrap();
        let caps = re.captures_iter(str::from_utf8(cmd)?);
        let mut lsns = caps.map(|cap| cap[1].parse::<Lsn>());
        let start_pos = lsns
            .next()
            .ok_or_else(|| anyhow!("Failed to parse start LSN from command"))??;
        assert!(lsns.next().is_none());
        Ok(start_pos)
    }

    /// Helper function for opening a wal file.
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

    ///
    /// Handle START_REPLICATION replication command
    ///
    pub fn run(
        &mut self,
        swh: &mut SendWalHandler,
        pgb: &mut PostgresBackend,
        cmd: &Bytes,
    ) -> Result<()> {
        // spawn the background thread which receives HotStandbyFeedback messages.
        let bg_timeline = Arc::clone(swh.timeline.get());
        let bg_stream_in = self.stream_in.take().unwrap();

        // TODO: here we got two threads, one for writing WAL and one for receiving
        // feedback. If one of them fails, we should shutdown the other one too.
        let _ = thread::Builder::new()
            .name("HotStandbyFeedback thread".into())
            .spawn(move || {
                if let Err(err) = Self::background_thread(bg_stream_in, bg_timeline) {
                    error!("Replication background thread failed: {}", err);
                }
            })
            .unwrap();

        let mut start_pos = Self::parse_start(cmd)?;

        let mut wal_seg_size: usize;
        loop {
            wal_seg_size = swh.timeline.get().get_info().server.wal_seg_size as usize;
            if wal_seg_size == 0 {
                error!("Cannot start replication before connecting to wal_proposer");
                sleep(Duration::from_secs(1));
            } else {
                break;
            }
        }
        let wal_end = swh.timeline.get().get_end_of_wal();
        // Walproposer gets special handling: safekeeper must give proposer all
        // local WAL till the end, whether committed or not (walproposer will
        // hang otherwise). That's because walproposer runs the consensus and
        // synchronizes safekeepers on the most advanced one.
        //
        // There is a small risk of this WAL getting concurrently garbaged if
        // another compute rises which collects majority and starts fixing log
        // on this safekeeper itself. That's ok as (old) proposer will never be
        // able to commit such WAL.
        let stop_pos: Option<Lsn> = if swh.appname == Some("wal_proposer_recovery".to_string()) {
            Some(wal_end)
        } else {
            None
        };
        info!("Start replication from {:?} till {:?}", start_pos, stop_pos);

        // switch to copy
        pgb.write_message(&BeMessage::CopyBothResponse)?;

        let mut end_pos = Lsn(0);
        let mut wal_file: Option<File> = None;

        loop {
            if let Some(stop_pos) = stop_pos {
                if start_pos >= stop_pos {
                    break; /* recovery finished */
                }
                end_pos = stop_pos;
            } else {
                /* Wait until we have some data to stream */
                if let Some(lsn) = swh.timeline.get().wait_for_lsn(start_pos) {
                    end_pos = lsn
                } else {
                    // timeout expired: request pageserver status
                    pgb.write_message(&BeMessage::KeepAlive(WalSndKeepAlive {
                        sent_ptr: end_pos.0,
                        timestamp: get_current_timestamp(),
                        request_reply: true,
                    }))
                    .context("Failed to send KeepAlive message")?;
                    continue;
                }
            }
            if end_pos == END_REPLICATION_MARKER {
                break;
            }

            // Take the `File` from `wal_file`, or open a new file.
            let mut file = match wal_file.take() {
                Some(file) => file,
                None => {
                    // Open a new file.
                    let segno = start_pos.segment_number(wal_seg_size);
                    let wal_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
                    let timeline_id = swh.timeline.get().timelineid;
                    let wal_file_path = swh.conf.timeline_dir(&timeline_id).join(wal_file_name);
                    Self::open_wal_file(&wal_file_path)?
                }
            };

            let xlogoff = start_pos.segment_offset(wal_seg_size) as usize;

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

            // Write some data to the network socket.
            pgb.write_message(&BeMessage::XLogData(XLogDataBody {
                wal_start: start_pos.0,
                wal_end: end_pos.0,
                timestamp: get_current_timestamp(),
                data: &file_buf,
            }))
            .context("Failed to send XLogData")?;

            start_pos += send_size as u64;

            debug!("sent WAL up to {}", start_pos);

            // Decide whether to reuse this file. If we don't set wal_file here
            // a new file will be opened next time.
            if start_pos.segment_offset(wal_seg_size) != 0 {
                wal_file = Some(file);
            }
        }
        Ok(())
    }
}
