//! This module contains timeline id -> safekeeper state map with file-backed
//! persistence and support for interaction between sending and receiving wal.

use anyhow::{bail, Result};
use fs2::FileExt;
use lazy_static::lazy_static;
use log::*;
use postgres_ffi::xlog_utils::find_end_of_wal;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use zenith_utils::bin_ser::LeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use crate::replication::{HotStandbyFeedback, END_REPLICATION_MARKER};
use crate::safekeeper::{
    AcceptorProposerMessage, ProposerAcceptorMessage, SafeKeeper, SafeKeeperState, Storage,
    SK_FORMAT_VERSION, SK_MAGIC,
};
use crate::WalAcceptorConf;
use postgres_ffi::xlog_utils::{XLogFileName, XLOG_BLCKSZ};

const CONTROL_FILE_NAME: &str = "safekeeper.control";
const POLL_STATE_TIMEOUT: Duration = Duration::from_secs(1);

/// Replica status: host standby feedback + disk consistent lsn
#[derive(Debug, Clone, Copy)]
pub struct ReplicaState {
    /// combined disk_consistent_lsn of pageservers
    pub disk_consistent_lsn: Lsn,
    /// combined hot standby feedback from all replicas
    pub hs_feedback: HotStandbyFeedback,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaState {
    pub fn new() -> ReplicaState {
        ReplicaState {
            disk_consistent_lsn: Lsn(u64::MAX),
            hs_feedback: HotStandbyFeedback {
                ts: 0,
                xmin: u64::MAX,
                catalog_xmin: u64::MAX,
            },
        }
    }
}

/// Shared state associated with database instance (tenant)
struct SharedState {
    /// Safekeeper object
    sk: SafeKeeper<FileStorage>,
    /// For receiving-sending wal cooperation
    /// quorum commit LSN we've notified walsenders about
    commit_lsn: Lsn,
    /// State of replicas
    replicas: Vec<Option<ReplicaState>>,
}

// A named boolean.
#[derive(Debug)]
pub enum CreateControlFile {
    True,
    False,
}

impl SharedState {
    /// Get combined stateof all alive replicas
    pub fn get_replicas_state(&self) -> ReplicaState {
        let mut acc = ReplicaState::new();
        for state in self.replicas.iter().flatten() {
            acc.hs_feedback.ts = max(acc.hs_feedback.ts, state.hs_feedback.ts);
            acc.hs_feedback.xmin = min(acc.hs_feedback.xmin, state.hs_feedback.xmin);
            acc.hs_feedback.catalog_xmin =
                min(acc.hs_feedback.catalog_xmin, state.hs_feedback.catalog_xmin);
            acc.disk_consistent_lsn = Lsn::min(acc.disk_consistent_lsn, state.disk_consistent_lsn);
        }
        acc
    }

    /// Assign new replica ID. We choose first empty cell in the replicas vector
    /// or extend the vector if there are not free items.
    pub fn add_replica(&mut self) -> usize {
        let len = self.replicas.len();
        for i in 0..len {
            if self.replicas[i].is_none() {
                return i;
            }
        }
        self.replicas.push(None);
        len
    }

    /// Restore SharedState from control file. Locks the control file along the
    /// way to prevent running more than one instance of safekeeper on the same
    /// data dir.
    /// If create=false and file doesn't exist, bails out.
    fn create_restore(
        conf: &WalAcceptorConf,
        timelineid: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<Self> {
        let (cf, state) = SharedState::load_control_file(conf, timelineid, create)?;
        let storage = FileStorage {
            control_file: cf,
            conf: conf.clone(),
        };
        let (flush_lsn, tli) = if state.server.wal_seg_size != 0 {
            let wal_dir = conf.data_dir.join(format!("{}", timelineid));
            find_end_of_wal(
                &wal_dir,
                state.server.wal_seg_size as usize,
                true,
                state.wal_start_lsn,
            )?
        } else {
            (0, 0)
        };

        Ok(Self {
            commit_lsn: Lsn(0),
            sk: SafeKeeper::new(Lsn(flush_lsn), tli, storage, state),
            replicas: Vec::new(),
        })
    }

    /// Fetch and lock control file (prevent running more than one instance of safekeeper)
    /// If create=false and file doesn't exist, bails out.
    fn load_control_file(
        conf: &WalAcceptorConf,
        timelineid: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<(File, SafeKeeperState)> {
        let control_file_path = conf
            .data_dir
            .join(timelineid.to_string())
            .join(CONTROL_FILE_NAME);
        info!(
            "loading control file {}, create={:?}",
            control_file_path.display(),
            create
        );
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);
        if let CreateControlFile::True = create {
            opts.create(true);
        }
        match opts.open(&control_file_path) {
            Ok(mut file) => {
                // Lock file to prevent two or more active wal_acceptors
                match file.try_lock_exclusive() {
                    Ok(()) => {}
                    Err(e) => {
                        bail!(
                            "control file {:?} is locked by some other process: {}",
                            &control_file_path,
                            e
                        );
                    }
                }
                // Empty file is legit on 'create', don't try to deser from it.
                if file.metadata().unwrap().len() == 0 {
                    if let CreateControlFile::False = create {
                        bail!("control file is empty");
                    }
                    Ok((file, SafeKeeperState::new()))
                } else {
                    match SafeKeeperState::des_from(&mut file) {
                        Err(e) => {
                            bail!("failed to read control file {:?}: {}", control_file_path, e);
                        }
                        Ok(s) => {
                            if s.magic != SK_MAGIC {
                                bail!("bad control file magic: {}", s.magic);
                            }
                            if s.format_version != SK_FORMAT_VERSION {
                                bail!(
                                    "incompatible format version: {} vs. {}",
                                    s.format_version,
                                    SK_FORMAT_VERSION
                                );
                            }
                            Ok((file, s))
                        }
                    }
                }
            }
            Err(e) => {
                bail!(
                    "failed to open control file {:?}: {}",
                    &control_file_path,
                    e
                );
            }
        }
    }
}

/// Database instance (tenant)
pub struct Timeline {
    pub timelineid: ZTimelineId,
    mutex: Mutex<SharedState>,
    /// conditional variable used to notify wal senders
    cond: Condvar,
}

impl Timeline {
    fn new(timelineid: ZTimelineId, shared_state: SharedState) -> Timeline {
        Timeline {
            timelineid,
            mutex: Mutex::new(shared_state),
            cond: Condvar::new(),
        }
    }

    /// Timed wait for an LSN to be committed.
    ///
    /// Returns the last committed LSN, which will be at least
    /// as high as the LSN waited for, or None if timeout expired.
    ///
    pub fn wait_for_lsn(&self, lsn: Lsn) -> Option<Lsn> {
        let mut shared_state = self.mutex.lock().unwrap();
        loop {
            let commit_lsn = shared_state.commit_lsn;
            // This must be `>`, not `>=`.
            if commit_lsn > lsn {
                return Some(commit_lsn);
            }
            let result = self
                .cond
                .wait_timeout(shared_state, POLL_STATE_TIMEOUT)
                .unwrap();
            if result.1.timed_out() {
                return None;
            }
            shared_state = result.0
        }
    }

    // Notify caught-up WAL senders about new WAL data received
    pub fn notify_wal_senders(&self, commit_lsn: Lsn) {
        let mut shared_state = self.mutex.lock().unwrap();
        if shared_state.commit_lsn < commit_lsn {
            shared_state.commit_lsn = commit_lsn;
            self.cond.notify_all();
        }
    }

    fn _stop_wal_senders(&self) {
        self.notify_wal_senders(END_REPLICATION_MARKER);
    }

    /// Pass arrived message to the safekeeper.
    pub fn process_msg(&self, msg: &ProposerAcceptorMessage) -> Result<AcceptorProposerMessage> {
        let mut rmsg: AcceptorProposerMessage;
        let commit_lsn: Lsn;
        {
            let mut shared_state = self.mutex.lock().unwrap();
            rmsg = shared_state.sk.process_msg(msg)?;
            // locally available commit lsn. flush_lsn can be smaller than
            // commit_lsn if we are catching up safekeeper.
            commit_lsn = shared_state.sk.commit_lsn;

            // if this is AppendResponse, fill in proper hot standby feedback and disk consistent lsn
            if let AcceptorProposerMessage::AppendResponse(ref mut resp) = rmsg {
                let state = shared_state.get_replicas_state();
                resp.hs_feedback = state.hs_feedback;
                resp.disk_consistent_lsn = state.disk_consistent_lsn;
            }
        }
        // Ping wal sender that new data might be available.
        self.notify_wal_senders(commit_lsn);
        Ok(rmsg)
    }

    pub fn get_info(&self) -> SafeKeeperState {
        self.mutex.lock().unwrap().sk.s.clone()
    }

    pub fn add_replica(&self) -> usize {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.add_replica()
    }

    pub fn update_replica_state(&self, id: usize, state: Option<ReplicaState>) {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.replicas[id] = state;
    }

    pub fn get_end_of_wal(&self) -> (Lsn, u32) {
        let shared_state = self.mutex.lock().unwrap();
        (shared_state.sk.flush_lsn, shared_state.sk.tli)
    }
}

// Utilities needed by various Connection-like objects
pub trait TimelineTools {
    fn set(
        &mut self,
        conf: &WalAcceptorConf,
        tenant_id: ZTenantId,
        timeline_id: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<()>;

    fn get(&self) -> &Arc<Timeline>;
}

impl TimelineTools for Option<Arc<Timeline>> {
    fn set(
        &mut self,
        conf: &WalAcceptorConf,
        tenant_id: ZTenantId,
        timeline_id: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<()> {
        // We will only set the timeline once. If it were to ever change,
        // anyone who cloned the Arc would be out of date.
        assert!(self.is_none());
        *self = Some(GlobalTimelines::get(conf, tenant_id, timeline_id, create)?);
        Ok(())
    }

    fn get(&self) -> &Arc<Timeline> {
        self.as_ref().unwrap()
    }
}

lazy_static! {
    pub static ref TIMELINES: Mutex<HashMap<(ZTenantId, ZTimelineId), Arc<Timeline>>> =
        Mutex::new(HashMap::new());
}

/// A zero-sized struct used to manage access to the global timelines map.
struct GlobalTimelines;

impl GlobalTimelines {
    /// Get a timeline with control file loaded from the global TIMELINES map.
    /// If control file doesn't exist and create=false, bails out.
    pub fn get(
        conf: &WalAcceptorConf,
        tenant_id: ZTenantId,
        timeline_id: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<Arc<Timeline>> {
        let mut timelines = TIMELINES.lock().unwrap();

        match timelines.get(&(tenant_id, timeline_id)) {
            Some(result) => Ok(Arc::clone(result)),
            None => {
                info!(
                    "creating timeline dir {}, create is {:?}",
                    timeline_id, create
                );
                fs::create_dir_all(timeline_id.to_string())?;

                let shared_state = SharedState::create_restore(conf, timeline_id, create)?;

                let new_tli = Arc::new(Timeline::new(timeline_id, shared_state));
                timelines.insert((tenant_id, timeline_id), Arc::clone(&new_tli));
                Ok(new_tli)
            }
        }
    }
}

#[derive(Debug)]
struct FileStorage {
    control_file: File,
    conf: WalAcceptorConf,
}

impl Storage for FileStorage {
    fn persist(&mut self, s: &SafeKeeperState, sync: bool) -> Result<()> {
        self.control_file.seek(SeekFrom::Start(0))?;
        s.ser_into(&mut self.control_file)?;
        if sync {
            self.control_file.sync_all()?;
        }
        Ok(())
    }

    fn write_wal(&mut self, s: &SafeKeeperState, startpos: Lsn, buf: &[u8]) -> Result<()> {
        let mut bytes_left: usize = buf.len();
        let mut bytes_written: usize = 0;
        let mut partial;
        let mut start_pos = startpos;
        const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];
        let wal_seg_size = s.server.wal_seg_size as usize;
        let ztli = s.server.ztli;

        /* Extract WAL location for this block */
        let mut xlogoff = start_pos.segment_offset(wal_seg_size) as usize;

        while bytes_left != 0 {
            let bytes_to_write;

            /*
             * If crossing a WAL boundary, only write up until we reach wal
             * segment size.
             */
            if xlogoff + bytes_left > wal_seg_size {
                bytes_to_write = wal_seg_size - xlogoff;
            } else {
                bytes_to_write = bytes_left;
            }

            /* Open file */
            let segno = start_pos.segment_number(wal_seg_size);
            // note: we basically don't support changing pg timeline
            let wal_file_name = XLogFileName(s.server.tli, segno, wal_seg_size);
            let wal_file_path = self
                .conf
                .data_dir
                .join(ztli.to_string())
                .join(wal_file_name.clone());
            let wal_file_partial_path = self
                .conf
                .data_dir
                .join(ztli.to_string())
                .join(wal_file_name.clone() + ".partial");

            {
                let mut wal_file: File;
                /* Try to open already completed segment */
                if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
                    wal_file = file;
                    partial = false;
                } else if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_partial_path)
                {
                    /* Try to open existed partial file */
                    wal_file = file;
                    partial = true;
                } else {
                    /* Create and fill new partial file */
                    partial = true;
                    match OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&wal_file_partial_path)
                    {
                        Ok(mut file) => {
                            for _ in 0..(wal_seg_size / XLOG_BLCKSZ) {
                                file.write_all(ZERO_BLOCK)?;
                            }
                            wal_file = file;
                        }
                        Err(e) => {
                            error!("Failed to open log file {:?}: {}", &wal_file_path, e);
                            return Err(e.into());
                        }
                    }
                }
                wal_file.seek(SeekFrom::Start(xlogoff as u64))?;
                wal_file.write_all(&buf[bytes_written..(bytes_written + bytes_to_write)])?;

                // Flush file, if not said otherwise
                if !self.conf.no_sync {
                    wal_file.sync_all()?;
                }
            }
            /* Write was successful, advance our position */
            bytes_written += bytes_to_write;
            bytes_left -= bytes_to_write;
            start_pos += bytes_to_write as u64;
            xlogoff += bytes_to_write;

            /* Did we reach the end of a WAL segment? */
            if start_pos.segment_offset(wal_seg_size) == 0 {
                xlogoff = 0;
                if partial {
                    fs::rename(&wal_file_partial_path, &wal_file_path)?;
                }
            }
        }
        Ok(())
    }
}
