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

/// Shared state associated with database instance (tenant)
#[derive(Debug)]
struct SharedState {
    /// Safekeeper object
    sk: SafeKeeper<FileStorage>,
    /// opened file control file handle (needed to hold exlusive file lock)
    control_file: File,
    /// For receiving-sending wal cooperation
    /// quorum commit LSN we've notified walsenders about
    commit_lsn: Lsn,
    /// combined hot standby feedback from all replicas
    hs_feedback: HotStandbyFeedback,
}

// A named boolean.
#[derive(Debug)]
pub enum CreateControlFile {
    True,
    False,
}

impl SharedState {
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
            control_file: cf.try_clone()?,
            conf: conf.clone(),
        };
        let (flush_lsn, tli) = if state.server.wal_seg_size != 0 {
            let wal_dir = conf.data_dir.join(format!("{}", timelineid));
            find_end_of_wal(&wal_dir, state.server.wal_seg_size as usize, true)
        } else {
            (0, 0)
        };

        Ok(Self {
            commit_lsn: Lsn(0),
            sk: SafeKeeper::new(Lsn(flush_lsn), tli, storage, state),
            control_file: cf,
            hs_feedback: HotStandbyFeedback {
                ts: 0,
                xmin: u64::MAX,
                catalog_xmin: u64::MAX,
            },
        })
    }

    /// Accumulate hot standby feedbacks from replicas
    pub fn add_hs_feedback(&mut self, feedback: HotStandbyFeedback) {
        self.hs_feedback.xmin = min(self.hs_feedback.xmin, feedback.xmin);
        self.hs_feedback.catalog_xmin = min(self.hs_feedback.catalog_xmin, feedback.catalog_xmin);
        self.hs_feedback.ts = max(self.hs_feedback.ts, feedback.ts);
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
#[derive(Debug)]
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

    /// Wait for an LSN to be committed.
    ///
    /// Returns the last committed LSN, which will be at least
    /// as high as the LSN waited for.
    ///
    pub fn wait_for_lsn(&self, lsn: Lsn) -> Lsn {
        let mut shared_state = self.mutex.lock().unwrap();
        loop {
            let commit_lsn = shared_state.commit_lsn;
            // This must be `>`, not `>=`.
            if commit_lsn > lsn {
                return commit_lsn;
            }
            shared_state = self.cond.wait(shared_state).unwrap();
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
            commit_lsn = min(shared_state.sk.flush_lsn, shared_state.sk.s.commit_lsn);

            // if this is AppendResponse, fill in proper hot standby feedback
            if let AcceptorProposerMessage::AppendResponse(ref mut resp) = rmsg {
                resp.hs_feedback = shared_state.hs_feedback.clone();
            }
        }
        // Ping wal sender that new data might be available.
        self.notify_wal_senders(commit_lsn);
        Ok(rmsg)
    }

    pub fn get_info(&self) -> SafeKeeperState {
        self.mutex.lock().unwrap().sk.s.clone()
    }

    // Accumulate hot standby feedbacks from replicas
    pub fn add_hs_feedback(&self, feedback: HotStandbyFeedback) {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.add_hs_feedback(feedback);
    }

    pub fn get_hs_feedback(&self) -> HotStandbyFeedback {
        let shared_state = self.mutex.lock().unwrap();
        shared_state.hs_feedback.clone()
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

    fn truncate_wal(&mut self, s: &SafeKeeperState, end_pos: Lsn) -> Result<()> {
        let partial;
        const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];
        let wal_seg_size = s.server.wal_seg_size as usize;
        let ztli = s.server.ztli;

        /* Extract WAL location for this block */
        let mut xlogoff = end_pos.segment_offset(wal_seg_size) as usize;

        /* Open file */
        let mut segno = end_pos.segment_number(wal_seg_size);
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
            } else {
                wal_file = OpenOptions::new()
                    .write(true)
                    .open(&wal_file_partial_path)?;
                partial = true;
            }
            wal_file.seek(SeekFrom::Start(xlogoff as u64))?;
            while xlogoff < wal_seg_size {
                let bytes_to_write = min(XLOG_BLCKSZ, wal_seg_size - xlogoff);
                wal_file.write_all(&ZERO_BLOCK[0..bytes_to_write])?;
                xlogoff += bytes_to_write;
            }
            // Flush file, if not said otherwise
            if !self.conf.no_sync {
                wal_file.sync_all()?;
            }
        }
        if !partial {
            // Make segment partial once again
            fs::rename(&wal_file_path, &wal_file_partial_path)?;
        }
        // Remove all subsequent segments
        loop {
            segno += 1;
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
            // TODO: better use fs::try_exists which is currenty avaialble only in nightly build
            if wal_file_path.exists() {
                fs::remove_file(&wal_file_path)?;
            } else if wal_file_partial_path.exists() {
                fs::remove_file(&wal_file_partial_path)?;
            } else {
                break;
            }
        }
        Ok(())
    }
}
