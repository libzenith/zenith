//
// WAL redo
//
// We rely on Postgres to perform WAL redo for us. We launch a
// postgres process in special "wal redo" mode that's similar to
// single-user mode. We then pass the the previous page image, if any,
// and all the WAL records we want to apply, to the postgress
// process. Then we get the page image back. Communication with the
// postgres process happens via stdin/stdout
//
// See src/backend/tcop/zenith_wal_redo.c for the other side of
// this communication.
//
// TODO: Even though the postgres code runs in a separate process,
// it's not a secure sandbox.
//
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use std::assert;
use std::cell::RefCell;
use std::fs;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::Error;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;
use tokio::io::AsyncBufReadExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use zenith_utils::lsn::Lsn;

use crate::page_cache::BufferTag;
use crate::page_cache::PageCache;
use crate::page_cache::WALRecord;
use crate::ZTimelineId;
use crate::{pg_constants, PageServerConf};

static TIMEOUT: Duration = Duration::from_secs(20);

///
/// A WAL redo manager consists of two parts: WalRedoManager, and
/// WalRedoManagerInternal. WalRedoManager is the public struct
/// that can be used to send redo requests to the manager.
/// WalRedoManagerInternal is used by the manager thread itself.
///
pub struct WalRedoManager {
    conf: PageServerConf,
    timelineid: ZTimelineId,

    request_tx: mpsc::UnboundedSender<WalRedoRequest>,
    request_rx: Mutex<Option<mpsc::UnboundedReceiver<WalRedoRequest>>>,
}

struct WalRedoManagerInternal {
    _conf: PageServerConf,
    timelineid: ZTimelineId,

    pcache: Arc<PageCache>,
    request_rx: mpsc::UnboundedReceiver<WalRedoRequest>,
}

#[derive(Debug)]
struct WalRedoRequest {
    tag: BufferTag,
    lsn: Lsn,

    response_channel: oneshot::Sender<Result<Bytes, WalRedoError>>,
}

/// An error happened in WAL redo
#[derive(Debug, thiserror::Error)]
pub enum WalRedoError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

///
/// Public interface of WAL redo manager
///
impl WalRedoManager {
    ///
    /// Create a new WalRedoManager.
    ///
    /// This only initializes the struct. You need to call WalRedoManager::launch to
    /// start the thread that processes the requests.
    pub fn new(conf: &PageServerConf, timelineid: ZTimelineId) -> WalRedoManager {
        let (tx, rx) = mpsc::unbounded_channel();

        WalRedoManager {
            conf: conf.clone(),
            timelineid,
            request_tx: tx,
            request_rx: Mutex::new(Some(rx)),
        }
    }

    ///
    /// Launch the WAL redo thread
    ///
    pub fn launch(&self, pcache: Arc<PageCache>) {
        // Get mutable references to the values that we need to pass to the
        // thread.
        let request_rx = self.request_rx.lock().unwrap().take().unwrap();
        let conf_copy = self.conf.clone();
        let timelineid = self.timelineid;

        // Currently, the join handle is not saved anywhere and we
        // won't try restart the thread if it dies.
        let _walredo_thread = std::thread::Builder::new()
            .name("WAL redo thread".into())
            .spawn(move || {
                // We block on waiting for requests on the walredo request channel, but
                // use async I/O to communicate with the child process. Initialize the
                // runtime for the async part.
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                let mut internal = WalRedoManagerInternal {
                    _conf: conf_copy,
                    timelineid: timelineid,
                    pcache: pcache,
                    request_rx: request_rx,
                };

                runtime.block_on(internal.wal_redo_main());
            })
            .unwrap();
    }

    ///
    /// Request the WAL redo manager to apply WAL records, to reconstruct the page image
    /// of the given page version.
    ///
    pub async fn request_redo(&self, tag: BufferTag, lsn: Lsn) -> Result<Bytes, WalRedoError> {
        // Create a channel where to receive the response
        let (tx, rx) = oneshot::channel::<Result<Bytes, WalRedoError>>();

        let request = WalRedoRequest {
            tag,
            lsn,
            response_channel: tx,
        };

        self.request_tx
            .send(request)
            .expect("could not send WAL redo request");

        rx.await
            .expect("could not receive response to WAL redo request")
    }
}

///
/// WAL redo thread
///
impl WalRedoManagerInternal {
    //
    // Main entry point for the WAL applicator thread.
    //
    async fn wal_redo_main(&mut self) {
        info!("WAL redo thread started {}", self.timelineid);

        // Loop forever, handling requests as they come.
        loop {
            let mut process: WalRedoProcess;
            let datadir = format!("wal-redo/{}", self.timelineid);

            info!("launching WAL redo postgres process {}", self.timelineid);

            process = WalRedoProcess::launch(&datadir).await.unwrap();
            info!("WAL redo postgres started");

            // Pretty arbitrarily, reuse the same Postgres process for 100000 requests.
            // After that, kill it and start a new one. This is mostly to avoid
            // using up all shared buffers in Postgres's shared buffer cache; we don't
            // want to write any pages to disk in the WAL redo process.
            for _i in 1..100000 {
                let request = self.request_rx.recv().await.unwrap();

                let result = self.handle_apply_request(&process, &request).await;
                let result_ok = result.is_ok();

                // Send the result to the requester
                let _ = request.response_channel.send(result);

                if !result_ok {
                    // Something went wrong with handling the request. It's not clear
                    // if the request was faulty, and the next request would succeed
                    // again, or if the 'postgres' process went haywire. To be safe,
                    // kill the 'postgres' process so that we will start from a clean
                    // slate, with a new process, for the next request.
                    break;
                }
            }

            // Time to kill the 'postgres' process. A new one will be launched on next
            // iteration of the loop.
            info!("killing WAL redo postgres process");
            let _ = process.stdin.get_mut().shutdown().await;
            let mut child = process.child;
            drop(process.stdin);
            let _ = child.wait().await;
        }
    }

    fn transaction_id_set_status_bit(
        &self,
        xl_info: u8,
        xl_rmid: u8,
        xl_xid: u32,
        record: WALRecord,
        page: &mut BytesMut,
    ) {
        let info = xl_info & pg_constants::XLOG_XACT_OPMASK;
        let mut status = 0;
        if info == pg_constants::XLOG_XACT_COMMIT {
            status = pg_constants::TRANSACTION_STATUS_COMMITTED;
        } else if info == pg_constants::XLOG_XACT_ABORT {
            status = pg_constants::TRANSACTION_STATUS_ABORTED;
        } else {
            trace!("handle_apply_request for RM_XACT_ID-{} NOT SUPPORTED YET. RETURN. lsn {} main_data_offset {}, rec.len {}",
                   status,
                   record.lsn,
                   record.main_data_offset, record.rec.len());
            return;
        }

        trace!("handle_apply_request for RM_XACT_ID-{} (1-commit, 2-abort) lsn {} main_data_offset {}, rec.len {}",
               status,
               record.lsn,
               record.main_data_offset, record.rec.len());

        let byteno: usize = ((xl_rmid as u32 % pg_constants::CLOG_XACTS_PER_PAGE as u32)
            / pg_constants::CLOG_XACTS_PER_BYTE) as usize;

        let byteptr = &mut page[byteno..byteno + 1];
        let bshift: u8 = ((xl_xid % pg_constants::CLOG_XACTS_PER_BYTE)
            * pg_constants::CLOG_BITS_PER_XACT as u32) as u8;

        let mut curval = byteptr[0];
        curval = (curval >> bshift) & pg_constants::CLOG_XACT_BITMASK;

        let mut byteval = [0];
        byteval[0] = curval;
        byteval[0] &= !(((1 << pg_constants::CLOG_BITS_PER_XACT as u8) - 1) << bshift);
        byteval[0] |= status << bshift;

        byteptr.copy_from_slice(&byteval);
        trace!(
            "xl_xid {} byteno {} curval {} byteval {}",
            xl_xid,
            byteno,
            curval,
            byteval[0]
        );
    }

    ///
    /// Process one request for WAL redo.
    ///
    async fn handle_apply_request(
        &self,
        process: &WalRedoProcess,
        request: &WalRedoRequest,
    ) -> Result<Bytes, WalRedoError> {
        let pcache = &self.pcache;
        let tag = request.tag;
        let lsn = request.lsn;
        let (base_img, records) = pcache.collect_records_for_apply(tag, lsn);

        let nrecords = records.len();

        let start = Instant::now();

        let apply_result: Result<Bytes, Error>;
        if tag.rel.forknum == pg_constants::PG_XACT_FORKNUM as u8 {
            //TODO use base image if any
            static ZERO_PAGE: [u8; 8192] = [0u8; 8192];
            let zero_page_bytes: &[u8] = &ZERO_PAGE;
            let mut page = BytesMut::from(zero_page_bytes);

            for record in records {
                let mut buf = record.rec.clone();

                // 1. Parse XLogRecord struct
                // FIXME: refactor to avoid code duplication.
                let _xl_tot_len = buf.get_u32_le();
                let xl_xid = buf.get_u32_le();
                let _xl_prev = buf.get_u64_le();
                let xl_info = buf.get_u8();
                let xl_rmid = buf.get_u8();
                buf.advance(2); // 2 bytes of padding
                let _xl_crc = buf.get_u32_le();

                if xl_rmid == pg_constants::RM_CLOG_ID {
                    let info = xl_info & !pg_constants::XLR_INFO_MASK;
                    if info == pg_constants::CLOG_ZEROPAGE {
                        page.clone_from_slice(zero_page_bytes);
                        trace!("handle_apply_request for RM_CLOG_ID-CLOG_ZEROPAGE lsn {} main_data_offset {}, rec.len {}",
                               record.lsn,
                               record.main_data_offset, record.rec.len());
                    }
                } else if xl_rmid == pg_constants::RM_XACT_ID {
                    self.transaction_id_set_status_bit(xl_info, xl_rmid, xl_xid, record, &mut page);
                }
            }

            apply_result = Ok::<Bytes, Error>(page.freeze());
        } else {
            apply_result = process.apply_wal_records(tag, base_img, records).await;
        }

        let duration = start.elapsed();

        let result: Result<Bytes, WalRedoError>;

        trace!(
            "applied {} WAL records in {} ms to reconstruct page image at LSN {}",
            nrecords,
            duration.as_millis(),
            lsn
        );

        if let Err(e) = apply_result {
            error!("could not apply WAL records: {}", e);
            result = Err(WalRedoError::IoError(e));
        } else {
            let img = apply_result.unwrap();

            // Should we put the image back to the page cache? We don't have to,
            // but then the next GetPage@LSN call will have to do the WAL redo
            // again
            pcache.put_page_image(tag, lsn, img.clone());

            result = Ok(img);
        }

        // The caller is responsible for sending the response
        result
    }
}

struct WalRedoProcess {
    child: Child,
    stdin: RefCell<ChildStdin>,
    stdout: RefCell<ChildStdout>,
}

impl WalRedoProcess {
    //
    // Start postgres binary in special WAL redo mode.
    //
    // Tests who run pageserver binary are setting proper PG_BIN_DIR
    // and PG_LIB_DIR so that WalRedo would start right postgres. We may later
    // switch to setting same things in pageserver config file.
    async fn launch(datadir: &str) -> Result<WalRedoProcess, Error> {
        // Create empty data directory for wal-redo postgres deleting old one.
        fs::remove_dir_all(datadir).ok();
        let initdb = Command::new("initdb")
            .args(&["-D", datadir])
            .arg("-N")
            .output()
            .await
            .expect("failed to execute initdb");

        if !initdb.status.success() {
            panic!(
                "initdb failed: {}\nstderr:\n{}",
                std::str::from_utf8(&initdb.stdout).unwrap(),
                std::str::from_utf8(&initdb.stderr).unwrap()
            );
        } else {
            // Limit shared cache for wal-redo-postres
            let mut config = OpenOptions::new()
                .append(true)
                .open(PathBuf::from(&datadir).join("postgresql.conf"))?;
            config.write(b"shared_buffers=128kB\n")?;
            config.write(b"fsync=off\n")?;
        }
        // Start postgres itself
        let mut child = Command::new("postgres")
            .arg("--wal-redo")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .env("PGDATA", datadir)
            .spawn()
            .expect("postgres --wal-redo command failed to start");

        info!("launched WAL redo postgres process on {}", datadir);

        let stdin = child.stdin.take().expect("failed to open child's stdin");
        let stderr = child.stderr.take().expect("failed to open child's stderr");
        let stdout = child.stdout.take().expect("failed to open child's stdout");

        // This async block reads the child's stderr, and forwards it to the logger
        let f_stderr = async {
            let mut stderr_buffered = tokio::io::BufReader::new(stderr);

            let mut line = String::new();
            loop {
                let res = stderr_buffered.read_line(&mut line).await;
                if res.is_err() {
                    debug!("could not convert line to utf-8");
                    continue;
                }
                if res.unwrap() == 0 {
                    break;
                }
                error!("wal-redo-postgres: {}", line.trim());
                line.clear();
            }
            Ok::<(), Error>(())
        };
        tokio::spawn(f_stderr);

        Ok(WalRedoProcess {
            child,
            stdin: RefCell::new(stdin),
            stdout: RefCell::new(stdout),
        })
    }

    //
    // Apply given WAL records ('records') over an old page image. Returns
    // new page image.
    //
    async fn apply_wal_records(
        &self,
        tag: BufferTag,
        base_img: Option<Bytes>,
        records: Vec<WALRecord>,
    ) -> Result<Bytes, std::io::Error> {
        let mut stdin = self.stdin.borrow_mut();
        let mut stdout = self.stdout.borrow_mut();

        let f_stdin = async {
            // Send base image, if any. (If the record initializes the page, previous page
            // version is not needed.)
            timeout(
                TIMEOUT,
                stdin.write_all(&build_begin_redo_for_block_msg(tag)),
            )
            .await??;
            if base_img.is_some() {
                timeout(
                    TIMEOUT,
                    stdin.write_all(&build_push_page_msg(tag, base_img.unwrap())),
                )
                .await??;
            }

            // Send WAL records.
            for rec in records.iter() {
                let r = rec.clone();

                stdin
                    .write_all(&build_apply_record_msg(r.lsn, r.rec))
                    .await?;

                //debug!("sent WAL record to wal redo postgres process ({:X}/{:X}",
                //       r.lsn >> 32, r.lsn & 0xffff_ffff);
            }
            //debug!("sent {} WAL records to wal redo postgres process ({:X}/{:X}",
            //       records.len(), lsn >> 32, lsn & 0xffff_ffff);

            // Send GetPage command to get the result back
            timeout(TIMEOUT, stdin.write_all(&build_get_page_msg(tag))).await??;
            timeout(TIMEOUT, stdin.flush()).await??;
            //debug!("sent GetPage for {}", tag.blknum);
            Ok::<(), Error>(())
        };

        // Read back new page image
        let f_stdout = async {
            let mut buf = [0u8; 8192];

            timeout(TIMEOUT, stdout.read_exact(&mut buf)).await??;
            //debug!("got response for {}", tag.blknum);
            Ok::<[u8; 8192], Error>(buf)
        };

        // Kill the process. This closes its stdin, which should signal the process
        // to terminate. TODO: SIGKILL if needed
        //child.wait();

        let res = futures::try_join!(f_stdout, f_stdin)?;

        let buf = res.0;

        Ok::<Bytes, Error>(Bytes::from(std::vec::Vec::from(buf)))
    }
}

// Functions for constructing messages to send to the postgres WAL redo
// process. See vendor/postgres/src/backend/tcop/zenith_wal_redo.c for
// explanation of the protocol.

fn build_begin_redo_for_block_msg(tag: BufferTag) -> Bytes {
    let len = 4 + 5 * 4;
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'B');
    buf.put_u32(len as u32);
    tag.pack(&mut buf);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}

fn build_push_page_msg(tag: BufferTag, base_img: Bytes) -> Bytes {
    assert!(base_img.len() == 8192);

    let len = 4 + 5 * 4 + base_img.len();
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'P');
    buf.put_u32(len as u32);
    tag.pack(&mut buf);
    buf.put(base_img);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}

fn build_apply_record_msg(endlsn: Lsn, rec: Bytes) -> Bytes {
    let len = 4 + 8 + rec.len();
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'A');
    buf.put_u32(len as u32);
    buf.put_u64(endlsn.0);
    buf.put(rec);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}

fn build_get_page_msg(tag: BufferTag) -> Bytes {
    let len = 4 + 5 * 4;
    let mut buf = BytesMut::with_capacity(1 + len);

    buf.put_u8(b'G');
    buf.put_u32(len as u32);
    tag.pack(&mut buf);

    assert!(buf.len() == 1 + len);

    buf.freeze()
}
