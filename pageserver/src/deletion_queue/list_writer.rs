//! The list writer is the first stage in the deletion queue.  It accumulates
//! layers to delete, and periodically writes out these layers into a persistent
//! DeletionList.
//!
//! The purpose of writing DeletionLists is to decouple the decision to
//! delete an object from the validation required to execute it: even if
//! validation is not possible, e.g. due to a control plane outage, we can
//! still persist our intent to delete an object, in a way that would
//! survive a restart.
//!
//! DeletionLists are passed onwards to the Validator.

use super::DeletionHeader;
use super::DeletionList;
use super::DeletionListSeq;
use super::FlushOp;
use super::ValidatorQueueMessage;

use std::collections::HashMap;
use std::fs::create_dir_all;
use std::time::Duration;

use pageserver_api::shard::TenantShardId;
use regex::Regex;
use remote_storage::RemotePath;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::info;
use tracing::warn;
use utils::generation::Generation;
use utils::id::TimelineId;

use crate::config::PageServerConf;
use crate::deletion_queue::TEMP_SUFFIX;
use crate::metrics;
use crate::tenant::remote_timeline_client::remote_layer_path;
use crate::tenant::remote_timeline_client::LayerFileMetadata;
use crate::tenant::storage_layer::LayerName;
use crate::virtual_file::on_fatal_io_error;
use crate::virtual_file::MaybeFatalIo;

// The number of keys in a DeletionList before we will proactively persist it
// (without reaching a flush deadline).  This aims to deliver objects of the order
// of magnitude 1MB when we are under heavy delete load.
const DELETION_LIST_TARGET_SIZE: usize = 16384;

// Ordinarily, we only flush to DeletionList periodically, to bound the window during
// which we might leak objects from not flushing a DeletionList after
// the objects are already unlinked from timeline metadata.
const FRONTEND_DEFAULT_TIMEOUT: Duration = Duration::from_millis(10000);

// If someone is waiting for a flush to DeletionList, only delay a little to accumulate
// more objects before doing the flush.
const FRONTEND_FLUSHING_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(super) struct DeletionOp {
    pub(super) tenant_shard_id: TenantShardId,
    pub(super) timeline_id: TimelineId,
    // `layers` and `objects` are both just lists of objects.  `layers` is used if you do not
    // have a config object handy to project it to a remote key, and need the consuming worker
    // to do it for you.
    pub(super) layers: Vec<(LayerName, LayerFileMetadata)>,
    pub(super) objects: Vec<RemotePath>,

    /// The _current_ generation of the Tenant shard attachment in which we are enqueuing
    /// this deletion.
    pub(super) generation: Generation,

    /// Return channel for the `DeletionList::sequence` this deletion is included in.
    pub(super) seq_tx: tokio::sync::oneshot::Sender<DeletionListSeq>,
}

#[derive(Debug)]
pub(super) struct RecoverOp {
    pub(super) attached_tenants: HashMap<TenantShardId, Generation>,
}

#[derive(Debug)]
pub(super) enum ListWriterQueueMessage {
    Delete(DeletionOp),
    // Wait until all prior deletions make it into a persistent DeletionList
    Flush(FlushOp),
    // Wait until all prior deletions have been executed (i.e. objects are actually deleted)
    FlushExecute(FlushOp),
    // Call once after re-attaching to control plane, to notify the deletion queue about
    // latest attached generations & load any saved deletion lists from disk.
    Recover(RecoverOp),
}

pub(super) struct ListWriter {
    conf: &'static PageServerConf,

    // Incoming frontend requests to delete some keys
    rx: tokio::sync::mpsc::UnboundedReceiver<ListWriterQueueMessage>,

    // Outbound requests to the backend to execute deletion lists we have composed.
    tx: tokio::sync::mpsc::Sender<ValidatorQueueMessage>,

    // The list we are currently building, contains a buffer of keys to delete
    // and our next sequence number
    pending: DeletionList,

    // These FlushOps should notify the next time we flush
    pending_flushes: Vec<FlushOp>,

    // Worker loop is torn down when this fires.
    cancel: CancellationToken,

    // Safety guard to do recovery exactly once
    recovered: bool,
}

impl ListWriter {
    // Initially DeletionHeader.validated_sequence is zero.  The place we start our
    // sequence numbers must be higher than that.
    const BASE_SEQUENCE: u64 = 1;

    pub(super) fn new(
        conf: &'static PageServerConf,
        rx: tokio::sync::mpsc::UnboundedReceiver<ListWriterQueueMessage>,
        tx: tokio::sync::mpsc::Sender<ValidatorQueueMessage>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            pending: DeletionList::new(Self::BASE_SEQUENCE),
            conf,
            rx,
            tx,
            pending_flushes: Vec::new(),
            cancel,
            recovered: false,
        }
    }

    /// Try to flush `list` to persistent storage
    ///
    /// This does not return errors, because on failure to flush we do not lose
    /// any state: flushing will be retried implicitly on the next deadline
    async fn flush(&mut self) {
        if self.pending.is_empty() {
            for f in self.pending_flushes.drain(..) {
                f.notify();
            }
            return;
        }

        match self.pending.save(self.conf).await {
            Ok(_) => {
                info!(sequence = self.pending.sequence, "Stored deletion list");

                for f in self.pending_flushes.drain(..) {
                    f.notify();
                }

                // Take the list we've accumulated, replace it with a fresh list for the next sequence
                let next_list = DeletionList::new(self.pending.sequence + 1);
                let list = std::mem::replace(&mut self.pending, next_list);

                if let Err(e) = self.tx.send(ValidatorQueueMessage::Delete(list)).await {
                    // This is allowed to fail: it will only happen if the backend worker is shut down,
                    // so we can just drop this on the floor.
                    info!("Deletion list dropped, this is normal during shutdown ({e:#})");
                }
            }
            Err(e) => {
                metrics::DELETION_QUEUE.unexpected_errors.inc();
                warn!(
                    sequence = self.pending.sequence,
                    "Failed to write deletion list, will retry later ({e:#})"
                );
            }
        }
    }

    /// Load the header, to learn the sequence number up to which deletions
    /// have been validated.  We will apply validated=true to DeletionLists
    /// <= this sequence when loading them.
    ///
    /// It is not an error for the header to not exist: we return None, and
    /// the caller should act as if validated_sequence is 0
    async fn load_validated_sequence(&self) -> Result<Option<DeletionListSeq>, anyhow::Error> {
        let header_path = self.conf.deletion_header_path();
        match tokio::fs::read(&header_path).await {
            Ok(header_bytes) => {
                match serde_json::from_slice::<DeletionHeader>(&header_bytes) {
                    Ok(h) => Ok(Some(h.validated_sequence)),
                    Err(e) => {
                        warn!(
                            "Failed to deserialize deletion header, ignoring {header_path}: {e:#}",
                        );
                        // This should never happen unless we make a mistake with our serialization.
                        // Ignoring a deletion header is not consequential for correctnes because all deletions
                        // are ultimately allowed to fail: worst case we leak some objects for the scrubber to clean up.
                        metrics::DELETION_QUEUE.unexpected_errors.inc();
                        Ok(None)
                    }
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    debug!("Deletion header {header_path} not found, first start?");
                    Ok(None)
                } else {
                    on_fatal_io_error(&e, "reading deletion header");
                }
            }
        }
    }

    async fn recover(
        &mut self,
        attached_tenants: HashMap<TenantShardId, Generation>,
    ) -> Result<(), anyhow::Error> {
        debug!(
            "recovering with {} attached tenants",
            attached_tenants.len()
        );

        // Load the header
        let validated_sequence = self.load_validated_sequence().await?.unwrap_or(0);

        self.pending.sequence = validated_sequence + 1;

        let deletion_directory = self.conf.deletion_prefix();
        let mut dir = tokio::fs::read_dir(&deletion_directory)
            .await
            .fatal_err("read deletion directory");

        let list_name_pattern =
            Regex::new("(?<sequence>[a-zA-Z0-9]{16})-(?<version>[a-zA-Z0-9]{2}).list").unwrap();

        let temp_extension = format!(".{TEMP_SUFFIX}");
        let header_path = self.conf.deletion_header_path();
        let mut seqs: Vec<DeletionListSeq> = Vec::new();
        while let Some(dentry) = dir.next_entry().await.fatal_err("read deletion dentry") {
            let file_name = dentry.file_name();
            let dentry_str = file_name.to_string_lossy();

            if file_name == header_path.file_name().unwrap_or("") {
                // Don't try and parse the header's name like a list
                continue;
            }

            if dentry_str.ends_with(&temp_extension) {
                info!("Cleaning up temporary file {dentry_str}");
                let absolute_path =
                    deletion_directory.join(dentry.file_name().to_str().expect("non-Unicode path"));
                tokio::fs::remove_file(&absolute_path)
                    .await
                    .fatal_err("delete temp file");

                continue;
            }

            let file_name = dentry.file_name().to_owned();
            let basename = file_name.to_string_lossy();
            let seq_part = if let Some(m) = list_name_pattern.captures(&basename) {
                m.name("sequence")
                    .expect("Non optional group should be present")
                    .as_str()
            } else {
                warn!("Unexpected key in deletion queue: {basename}");
                metrics::DELETION_QUEUE.unexpected_errors.inc();
                continue;
            };

            let seq: u64 = match u64::from_str_radix(seq_part, 16) {
                Ok(s) => s,
                Err(e) => {
                    warn!("Malformed key '{basename}': {e}");
                    metrics::DELETION_QUEUE.unexpected_errors.inc();
                    continue;
                }
            };
            seqs.push(seq);
        }
        seqs.sort();

        // Start our next deletion list from after the last location validated by
        // previous process lifetime, or after the last location found (it is updated
        // below after enumerating the deletion lists)
        self.pending.sequence = validated_sequence + 1;
        if let Some(max_list_seq) = seqs.last() {
            self.pending.sequence = std::cmp::max(self.pending.sequence, max_list_seq + 1);
        }

        for s in seqs {
            let list_path = self.conf.deletion_list_path(s);

            let list_bytes = tokio::fs::read(&list_path)
                .await
                .fatal_err("read deletion list");

            let mut deletion_list = match serde_json::from_slice::<DeletionList>(&list_bytes) {
                Ok(l) => l,
                Err(e) => {
                    // Drop the list on the floor: any objects it referenced will be left behind
                    // for scrubbing to clean up.  This should never happen unless we have a serialization bug.
                    warn!(sequence = s, "Failed to deserialize deletion list: {e}");
                    metrics::DELETION_QUEUE.unexpected_errors.inc();
                    continue;
                }
            };

            if deletion_list.sequence <= validated_sequence {
                // If the deletion list falls below valid_seq, we may assume that it was
                // already validated the last time this pageserver ran.  Otherwise, we still
                // load it, as it may still contain content valid in this generation.
                deletion_list.validated = true;
            } else {
                // Special case optimization: if a tenant is still attached, and no other
                // generation was issued to another node in the interval while we restarted,
                // then we may treat deletion lists from the previous generation as if they
                // belong to our currently attached generation, and proceed to validate & execute.
                for (tenant_shard_id, tenant_list) in &mut deletion_list.tenants {
                    if let Some(attached_gen) = attached_tenants.get(tenant_shard_id) {
                        if attached_gen.previous() == tenant_list.generation {
                            info!(
                                seq=%s, tenant_id=%tenant_shard_id.tenant_id,
                                shard_id=%tenant_shard_id.shard_slug(),
                                old_gen=?tenant_list.generation, new_gen=?attached_gen,
                                "Updating gen on recovered list");
                            tenant_list.generation = *attached_gen;
                        } else {
                            info!(
                                seq=%s, tenant_id=%tenant_shard_id.tenant_id,
                                shard_id=%tenant_shard_id.shard_slug(),
                                old_gen=?tenant_list.generation, new_gen=?attached_gen,
                                "Encountered stale generation on recovered list");
                        }
                    }
                }
            }

            info!(
                validated = deletion_list.validated,
                sequence = deletion_list.sequence,
                "Recovered deletion list"
            );

            // We will drop out of recovery if this fails: it indicates that we are shutting down
            // or the backend has panicked
            metrics::DELETION_QUEUE
                .keys_submitted
                .inc_by(deletion_list.len() as u64);
            self.tx
                .send(ValidatorQueueMessage::Delete(deletion_list))
                .await?;
        }

        info!(next_sequence = self.pending.sequence, "Replay complete");

        Ok(())
    }

    /// This is the front-end ingest, where we bundle up deletion requests into DeletionList
    /// and write them out, for later validation by the backend and execution by the executor.
    pub(super) async fn background(&mut self) {
        info!("Started deletion frontend worker");

        // Synchronous, but we only do it once per process lifetime so it's tolerable
        if let Err(e) = create_dir_all(self.conf.deletion_prefix()) {
            tracing::error!(
                "Failed to create deletion list directory {}, deletions will not be executed ({e})",
                self.conf.deletion_prefix(),
            );
            metrics::DELETION_QUEUE.unexpected_errors.inc();
            return;
        }

        while !self.cancel.is_cancelled() {
            let timeout = if self.pending_flushes.is_empty() {
                FRONTEND_DEFAULT_TIMEOUT
            } else {
                FRONTEND_FLUSHING_TIMEOUT
            };

            let msg = match tokio::time::timeout(timeout, self.rx.recv()).await {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    // Queue sender destroyed, shutting down
                    break;
                }
                Err(_) => {
                    // Hit deadline, flush.
                    self.flush().await;
                    continue;
                }
            };

            match msg {
                ListWriterQueueMessage::Delete(op) => {
                    assert!(
                        self.recovered,
                        "Cannot process deletions before recovery.  This is a bug."
                    );

                    debug!(
                        "Delete: ingesting {} layers, {} other objects",
                        op.layers.len(),
                        op.objects.len()
                    );

                    let mut layer_paths = Vec::new();
                    for (layer, meta) in op.layers {
                        layer_paths.push(remote_layer_path(
                            &op.tenant_shard_id.tenant_id,
                            &op.timeline_id,
                            meta.shard,
                            &layer,
                            meta.generation,
                        ));
                    }
                    layer_paths.extend(op.objects);

                    if !self.pending.push(
                        &op.tenant_shard_id,
                        &op.timeline_id,
                        op.generation,
                        &mut layer_paths,
                    ) {
                        self.flush().await;
                        let retry_succeeded = self.pending.push(
                            &op.tenant_shard_id,
                            &op.timeline_id,
                            op.generation,
                            &mut layer_paths,
                        );
                        if !retry_succeeded {
                            // Unexpected: after we flush, we should have
                            // drained self.pending, so a conflict on
                            // generation numbers should be impossible.
                            tracing::error!(
                                "Failed to enqueue deletions, leaking objects.  This is a bug."
                            );
                            metrics::DELETION_QUEUE.unexpected_errors.inc();
                        }
                    }

                    // Notify the client about the sequence number of this deletion.
                    op.seq_tx.send(self.pending.sequence).ok();
                }
                ListWriterQueueMessage::Flush(op) => {
                    if self.pending.is_empty() {
                        // Execute immediately
                        debug!("Flush: No pending objects, flushing immediately");
                        op.notify()
                    } else {
                        // Execute next time we flush
                        debug!("Flush: adding to pending flush list for next deadline flush");
                        self.pending_flushes.push(op);
                    }
                }
                ListWriterQueueMessage::FlushExecute(op) => {
                    debug!("FlushExecute: passing through to backend");
                    // We do not flush to a deletion list here: the client sends a Flush before the FlushExecute
                    if let Err(e) = self.tx.send(ValidatorQueueMessage::Flush(op)).await {
                        info!("Can't flush, shutting down ({e})");
                        // Caller will get error when their oneshot sender was dropped.
                    }
                }
                ListWriterQueueMessage::Recover(op) => {
                    if self.recovered {
                        tracing::error!(
                            "Deletion queue recovery called more than once.  This is a bug."
                        );
                        metrics::DELETION_QUEUE.unexpected_errors.inc();
                        // Non-fatal: although this is a bug, since we did recovery at least once we may proceed.
                        continue;
                    }

                    if let Err(e) = self.recover(op.attached_tenants).await {
                        // This should only happen in truly unrecoverable cases, like the recovery finding that the backend
                        // queue receiver has been dropped, or something is critically broken with
                        // the local filesystem holding deletion lists.
                        info!(
                            "Deletion queue recover aborted, deletion queue will not proceed ({e})"
                        );
                        metrics::DELETION_QUEUE.unexpected_errors.inc();
                        return;
                    } else {
                        self.recovered = true;
                    }
                }
            }

            if self.pending.len() > DELETION_LIST_TARGET_SIZE || !self.pending_flushes.is_empty() {
                self.flush().await;
            }
        }
        info!("Deletion queue shut down.");
    }
}
