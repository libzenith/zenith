//! The validator is responsible for validating DeletionLists for execution,
//! based on whethe the generation in the DeletionList is still the latest
//! generation for a tenant.
//!
//! The purpose of validation is to ensure split-brain safety in the cluster
//! of pageservers: a deletion may only be executed if the tenant generation
//! that originated it is still current.  See docs/rfcs/025-generation-numbers.md
//! The purpose of accumulating lists before validating them is to reduce load
//! on the control plane API by issuing fewer, larger requests.
//!
//! In addition to validating DeletionLists, the validator validates updates to remote_consistent_lsn
//! for timelines: these are logically deletions because the safekeepers use remote_consistent_lsn
//! to decide when old
//!
//! Deletions are passed onward to the Deleter.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use camino::Utf8PathBuf;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::config::PageServerConf;
use crate::controller_upcall_client::ControlPlaneGenerationsApi;
use crate::controller_upcall_client::RetryForeverError;
use crate::metrics;
use crate::virtual_file::MaybeFatalIo;

use super::deleter::DeleterMessage;
use super::DeletionHeader;
use super::DeletionList;
use super::DeletionQueueError;
use super::FlushOp;
use super::VisibleLsnUpdates;

// After this length of time, do any validation work that is pending,
// even if we haven't accumulated many keys to delete.
//
// This also causes updates to remote_consistent_lsn to be validated, even
// if there were no deletions enqueued.
const AUTOFLUSH_INTERVAL: Duration = Duration::from_secs(10);

// If we have received this number of keys, proceed with attempting to execute
const AUTOFLUSH_KEY_COUNT: usize = 16384;

#[derive(Debug)]
pub(super) enum ValidatorQueueMessage {
    Delete(DeletionList),
    Flush(FlushOp),
}
pub(super) struct Validator<C>
where
    C: ControlPlaneGenerationsApi,
{
    conf: &'static PageServerConf,
    rx: tokio::sync::mpsc::Receiver<ValidatorQueueMessage>,
    tx: tokio::sync::mpsc::Sender<DeleterMessage>,

    // Client for calling into control plane API for validation of deletes
    controller_upcall_client: C,

    // DeletionLists which are waiting generation validation.  Not safe to
    // execute until [`validate`] has processed them.
    pending_lists: Vec<DeletionList>,

    // DeletionLists which have passed validation and are ready to execute.
    validated_lists: Vec<DeletionList>,

    // Sum of all the lengths of lists in pending_lists
    pending_key_count: usize,

    // Lsn validation state: we read projected LSNs and write back visible LSNs
    // after validation.  This is the LSN equivalent of `pending_validation_lists`:
    // it is drained in [`validate`]
    lsn_table: Arc<std::sync::RwLock<VisibleLsnUpdates>>,

    // If we failed to rewrite a deletion list due to local filesystem I/O failure,
    // we must remember that and refuse to advance our persistent validated sequence
    // number past the failure.
    list_write_failed: Option<u64>,

    cancel: CancellationToken,
}

impl<C> Validator<C>
where
    C: ControlPlaneGenerationsApi,
{
    pub(super) fn new(
        conf: &'static PageServerConf,
        rx: tokio::sync::mpsc::Receiver<ValidatorQueueMessage>,
        tx: tokio::sync::mpsc::Sender<DeleterMessage>,
        controller_upcall_client: C,
        lsn_table: Arc<std::sync::RwLock<VisibleLsnUpdates>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            conf,
            rx,
            tx,
            controller_upcall_client,
            lsn_table,
            pending_lists: Vec::new(),
            validated_lists: Vec::new(),
            pending_key_count: 0,
            list_write_failed: None,
            cancel,
        }
    }
    /// Process any outstanding validations of generations of pending LSN updates or pending
    /// DeletionLists.
    ///
    /// Valid LSN updates propagate back to Timelines immediately, valid DeletionLists
    /// go into the queue of ready-to-execute lists.
    async fn validate(&mut self) -> Result<(), DeletionQueueError> {
        let mut tenant_generations = HashMap::new();
        for list in &self.pending_lists {
            for (tenant_id, tenant_list) in &list.tenants {
                // Note: DeletionLists are in logical time order, so generation always
                // goes up.  By doing a simple insert() we will always end up with
                // the latest generation seen for a tenant.
                tenant_generations.insert(*tenant_id, tenant_list.generation);
            }
        }

        let pending_lsn_updates = {
            let mut lsn_table = self.lsn_table.write().expect("Lock should not be poisoned");
            std::mem::take(&mut *lsn_table)
        };
        for (tenant_id, update) in &pending_lsn_updates.tenants {
            let entry = tenant_generations
                .entry(*tenant_id)
                .or_insert(update.generation);
            if update.generation > *entry {
                *entry = update.generation;
            }
        }

        if tenant_generations.is_empty() {
            // No work to do
            return Ok(());
        }

        let tenants_valid = match self
            .controller_upcall_client
            .validate(tenant_generations.iter().map(|(k, v)| (*k, *v)).collect())
            .await
        {
            Ok(tenants) => tenants,
            Err(RetryForeverError::ShuttingDown) => {
                // The only way a validation call returns an error is when the cancellation token fires
                return Err(DeletionQueueError::ShuttingDown);
            }
        };

        let mut validated_sequence: Option<u64> = None;

        // Apply the validation results to the pending LSN updates
        for (tenant_id, tenant_lsn_state) in pending_lsn_updates.tenants {
            let validated_generation = tenant_generations
                .get(&tenant_id)
                .expect("Map was built from the same keys we're reading");

            let valid = tenants_valid
                .get(&tenant_id)
                .copied()
                // If the tenant was missing from the validation response, it has been deleted.
                // The Timeline that requested the LSN update is probably already torn down,
                // or will be torn down soon.  In this case, drop the update by setting valid=false.
                .unwrap_or(false);

            if valid && *validated_generation == tenant_lsn_state.generation {
                for (timeline_id, pending_lsn) in tenant_lsn_state.timelines {
                    tracing::debug!(
                        %tenant_id,
                        %timeline_id,
                        current = %pending_lsn.result_slot.load(),
                        projected = %pending_lsn.projected,
                        "advancing validated remote_consistent_lsn",
                    );
                    pending_lsn.result_slot.store(pending_lsn.projected);
                }
            } else {
                // If we failed validation, then do not apply any of the projected updates
                info!("Dropped remote consistent LSN updates for tenant {tenant_id} in stale generation {:?}", tenant_lsn_state.generation);
                metrics::DELETION_QUEUE.dropped_lsn_updates.inc();
            }
        }

        // Apply the validation results to the pending deletion lists
        for list in &mut self.pending_lists {
            // Filter the list based on whether the server responded valid: true.
            // If a tenant is omitted in the response, it has been deleted, and we should
            // proceed with deletion.
            let mut mutated = false;
            list.tenants.retain(|tenant_id, tenant| {
                let validated_generation = tenant_generations
                    .get(tenant_id)
                    .expect("Map was built from the same keys we're reading");

                // If the tenant was missing from the validation response, it has been deleted.
                // This means that a deletion is valid, but also redundant since the tenant's
                // objects should have already been deleted.  Treat it as invalid to drop the
                // redundant deletion.
                let valid = tenants_valid.get(tenant_id).copied().unwrap_or(false);

                // A list is valid if it comes from the current _or previous_ generation.
                // - The previous generation case is permitted due to how we store deletion lists locally:
                // if we see the immediately previous generation in a locally stored deletion list,
                // it proves that this node's disk was used for both current & previous generations,
                // and therefore no other node was involved in between: the two generations may be
                // logically treated as the same.
                // - In that previous generation case, we rewrote it to the current generation
                // in recover(), so the comparison here is simply an equality.

                let this_list_valid = valid
                    && (tenant.generation == *validated_generation);

                if !this_list_valid {
                    info!("Dropping stale deletions for tenant {tenant_id} in generation {:?}, objects may be leaked", tenant.generation);
                    metrics::DELETION_QUEUE.keys_dropped.inc_by(tenant.len() as u64);
                    mutated = true;
                } else {
                    metrics::DELETION_QUEUE.keys_validated.inc_by(tenant.len() as u64);
                }
                this_list_valid
            });
            list.validated = true;

            if mutated {
                // Save the deletion list if we had to make changes due to stale generations.  The
                // saved list is valid for execution.
                if let Err(e) = list.save(self.conf).await {
                    // Highly unexpected.  Could happen if e.g. disk full.
                    // If we didn't save the trimmed list, it is _not_ valid to execute.
                    warn!("Failed to save modified deletion list {list}: {e:#}");
                    metrics::DELETION_QUEUE.unexpected_errors.inc();

                    // Rather than have a complex retry process, just drop it and leak the objects,
                    // scrubber will clean up eventually.
                    list.tenants.clear(); // Result is a valid-but-empty list, which is a no-op for execution.

                    // We must remember this failure, to prevent later writing out a header that
                    // would imply the unwritable list was valid on disk.
                    if self.list_write_failed.is_none() {
                        self.list_write_failed = Some(list.sequence);
                    }
                }
            }

            validated_sequence = Some(list.sequence);
        }

        if let Some(validated_sequence) = validated_sequence {
            if let Some(list_write_failed) = self.list_write_failed {
                // Rare error case: we failed to write out a deletion list to excise invalid
                // entries, so we cannot advance the header's valid sequence number past that point.
                //
                // In this state we will continue to validate, execute and delete deletion lists,
                // we just cannot update the header.  It should be noticed and fixed by a human due to
                // the nonzero value of our unexpected_errors metric.
                warn!(
                    sequence_number = list_write_failed,
                    "Cannot write header because writing a deletion list failed earlier",
                );
            } else {
                // Write the queue header to record how far validation progressed.  This avoids having
                // to rewrite each DeletionList to set validated=true in it.
                let header = DeletionHeader::new(validated_sequence);

                // Drop result because the validated_sequence is an optimization.  If we fail to save it,
                // then restart, we will drop some deletion lists, creating work for scrubber.
                // The save() function logs a warning on error.
                if let Err(e) = header.save(self.conf).await {
                    warn!("Failed to write deletion queue header: {e:#}");
                    metrics::DELETION_QUEUE.unexpected_errors.inc();
                }
            }
        }

        // Transfer the validated lists to the validated queue, for eventual execution
        self.validated_lists.append(&mut self.pending_lists);

        Ok(())
    }

    async fn cleanup_lists(&mut self, list_paths: Vec<Utf8PathBuf>) {
        for list_path in list_paths {
            debug!("Removing deletion list {list_path}");
            tokio::fs::remove_file(&list_path)
                .await
                .fatal_err("remove deletion list");
        }
    }

    async fn flush(&mut self) -> Result<(), DeletionQueueError> {
        tracing::debug!("Flushing with {} pending lists", self.pending_lists.len());

        // Issue any required generation validation calls to the control plane
        self.validate().await?;

        // After successful validation, nothing is pending: any lists that
        // made it through validation will be in validated_lists.
        assert!(self.pending_lists.is_empty());
        self.pending_key_count = 0;

        tracing::debug!(
            "Validation complete, have {} validated lists",
            self.validated_lists.len()
        );

        // Return quickly if we have no validated lists to execute.  This avoids flushing the
        // executor when an idle backend hits its autoflush interval
        if self.validated_lists.is_empty() {
            return Ok(());
        }

        // Drain `validated_lists` into the executor
        let mut executing_lists = Vec::new();
        for list in self.validated_lists.drain(..) {
            let list_path = self.conf.deletion_list_path(list.sequence);
            let objects = list.into_remote_paths();
            self.tx
                .send(DeleterMessage::Delete(objects))
                .await
                .map_err(|_| DeletionQueueError::ShuttingDown)?;
            executing_lists.push(list_path);
        }

        self.flush_executor().await?;

        // Erase the deletion lists whose keys have all be deleted from remote storage
        self.cleanup_lists(executing_lists).await;

        Ok(())
    }

    async fn flush_executor(&mut self) -> Result<(), DeletionQueueError> {
        // Flush the executor, so that all the keys referenced by these deletion lists
        // are actually removed from remote storage.  This is a precondition to deleting
        // the deletion lists themselves.
        let (flush_op, rx) = FlushOp::new();
        self.tx
            .send(DeleterMessage::Flush(flush_op))
            .await
            .map_err(|_| DeletionQueueError::ShuttingDown)?;

        rx.await.map_err(|_| DeletionQueueError::ShuttingDown)
    }

    pub(super) async fn background(&mut self) {
        tracing::info!("Started deletion backend worker");

        while !self.cancel.is_cancelled() {
            let msg = match tokio::time::timeout(AUTOFLUSH_INTERVAL, self.rx.recv()).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    // All queue senders closed
                    info!("Shutting down");
                    break;
                }
                Err(_) => {
                    // Timeout, we hit deadline to execute whatever we have in hand.  These functions will
                    // return immediately if no work is pending.
                    match self.flush().await {
                        Ok(()) => {}
                        Err(DeletionQueueError::ShuttingDown) => {
                            // If we are shutting down, then auto-flush can safely be skipped
                        }
                    }

                    continue;
                }
            };

            match msg {
                ValidatorQueueMessage::Delete(list) => {
                    if list.validated {
                        // A pre-validated list may only be seen during recovery, if we are recovering
                        // a DeletionList whose on-disk state has validated=true
                        self.validated_lists.push(list)
                    } else {
                        self.pending_key_count += list.len();
                        self.pending_lists.push(list);
                    }

                    if self.pending_key_count > AUTOFLUSH_KEY_COUNT {
                        match self.flush().await {
                            Ok(()) => {}
                            Err(DeletionQueueError::ShuttingDown) => {
                                // If we are shutting down, then auto-flush can safely be skipped
                            }
                        }
                    }
                }
                ValidatorQueueMessage::Flush(op) => {
                    match self.flush().await {
                        Ok(()) => {
                            op.notify();
                        }
                        Err(DeletionQueueError::ShuttingDown) => {
                            // If we fail due to shutting down, we will just drop `op` to propagate that status.
                        }
                    }
                }
            }
        }
    }
}
