use std::sync::Arc;

use super::{layer_manager::LayerManager, Timeline};
use crate::{
    context::{DownloadBehavior, RequestContext},
    task_mgr::TaskKind,
    tenant::{
        storage_layer::{AsLayerDesc as _, DeltaLayerWriter, Layer, ResidentLayer},
        Tenant,
    },
    virtual_file::{MaybeFatalIo, VirtualFile},
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{completion, generation::Generation, id::TimelineId, lsn::Lsn};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("no ancestors")]
    NoAncestor,
    #[error("too many ancestors")]
    TooManyAncestors,
    #[error("shutting down, please retry later")]
    ShuttingDown,
    #[error("detached timeline must receive writes before the operation")]
    DetachedTimelineNeedsWrites,
    #[error("flushing failed")]
    FlushAncestor(#[source] anyhow::Error),
    #[error("layer download failed")]
    RewrittenDeltaDownloadFailed(#[source] anyhow::Error),
    #[error("copying LSN prefix locally failed")]
    CopyDeltaPrefix(#[source] anyhow::Error),
    #[error("upload rewritten layer")]
    UploadRewritten(#[source] anyhow::Error),

    #[error("ancestor is already being detached by: {}", .0)]
    OtherTimelineDetachOngoing(TimelineId),

    #[error("remote copying layer failed")]
    CopyFailed(#[source] anyhow::Error),

    #[error("unexpected error")]
    Unexpected(#[source] anyhow::Error),
}

pub(crate) struct PreparedTimelineDetach {
    layers: Vec<Layer>,
}

/// TODO: this should be part of PageserverConf because we cannot easily modify cplane arguments.
#[derive(Debug)]
pub(crate) struct Options {
    pub(crate) rewrite_concurrency: std::num::NonZeroUsize,
    pub(crate) copy_concurrency: std::num::NonZeroUsize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            rewrite_concurrency: std::num::NonZeroUsize::new(2).unwrap(),
            copy_concurrency: std::num::NonZeroUsize::new(10).unwrap(),
        }
    }
}

/// See [`Timeline::prepare_to_detach_from_ancestor`]
pub(super) async fn prepare(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
    options: Options,
    ctx: &RequestContext,
) -> Result<(completion::Completion, PreparedTimelineDetach), Error> {
    use Error::*;

    if detached.remote_client.as_ref().is_none() {
        unimplemented!("no new code for running without remote storage");
    }

    let Some((ancestor, ancestor_lsn)) = detached
        .ancestor_timeline
        .as_ref()
        .map(|tl| (tl.clone(), detached.ancestor_lsn))
    else {
        return Err(NoAncestor);
    };

    if !ancestor_lsn.is_valid() {
        return Err(NoAncestor);
    }

    if ancestor.ancestor_timeline.is_some() {
        // non-technical requirement; we could flatten N ancestors just as easily but we chose
        // not to
        return Err(TooManyAncestors);
    }

    if detached.get_prev_record_lsn() == Lsn::INVALID
        || detached.disk_consistent_lsn.load() == ancestor_lsn
    {
        // this is to avoid a problem that after detaching we would be unable to start up the
        // compute because of "PREV_LSN: invalid".
        return Err(DetachedTimelineNeedsWrites);
    }

    // before we acquire the gate, we must mark the ancestor as having a detach operation
    // ongoing which will block other concurrent detach operations so we don't get to ackward
    // situations where there would be two branches trying to reparent earlier branches.
    let (guard, barrier) = completion::channel();

    {
        let mut guard = tenant.ongoing_timeline_detach.lock().unwrap();
        if let Some((tl, other)) = guard.as_ref() {
            if !other.is_ready() {
                return Err(OtherTimelineDetachOngoing(*tl));
            }
        }
        *guard = Some((detached.timeline_id, barrier));
    }

    let _gate_entered = detached.gate.enter().map_err(|_| ShuttingDown)?;

    if ancestor_lsn >= ancestor.get_disk_consistent_lsn() {
        let span =
            tracing::info_span!("freeze_and_flush", ancestor_timeline_id=%ancestor.timeline_id);
        async {
            let started_at = std::time::Instant::now();
            let freeze_and_flush = ancestor.freeze_and_flush0();
            let mut freeze_and_flush = std::pin::pin!(freeze_and_flush);

            let res =
                tokio::time::timeout(std::time::Duration::from_secs(1), &mut freeze_and_flush)
                    .await;

            let res = match res {
                Ok(res) => res,
                Err(_elapsed) => {
                    tracing::info!("freezing and flushing ancestor is still ongoing");
                    freeze_and_flush.await
                }
            };

            res.map_err(FlushAncestor)?;

            // we do not need to wait for uploads to complete but we do need `struct Layer`,
            // copying delta prefix is unsupported currently for `InMemoryLayer`.
            tracing::info!(
                elapsed_ms = started_at.elapsed().as_millis(),
                "froze and flushed the ancestor"
            );
            Ok(())
        }
        .instrument(span)
        .await?;
    }

    let end_lsn = ancestor_lsn + 1;

    let (filtered_layers, straddling_branchpoint, rest_of_historic) = {
        // we do not need to start from our layers, because they can only be layers that come
        // *after* ancestor_lsn
        let layers = tokio::select! {
            guard = ancestor.layers.read() => guard,
            _ = detached.cancel.cancelled() => {
                return Err(ShuttingDown);
            }
            _ = ancestor.cancel.cancelled() => {
                return Err(ShuttingDown);
            }
        };

        // between retries, these can change if compaction or gc ran in between. this will mean
        // we have to redo work.
        partition_work(ancestor_lsn, &layers)
    };

    // TODO: layers are already sorted by something: use that to determine how much of remote
    // copies are already done.
    tracing::info!(filtered=%filtered_layers, to_rewrite = straddling_branchpoint.len(), historic=%rest_of_historic.len(), "collected layers");

    // TODO: copying and lsn prefix copying could be done at the same time with a single fsync after
    let mut new_layers: Vec<Layer> =
        Vec::with_capacity(straddling_branchpoint.len() + rest_of_historic.len());

    {
        tracing::debug!(to_rewrite = %straddling_branchpoint.len(), "copying prefix of delta layers");

        let mut tasks = tokio::task::JoinSet::new();

        let mut wrote_any = false;

        let limiter = Arc::new(tokio::sync::Semaphore::new(
            options.rewrite_concurrency.get(),
        ));

        for layer in straddling_branchpoint {
            let limiter = limiter.clone();
            let timeline = detached.clone();
            let ctx = ctx.detached_child(TaskKind::DetachAncestor, DownloadBehavior::Download);

            tasks.spawn(async move {
                let _permit = limiter.acquire().await;
                let copied =
                    upload_rewritten_layer(end_lsn, &layer, &timeline, &timeline.cancel, &ctx)
                        .await?;
                Ok(copied)
            });
        }

        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(Ok(Some(copied))) => {
                    wrote_any = true;
                    tracing::info!(layer=%copied, "rewrote and uploaded");
                    new_layers.push(copied);
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => return Err(e),
                Err(je) => return Err(Unexpected(je.into())),
            }
        }

        // FIXME: the fsync should be mandatory, after both rewrites and copies
        if wrote_any {
            let timeline_dir = VirtualFile::open(
                &detached
                    .conf
                    .timeline_path(&detached.tenant_shard_id, &detached.timeline_id),
            )
            .await
            .fatal_err("VirtualFile::open for timeline dir fsync");
            timeline_dir
                .sync_all()
                .await
                .fatal_err("VirtualFile::sync_all timeline dir");
        }
    }

    let mut tasks = tokio::task::JoinSet::new();
    let limiter = Arc::new(tokio::sync::Semaphore::new(options.copy_concurrency.get()));

    for adopted in rest_of_historic {
        let limiter = limiter.clone();
        let timeline = detached.clone();

        tasks.spawn(
            async move {
                let _permit = limiter.acquire().await;
                let owned =
                    remote_copy(&adopted, &timeline, timeline.generation, &timeline.cancel).await?;
                tracing::info!(layer=%owned, "remote copied");
                Ok(owned)
            }
            .in_current_span(),
        );
    }

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(owned)) => {
                new_layers.push(owned);
            }
            Ok(Err(failed)) => {
                return Err(failed);
            }
            Err(je) => return Err(Unexpected(je.into())),
        }
    }

    // TODO: fsync directory again if we hardlinked something

    let prepared = PreparedTimelineDetach { layers: new_layers };

    Ok((guard, prepared))
}

fn partition_work(
    ancestor_lsn: Lsn,
    source_layermap: &LayerManager,
) -> (usize, Vec<Layer>, Vec<Layer>) {
    let mut straddling_branchpoint = vec![];
    let mut rest_of_historic = vec![];

    let mut later_by_lsn = 0;

    for desc in source_layermap.layer_map().iter_historic_layers() {
        // off by one chances here:
        // - start is inclusive
        // - end is exclusive
        if desc.lsn_range.start > ancestor_lsn {
            later_by_lsn += 1;
            continue;
        }

        let target = if desc.lsn_range.start <= ancestor_lsn
            && desc.lsn_range.end > ancestor_lsn
            && desc.is_delta
        {
            // TODO: image layer at Lsn optimization
            &mut straddling_branchpoint
        } else {
            &mut rest_of_historic
        };

        target.push(source_layermap.get_from_desc(&desc));
    }

    (later_by_lsn, straddling_branchpoint, rest_of_historic)
}

async fn upload_rewritten_layer(
    end_lsn: Lsn,
    layer: &Layer,
    target: &Arc<Timeline>,
    cancel: &CancellationToken,
    ctx: &RequestContext,
) -> Result<Option<Layer>, Error> {
    use Error::UploadRewritten;
    let copied = copy_lsn_prefix(end_lsn, layer, target, ctx).await?;

    let Some(copied) = copied else {
        return Ok(None);
    };

    // FIXME: better shuttingdown error
    target
        .remote_client
        .as_ref()
        .unwrap()
        .upload_layer_file(&copied, cancel)
        .await
        .map_err(UploadRewritten)?;

    Ok(Some(copied.into()))
}

async fn copy_lsn_prefix(
    end_lsn: Lsn,
    layer: &Layer,
    target_timeline: &Arc<Timeline>,
    ctx: &RequestContext,
) -> Result<Option<ResidentLayer>, Error> {
    use Error::{CopyDeltaPrefix, RewrittenDeltaDownloadFailed};

    tracing::debug!(%layer, %end_lsn, "copying lsn prefix");

    let mut writer = DeltaLayerWriter::new(
        target_timeline.conf,
        target_timeline.timeline_id,
        target_timeline.tenant_shard_id,
        layer.layer_desc().key_range.start,
        layer.layer_desc().lsn_range.start..end_lsn,
    )
    .await
    .map_err(CopyDeltaPrefix)?;

    let resident = layer
        .download_and_keep_resident()
        .await
        // likely shutdown
        .map_err(RewrittenDeltaDownloadFailed)?;

    let records = resident
        .copy_delta_prefix(&mut writer, end_lsn, ctx)
        .await
        .map_err(CopyDeltaPrefix)?;

    drop(resident);

    tracing::debug!(%layer, records, "copied records");

    if records == 0 {
        drop(writer);
        // TODO: we might want to store an empty marker in remote storage for this
        // layer so that we will not needlessly walk `layer` on repeated attempts.
        Ok(None)
    } else {
        // reuse the key instead of adding more holes between layers by using the real
        // highest key in the layer.
        let reused_highest_key = layer.layer_desc().key_range.end;
        let copied = writer
            .finish(reused_highest_key, target_timeline, ctx)
            .await
            .map_err(CopyDeltaPrefix)?;

        tracing::debug!(%layer, %copied, "new layer produced");

        Ok(Some(copied))
    }
}

/// Creates a new Layer instance for the adopted layer, and ensures it is found from the remote
/// storage on successful return without the adopted layer being added to `index_part.json`.
async fn remote_copy(
    adopted: &Layer,
    adoptee: &Arc<Timeline>,
    generation: Generation,
    cancel: &CancellationToken,
) -> Result<Layer, Error> {
    use Error::CopyFailed;

    // depending if Layer::keep_resident we could hardlink

    let mut metadata = adopted.metadata();
    debug_assert!(metadata.generation <= generation);
    metadata.generation = generation;

    let owned = crate::tenant::storage_layer::Layer::for_evicted(
        adoptee.conf,
        adoptee,
        adopted.layer_desc().filename(),
        metadata,
    );

    // FIXME: better shuttingdown error
    adoptee
        .remote_client
        .as_ref()
        .unwrap()
        .copy_timeline_layer(adopted, &owned, cancel)
        .await
        .map(move |()| owned)
        .map_err(CopyFailed)
}

/// See [`Timeline::complete_detaching_timeline_ancestor`].
pub(super) async fn complete(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
    prepared: PreparedTimelineDetach,
    _ctx: &RequestContext,
) -> Result<Vec<TimelineId>, anyhow::Error> {
    let rtc = detached
        .remote_client
        .as_ref()
        .expect("has to have a remote timeline client for timeline ancestor detach");

    let PreparedTimelineDetach { layers } = prepared;

    let ancestor = detached
        .get_ancestor_timeline()
        .expect("must still have a ancestor");
    let ancestor_lsn = detached.get_ancestor_lsn();

    // publish the prepared layers before we reparent any of the timelines, so that on restart
    // reparented timelines find layers. also do the actual detaching.
    //
    // if we crash after this operation, we will at least come up having detached a timeline, but
    // we cannot go back and reparent the timelines which would had been reparented in normal
    // execution.
    //
    // this is not perfect, but it avoids us a retry happening after a compaction or gc on restart
    // which could give us a completely wrong layer combination.
    rtc.schedule_adding_existing_layers_to_index_detach_and_wait(
        &layers,
        (ancestor.timeline_id, ancestor_lsn),
    )
    .await?;

    let mut tasks = tokio::task::JoinSet::new();

    // because we are now keeping the slot in progress, it is unlikely that there will be any
    // timeline deletions during this time. if we raced one, then we'll just ignore it.
    tenant
        .timelines
        .lock()
        .unwrap()
        .values()
        .filter_map(|tl| {
            if Arc::ptr_eq(tl, detached) {
                return None;
            }

            if !tl.is_active() {
                return None;
            }

            let tl_ancestor = tl.ancestor_timeline.as_ref()?;
            let is_same = Arc::ptr_eq(&ancestor, tl_ancestor);
            let is_earlier = tl.get_ancestor_lsn() <= ancestor_lsn;

            let is_deleting = tl
                .delete_progress
                .try_lock()
                .map(|flow| !flow.is_not_started())
                .unwrap_or(true);

            if is_same && is_earlier && !is_deleting {
                Some(tl.clone())
            } else {
                None
            }
        })
        .for_each(|timeline| {
            // important in this scope: we are holding the Tenant::timelines lock
            let span = tracing::info_span!("reparent", reparented=%timeline.timeline_id);
            let new_parent = detached.timeline_id;

            tasks.spawn(
                async move {
                    let res = timeline
                        .remote_client
                        .as_ref()
                        .expect("reparented has to have remote client because detached has one")
                        .schedule_reparenting_and_wait(&new_parent)
                        .await;

                    match res {
                        Ok(()) => Some(timeline),
                        Err(e) => {
                            // with the use of tenant slot, we no longer expect these.
                            tracing::warn!("reparenting failed: {e:#}");
                            None
                        }
                    }
                }
                .instrument(span),
            );
        });

    let reparenting_candidates = tasks.len();
    let mut reparented = Vec::with_capacity(tasks.len());

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Some(timeline)) => {
                tracing::info!(reparented=%timeline.timeline_id, "reparenting done");
                reparented.push(timeline.timeline_id);
            }
            Ok(None) => {
                // lets just ignore this for now. one or all reparented timelines could had
                // started deletion, and that is fine.
            }
            Err(je) if je.is_cancelled() => unreachable!("not used"),
            Err(je) if je.is_panic() => {
                // ignore; it's better to continue with a single reparenting failing (or even
                // all of them) in order to get to the goal state.
                //
                // these timelines will never be reparentable, but they can be always detached as
                // separate tree roots.
            }
            Err(je) => tracing::error!("unexpected join error: {je:?}"),
        }
    }

    if reparenting_candidates != reparented.len() {
        tracing::info!("failed to reparent some candidates");
    }

    Ok(reparented)
}
