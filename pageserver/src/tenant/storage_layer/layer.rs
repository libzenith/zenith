use anyhow::Context;
use pageserver_api::models::{
    HistoricLayerInfo, LayerAccessKind, LayerResidenceEventReason, LayerResidenceStatus,
};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tracing::Instrument;
use utils::lsn::Lsn;
use utils::sync::heavier_once_cell;

use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::repository::Key;
use crate::tenant::{remote_timeline_client::LayerFileMetadata, RemoteTimelineClient, Timeline};

use super::delta_layer::{self, DeltaEntry};
use super::image_layer;
use super::{
    AsLayerDesc, LayerAccessStats, LayerAccessStatsReset, LayerFileName, PersistentLayerDesc,
    ValueReconstructResult, ValueReconstructState,
};

/// A Layer contains all data in a "rectangle" consisting of a range of keys and
/// range of LSNs.
///
/// There are two kinds of layers, in-memory and on-disk layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access to the
/// recent page versions. On-disk layers are stored as files on disk, and are
/// immutable. This type represents the on-disk kind.
///
/// Furthermore, there are two kinds of on-disk layers: delta and image layers.
/// A delta layer contains all modifications within a range of LSNs and keys.
/// An image layer is a snapshot of all the data in a key-range, at a single
/// LSN.
///
/// This type models the on-disk layers, which can be evicted and on-demand downloaded.
#[derive(Clone)]
pub(crate) struct Layer(Arc<LayerInner>);

impl std::fmt::Display for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl std::fmt::Debug for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl AsLayerDesc for Layer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        self.0.layer_desc()
    }
}

impl Layer {
    pub(crate) fn for_evicted(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        file_name: LayerFileName,
        metadata: LayerFileMetadata,
    ) -> Self {
        let desc = PersistentLayerDesc::from_filename(
            timeline.tenant_id,
            timeline.timeline_id,
            file_name,
            metadata.file_size(),
        );

        let access_stats = LayerAccessStats::for_loading_layer(LayerResidenceStatus::Evicted);

        let owner = Layer(Arc::new(LayerInner::new(
            conf,
            timeline,
            access_stats,
            desc,
            None,
        )));

        debug_assert!(owner.0.needs_download_blocking().unwrap().is_some());

        owner
    }

    pub(crate) fn for_resident(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        file_name: LayerFileName,
        metadata: LayerFileMetadata,
    ) -> ResidentLayer {
        let desc = PersistentLayerDesc::from_filename(
            timeline.tenant_id,
            timeline.timeline_id,
            file_name,
            metadata.file_size(),
        );

        let access_stats = LayerAccessStats::for_loading_layer(LayerResidenceStatus::Resident);

        let mut resident = None;

        let owner = Layer(Arc::new_cyclic(|owner| {
            let inner = Arc::new(DownloadedLayer {
                owner: owner.clone(),
                kind: tokio::sync::OnceCell::default(),
            });
            resident = Some(inner.clone());

            LayerInner::new(conf, timeline, access_stats, desc, Some(inner))
        }));

        debug_assert!(owner.0.needs_download_blocking().unwrap().is_none());

        let downloaded = resident.expect("just initialized");

        ResidentLayer { downloaded, owner }
    }

    /// Creates a Layer value for freshly written out new layer file by renaming it from a
    /// temporary path.
    pub(crate) fn for_written_tempfile(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        desc: PersistentLayerDesc,
        temp_path: &Path,
    ) -> anyhow::Result<ResidentLayer> {
        let mut resident = None;

        let owner = Layer(Arc::new_cyclic(|owner| {
            let inner = Arc::new(DownloadedLayer {
                owner: owner.clone(),
                kind: tokio::sync::OnceCell::default(),
            });
            resident = Some(inner.clone());
            let access_stats = LayerAccessStats::empty_will_record_residence_event_later();
            access_stats.record_residence_event(
                LayerResidenceStatus::Resident,
                LayerResidenceEventReason::LayerCreate,
            );
            LayerInner::new(conf, timeline, access_stats, desc, Some(inner))
        }));

        let downloaded = resident.expect("just initialized");

        // if the rename works, the path is as expected
        std::fs::rename(temp_path, owner.local_path())
            .context("rename temporary file as correct path for {owner}")?;

        Ok(ResidentLayer { downloaded, owner })
    }

    pub(crate) async fn evict_and_wait(
        &self,
        rtc: &RemoteTimelineClient,
    ) -> Result<(), EvictionError> {
        self.0.evict_and_wait(rtc).await
    }

    /// Delete the layer file when the `self` gets dropped, also schedule a remote index upload
    /// then perhaps.
    pub(crate) fn garbage_collect(&self) {
        self.0.garbage_collect();
    }

    /// Return data needed to reconstruct given page at LSN.
    ///
    /// It is up to the caller to collect more data from previous layer and
    /// perform WAL redo, if necessary.
    ///
    /// See PageReconstructResult for possible return values. The collected data
    /// is appended to reconstruct_data; the caller should pass an empty struct
    /// on first call, or a struct with a cached older image of the page if one
    /// is available. If this returns ValueReconstructResult::Continue, look up
    /// the predecessor layer and call again with the same 'reconstruct_data' to
    /// collect more data.
    pub(crate) async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<ValueReconstructResult> {
        use anyhow::ensure;

        let layer = self.0.get_or_maybe_download(true, Some(ctx)).await?;
        self.0
            .access_stats
            .record_access(LayerAccessKind::GetValueReconstructData, ctx);

        if self.layer_desc().is_delta {
            ensure!(lsn_range.start >= self.layer_desc().lsn_range.start);
            ensure!(self.layer_desc().key_range.contains(&key));
        } else {
            ensure!(self.layer_desc().key_range.contains(&key));
            ensure!(lsn_range.start >= self.layer_desc().image_layer_lsn());
            ensure!(lsn_range.end >= self.layer_desc().image_layer_lsn());
        }

        layer
            .get_value_reconstruct_data(key, lsn_range, reconstruct_data, &self.0)
            .await
    }

    /// Download the layer if evicted.
    ///
    /// Will not error when it is already downloaded.
    pub(crate) async fn get_or_download(&self) -> anyhow::Result<()> {
        self.0.get_or_maybe_download(true, None).await?;
        Ok(())
    }

    /// Creates a guard object which prohibit evicting this layer as long as the value is kept
    /// around.
    pub(crate) async fn guard_against_eviction(
        &self,
        allow_download: bool,
    ) -> anyhow::Result<ResidentLayer> {
        let downloaded = self.0.get_or_maybe_download(allow_download, None).await?;

        Ok(ResidentLayer {
            downloaded,
            owner: self.clone(),
        })
    }

    pub(crate) fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        self.0.info(reset)
    }

    pub(crate) fn access_stats(&self) -> &LayerAccessStats {
        &self.0.access_stats
    }

    pub(crate) fn local_path(&self) -> &Path {
        &self.0.path
    }
}

/// The download-ness ([`DownloadedLayer`]) can be either resident or wanted evicted.
///
/// However when we want something evicted, we cannot evict it right away as there might be current
/// reads happening on it. It has been for example searched from [`LayerMap`] but not yet
/// [`Layer::get_value_reconstruct_data`].
///
/// [`LayerMap`]: crate::tenant::layer_map::LayerMap
enum ResidentOrWantedEvicted {
    Resident(Arc<DownloadedLayer>),
    WantedEvicted(Weak<DownloadedLayer>),
}

impl ResidentOrWantedEvicted {
    fn get(&self) -> Option<Arc<DownloadedLayer>> {
        match self {
            ResidentOrWantedEvicted::Resident(strong) => Some(strong.clone()),
            ResidentOrWantedEvicted::WantedEvicted(weak) => weak.upgrade(),
        }
    }
    /// When eviction is first requested, drop down to holding a [`Weak`].
    ///
    /// Returns `true` if this was the first time eviction was requested.
    fn downgrade(&mut self) -> &Weak<DownloadedLayer> {
        let _was_first = match self {
            ResidentOrWantedEvicted::Resident(strong) => {
                let weak = Arc::downgrade(strong);
                *self = ResidentOrWantedEvicted::WantedEvicted(weak);
                // returning the weak is not useful, because the drop could had already ran with
                // the replacement above, and that will take care of cleaning the Option we are in
                true
            }
            ResidentOrWantedEvicted::WantedEvicted(_) => false,
        };

        match self {
            ResidentOrWantedEvicted::WantedEvicted(ref weak) => weak,
            _ => unreachable!("just wrote wanted evicted"),
        }
    }
}

struct LayerInner {
    /// Only needed to check ondemand_download_behavior_treat_error_as_warn and creation of
    /// [`Self::path`].
    conf: &'static PageServerConf,
    path: PathBuf,

    desc: PersistentLayerDesc,

    /// Timeline access is needed for metrics.
    timeline: Weak<Timeline>,

    /// Cached knowledge of [`Timeline::remote_client`] being `Some`.
    have_remote_client: bool,

    access_stats: LayerAccessStats,

    /// This custom OnceCell is backed by std mutex, but only held for short time periods.
    /// Initialization and deinitialization is done while holding a permit.
    inner: heavier_once_cell::OnceCell<ResidentOrWantedEvicted>,

    /// Do we want to garbage collect this when `LayerInner` is dropped, where garbage collection
    /// means:
    /// - schedule remote deletion
    /// - instant local deletion
    wanted_garbage_collected: AtomicBool,

    /// Do we want to evict this layer as soon as possible? After being set to `true`, all accesses
    /// will try to downgrade [`ResidentOrWantedEvicted`], which will eventually trigger
    /// [`LayerInner::on_drop`].
    wanted_evicted: AtomicBool,

    /// Version is to make sure we will in fact only evict a file if no new guard has been created
    /// for it.
    version: AtomicUsize,

    /// Allow subscribing to when the layer actually gets evicted.
    status: tokio::sync::broadcast::Sender<Status>,
}

impl std::fmt::Display for LayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl AsLayerDesc for LayerInner {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        &self.desc
    }
}

#[derive(Debug, Clone, Copy)]
enum Status {
    Evicted,
    Downloaded,
}

impl Drop for LayerInner {
    fn drop(&mut self) {
        if !*self.wanted_garbage_collected.get_mut() {
            // should we try to evict if the last wish was for eviction?
            // feels like there's some hazard of overcrowding near shutdown near by, but we don't
            // run drops during shutdown (yet)
            return;
        }

        // SEMITODO: this could be spawn_blocking or not; we are only doing the filesystem delete
        // right now, later this will be a submit to the global deletion queue.
        let span = tracing::info_span!(parent: None, "layer_drop", tenant_id = %self.layer_desc().tenant_id, timeline_id = %self.layer_desc().timeline_id, layer = %self);
        let _g = span.entered();

        let mut removed = false;
        match std::fs::remove_file(&self.path) {
            Ok(()) => {
                removed = true;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // until we no longer do detaches by removing all local files before removing the
                // tenant from the global map, we will always get these errors even if we knew what
                // is the latest state.
                //
                // we currently do not track the latest state, so we'll also end up here on evicted
                // layers.
            }
            Err(e) => {
                tracing::error!(layer = %self, "failed to remove garbage collected layer: {e}");
            }
        }

        if let Some(timeline) = self.timeline.upgrade() {
            if removed {
                timeline
                    .metrics
                    .resident_physical_size_gauge
                    .sub(self.layer_desc().file_size);
            }
            if let Some(remote_client) = timeline.remote_client.as_ref() {
                let res =
                    remote_client.schedule_layer_file_deletion(&[self.layer_desc().filename()]);

                if let Err(e) = res {
                    if !timeline.is_active() {
                        // downgrade the warning to info maybe?
                    }
                    tracing::warn!(layer=%self, "scheduling deletion on drop failed: {e:#}");
                }
            }
        } else {
            // no need to nag that timeline is gone: under normal situation on
            // task_mgr::remove_tenant_from_memory the timeline is gone before we get dropped.
        }
    }
}

impl LayerInner {
    fn new(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        access_stats: LayerAccessStats,
        desc: PersistentLayerDesc,
        downloaded: Option<Arc<DownloadedLayer>>,
    ) -> Self {
        let path = conf
            .timeline_path(&timeline.tenant_id, &timeline.timeline_id)
            .join(desc.filename().to_string());

        LayerInner {
            conf,
            path,
            desc,
            timeline: Arc::downgrade(timeline),
            have_remote_client: timeline.remote_client.is_some(),
            access_stats,
            wanted_garbage_collected: AtomicBool::new(false),
            wanted_evicted: AtomicBool::new(false),
            inner: if let Some(inner) = downloaded {
                heavier_once_cell::OnceCell::new(ResidentOrWantedEvicted::Resident(inner))
            } else {
                heavier_once_cell::OnceCell::default()
            },
            version: AtomicUsize::new(0),
            status: tokio::sync::broadcast::channel(1).0,
        }
    }

    fn garbage_collect(&self) {
        self.wanted_garbage_collected.store(true, Ordering::Release);
    }

    pub(crate) async fn evict_and_wait(
        &self,
        _: &RemoteTimelineClient,
    ) -> Result<(), EvictionError> {
        use tokio::sync::broadcast::error::RecvError;

        assert!(self.have_remote_client);

        self.wanted_evicted.store(true, Ordering::Release);

        let mut rx = self.status.subscribe();

        // why call get instead of looking at the watch? because get will downgrade any
        // Arc<_> it finds, because we set the wanted_evicted
        if self.get().is_none() {
            // it was not evictable in the first place
            // our store to the wanted_evicted does not matter; it will be reset by next download
            return Err(EvictionError::NotFound);
        }

        match rx.recv().await {
            Ok(Status::Evicted) => Ok(()),
            Ok(Status::Downloaded) => Err(EvictionError::Downloaded),
            Err(RecvError::Closed) => {
                unreachable!("sender cannot be dropped while we are in &self method")
            }
            Err(RecvError::Lagged(_)) => {
                // this is quite unlikely, but we are blocking a lot in the async context, so
                // we might be missing this because we are stuck on a LIFO slot on a thread
                // which is busy blocking for a 1TB database create_image_layers.
                //
                // use however late (compared to the initial expressing of wanted) as the
                // "outcome" now
                match self.get() {
                    Some(_) => Err(EvictionError::Downloaded),
                    None => Ok(()),
                }
            }
        }
    }

    /// Access the current state without waiting for the file to be downloaded.
    ///
    /// Used by eviction only. Requires that we've initialized to state which is respective to the
    /// actual residency state.
    fn get(&self) -> Option<Arc<DownloadedLayer>> {
        let locked = self.inner.get();
        Self::get_or_apply_evictedness(locked, &self.wanted_evicted)
    }

    fn get_or_apply_evictedness(
        guard: Option<heavier_once_cell::Guard<'_, ResidentOrWantedEvicted>>,
        wanted_evicted: &AtomicBool,
    ) -> Option<Arc<DownloadedLayer>> {
        if let Some(mut x) = guard {
            if let Some(won) = x.get() {
                // there are no guarantees that we will always get to observe a concurrent call
                // to evict
                if wanted_evicted.load(Ordering::Acquire) {
                    x.downgrade();
                }
                return Some(won);
            }
        }

        None
    }

    /// Cancellation safe.
    async fn get_or_maybe_download(
        self: &Arc<Self>,
        allow_download: bool,
        ctx: Option<&RequestContext>,
    ) -> Result<Arc<DownloadedLayer>, DownloadError> {
        let download = move || async move {
            // disable any scheduled but not yet running eviction deletions for this
            self.version.fetch_add(1, Ordering::Relaxed);

            // no need to make the evict_and_wait wait for the actual download to complete
            drop(self.status.send(Status::Downloaded));

            // technically the mutex could be dropped here.
            let timeline = self
                .timeline
                .upgrade()
                .ok_or_else(|| DownloadError::TimelineShutdown)?;

            let can_ever_evict = timeline.remote_client.as_ref().is_some();

            // check if we really need to be downloaded; could have been already downloaded by a
            // cancelled previous attempt.
            let needs_download = self
                .needs_download()
                .await
                .map_err(DownloadError::PreStatFailed)?;

            if let Some(reason) = needs_download {
                // only reset this after we've decided we really need to download. otherwise it'd
                // be impossible to mark cancelled downloads for eviction, like one could imagine
                // we would like to do for prefetching which was not needed.
                self.wanted_evicted.store(false, Ordering::Release);

                if !can_ever_evict {
                    return Err(DownloadError::NoRemoteStorage);
                }

                if self.wanted_garbage_collected.load(Ordering::Acquire) {
                    // it will fail because we should had already scheduled a delete and an
                    // index update
                    tracing::info!(%reason, "downloading a wanted garbage collected layer, this might fail");
                    // FIXME: we probably do not gc delete until the file goes away...? unsure
                } else {
                    tracing::debug!(%reason, "downloading layer");
                }

                if let Some(ctx) = ctx {
                    use crate::context::DownloadBehavior::*;
                    let b = ctx.download_behavior();
                    match b {
                        Download => {}
                        Warn | Error => {
                            tracing::warn!(
                                "unexpectedly on-demand downloading remote layer {self} for task kind {:?}",
                                ctx.task_kind()
                            );
                            crate::metrics::UNEXPECTED_ONDEMAND_DOWNLOADS.inc();

                            let really_error = matches!(b, Error)
                                && !self.conf.ondemand_download_behavior_treat_error_as_warn;

                            if really_error {
                                // this check is only probablistic, seems like flakyness footgun
                                return Err(DownloadError::ContextAndConfigReallyDeniesDownloads);
                            }
                        }
                    }
                }

                if !allow_download {
                    // this does look weird, but for LayerInner the "downloading" means also changing
                    // internal once related state ...
                    return Err(DownloadError::DownloadRequired);
                }

                let task_name = format!("download layer {}", self);

                let (tx, rx) = tokio::sync::oneshot::channel();
                // this is sadly needed because of task_mgr::shutdown_tasks, otherwise we cannot
                // block tenant::mgr::remove_tenant_from_memory.

                let this: Arc<Self> = self.clone();
                crate::task_mgr::spawn(
                    &tokio::runtime::Handle::current(),
                    crate::task_mgr::TaskKind::RemoteDownloadTask,
                    Some(self.desc.tenant_id),
                    Some(self.desc.timeline_id),
                    &task_name,
                    false,
                    async move {
                        let client = timeline
                            .remote_client
                            .as_ref()
                            .expect("checked above with have_remote_client");

                        let result = client.download_layer_file(
                            &this.desc.filename(),
                            &LayerFileMetadata::new(
                                this.desc.file_size,
                            ),
                        )
                        .await;

                        let result = match result {
                            Ok(size) => {
                                timeline.metrics.resident_physical_size_gauge.add(size);
                                Ok(())
                            }
                            Err(e) => {
                                Err(e)
                            }
                        };

                        if let Err(res) = tx.send(result) {
                            match res {
                                Ok(()) => {
                                    // our caller is cancellation safe so this is fine; if someone
                                    // else requests the layer, they'll find it already downloaded
                                    // or redownload.
                                    //
                                    // however, could be that we should consider marking the layer
                                    // for eviction? alas, cannot: because only DownloadedLayer
                                    // will handle that.
                                },
                                Err(e) => {
                                    // our caller is cancellation safe, but we might be racing with
                                    // another attempt to reinitialize. before we have cancellation
                                    // token support: these attempts should converge regardless of
                                    // their completion order.
                                    tracing::error!("layer file download failed, and additionally failed to communicate this to caller: {e:?}");
                                }
                            }
                        }

                        Ok(())
                    }
                    .in_current_span(),
                );
                match rx.await {
                    Ok(Ok(())) => {
                        if let Some(reason) = self
                            .needs_download()
                            .await
                            .map_err(DownloadError::PostStatFailed)?
                        {
                            // this is really a bug in needs_download or remote timeline client
                            panic!("post-condition failed: needs_download returned {reason:?}");
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::error!("layer file download failed: {e:#}");
                        return Err(DownloadError::DownloadFailed);
                        // FIXME: we need backoff here so never spiral to download loop, maybe,
                        // because remote timeline client already retries
                    }
                    Err(_gone) => {
                        return Err(DownloadError::DownloadCancelled);
                    }
                }
            } else {
                // the file is present locally and we could even be running without remote
                // storage
            }

            let res = Arc::new(DownloadedLayer {
                owner: Arc::downgrade(self),
                kind: tokio::sync::OnceCell::default(),
            });

            self.access_stats.record_residence_event(
                LayerResidenceStatus::Resident,
                LayerResidenceEventReason::ResidenceChange,
            );

            Ok(if self.wanted_evicted.load(Ordering::Acquire) {
                // because we reset wanted_evictness earlier, this most likely means when we were downloading someone
                // wanted to evict this layer.
                ResidentOrWantedEvicted::WantedEvicted(Arc::downgrade(&res))
            } else {
                ResidentOrWantedEvicted::Resident(res.clone())
            })
        };

        let locked = self.inner.get_or_init(download).await?;

        Ok(
            Self::get_or_apply_evictedness(Some(locked), &self.wanted_evicted)
                .expect("It is not none, we just received it"),
        )
    }

    async fn needs_download(&self) -> Result<Option<NeedsDownload>, std::io::Error> {
        match tokio::fs::metadata(&self.path).await {
            Ok(m) => Ok(self.is_file_present_and_good_size(&m)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(NeedsDownload::NotFound)),
            Err(e) => Err(e),
        }
    }

    fn needs_download_blocking(&self) -> Result<Option<NeedsDownload>, std::io::Error> {
        match self.path.metadata() {
            Ok(m) => Ok(self.is_file_present_and_good_size(&m)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(NeedsDownload::NotFound)),
            Err(e) => Err(e),
        }
    }

    fn is_file_present_and_good_size(&self, m: &std::fs::Metadata) -> Option<NeedsDownload> {
        // in future, this should include sha2-256 the file, hopefully rarely, because info uses
        // this as well
        if !m.is_file() {
            Some(NeedsDownload::NotFile)
        } else if m.len() != self.desc.file_size {
            Some(NeedsDownload::WrongSize {
                actual: m.len(),
                expected: self.desc.file_size,
            })
        } else {
            None
        }
    }

    fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        let layer_file_name = self.desc.filename().file_name();

        let remote = self
            .needs_download_blocking()
            .map(|maybe| maybe.is_some())
            .unwrap_or(true);

        let access_stats = self.access_stats.as_api_model(reset);

        if self.desc.is_delta {
            let lsn_range = &self.desc.lsn_range;

            HistoricLayerInfo::Delta {
                layer_file_name,
                layer_file_size: self.desc.file_size,
                lsn_start: lsn_range.start,
                lsn_end: lsn_range.end,
                remote,
                access_stats,
            }
        } else {
            let lsn = self.desc.image_layer_lsn();

            HistoricLayerInfo::Image {
                layer_file_name,
                layer_file_size: self.desc.file_size,
                lsn_start: lsn,
                remote,
                access_stats,
            }
        }
    }

    /// Our resident layer has been dropped, we might hold the lock elsewhere.
    fn on_drop(self: Arc<LayerInner>) {
        let gc = self.wanted_garbage_collected.load(Ordering::Acquire);
        let evict = self.wanted_evicted.load(Ordering::Acquire);
        let can_evict = self.have_remote_client;

        if gc {
            // do nothing now, only when the whole layer is dropped. gc will end up deleting the
            // whole layer, in case there is no reference cycle.
        } else if can_evict && evict {
            // we can remove this right now, but ... we really should not block or do anything.
            // spawn a task which first does a version check, and that version is also incremented
            // on get_or_download, so we will not collide?
            let version = self.version.load(Ordering::Relaxed);

            let span = tracing::info_span!(parent: None, "layer_evict", tenant_id = %self.desc.tenant_id, timeline_id = %self.desc.timeline_id, layer=%self);

            // downgrade in case there's a queue backing up, or we are just tearing stuff down, and
            // would soon delete anyways.
            let this = Arc::downgrade(&self);
            drop(self);

            let eviction = {
                let span = tracing::info_span!(parent: span.clone(), "blocking");
                async move {
                    // the layer is already gone, don't do anything. LayerInner drop has already ran.
                    let Some(this) = this.upgrade() else { return; };

                    // deleted or detached timeline, don't do anything.
                    let Some(timeline) = this.timeline.upgrade() else { return; };

                    // to avoid starting a new download while we evict, keep holding on to the
                    // permit. note that we will not close the semaphore when done, because it will
                    // be used by the re-download.
                    let _permit = {
                        let maybe_downloaded = this.inner.get();
                        // relaxed ordering: we dont have any other atomics pending
                        if version != this.version.load(Ordering::Relaxed) {
                            // downloadness-state has advanced, we might no longer be the latest eviction
                            // work; don't do anything.
                            return;
                        }

                        // free the DownloadedLayer allocation
                        match maybe_downloaded.map(|mut g| g.take_and_deinit()) {
                            Some((taken, permit)) => {
                                assert!(matches!(taken, ResidentOrWantedEvicted::WantedEvicted(_)));
                                permit
                            }
                            None => {
                                unreachable!("we do the version checking for this exact reason")
                            }
                        }
                    };

                    if !this.wanted_evicted.load(Ordering::Acquire) {
                        // if there's already interest, should we just early exit? this is not
                        // currently *cleared* on interest, maybe it shouldn't?
                        // FIXME: wanted_evicted cannot be unset right now
                        //
                        // NOTE: us holding the permit prevents a new round of download happening
                        // right now
                        return;
                    }

                    let path = this.path.to_owned();

                    let capture_mtime_and_delete = tokio::task::spawn_blocking({
                        let span = span.clone();
                        move || {
                            let _e = span.entered();
                            // FIXME: we can now initialize the mtime during first get_or_download,
                            // and track that in-memory for the following? does that help?
                            let m = path.metadata()?;
                            let local_layer_mtime = m.modified()?;
                            std::fs::remove_file(&path)?;
                            Ok::<_, std::io::Error>(local_layer_mtime)
                        }
                    });

                    let res = capture_mtime_and_delete.await;

                    this.access_stats.record_residence_event(LayerResidenceStatus::Evicted, LayerResidenceEventReason::ResidenceChange);

                    drop(this.status.send(Status::Evicted));

                    match res {
                        Ok(Ok(local_layer_mtime)) => {
                            let duration =
                                std::time::SystemTime::now().duration_since(local_layer_mtime);
                            match duration {
                                Ok(elapsed) => {
                                    timeline
                                        .metrics
                                        .evictions_with_low_residence_duration
                                        .read()
                                        .unwrap()
                                        .observe(elapsed);
                                    tracing::info!(
                                        residence_millis = elapsed.as_millis(),
                                        "evicted layer after known residence period"
                                    );
                                }
                                Err(_) => {
                                    tracing::info!("evicted layer after unknown residence period");
                                }
                            }
                            timeline
                                .metrics
                                .resident_physical_size_gauge
                                .sub(this.desc.file_size);
                        }
                        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                            tracing::info!("failed to evict file from disk, it was already gone");
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("failed to evict file from disk: {e:#}");
                        }
                        Err(je) if je.is_cancelled() => unreachable!("unsupported"),
                        Err(je) if je.is_panic() => { /* already logged */ }
                        Err(je) => {
                            tracing::warn!(error = ?je, "unexpected join_error while evicting the file")
                        }
                    }
                }
            }
            .instrument(span);

            crate::task_mgr::BACKGROUND_RUNTIME.spawn(eviction);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EvictionError {
    #[error("layer was already evicted")]
    NotFound,

    /// Evictions must always lose to downloads in races, and this time it happened.
    #[error("layer was downloaded instead")]
    Downloaded,
}

#[derive(Debug, thiserror::Error)]
enum DownloadError {
    #[error("timeline has already shutdown")]
    TimelineShutdown,
    #[error("no remote storage configured")]
    NoRemoteStorage,
    #[error("context denies downloading")]
    ContextAndConfigReallyDeniesDownloads,
    #[error("downloading is really required but not allowed by this method")]
    DownloadRequired,
    /// Why no error here? Because it will be reported by page_service. We should had also done
    /// retries already.
    #[error("downloading evicted layer file failed")]
    DownloadFailed,
    #[error("downloading failed, possibly for shutdown")]
    DownloadCancelled,
    #[error("pre-condition: stat before download failed")]
    PreStatFailed(#[source] std::io::Error),
    #[error("post-condition: stat after download failed")]
    PostStatFailed(#[source] std::io::Error),
}

#[derive(Debug, PartialEq)]
pub(crate) enum NeedsDownload {
    NotFound,
    NotFile,
    WrongSize { actual: u64, expected: u64 },
}

impl std::fmt::Display for NeedsDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NeedsDownload::NotFound => write!(f, "file was not found"),
            NeedsDownload::NotFile => write!(f, "path is not a file"),
            NeedsDownload::WrongSize { actual, expected } => {
                write!(f, "file size mismatch {actual} vs. {expected}")
            }
        }
    }
}

/// Holds both Arc requriring that both components stay resident while holding this alive and no evictions
/// nor garbage collection happens.
#[derive(Clone)]
pub(crate) struct ResidentLayer {
    owner: Layer,
    downloaded: Arc<DownloadedLayer>,
}

impl std::fmt::Display for ResidentLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.owner)
    }
}

impl std::fmt::Debug for ResidentLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.owner)
    }
}

impl ResidentLayer {
    pub(crate) fn drop_eviction_guard(self) -> Layer {
        self.into()
    }

    /// Loads all keys stored in the layer. Returns key, lsn and value size.
    pub(crate) async fn load_keys(
        &self,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<DeltaEntry<'_>>> {
        use LayerKind::*;

        let inner = &self.owner.0;

        match self.downloaded.get(inner).await? {
            Delta(d) => {
                inner
                    .access_stats
                    .record_access(LayerAccessKind::KeyIter, ctx);

                // this is valid because the DownloadedLayer::kind is a OnceCell, not a
                // Mutex<OnceCell>, so we cannot go and deinitialize the value with OnceCell::take
                // while it's being held.
                d.load_keys().await.context("Layer index is corrupted")
            }
            Image(_) => anyhow::bail!("cannot load_keys on a image layer"),
        }
    }

    pub(crate) fn local_path(&self) -> &Path {
        &self.owner.0.path
    }

    pub(crate) fn access_stats(&self) -> &LayerAccessStats {
        self.owner.access_stats()
    }
}

impl AsLayerDesc for ResidentLayer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        self.owner.layer_desc()
    }
}

impl AsRef<Layer> for ResidentLayer {
    fn as_ref(&self) -> &Layer {
        &self.owner
    }
}

/// Allow slimming down if we don't want the `2*usize` with eviction candidates?
impl From<ResidentLayer> for Layer {
    fn from(value: ResidentLayer) -> Self {
        value.owner
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Layer has been removed from LayerMap already")]
pub(crate) struct RemovedFromLayerMap;

/// Holds the actual downloaded layer, and handles evicting the file on drop.
pub(crate) struct DownloadedLayer {
    owner: Weak<LayerInner>,
    kind: tokio::sync::OnceCell<anyhow::Result<LayerKind>>,
}

impl std::fmt::Debug for DownloadedLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadedLayer")
            // FIXME: this is not useful, always "Weak"
            .field("owner", &self.owner)
            .field("kind", &self.kind)
            .finish()
    }
}

impl Drop for DownloadedLayer {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.on_drop();
        } else {
            // no need to do anything, we are shutting down
        }
    }
}

impl DownloadedLayer {
    async fn get(&self, owner: &LayerInner) -> anyhow::Result<&LayerKind> {
        // the owner is required so that we don't have to upgrade the self.owner, which will only
        // be used on drop. this way, initializing a DownloadedLayer without an owner is statically
        // impossible, so we can just not worry about it.
        let init = || async {
            // there is nothing async here, but it should be async
            if owner.desc.is_delta {
                let summary = Some(delta_layer::Summary::expected(
                    owner.desc.tenant_id,
                    owner.desc.timeline_id,
                    owner.desc.key_range.clone(),
                    owner.desc.lsn_range.clone(),
                ));
                delta_layer::DeltaLayerInner::load(&owner.path, summary).map(LayerKind::Delta)
            } else {
                let lsn = owner.desc.image_layer_lsn();
                let summary = Some(image_layer::Summary::expected(
                    owner.desc.tenant_id,
                    owner.desc.timeline_id,
                    owner.desc.key_range.clone(),
                    lsn,
                ));
                image_layer::ImageLayerInner::load(&owner.path, lsn, summary).map(LayerKind::Image)
            }
            // this will be a permanent failure
            .context("load layer")
        };
        self.kind.get_or_init(init).await.as_ref().map_err(|e| {
            // errors are not clonabled, cannot but stringify
            // test_broken_timeline matches this string
            anyhow::anyhow!("layer loading failed: {e:#}")
        })
    }

    async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
        owner: &LayerInner,
    ) -> anyhow::Result<ValueReconstructResult> {
        use LayerKind::*;

        match self.get(owner).await? {
            Delta(d) => {
                d.get_value_reconstruct_data(key, lsn_range, reconstruct_data)
                    .await
            }
            Image(i) => i.get_value_reconstruct_data(key, reconstruct_data).await,
        }
    }
}

/// Wrapper around an actual layer implementation.
#[derive(Debug)]
enum LayerKind {
    Delta(delta_layer::DeltaLayerInner),
    Image(image_layer::ImageLayerInner),
}
