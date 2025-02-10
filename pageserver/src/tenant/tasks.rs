//! This module contains per-tenant background processes, e.g. compaction and GC.

use std::cmp::max;
use std::future::Future;
use std::ops::{ControlFlow, RangeInclusive};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use once_cell::sync::Lazy;
use rand::Rng;
use scopeguard::defer;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_util::sync::CancellationToken;
use tracing::*;

use crate::context::{DownloadBehavior, RequestContext};
use crate::metrics::{BackgroundLoopSemaphoreMetricsRecorder, TENANT_TASK_EVENTS};
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME, TOKIO_WORKER_THREADS};
use crate::tenant::throttle::Stats;
use crate::tenant::timeline::compaction::CompactionOutcome;
use crate::tenant::timeline::CompactionError;
use crate::tenant::{Tenant, TenantState};
use pageserver_api::config::tenant_conf_defaults::DEFAULT_COMPACTION_PERIOD;
use utils::backoff::exponential_backoff_duration;
use utils::completion::Barrier;
use utils::pausable_failpoint;
use utils::rate_limit::RateLimit;

/// Semaphore limiting concurrent background tasks (across all tenants).
///
/// We use 3/4 Tokio threads, to avoid blocking all threads in case we do any CPU-heavy work.
static CONCURRENT_BACKGROUND_TASKS: Lazy<Semaphore> = Lazy::new(|| {
    let total_threads = TOKIO_WORKER_THREADS.get();
    let permits = max(1, (total_threads * 3).checked_div(4).unwrap_or(0));
    assert_ne!(permits, 0, "we will not be adding in permits later");
    assert!(permits < total_threads, "need threads for other work");
    Semaphore::new(permits)
});

/// Semaphore limiting concurrent compaction tasks (across all tenants). This is disabled by
/// default, see `use_compaction_semaphore`.
///
/// We use 3/4 Tokio threads, to avoid blocking all threads in case we do any CPU-heavy work.
///
/// This is a separate semaphore from background tasks, because L0 compaction needs to be responsive
/// to avoid high read amp during heavy write workloads.
///
/// TODO: split image compaction and L0 compaction, and move image compaction to background tasks.
/// Only L0 compaction needs to be responsive, and it shouldn't block on image compaction.
static CONCURRENT_COMPACTION_TASKS: Lazy<Semaphore> = Lazy::new(|| {
    let total_threads = TOKIO_WORKER_THREADS.get();
    let permits = max(1, (total_threads * 3).checked_div(4).unwrap_or(0));
    assert_ne!(permits, 0, "we will not be adding in permits later");
    assert!(permits < total_threads, "need threads for other work");
    Semaphore::new(permits)
});

/// Background jobs.
///
/// NB: not all of these acquire a CONCURRENT_BACKGROUND_TASKS semaphore permit, only the ones that
/// do any significant IO.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    strum_macros::IntoStaticStr,
    strum_macros::Display,
    enum_map::Enum,
)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum BackgroundLoopKind {
    Compaction,
    Gc,
    Eviction,
    TenantHouseKeeping,
    ConsumptionMetricsCollectMetrics,
    ConsumptionMetricsSyntheticSizeWorker,
    InitialLogicalSizeCalculation,
    HeatmapUpload,
    SecondaryDownload,
}

pub struct BackgroundLoopSemaphorePermit<'a> {
    _permit: SemaphorePermit<'static>,
    _recorder: BackgroundLoopSemaphoreMetricsRecorder<'a>,
}

/// Acquires a semaphore permit, to limit concurrent background jobs.
pub(crate) async fn acquire_concurrency_permit(
    loop_kind: BackgroundLoopKind,
    use_compaction_semaphore: bool,
    _ctx: &RequestContext,
) -> BackgroundLoopSemaphorePermit<'static> {
    // TODO: use a lower threshold and remove the pacer once we resolve some blockage.
    const WARN_THRESHOLD: Duration = Duration::from_secs(600);
    static WARN_PACER: Lazy<Mutex<RateLimit>> =
        Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(10))));

    let mut recorder = crate::metrics::BACKGROUND_LOOP_SEMAPHORE.record(loop_kind);

    if loop_kind == BackgroundLoopKind::InitialLogicalSizeCalculation {
        pausable_failpoint!("initial-size-calculation-permit-pause");
    }

    // TODO: assert that we run on BACKGROUND_RUNTIME; requires tokio_unstable Handle::id();
    let permit = if loop_kind == BackgroundLoopKind::Compaction && use_compaction_semaphore {
        CONCURRENT_COMPACTION_TASKS.acquire().await
    } else {
        assert!(!use_compaction_semaphore);
        CONCURRENT_BACKGROUND_TASKS.acquire().await
    }
    .expect("should never close");

    let waited = recorder.acquired();
    if waited >= WARN_THRESHOLD {
        let waited = waited.as_secs_f64();
        WARN_PACER
            .lock()
            .unwrap()
            .call(|| warn!("{loop_kind} task waited {waited:.3}s for semaphore permit"));
    }

    BackgroundLoopSemaphorePermit {
        _permit: permit,
        _recorder: recorder,
    }
}

/// Start per tenant background loops: compaction, GC, and ingest housekeeping.
pub fn start_background_loops(tenant: &Arc<Tenant>, can_start: Option<&Barrier>) {
    let tenant_shard_id = tenant.tenant_shard_id;

    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::Compaction,
        tenant_shard_id,
        None,
        &format!("compactor for tenant {tenant_shard_id}"),
        {
            let tenant = Arc::clone(tenant);
            let can_start = can_start.cloned();
            async move {
                let cancel = task_mgr::shutdown_token(); // NB: must be in async context
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = Barrier::maybe_wait(can_start) => {}
                };
                TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
                defer!(TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc());
                compaction_loop(tenant, cancel)
                    // If you rename this span, change the RUST_LOG env variable in test_runner/performance/test_branch_creation.py
                    .instrument(info_span!("compaction_loop", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug()))
                    .await;
                Ok(())
            }
        },
    );

    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::GarbageCollector,
        tenant_shard_id,
        None,
        &format!("garbage collector for tenant {tenant_shard_id}"),
        {
            let tenant = Arc::clone(tenant);
            let can_start = can_start.cloned();
            async move {
                let cancel = task_mgr::shutdown_token(); // NB: must be in async context
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = Barrier::maybe_wait(can_start) => {}
                };
                TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
                defer!(TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc());
                gc_loop(tenant, cancel)
                    .instrument(info_span!("gc_loop", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug()))
                    .await;
                Ok(())
            }
        },
    );

    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::TenantHousekeeping,
        tenant_shard_id,
        None,
        &format!("housekeeping for tenant {tenant_shard_id}"),
        {
            let tenant = Arc::clone(tenant);
            let can_start = can_start.cloned();
            async move {
                let cancel = task_mgr::shutdown_token(); // NB: must be in async context
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = Barrier::maybe_wait(can_start) => {}
                };
                TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
                defer!(TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc());
                tenant_housekeeping_loop(tenant, cancel)
                    .instrument(info_span!("tenant_housekeeping_loop", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug()))
                    .await;
                Ok(())
            }
        },
    );
}

/// Compaction task's main loop.
async fn compaction_loop(tenant: Arc<Tenant>, cancel: CancellationToken) {
    const BASE_BACKOFF_SECS: f64 = 1.0;
    const MAX_BACKOFF_SECS: f64 = 300.0;
    const RECHECK_CONFIG_INTERVAL: Duration = Duration::from_secs(10);

    let ctx = RequestContext::todo_child(TaskKind::Compaction, DownloadBehavior::Download);
    let mut period = tenant.get_compaction_period();
    let mut error_run = 0; // consecutive errors

    // Stagger the compaction loop across tenants.
    if wait_for_active_tenant(&tenant, &cancel).await.is_break() {
        return;
    }
    if sleep_random(period, &cancel).await.is_err() {
        return;
    }

    loop {
        // Recheck that we're still active.
        if wait_for_active_tenant(&tenant, &cancel).await.is_break() {
            return;
        }

        // Refresh the period. If compaction is disabled, check again in a bit.
        period = tenant.get_compaction_period();
        if period == Duration::ZERO {
            #[cfg(not(feature = "testing"))]
            info!("automatic compaction is disabled");
            tokio::select! {
                _ = tokio::time::sleep(RECHECK_CONFIG_INTERVAL) => {},
                _ = cancel.cancelled() => return,
            }
            continue;
        }

        // Wait for the next compaction run.
        let backoff = exponential_backoff_duration(error_run, BASE_BACKOFF_SECS, MAX_BACKOFF_SECS);
        tokio::select! {
            _ = tokio::time::sleep(backoff), if error_run > 0 => {},
            _ = tokio::time::sleep(period), if error_run == 0 => {},
            _ = tenant.l0_compaction_trigger.notified(), if error_run == 0 => {},
            _ = cancel.cancelled() => return,
        }

        // Run compaction.
        let iteration = Iteration {
            started_at: Instant::now(),
            period,
            kind: BackgroundLoopKind::Compaction,
        };
        let IterationResult { output, elapsed } = iteration
            .run(tenant.compaction_iteration(&cancel, &ctx))
            .await;

        match output {
            Ok(outcome) => {
                error_run = 0;
                // If there's more compaction work pending, reschedule immediately. This isn't
                // necessarily L0 compaction, but that's fine for now.
                //
                // TODO: differentiate between L0 compaction and other compaction. The former needs
                // to be responsive, the latter doesn't.
                if outcome == CompactionOutcome::Pending {
                    tenant.l0_compaction_trigger.notify_one();
                }
            }

            Err(err) => {
                error_run += 1;
                let backoff =
                    exponential_backoff_duration(error_run, BASE_BACKOFF_SECS, MAX_BACKOFF_SECS);
                log_compaction_error(&err, error_run, backoff, cancel.is_cancelled());
                continue;
            }
        }

        // NB: this log entry is recorded by performance tests.
        debug!(
            elapsed_ms = elapsed.as_millis(),
            "compaction iteration complete"
        );
    }
}

fn log_compaction_error(
    err: &CompactionError,
    error_count: u32,
    sleep_duration: Duration,
    task_cancelled: bool,
) {
    use crate::tenant::upload_queue::NotInitialized;
    use crate::tenant::PageReconstructError;
    use CompactionError::*;

    let level = match err {
        ShuttingDown => return,
        Offload(_) => Level::ERROR,
        _ if task_cancelled => Level::INFO,
        Other(err) => {
            let root_cause = err.root_cause();

            let upload_queue = root_cause
                .downcast_ref::<NotInitialized>()
                .is_some_and(|e| e.is_stopping());
            let timeline = root_cause
                .downcast_ref::<PageReconstructError>()
                .is_some_and(|e| e.is_stopping());
            let is_stopping = upload_queue || timeline;

            if is_stopping {
                Level::INFO
            } else {
                Level::ERROR
            }
        }
    };

    match level {
        Level::ERROR => {
            error!("Compaction failed {error_count} times, retrying in {sleep_duration:?}: {err:#}")
        }
        Level::INFO => {
            info!("Compaction failed {error_count} times, retrying in {sleep_duration:?}: {err:#}")
        }
        level => unimplemented!("unexpected level {level:?}"),
    }
}

/// GC task's main loop.
async fn gc_loop(tenant: Arc<Tenant>, cancel: CancellationToken) {
    const MAX_BACKOFF_SECS: f64 = 300.0;
    let mut error_run = 0; // consecutive errors

    // GC might require downloading, to find the cutoff LSN that corresponds to the
    // cutoff specified as time.
    let ctx = RequestContext::todo_child(TaskKind::GarbageCollector, DownloadBehavior::Download);
    let mut first = true;

    loop {
        if wait_for_active_tenant(&tenant, &cancel).await.is_break() {
            return;
        }

        let period = tenant.get_gc_period();

        if first {
            first = false;
            if sleep_random(period, &cancel).await.is_err() {
                break;
            }
        }

        let gc_horizon = tenant.get_gc_horizon();
        let sleep_duration;
        if period == Duration::ZERO || gc_horizon == 0 {
            #[cfg(not(feature = "testing"))]
            info!("automatic GC is disabled");
            // check again in 10 seconds, in case it's been enabled again.
            sleep_duration = Duration::from_secs(10);
        } else {
            let iteration = Iteration {
                started_at: Instant::now(),
                period,
                kind: BackgroundLoopKind::Gc,
            };
            // Run gc
            let IterationResult { output, elapsed: _ } = iteration
                .run(tenant.gc_iteration(
                    None,
                    gc_horizon,
                    tenant.get_pitr_interval(),
                    &cancel,
                    &ctx,
                ))
                .await;
            match output {
                Ok(_) => {
                    error_run = 0;
                    sleep_duration = period;
                }
                Err(crate::tenant::GcError::TenantCancelled) => {
                    return;
                }
                Err(e) => {
                    error_run += 1;
                    let wait_duration =
                        exponential_backoff_duration(error_run, 1.0, MAX_BACKOFF_SECS);

                    if matches!(e, crate::tenant::GcError::TimelineCancelled) {
                        // Timeline was cancelled during gc. We might either be in an event
                        // that affects the entire tenant (tenant deletion, pageserver shutdown),
                        // or in one that affects the timeline only (timeline deletion).
                        // Therefore, don't exit the loop.
                        info!("Gc failed {error_run} times, retrying in {wait_duration:?}: {e:?}");
                    } else {
                        error!("Gc failed {error_run} times, retrying in {wait_duration:?}: {e:?}");
                    }

                    sleep_duration = wait_duration;
                }
            }
        };

        if tokio::time::timeout(sleep_duration, cancel.cancelled())
            .await
            .is_ok()
        {
            break;
        }
    }
}

/// Tenant housekeeping's main loop.
async fn tenant_housekeeping_loop(tenant: Arc<Tenant>, cancel: CancellationToken) {
    let mut last_throttle_flag_reset_at = Instant::now();
    loop {
        if wait_for_active_tenant(&tenant, &cancel).await.is_break() {
            return;
        }

        // Use the same period as compaction; it's not worth a separate setting. But if it's set to
        // zero (to disable compaction), then use a reasonable default. Jitter it by 5%.
        let period = match tenant.get_compaction_period() {
            Duration::ZERO => humantime::parse_duration(DEFAULT_COMPACTION_PERIOD).unwrap(),
            period => period,
        };

        let Ok(period) = sleep_jitter(period, period * 5 / 100, &cancel).await else {
            break;
        };

        // Do tenant housekeeping.
        let iteration = Iteration {
            started_at: Instant::now(),
            period,
            kind: BackgroundLoopKind::TenantHouseKeeping,
        };
        iteration.run(tenant.housekeeping()).await;

        // Log any getpage throttling.
        info_span!(parent: None, "pagestream_throttle", tenant_id=%tenant.tenant_shard_id, shard_id=%tenant.tenant_shard_id.shard_slug()).in_scope(|| {
            let now = Instant::now();
            let prev = std::mem::replace(&mut last_throttle_flag_reset_at, now);
            let Stats { count_accounted_start, count_accounted_finish, count_throttled, sum_throttled_usecs} = tenant.pagestream_throttle.reset_stats();
            if count_throttled == 0 {
                return;
            }
            let allowed_rps = tenant.pagestream_throttle.steady_rps();
            let delta = now - prev;
            info!(
                n_seconds=%format_args!("{:.3}", delta.as_secs_f64()),
                count_accounted = count_accounted_finish,  // don't break existing log scraping
                count_throttled,
                sum_throttled_usecs,
                count_accounted_start, // log after pre-existing fields to not break existing log scraping
                allowed_rps=%format_args!("{allowed_rps:.0}"),
                "shard was throttled in the last n_seconds"
            );
        });
    }
}

/// Waits until the tenant becomes active, or returns `ControlFlow::Break()` to shut down.
async fn wait_for_active_tenant(
    tenant: &Arc<Tenant>,
    cancel: &CancellationToken,
) -> ControlFlow<()> {
    if tenant.current_state() == TenantState::Active {
        return ControlFlow::Continue(());
    }

    let mut update_rx = tenant.subscribe_for_state_updates();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return ControlFlow::Break(()),
            result = update_rx.changed() => if result.is_err() {
                return ControlFlow::Break(());
            }
        }

        match &*update_rx.borrow() {
            TenantState::Active => {
                debug!("Tenant state changed to active, continuing the task loop");
                return ControlFlow::Continue(());
            }
            state => debug!("Not running the task loop, tenant is not active: {state:?}"),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("cancelled")]
pub(crate) struct Cancelled;

/// Sleeps for a random interval up to the given max value.
///
/// This delay prevents a thundering herd of background tasks and will likely keep them running on
/// different periods for more stable load.
pub(crate) async fn sleep_random(
    max: Duration,
    cancel: &CancellationToken,
) -> Result<Duration, Cancelled> {
    sleep_random_range(Duration::ZERO..=max, cancel).await
}

/// Sleeps for a random interval in the given range. Returns the duration.
pub(crate) async fn sleep_random_range(
    interval: RangeInclusive<Duration>,
    cancel: &CancellationToken,
) -> Result<Duration, Cancelled> {
    let delay = rand::thread_rng().gen_range(interval);
    if delay == Duration::ZERO {
        return Ok(delay);
    }
    tokio::select! {
        _ = cancel.cancelled() => Err(Cancelled),
        _ = tokio::time::sleep(delay) => Ok(delay),
    }
}

/// Sleeps for an interval with a random jitter.
pub(crate) async fn sleep_jitter(
    duration: Duration,
    jitter: Duration,
    cancel: &CancellationToken,
) -> Result<Duration, Cancelled> {
    let from = duration.saturating_sub(jitter);
    let to = duration.saturating_add(jitter);
    sleep_random_range(from..=to, cancel).await
}

struct Iteration {
    started_at: Instant,
    period: Duration,
    kind: BackgroundLoopKind,
}

struct IterationResult<O> {
    output: O,
    elapsed: Duration,
}

impl Iteration {
    #[instrument(skip_all)]
    pub(crate) async fn run<F: Future<Output = O>, O>(self, fut: F) -> IterationResult<O> {
        let mut fut = pin!(fut);

        // Wrap `fut` into a future that logs a message every `period` so that we get a
        // very obvious breadcrumb in the logs _while_ a slow iteration is happening.
        let output = loop {
            match tokio::time::timeout(self.period, &mut fut).await {
                Ok(r) => break r,
                Err(_) => info!("still running"),
            }
        };
        let elapsed = self.started_at.elapsed();
        warn_when_period_overrun(elapsed, self.period, self.kind);

        IterationResult { output, elapsed }
    }
}

// NB: the `task` and `period` are used for metrics labels.
pub(crate) fn warn_when_period_overrun(
    elapsed: Duration,
    period: Duration,
    task: BackgroundLoopKind,
) {
    // Duration::ZERO will happen because it's the "disable [bgtask]" value.
    if elapsed >= period && period != Duration::ZERO {
        // humantime does no significant digits clamping whereas Duration's debug is a bit more
        // intelligent. however it makes sense to keep the "configuration format" for period, even
        // though there's no way to output the actual config value.
        info!(
            ?elapsed,
            period = %humantime::format_duration(period),
            ?task,
            "task iteration took longer than the configured period"
        );
        crate::metrics::BACKGROUND_LOOP_PERIOD_OVERRUN_COUNT
            .with_label_values(&[task.into(), &format!("{}", period.as_secs())])
            .inc();
    }
}
