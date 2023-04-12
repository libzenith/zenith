use metrics::core::{AtomicU64, GenericCounter};
use metrics::{
    register_counter_vec, register_histogram, register_histogram_vec, register_int_counter,
    register_int_counter_vec, register_int_gauge, register_int_gauge_vec, register_uint_gauge_vec,
    Counter, CounterVec, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    UIntGauge, UIntGaugeVec,
};
use once_cell::sync::Lazy;
use pageserver_api::models::TenantState;
use strum::VariantNames;
use utils::id::{TenantId, TimelineId};

/// Prometheus histogram buckets (in seconds) for operations in the critical
/// path. In other words, operations that directly affect that latency of user
/// queries.
///
/// The buckets capture the majority of latencies in the microsecond and
/// millisecond range but also extend far enough up to distinguish "bad" from
/// "really bad".
const CRITICAL_OP_BUCKETS: &[f64] = &[
    0.000_001, 0.000_010, 0.000_100, // 1 us, 10 us, 100 us
    0.001_000, 0.010_000, 0.100_000, // 1 ms, 10 ms, 100 ms
    1.0, 10.0, 100.0, // 1 s, 10 s, 100 s
];

// Metrics collected on operations on the storage repository.
const STORAGE_TIME_OPERATIONS: &[&str] = &[
    "layer flush",
    "compact",
    "create images",
    "init logical size",
    "logical size",
    "load layer map",
    "gc",
];

pub static STORAGE_TIME_SUM_PER_TIMELINE: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "pageserver_storage_operations_seconds_sum",
        "Total time spent on storage operations with operation, tenant and timeline dimensions",
        &["operation", "tenant_id", "timeline_id"],
    )
    .expect("failed to define a metric")
});

pub static STORAGE_TIME_COUNT_PER_TIMELINE: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_storage_operations_seconds_count",
        "Count of storage operations with operation, tenant and timeline dimensions",
        &["operation", "tenant_id", "timeline_id"],
    )
    .expect("failed to define a metric")
});

// Buckets for background operations like compaction, GC, size calculation
const STORAGE_OP_BUCKETS: &[f64] = &[0.010, 0.100, 1.0, 10.0, 100.0, 1000.0];

pub static STORAGE_TIME_GLOBAL: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_storage_operations_seconds_global",
        "Time spent on storage operations",
        &["operation"],
        STORAGE_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

// Metrics collected on operations on the storage repository.
static RECONSTRUCT_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_getpage_reconstruct_seconds",
        "Time spent in reconstruct_value",
        &["tenant_id", "timeline_id"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

static MATERIALIZED_PAGE_CACHE_HIT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_materialized_cache_hits_total",
        "Number of cache hits from materialized page cache",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static WAIT_LSN_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_wait_lsn_seconds",
        "Time spent waiting for WAL to arrive",
        &["tenant_id", "timeline_id"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

static LAST_RECORD_LSN: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_last_record_lsn",
        "Last record LSN grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static RESIDENT_PHYSICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_resident_physical_size",
        "The size of the layer files present in the pageserver's filesystem.",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static REMOTE_PHYSICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_remote_physical_size",
        "The size of the layer files present in the remote storage that are listed in the the remote index_part.json.",
        // Corollary: If any files are missing from the index part, they won't be included here.
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

pub static REMOTE_ONDEMAND_DOWNLOADED_LAYERS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_remote_ondemand_downloaded_layers_total",
        "Total on-demand downloaded layers"
    )
    .unwrap()
});

pub static REMOTE_ONDEMAND_DOWNLOADED_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_remote_ondemand_downloaded_bytes_total",
        "Total bytes of layers on-demand downloaded",
    )
    .unwrap()
});

static CURRENT_LOGICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_current_logical_size",
        "Current logical size grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define current logical size metric")
});

pub static TENANT_STATE_METRIC: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_tenant_states_count",
        "Count of tenants per state",
        &["tenant_id", "state"]
    )
    .expect("Failed to register pageserver_tenant_states_count metric")
});

pub static TENANT_SYNTHETIC_SIZE_METRIC: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_tenant_synthetic_size",
        "Synthetic size of each tenant",
        &["tenant_id"]
    )
    .expect("Failed to register pageserver_tenant_synthetic_size metric")
});

// Metrics for cloud upload. These metrics reflect data uploaded to cloud storage,
// or in testing they estimate how much we would upload if we did.
static NUM_PERSISTENT_FILES_CREATED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_created_persistent_files_total",
        "Number of files created that are meant to be uploaded to cloud storage",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static PERSISTENT_BYTES_WRITTEN: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_written_persistent_bytes_total",
        "Total bytes written that are meant to be uploaded to cloud storage",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static EVICTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_evictions",
        "Number of layers evicted from the pageserver",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static EVICTIONS_WITH_LOW_RESIDENCE_DURATION: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_evictions_with_low_residence_duration",
        "If a layer is evicted that was resident for less than `low_threshold`, it is counted to this counter. \
         Residence duration is determined using the `residence_duration_data_source`.",
        &["tenant_id", "timeline_id", "residence_duration_data_source", "low_threshold_secs"]
    )
    .expect("failed to define a metric")
});

/// Each [`Timeline`]'s  [`EVICTIONS_WITH_LOW_RESIDENCE_DURATION`] metric.
#[derive(Debug)]
pub struct EvictionsWithLowResidenceDuration {
    data_source: &'static str,
    threshold: Duration,
    counter: Option<IntCounter>,
}

pub struct EvictionsWithLowResidenceDurationBuilder {
    data_source: &'static str,
    threshold: Duration,
}

impl EvictionsWithLowResidenceDurationBuilder {
    pub fn new(data_source: &'static str, threshold: Duration) -> Self {
        Self {
            data_source,
            threshold,
        }
    }

    fn build(&self, tenant_id: &str, timeline_id: &str) -> EvictionsWithLowResidenceDuration {
        let counter = EVICTIONS_WITH_LOW_RESIDENCE_DURATION
            .get_metric_with_label_values(&[
                tenant_id,
                timeline_id,
                self.data_source,
                &EvictionsWithLowResidenceDuration::threshold_label_value(self.threshold),
            ])
            .unwrap();
        EvictionsWithLowResidenceDuration {
            data_source: self.data_source,
            threshold: self.threshold,
            counter: Some(counter),
        }
    }
}

impl EvictionsWithLowResidenceDuration {
    fn threshold_label_value(threshold: Duration) -> String {
        format!("{}", threshold.as_secs())
    }

    pub fn observe(&self, observed_value: Duration) {
        if observed_value < self.threshold {
            self.counter
                .as_ref()
                .expect("nobody calls this function after `remove_from_vec`")
                .inc();
        }
    }

    // This could be a `Drop` impl, but, we need the `tenant_id` and `timeline_id`.
    fn remove(&mut self, tenant_id: &str, timeline_id: &str) {
        let Some(_counter) = self.counter.take() else {
            return;
        };
        EVICTIONS_WITH_LOW_RESIDENCE_DURATION
            .remove_label_values(&[
                tenant_id,
                timeline_id,
                self.data_source,
                &Self::threshold_label_value(self.threshold),
            ])
            .expect("we own the metric, no-one else should remove it");
    }
}

// Metrics collected on disk IO operations
//
// Roughly logarithmic scale.
const STORAGE_IO_TIME_BUCKETS: &[f64] = &[
    0.000030, // 30 usec
    0.001000, // 1000 usec
    0.030,    // 30 ms
    1.000,    // 1000 ms
];

const STORAGE_IO_TIME_OPERATIONS: &[&str] = &[
    "open", "close", "read", "write", "seek", "fsync", "gc", "metadata",
];

const STORAGE_IO_SIZE_OPERATIONS: &[&str] = &["read", "write"];

pub static STORAGE_IO_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_io_operations_seconds",
        "Time spent in IO operations",
        &["operation", "tenant_id", "timeline_id"],
        STORAGE_IO_TIME_BUCKETS.into()
    )
    .expect("failed to define a metric")
});

pub static STORAGE_IO_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_io_operations_bytes_total",
        "Total amount of bytes read/written in IO operations",
        &["operation", "tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

const SMGR_QUERY_TIME_OPERATIONS: &[&str] = &[
    "get_rel_exists",
    "get_rel_size",
    "get_page_at_lsn",
    "get_db_size",
];

pub static SMGR_QUERY_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_smgr_query_seconds",
        "Time spent on smgr query handling",
        &["smgr_query_type", "tenant_id", "timeline_id"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

pub static LIVE_CONNECTIONS_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_live_connections",
        "Number of live network connections",
        &["pageserver_connection_kind"]
    )
    .expect("failed to define a metric")
});

pub static NUM_ONDISK_LAYERS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("pageserver_ondisk_layers", "Number of layers on-disk")
        .expect("failed to define a metric")
});

// remote storage metrics

/// NB: increment _after_ recording the current value into [`REMOTE_TIMELINE_CLIENT_CALLS_STARTED_HIST`].
static REMOTE_TIMELINE_CLIENT_CALLS_UNFINISHED_GAUGE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_remote_timeline_client_calls_unfinished",
        "Number of ongoing calls to remote timeline client. \
         Used to populate pageserver_remote_timeline_client_calls_started. \
         This metric is not useful for sampling from Prometheus, but useful in tests.",
        &["tenant_id", "timeline_id", "file_kind", "op_kind"],
    )
    .expect("failed to define a metric")
});

static REMOTE_TIMELINE_CLIENT_CALLS_STARTED_HIST: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_remote_timeline_client_calls_started",
        "When calling a remote timeline client method, we record the current value \
         of the calls_unfinished gauge in this histogram. Plot the histogram \
         over time in a heatmap to visualize how many operations were ongoing \
         at a given instant. It gives you a better idea of the queue depth \
         than plotting the gauge directly, since operations may complete faster \
         than the sampling interval.",
        &["tenant_id", "timeline_id", "file_kind", "op_kind"],
        // The calls_unfinished gauge is an integer gauge, hence we have integer buckets.
        vec![0.0, 1.0, 2.0, 4.0, 6.0, 8.0, 10.0, 15.0, 20.0, 40.0, 60.0, 80.0, 100.0, 500.0],
    )
    .expect("failed to define a metric")
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemoteOpKind {
    Upload,
    Download,
    Delete,
}
impl RemoteOpKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Download => "download",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum RemoteOpFileKind {
    Layer,
    Index,
}
impl RemoteOpFileKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Layer => "layer",
            Self::Index => "index",
        }
    }
}

pub static REMOTE_OPERATION_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_remote_operation_seconds",
        "Time spent on remote storage operations. \
        Grouped by tenant, timeline, operation_kind and status. \
        Does not account for time spent waiting in remote timeline client's queues.",
        &["tenant_id", "timeline_id", "file_kind", "op_kind", "status"]
    )
    .expect("failed to define a metric")
});

pub static TENANT_TASK_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_tenant_task_events",
        "Number of task start/stop/fail events.",
        &["event"],
    )
    .expect("Failed to register tenant_task_events metric")
});

// Metrics collected on WAL redo operations
//
// We collect the time spent in actual WAL redo ('redo'), and time waiting
// for access to the postgres process ('wait') since there is only one for
// each tenant.

/// Time buckets are small because we want to be able to measure the
/// smallest redo processing times. These buckets allow us to measure down
/// to 5us, which equates to 200'000 pages/sec, which equates to 1.6GB/sec.
/// This is much better than the previous 5ms aka 200 pages/sec aka 1.6MB/sec.
///
/// Values up to 1s are recorded because metrics show that we have redo
/// durations and lock times larger than 0.250s.
macro_rules! redo_histogram_time_buckets {
    () => {
        vec![
            0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001_000,
            0.002_500, 0.005_000, 0.010_000, 0.025_000, 0.050_000, 0.100_000, 0.250_000, 0.500_000,
            1.000_000,
        ]
    };
}

/// While we're at it, also measure the amount of records replayed in each
/// operation. We have a global 'total replayed' counter, but that's not
/// as useful as 'what is the skew for how many records we replay in one
/// operation'.
macro_rules! redo_histogram_count_buckets {
    () => {
        vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0]
    };
}

macro_rules! redo_bytes_histogram_count_buckets {
    () => {
        // powers of (2^.5), from 2^4.5 to 2^15 (22 buckets)
        // rounded up to the next multiple of 8 to capture any MAXALIGNed record of that size, too.
        vec![
            24.0, 32.0, 48.0, 64.0, 96.0, 128.0, 184.0, 256.0, 368.0, 512.0, 728.0, 1024.0, 1456.0,
            2048.0, 2904.0, 4096.0, 5800.0, 8192.0, 11592.0, 16384.0, 23176.0, 32768.0,
        ]
    };
}

pub static WAL_REDO_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_seconds",
        "Time spent on WAL redo",
        redo_histogram_time_buckets!()
    )
    .expect("failed to define a metric")
});

pub static WAL_REDO_WAIT_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_wait_seconds",
        "Time spent waiting for access to the WAL redo process",
        redo_histogram_time_buckets!(),
    )
    .expect("failed to define a metric")
});

pub static WAL_REDO_RECORDS_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_records_histogram",
        "Histogram of number of records replayed per redo",
        redo_histogram_count_buckets!(),
    )
    .expect("failed to define a metric")
});

pub static WAL_REDO_BYTES_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_bytes_histogram",
        "Histogram of number of records replayed per redo",
        redo_bytes_histogram_count_buckets!(),
    )
    .expect("failed to define a metric")
});

pub static WAL_REDO_RECORD_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_replayed_wal_records_total",
        "Number of WAL records replayed in WAL redo process"
    )
    .unwrap()
});

/// Similar to [`prometheus::HistogramTimer`] but does not record on drop.
pub struct StorageTimeMetricsTimer {
    metrics: StorageTimeMetrics,
    start: Instant,
}

impl StorageTimeMetricsTimer {
    fn new(metrics: StorageTimeMetrics) -> Self {
        Self {
            metrics,
            start: Instant::now(),
        }
    }

    /// Record the time from creation to now.
    pub fn stop_and_record(self) {
        let duration = self.start.elapsed().as_secs_f64();
        self.metrics.timeline_sum.inc_by(duration);
        self.metrics.timeline_count.inc();
        self.metrics.global_histogram.observe(duration);
    }
}

/// Timing facilities for an globally histogrammed metric, which is supported by per tenant and
/// timeline total sum and count.
#[derive(Clone, Debug)]
pub struct StorageTimeMetrics {
    /// Sum of f64 seconds, per operation, tenant_id and timeline_id
    timeline_sum: Counter,
    /// Number of oeprations, per operation, tenant_id and timeline_id
    timeline_count: IntCounter,
    /// Global histogram having only the "operation" label.
    global_histogram: Histogram,
}

impl StorageTimeMetrics {
    pub fn new(operation: &str, tenant_id: &str, timeline_id: &str) -> Self {
        let timeline_sum = STORAGE_TIME_SUM_PER_TIMELINE
            .get_metric_with_label_values(&[operation, tenant_id, timeline_id])
            .unwrap();
        let timeline_count = STORAGE_TIME_COUNT_PER_TIMELINE
            .get_metric_with_label_values(&[operation, tenant_id, timeline_id])
            .unwrap();
        let global_histogram = STORAGE_TIME_GLOBAL
            .get_metric_with_label_values(&[operation])
            .unwrap();

        StorageTimeMetrics {
            timeline_sum,
            timeline_count,
            global_histogram,
        }
    }

    /// Starts timing a new operation.
    ///
    /// Note: unlike [`prometheus::HistogramTimer`] the returned timer does not record on drop.
    pub fn start_timer(&self) -> StorageTimeMetricsTimer {
        StorageTimeMetricsTimer::new(self.clone())
    }
}

#[derive(Debug)]
pub struct TimelineMetrics {
    tenant_id: String,
    timeline_id: String,
    pub reconstruct_time_histo: Histogram,
    pub materialized_page_cache_hit_counter: GenericCounter<AtomicU64>,
    pub flush_time_histo: StorageTimeMetrics,
    pub compact_time_histo: StorageTimeMetrics,
    pub create_images_time_histo: StorageTimeMetrics,
    pub logical_size_histo: StorageTimeMetrics,
    pub load_layer_map_histo: StorageTimeMetrics,
    pub garbage_collect_histo: StorageTimeMetrics,
    pub last_record_gauge: IntGauge,
    pub wait_lsn_time_histo: Histogram,
    pub resident_physical_size_gauge: UIntGauge,
    /// copy of LayeredTimeline.current_logical_size
    pub current_logical_size_gauge: UIntGauge,
    pub num_persistent_files_created: IntCounter,
    pub persistent_bytes_written: IntCounter,
    pub evictions: IntCounter,
    pub evictions_with_low_residence_duration: EvictionsWithLowResidenceDuration,
}

impl TimelineMetrics {
    pub fn new(
        tenant_id: &TenantId,
        timeline_id: &TimelineId,
        evictions_with_low_residence_duration_builder: EvictionsWithLowResidenceDurationBuilder,
    ) -> Self {
        let tenant_id = tenant_id.to_string();
        let timeline_id = timeline_id.to_string();
        let reconstruct_time_histo = RECONSTRUCT_TIME
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let materialized_page_cache_hit_counter = MATERIALIZED_PAGE_CACHE_HIT
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let flush_time_histo = StorageTimeMetrics::new("layer flush", &tenant_id, &timeline_id);
        let compact_time_histo = StorageTimeMetrics::new("compact", &tenant_id, &timeline_id);
        let create_images_time_histo =
            StorageTimeMetrics::new("create images", &tenant_id, &timeline_id);
        let logical_size_histo = StorageTimeMetrics::new("logical size", &tenant_id, &timeline_id);
        let load_layer_map_histo =
            StorageTimeMetrics::new("load layer map", &tenant_id, &timeline_id);
        let garbage_collect_histo = StorageTimeMetrics::new("gc", &tenant_id, &timeline_id);
        let last_record_gauge = LAST_RECORD_LSN
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let wait_lsn_time_histo = WAIT_LSN_TIME
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let resident_physical_size_gauge = RESIDENT_PHYSICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let current_logical_size_gauge = CURRENT_LOGICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let num_persistent_files_created = NUM_PERSISTENT_FILES_CREATED
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let persistent_bytes_written = PERSISTENT_BYTES_WRITTEN
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let evictions = EVICTIONS
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let evictions_with_low_residence_duration =
            evictions_with_low_residence_duration_builder.build(&tenant_id, &timeline_id);

        TimelineMetrics {
            tenant_id,
            timeline_id,
            reconstruct_time_histo,
            materialized_page_cache_hit_counter,
            flush_time_histo,
            compact_time_histo,
            create_images_time_histo,
            logical_size_histo,
            garbage_collect_histo,
            load_layer_map_histo,
            last_record_gauge,
            wait_lsn_time_histo,
            resident_physical_size_gauge,
            current_logical_size_gauge,
            num_persistent_files_created,
            persistent_bytes_written,
            evictions,
            evictions_with_low_residence_duration,
        }
    }
}

impl Drop for TimelineMetrics {
    fn drop(&mut self) {
        let tenant_id = &self.tenant_id;
        let timeline_id = &self.timeline_id;
        let _ = RECONSTRUCT_TIME.remove_label_values(&[tenant_id, timeline_id]);
        let _ = MATERIALIZED_PAGE_CACHE_HIT.remove_label_values(&[tenant_id, timeline_id]);
        let _ = LAST_RECORD_LSN.remove_label_values(&[tenant_id, timeline_id]);
        let _ = WAIT_LSN_TIME.remove_label_values(&[tenant_id, timeline_id]);
        let _ = RESIDENT_PHYSICAL_SIZE.remove_label_values(&[tenant_id, timeline_id]);
        let _ = CURRENT_LOGICAL_SIZE.remove_label_values(&[tenant_id, timeline_id]);
        let _ = NUM_PERSISTENT_FILES_CREATED.remove_label_values(&[tenant_id, timeline_id]);
        let _ = PERSISTENT_BYTES_WRITTEN.remove_label_values(&[tenant_id, timeline_id]);
        let _ = EVICTIONS.remove_label_values(&[tenant_id, timeline_id]);
        self.evictions_with_low_residence_duration
            .remove(tenant_id, timeline_id);
        for op in STORAGE_TIME_OPERATIONS {
            let _ =
                STORAGE_TIME_SUM_PER_TIMELINE.remove_label_values(&[op, tenant_id, timeline_id]);
            let _ =
                STORAGE_TIME_COUNT_PER_TIMELINE.remove_label_values(&[op, tenant_id, timeline_id]);
        }
        for op in STORAGE_IO_TIME_OPERATIONS {
            let _ = STORAGE_IO_TIME.remove_label_values(&[op, tenant_id, timeline_id]);
        }

        for op in STORAGE_IO_SIZE_OPERATIONS {
            let _ = STORAGE_IO_SIZE.remove_label_values(&[op, tenant_id, timeline_id]);
        }

        for op in SMGR_QUERY_TIME_OPERATIONS {
            let _ = SMGR_QUERY_TIME.remove_label_values(&[op, tenant_id, timeline_id]);
        }
    }
}

pub fn remove_tenant_metrics(tenant_id: &TenantId) {
    let tid = tenant_id.to_string();
    let _ = TENANT_SYNTHETIC_SIZE_METRIC.remove_label_values(&[&tid]);
    for state in TenantState::VARIANTS {
        let _ = TENANT_STATE_METRIC.remove_label_values(&[&tid, state]);
    }
}

use futures::Future;
use pin_project_lite::pin_project;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub struct RemoteTimelineClientMetrics {
    tenant_id: String,
    timeline_id: String,
    remote_physical_size_gauge: Mutex<Option<UIntGauge>>,
    remote_operation_time: Mutex<HashMap<(&'static str, &'static str, &'static str), Histogram>>,
    calls_unfinished_gauge: Mutex<HashMap<(&'static str, &'static str), IntGauge>>,
    calls_started_hist: Mutex<HashMap<(&'static str, &'static str), Histogram>>,
}

impl RemoteTimelineClientMetrics {
    pub fn new(tenant_id: &TenantId, timeline_id: &TimelineId) -> Self {
        RemoteTimelineClientMetrics {
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            remote_operation_time: Mutex::new(HashMap::default()),
            calls_unfinished_gauge: Mutex::new(HashMap::default()),
            calls_started_hist: Mutex::new(HashMap::default()),
            remote_physical_size_gauge: Mutex::new(None),
        }
    }
    pub fn remote_physical_size_gauge(&self) -> UIntGauge {
        let mut guard = self.remote_physical_size_gauge.lock().unwrap();
        guard
            .get_or_insert_with(|| {
                REMOTE_PHYSICAL_SIZE
                    .get_metric_with_label_values(&[
                        &self.tenant_id.to_string(),
                        &self.timeline_id.to_string(),
                    ])
                    .unwrap()
            })
            .clone()
    }
    pub fn remote_operation_time(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
        status: &'static str,
    ) -> Histogram {
        // XXX would be nice to have an upgradable RwLock
        let mut guard = self.remote_operation_time.lock().unwrap();
        let key = (file_kind.as_str(), op_kind.as_str(), status);
        let metric = guard.entry(key).or_insert_with(move || {
            REMOTE_OPERATION_TIME
                .get_metric_with_label_values(&[
                    &self.tenant_id.to_string(),
                    &self.timeline_id.to_string(),
                    key.0,
                    key.1,
                    key.2,
                ])
                .unwrap()
        });
        metric.clone()
    }
    fn calls_unfinished_gauge(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> IntGauge {
        // XXX would be nice to have an upgradable RwLock
        let mut guard = self.calls_unfinished_gauge.lock().unwrap();
        let key = (file_kind.as_str(), op_kind.as_str());
        let metric = guard.entry(key).or_insert_with(move || {
            REMOTE_TIMELINE_CLIENT_CALLS_UNFINISHED_GAUGE
                .get_metric_with_label_values(&[
                    &self.tenant_id.to_string(),
                    &self.timeline_id.to_string(),
                    key.0,
                    key.1,
                ])
                .unwrap()
        });
        metric.clone()
    }

    fn calls_started_hist(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> Histogram {
        // XXX would be nice to have an upgradable RwLock
        let mut guard = self.calls_started_hist.lock().unwrap();
        let key = (file_kind.as_str(), op_kind.as_str());
        let metric = guard.entry(key).or_insert_with(move || {
            REMOTE_TIMELINE_CLIENT_CALLS_STARTED_HIST
                .get_metric_with_label_values(&[
                    &self.tenant_id.to_string(),
                    &self.timeline_id.to_string(),
                    key.0,
                    key.1,
                ])
                .unwrap()
        });
        metric.clone()
    }
}

/// See [`RemoteTimelineClientMetrics::call_begin`].
#[must_use]
pub(crate) struct RemoteTimelineClientCallMetricGuard(Option<IntGauge>);

impl RemoteTimelineClientCallMetricGuard {
    /// Consume this guard object without decrementing the metric.
    /// The caller vouches to do this manually, so that the prior increment of the gauge will cancel out.
    pub fn will_decrement_manually(mut self) {
        self.0 = None; // prevent drop() from decrementing
    }
}

impl Drop for RemoteTimelineClientCallMetricGuard {
    fn drop(&mut self) {
        if let RemoteTimelineClientCallMetricGuard(Some(guard)) = self {
            guard.dec();
        }
    }
}

impl RemoteTimelineClientMetrics {
    /// Increment the metrics that track ongoing calls to the remote timeline client instance.
    ///
    /// Drop the returned guard object once the operation is finished to decrement the values.
    /// Or, use [`RemoteTimelineClientCallMetricGuard::will_decrement_manually`] and [`call_end`] if that
    /// is more suitable.
    /// Never do both.
    pub(crate) fn call_begin(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> RemoteTimelineClientCallMetricGuard {
        let unfinished_metric = self.calls_unfinished_gauge(file_kind, op_kind);
        self.calls_started_hist(file_kind, op_kind)
            .observe(unfinished_metric.get() as f64);
        unfinished_metric.inc();
        RemoteTimelineClientCallMetricGuard(Some(unfinished_metric))
    }

    /// Manually decrement the metric instead of using the guard object.
    /// Using the guard object is generally preferable.
    /// See [`call_begin`] for more context.
    pub(crate) fn call_end(&self, file_kind: &RemoteOpFileKind, op_kind: &RemoteOpKind) {
        let unfinished_metric = self.calls_unfinished_gauge(file_kind, op_kind);
        debug_assert!(
            unfinished_metric.get() > 0,
            "begin and end should cancel out"
        );
        unfinished_metric.dec();
    }
}

impl Drop for RemoteTimelineClientMetrics {
    fn drop(&mut self) {
        let RemoteTimelineClientMetrics {
            tenant_id,
            timeline_id,
            remote_physical_size_gauge,
            remote_operation_time,
            calls_unfinished_gauge,
            calls_started_hist,
        } = self;
        for ((a, b, c), _) in remote_operation_time.get_mut().unwrap().drain() {
            let _ = REMOTE_OPERATION_TIME.remove_label_values(&[tenant_id, timeline_id, a, b, c]);
        }
        for ((a, b), _) in calls_unfinished_gauge.get_mut().unwrap().drain() {
            let _ = REMOTE_TIMELINE_CLIENT_CALLS_UNFINISHED_GAUGE.remove_label_values(&[
                tenant_id,
                timeline_id,
                a,
                b,
            ]);
        }
        for ((a, b), _) in calls_started_hist.get_mut().unwrap().drain() {
            let _ = REMOTE_TIMELINE_CLIENT_CALLS_STARTED_HIST.remove_label_values(&[
                tenant_id,
                timeline_id,
                a,
                b,
            ]);
        }
        {
            let _ = remote_physical_size_gauge; // use to avoid 'unused' warning in desctructuring above
            let _ = REMOTE_PHYSICAL_SIZE.remove_label_values(&[tenant_id, timeline_id]);
        }
    }
}

/// Wrapper future that measures the time spent by a remote storage operation,
/// and records the time and success/failure as a prometheus metric.
pub trait MeasureRemoteOp: Sized {
    fn measure_remote_op(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        file_kind: RemoteOpFileKind,
        op: RemoteOpKind,
        metrics: Arc<RemoteTimelineClientMetrics>,
    ) -> MeasuredRemoteOp<Self> {
        let start = Instant::now();
        MeasuredRemoteOp {
            inner: self,
            tenant_id,
            timeline_id,
            file_kind,
            op,
            start,
            metrics,
        }
    }
}

impl<T: Sized> MeasureRemoteOp for T {}

pin_project! {
    pub struct MeasuredRemoteOp<F>
    {
        #[pin]
        inner: F,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        file_kind: RemoteOpFileKind,
        op: RemoteOpKind,
        start: Instant,
        metrics: Arc<RemoteTimelineClientMetrics>,
    }
}

impl<F: Future<Output = Result<O, E>>, O, E> Future for MeasuredRemoteOp<F> {
    type Output = Result<O, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let poll_result = this.inner.poll(cx);
        if let Poll::Ready(ref res) = poll_result {
            let duration = this.start.elapsed();
            let status = if res.is_ok() { &"success" } else { &"failure" };
            this.metrics
                .remote_operation_time(this.file_kind, this.op, status)
                .observe(duration.as_secs_f64());
        }
        poll_result
    }
}
