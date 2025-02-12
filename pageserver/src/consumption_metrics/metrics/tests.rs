use crate::consumption_metrics::RawMetric;

use super::*;
use std::collections::HashMap;

#[test]
fn startup_collected_timeline_metrics_before_advancing() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let mut metrics = Vec::new();
    let cache = HashMap::new();

    let initdb_lsn = Lsn(0x10000);
    let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);

    let snap = TimelineSnapshot {
        loaded_at: (disk_consistent_lsn, SystemTime::now()),
        last_record_lsn: disk_consistent_lsn,
        current_exact_logical_size: Some(0x42000),
    };

    let now = DateTime::<Utc>::from(SystemTime::now());

    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(
                snap.loaded_at.1.into(),
                now,
                0
            ),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
            MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 0x42000)
        ]
    );
}

#[test]
fn startup_collected_timeline_metrics_second_round() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [now, before, init] = time_backwards();

    let now = DateTime::<Utc>::from(now);
    let before = DateTime::<Utc>::from(before);

    let initdb_lsn = Lsn(0x10000);
    let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);

    let mut metrics = Vec::new();
    let cache = HashMap::from([MetricsKey::written_size(tenant_id, timeline_id)
        .at(before, disk_consistent_lsn.0)
        .to_kv_pair()]);

    let snap = TimelineSnapshot {
        loaded_at: (disk_consistent_lsn, init),
        last_record_lsn: disk_consistent_lsn,
        current_exact_logical_size: Some(0x42000),
    };

    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(before, now, 0),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
            MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 0x42000)
        ]
    );
}

#[test]
fn startup_collected_timeline_metrics_nth_round_at_same_lsn() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [now, just_before, before, init] = time_backwards();

    let now = DateTime::<Utc>::from(now);
    let just_before = DateTime::<Utc>::from(just_before);
    let before = DateTime::<Utc>::from(before);

    let initdb_lsn = Lsn(0x10000);
    let disk_consistent_lsn = Lsn(initdb_lsn.0 * 2);

    let mut metrics = Vec::new();
    let cache = HashMap::from([
        // at t=before was the last time the last_record_lsn changed
        MetricsKey::written_size(tenant_id, timeline_id)
            .at(before, disk_consistent_lsn.0)
            .to_kv_pair(),
        // end time of this event is used for the next ones
        MetricsKey::written_size_delta(tenant_id, timeline_id)
            .from_until(before, just_before, 0)
            .to_kv_pair(),
    ]);

    let snap = TimelineSnapshot {
        loaded_at: (disk_consistent_lsn, init),
        last_record_lsn: disk_consistent_lsn,
        current_exact_logical_size: Some(0x42000),
    };

    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(just_before, now, 0),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, disk_consistent_lsn.0),
            MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 0x42000)
        ]
    );
}

#[test]
fn post_restart_written_sizes_with_rolled_back_last_record_lsn() {
    // it can happen that we lose the inmemorylayer but have previously sent metrics and we
    // should never go backwards

    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [later, now, at_restart] = time_backwards();

    // FIXME: tests would be so much easier if we did not need to juggle back and forth
    // SystemTime and DateTime::<Utc> ... Could do the conversion only at upload time?
    let now = DateTime::<Utc>::from(now);
    let later = DateTime::<Utc>::from(later);
    let before_restart = at_restart - std::time::Duration::from_secs(5 * 60);
    let way_before = before_restart - std::time::Duration::from_secs(10 * 60);
    let before_restart = DateTime::<Utc>::from(before_restart);
    let way_before = DateTime::<Utc>::from(way_before);

    let snap = TimelineSnapshot {
        loaded_at: (Lsn(50), at_restart),
        last_record_lsn: Lsn(50),
        current_exact_logical_size: None,
    };

    let mut cache = HashMap::from([
        MetricsKey::written_size(tenant_id, timeline_id)
            .at(before_restart, 100)
            .to_kv_pair(),
        MetricsKey::written_size_delta(tenant_id, timeline_id)
            .from_until(
                way_before,
                before_restart,
                // not taken into account, but the timestamps are important
                999_999_999,
            )
            .to_kv_pair(),
    ]);

    let mut metrics = Vec::new();
    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(
                before_restart,
                now,
                0
            ),
            MetricsKey::written_size(tenant_id, timeline_id).at(now, 100),
        ]
    );

    // now if we cache these metrics, and re-run while "still in recovery"
    cache.extend(metrics.drain(..).map(|x| x.to_kv_pair()));

    // "still in recovery", because our snapshot did not change
    snap.to_metrics(tenant_id, timeline_id, later, &mut metrics, &cache);

    assert_eq!(
        metrics,
        &[
            MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(now, later, 0),
            MetricsKey::written_size(tenant_id, timeline_id).at(later, 100),
        ]
    );
}

#[test]
fn post_restart_current_exact_logical_size_uses_cached() {
    let tenant_id = TenantId::generate();
    let timeline_id = TimelineId::generate();

    let [now, at_restart] = time_backwards();

    let now = DateTime::<Utc>::from(now);
    let before_restart = at_restart - std::time::Duration::from_secs(5 * 60);
    let before_restart = DateTime::<Utc>::from(before_restart);

    let snap = TimelineSnapshot {
        loaded_at: (Lsn(50), at_restart),
        last_record_lsn: Lsn(50),
        current_exact_logical_size: None,
    };

    let cache = HashMap::from([MetricsKey::timeline_logical_size(tenant_id, timeline_id)
        .at(before_restart, 100)
        .to_kv_pair()]);

    let mut metrics = Vec::new();
    snap.to_metrics(tenant_id, timeline_id, now, &mut metrics, &cache);

    metrics.retain(|item| item.key.metric == Name::LogicalSize);

    assert_eq!(
        metrics,
        &[MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 100)]
    );
}

#[test]
fn post_restart_synthetic_size_uses_cached_if_available() {
    let tenant_id = TenantId::generate();

    let ts = TenantSnapshot {
        remote_size: 1000,
        // not yet calculated
        synthetic_size: 0,
    };

    let now = SystemTime::now();
    let before_restart = DateTime::<Utc>::from(now - std::time::Duration::from_secs(5 * 60));
    let now = DateTime::<Utc>::from(now);

    let cached = HashMap::from([MetricsKey::synthetic_size(tenant_id)
        .at(before_restart, 1000)
        .to_kv_pair()]);

    let mut metrics = Vec::new();
    ts.to_metrics(tenant_id, now, &cached, &mut metrics);

    assert_eq!(
        metrics,
        &[
            MetricsKey::remote_storage_size(tenant_id).at(now, 1000),
            MetricsKey::synthetic_size(tenant_id).at(now, 1000),
        ]
    );
}

#[test]
fn post_restart_synthetic_size_is_not_sent_when_not_cached() {
    let tenant_id = TenantId::generate();

    let ts = TenantSnapshot {
        remote_size: 1000,
        // not yet calculated
        synthetic_size: 0,
    };

    let now = SystemTime::now();
    let now = DateTime::<Utc>::from(now);

    let cached = HashMap::new();

    let mut metrics = Vec::new();
    ts.to_metrics(tenant_id, now, &cached, &mut metrics);

    assert_eq!(
        metrics,
        &[
            MetricsKey::remote_storage_size(tenant_id).at(now, 1000),
            // no synthetic size here
        ]
    );
}

fn time_backwards<const N: usize>() -> [std::time::SystemTime; N] {
    let mut times = [std::time::SystemTime::UNIX_EPOCH; N];
    times[0] = std::time::SystemTime::now();
    for behind in 1..N {
        times[behind] = times[0] - std::time::Duration::from_secs(behind as u64);
    }

    times
}

pub(crate) const fn metric_examples_old(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    now: DateTime<Utc>,
    before: DateTime<Utc>,
) -> [RawMetric; 5] {
    [
        MetricsKey::written_size(tenant_id, timeline_id).at_old_format(now, 0),
        MetricsKey::written_size_delta(tenant_id, timeline_id)
            .from_until_old_format(before, now, 0),
        MetricsKey::timeline_logical_size(tenant_id, timeline_id).at_old_format(now, 0),
        MetricsKey::remote_storage_size(tenant_id).at_old_format(now, 0),
        MetricsKey::synthetic_size(tenant_id).at_old_format(now, 1),
    ]
}

pub(crate) const fn metric_examples(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    now: DateTime<Utc>,
    before: DateTime<Utc>,
) -> [NewRawMetric; 5] {
    [
        MetricsKey::written_size(tenant_id, timeline_id).at(now, 0),
        MetricsKey::written_size_delta(tenant_id, timeline_id).from_until(before, now, 0),
        MetricsKey::timeline_logical_size(tenant_id, timeline_id).at(now, 0),
        MetricsKey::remote_storage_size(tenant_id).at(now, 0),
        MetricsKey::synthetic_size(tenant_id).at(now, 1),
    ]
}
