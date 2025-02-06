use anyhow::bail;
use anyhow::Result;
use postgres::{NoTls, SimpleQueryMessage};
use std::time::SystemTime;
use std::{str::FromStr, sync::Arc, thread, time::Duration};
use utils::id::TenantId;
use utils::id::TimelineId;

use compute_api::spec::ComputeMode;
use tracing::{info, warn};
use utils::{
    lsn::Lsn,
    shard::{ShardCount, ShardNumber, TenantShardId},
};

use crate::compute::ComputeNode;

/// Spawns a background thread to periodically renew LSN leases for static compute.
/// Do nothing if the compute is not in static mode.
pub fn launch_lsn_lease_bg_task_for_static(compute: &Arc<ComputeNode>) {
    let (tenant_id, timeline_id, lsn) = {
        let state = compute.state.lock().unwrap();
        let pspec = &state.pspec;
        match pspec.spec.mode {
            ComputeMode::Static(lsn) => (pspec.tenant_id, pspec.timeline_id, lsn),
            _ => return,
        }
    };
    let compute = compute.clone();

    let span = tracing::info_span!("lsn_lease_bg_task", %tenant_id, %timeline_id, %lsn);
    thread::spawn(move || {
        let _entered = span.entered();
        if let Err(e) = lsn_lease_bg_task(compute, tenant_id, timeline_id, lsn) {
            // TODO: might need stronger error feedback than logging an warning.
            warn!("Exited with error: {e}");
        }
    });
}

/// Renews lsn lease periodically so static compute are not affected by GC.
fn lsn_lease_bg_task(
    compute: Arc<ComputeNode>,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Lsn,
) -> Result<()> {
    loop {
        let valid_until = acquire_lsn_lease_with_retry(&compute, tenant_id, timeline_id, lsn)?;
        let valid_duration = valid_until
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);

        // Sleep for 60 seconds less than the valid duration but no more than half of the valid duration.
        let sleep_duration = valid_duration
            .saturating_sub(Duration::from_secs(60))
            .max(valid_duration / 2);

        info!(
            "Request succeeded, sleeping for {} seconds",
            sleep_duration.as_secs()
        );
        compute.wait_timeout_while_pageserver_connstr_unchanged(sleep_duration);
    }
}

/// Acquires lsn lease in a retry loop. Returns the expiration time if a lease is granted.
/// Returns an error if a lease is explicitly not granted. Otherwise, we keep sending requests.
fn acquire_lsn_lease_with_retry(
    compute: &Arc<ComputeNode>,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Lsn,
) -> Result<SystemTime> {
    let mut attempts = 0usize;
    let mut retry_period_ms: f64 = 500.0;
    const MAX_RETRY_PERIOD_MS: f64 = 60.0 * 1000.0;

    loop {
        // Note: List of pageservers is dynamic, need to re-read configs before each attempt.
        let configs = {
            let state = compute.state.lock().unwrap();

            let spec = &state.pspec;

            let conn_strings = spec.pageserver_connstr.split(',');

            conn_strings
                .map(|connstr| {
                    let mut config = postgres::Config::from_str(connstr).expect("Invalid connstr");
                    if let Some(storage_auth_token) = &spec.storage_auth_token {
                        config.password(storage_auth_token.clone());
                    }
                    config
                })
                .collect::<Vec<_>>()
        };

        let result = try_acquire_lsn_lease(tenant_id, timeline_id, lsn, &configs);
        match result {
            Ok(Some(res)) => {
                return Ok(res);
            }
            Ok(None) => {
                bail!("Permanent error: lease could not be obtained, LSN is behind the GC cutoff");
            }
            Err(e) => {
                warn!("Failed to acquire lsn lease: {e} (attempt {attempts})");

                compute.wait_timeout_while_pageserver_connstr_unchanged(Duration::from_millis(
                    retry_period_ms as u64,
                ));
                retry_period_ms *= 1.5;
                retry_period_ms = retry_period_ms.min(MAX_RETRY_PERIOD_MS);
            }
        }
        attempts += 1;
    }
}

/// Tries to acquire an LSN lease through PS page_service API.
fn try_acquire_lsn_lease(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Lsn,
    configs: &[postgres::Config],
) -> Result<Option<SystemTime>> {
    fn get_valid_until(
        config: &postgres::Config,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Result<Option<SystemTime>> {
        let mut client = config.connect(NoTls)?;
        let cmd = format!("lease lsn {} {} {} ", tenant_shard_id, timeline_id, lsn);
        let res = client.simple_query(&cmd)?;
        let msg = match res.first() {
            Some(msg) => msg,
            None => bail!("empty response"),
        };
        let row = match msg {
            SimpleQueryMessage::Row(row) => row,
            _ => bail!("error parsing lsn lease response"),
        };

        // Note: this will be None if a lease is explicitly not granted.
        let valid_until_str = row.get("valid_until");

        let valid_until = valid_until_str.map(|s| {
            SystemTime::UNIX_EPOCH
                .checked_add(Duration::from_millis(u128::from_str(s).unwrap() as u64))
                .expect("Time larger than max SystemTime could handle")
        });
        Ok(valid_until)
    }

    let shard_count = configs.len();

    let valid_until = if shard_count > 1 {
        configs
            .iter()
            .enumerate()
            .map(|(shard_number, config)| {
                let tenant_shard_id = TenantShardId {
                    tenant_id,
                    shard_count: ShardCount::new(shard_count as u8),
                    shard_number: ShardNumber(shard_number as u8),
                };
                get_valid_until(config, tenant_shard_id, timeline_id, lsn)
            })
            .collect::<Result<Vec<Option<SystemTime>>>>()?
            .into_iter()
            .min()
            .unwrap()
    } else {
        get_valid_until(
            &configs[0],
            TenantShardId::unsharded(tenant_id),
            timeline_id,
            lsn,
        )?
    };

    Ok(valid_until)
}
