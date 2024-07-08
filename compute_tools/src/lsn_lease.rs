use anyhow::bail;
use anyhow::Result;
use postgres::{NoTls, SimpleQueryMessage};
use std::{
    str::FromStr,
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};
use utils::id::TenantId;
use utils::id::TimelineId;

use compute_api::spec::ComputeMode;
use tracing::{error, info};
use utils::{
    lsn::Lsn,
    shard::{ShardCount, ShardNumber, TenantShardId},
};

use crate::compute::{ComputeNode, ComputeState};

pub fn launch_lsn_lease_loop_for_static(compute: &Arc<ComputeNode>) {
    let lsn = {
        let state = compute.state.lock().unwrap();
        let spec = state.pspec.as_ref().expect("spec must be set");
        match spec.spec.mode {
            ComputeMode::Static(lsn) => lsn,
            _ => return,
        }
    };
    let compute = compute.clone();
    thread::spawn(move || lsn_lease_loop(compute, lsn));
}

fn postgres_configs_from_state(compute_state: &ComputeState) -> Vec<postgres::Config> {
    let spec = compute_state.pspec.as_ref().expect("spec must be set");
    let conn_strings = spec.pageserver_connstr.split(',');

    conn_strings
        .map(|connstr| {
            let mut config = postgres::Config::from_str(connstr).expect("invalid connstr");
            if let Some(storage_auth_token) = &spec.storage_auth_token {
                info!("Got storage auth token from spec file");
                config.password(storage_auth_token.clone());
            } else {
                info!("Storage auth token not set");
            }
            config
        })
        .collect::<Vec<_>>()
}

fn lsn_lease_loop(compute: Arc<ComputeNode>, lsn: Lsn) {
    loop {
        let (tenant_id, timeline_id, configs) = {
            let state = compute.state.lock().unwrap();

            let spec = state.pspec.as_ref().expect("spec must be set");

            let configs = postgres_configs_from_state(&state);
            (spec.tenant_id, spec.timeline_id, configs)
        };

        match lsn_lease_request(tenant_id, timeline_id, lsn, &configs) {
            Ok(valid_until) => {
                let valid_until_duration = Duration::from_millis(valid_until as u64);

                let sleep_duration = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO)
                    .checked_sub(valid_until_duration)
                    .unwrap_or(Duration::ZERO)
                    .checked_sub(Duration::from_secs(60))
                    .unwrap_or(Duration::ZERO);

                // Ensure the sleep duration is at least 60 seconds to avoid busy loops
                let sleep_duration = std::cmp::max(sleep_duration, Duration::from_secs(60));

                info!(
                    "lsn_lease_request succeeded, sleeping for {} seconds",
                    sleep_duration.as_secs()
                );
                thread::sleep(sleep_duration);
            }
            Err(e) => {
                error!("lsn_lease_request failed: {:#}", e);
                thread::sleep(Duration::from_secs(10));
            }
        }
    }
}

fn lsn_lease_request(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Lsn,
    configs: &[postgres::Config],
) -> Result<u128> {
    fn get_valid_until(
        config: &postgres::Config,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Result<u128> {
        let mut client = config.connect(NoTls)?;
        let cmd = format!("lease lsn {} {} {} ", tenant_shard_id, timeline_id, lsn);
        info!("lsn_lease_request: {}", cmd);
        let res = client.simple_query(&cmd)?;
        let msg = match res.first() {
            Some(msg) => msg,
            None => bail!("empty response"),
        };
        let row = match msg {
            SimpleQueryMessage::Row(row) => row,
            _ => bail!("error parsing lsn lease response"),
        };
        let valid_until_str = match row.get("valid_until") {
            Some(valid_until) => valid_until,
            None => bail!("valid_until not found"),
        };
        Ok(u128::from_str(valid_until_str)?)
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
            .collect::<Result<Vec<u128>>>()?
            .into_iter()
            .min()
            .unwrap()
    } else {
        info!("Unsharded!");
        get_valid_until(
            &configs[0],
            TenantShardId::unsharded(tenant_id),
            timeline_id,
            lsn,
        )?
    };

    Ok(valid_until)
}
