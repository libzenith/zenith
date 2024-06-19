//! An utilization metric which is used to decide on which pageserver to put next tenant.
//!
//! The metric is exposed via `GET /v1/utilization`. Refer and maintain it's openapi spec as the
//! truth.

use anyhow::Context;
use std::path::Path;

use pageserver_api::models::PageserverUtilization;

pub(crate) fn regenerate(tenants_path: &Path) -> anyhow::Result<PageserverUtilization> {
    // TODO: currently the http api ratelimits this to 1Hz at most, which is probably good enough

    let statvfs = nix::sys::statvfs::statvfs(tenants_path)
        .map_err(std::io::Error::from)
        .context("statvfs tenants directory")?;

    // https://unix.stackexchange.com/a/703650
    let blocksz = if statvfs.fragment_size() > 0 {
        statvfs.fragment_size()
    } else {
        statvfs.block_size()
    };

    #[cfg_attr(not(target_os = "macos"), allow(clippy::unnecessary_cast))]
    let free = statvfs.blocks_available() as u64 * blocksz;

    #[cfg_attr(not(target_os = "macos"), allow(clippy::unnecessary_cast))]
    let used = statvfs
        .blocks()
        // use blocks_free instead of available here to match df in case someone compares
        .saturating_sub(statvfs.blocks_free()) as u64
        * blocksz;

    let captured_at = std::time::SystemTime::now();

    let doc = PageserverUtilization {
        disk_usage_bytes: used,
        free_space_bytes: free,
        // lower is better; start with a constant
        //
        // note that u64::MAX will be output as i64::MAX as u64, but that should not matter
        utilization_score: u64::MAX,
        captured_at: utils::serde_system_time::SystemTime(captured_at),
    };

    // TODO: make utilization_score into a metric

    Ok(doc)
}
