use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crate::checks::{list_timeline_blobs, BlobDataParseResult};
use crate::metadata_stream::{stream_tenant_timelines, stream_tenants};
use crate::{init_remote, BucketConfig, NodeKind, RootTarget, TenantShardTimelineId};
use aws_sdk_s3::Client;
use futures_util::{StreamExt, TryStreamExt};
use pageserver::tenant::remote_timeline_client::{parse_remote_index_path, remote_layer_path};
use pageserver::tenant::storage_layer::LayerName;
use pageserver::tenant::IndexPart;
use pageserver_api::shard::{ShardIndex, TenantShardId};
use remote_storage::RemotePath;
use serde::Serialize;
use tracing::{info_span, Instrument};
use utils::generation::Generation;
use utils::id::{TenantId, TenantTimelineId};

#[derive(Serialize, Default)]
pub struct GcSummary {
    indices_deleted: usize,
    remote_storage_errors: usize,
    ancestor_layers_deleted: usize,
}

impl GcSummary {
    fn merge(&mut self, other: Self) {
        let Self {
            indices_deleted,
            remote_storage_errors,
            ancestor_layers_deleted,
        } = other;

        self.indices_deleted = indices_deleted;
        self.remote_storage_errors += remote_storage_errors;
        self.ancestor_layers_deleted += ancestor_layers_deleted;
    }
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum GcMode {
    // Delete nothing
    DryRun,

    // Enable only removing old-generation indices
    IndicesOnly,

    // Enable all forms of GC
    Full,
}

impl std::fmt::Display for GcMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcMode::DryRun => write!(f, "dry-run"),
            GcMode::IndicesOnly => write!(f, "indices-only"),
            GcMode::Full => write!(f, "full"),
        }
    }
}

// Map of cross-shard layer references, giving a refcount for each layer in each shard that is referenced by some other
// shard in the same tenant.  This is sparse!  The vast majority of timelines will have no cross-shard refs, and those that
// do have cross shard refs should eventually drop most of them via compaction.
type AncestorRefs = BTreeMap<TenantTimelineId, HashMap<(ShardIndex, LayerName), usize>>;

// As we see shards for a tenant, acccumulate knowledge needed for cross-shard GC:
// - Are there any ancestor shards?
// - Are there any refs to ancestor shards' layers?
#[derive(Default)]
struct TenantRefAccumulator {
    shards_seen: HashMap<TenantId, Vec<ShardIndex>>,

    // For each shard that has refs to an ancestor's layers, the set of ancestor layers referred to
    ancestor_ref_shards: AncestorRefs,
}

impl TenantRefAccumulator {
    fn update(&mut self, ttid: TenantShardTimelineId, index_part: &IndexPart) {
        let this_shard_idx = ttid.tenant_shard_id.to_index();
        (*self
            .shards_seen
            .entry(ttid.tenant_shard_id.tenant_id)
            .or_default())
        .push(this_shard_idx);

        let mut ancestor_refs = Vec::new();
        for (layer_name, layer_metadata) in &index_part.layer_metadata {
            if layer_metadata.shard != this_shard_idx {
                // This is a reference from this shard to a layer in an ancestor shard: we must track this
                // as a marker to not GC this layer from the parent.
                ancestor_refs.push((layer_name.clone(), layer_metadata.clone()));
            }
        }

        if !ancestor_refs.is_empty() {
            tracing::info!(%ttid, "Found {} ancestor refs", ancestor_refs.len());
            let ttid_refs = self
                .ancestor_ref_shards
                .entry(ttid.as_tenant_timeline_id())
                .or_default();
            for (layer_name, layer_metadata) in ancestor_refs {
                // Increment refcount of this layer in the ancestor shard
                *(ttid_refs
                    .entry((layer_metadata.shard, layer_name))
                    .or_default()) += 1;
            }
        }
    }

    /// Consume Self and return a vector of ancestor tenant shards that should be GC'd, and map of referenced ancestor layers to preserve
    fn into_gc_ancestors(self) -> (Vec<TenantShardId>, AncestorRefs) {
        let mut ancestors_to_gc = Vec::new();
        for (tenant_id, mut shard_indices) in self.shards_seen {
            // Find the highest shard count
            let latest_count = shard_indices
                .iter()
                .map(|i| i.shard_count)
                .max()
                .expect("Always at least one shard");

            let (latest_shards, ancestor_shards) = {
                let at =
                    itertools::partition(&mut shard_indices, |i| i.shard_count == latest_count);
                (&shard_indices[0..at], &shard_indices[at..])
            };

            // Check that we have a complete view of the latest shard count: this should always be the case unless we happened
            // to scan the S3 bucket halfway through a shard split.
            if latest_shards.len() != latest_count.count() as usize {
                // This should be extremely rare, so we warn on it.
                tracing::warn!(%tenant_id, "Missed some shards at count {:?}", latest_count);
                continue;
            }

            // Check if we have any non-latest-count shards
            if ancestor_shards.is_empty() {
                tracing::debug!(%tenant_id, "No ancestor shards to clean up");
                continue;
            }

            // GC ancestor shards
            for ancestor_shard in ancestor_shards.iter().map(|idx| TenantShardId {
                tenant_id,
                shard_count: idx.shard_count,
                shard_number: idx.shard_number,
            }) {
                ancestors_to_gc.push(ancestor_shard);
            }
        }

        (ancestors_to_gc, self.ancestor_ref_shards)
    }
}

async fn is_old_enough(
    s3_client: &Client,
    bucket_config: &BucketConfig,
    min_age: &Duration,
    key: &str,
    summary: &mut GcSummary,
) -> bool {
    // Validation: we will only delete indices after one week, so that during incidents we will have
    // easy access to recent indices.
    let age: Duration = match s3_client
        .head_object()
        .bucket(&bucket_config.bucket)
        .key(key)
        .send()
        .await
    {
        Ok(response) => match response.last_modified {
            None => {
                tracing::warn!("Missing last_modified");
                summary.remote_storage_errors += 1;
                return false;
            }
            Some(last_modified) => {
                let last_modified =
                    UNIX_EPOCH + Duration::from_secs_f64(last_modified.as_secs_f64());
                match last_modified.elapsed() {
                    Ok(e) => e,
                    Err(_) => {
                        tracing::warn!("Bad last_modified time: {last_modified:?}");
                        return false;
                    }
                }
            }
        },
        Err(e) => {
            tracing::warn!("Failed to HEAD {key}: {e}");
            summary.remote_storage_errors += 1;
            return false;
        }
    };
    let old_enough = &age > min_age;

    if !old_enough {
        tracing::info!(
            "Skipping young object {} < {}",
            humantime::format_duration(age),
            humantime::format_duration(*min_age)
        );
    }

    old_enough
}

async fn maybe_delete_index(
    s3_client: &Client,
    bucket_config: &BucketConfig,
    min_age: &Duration,
    latest_gen: Generation,
    key: &str,
    mode: GcMode,
    summary: &mut GcSummary,
) {
    // Validation: we will only delete things that parse cleanly
    let basename = key.rsplit_once('/').unwrap().1;
    let candidate_generation =
        match parse_remote_index_path(RemotePath::from_string(basename).unwrap()) {
            Some(g) => g,
            None => {
                if basename == IndexPart::FILE_NAME {
                    // A legacy pre-generation index
                    Generation::none()
                } else {
                    // A strange key: we will not delete this because we don't understand it.
                    tracing::warn!("Bad index key");
                    return;
                }
            }
        };

    // Validation: we will only delete indices more than one generation old, to avoid interfering
    // in typical migrations, even if they are very long running.
    if candidate_generation >= latest_gen {
        // This shouldn't happen: when we loaded metadata, it should have selected the latest
        // generation already, and only populated [`S3TimelineBlobData::unused_index_keys`]
        // with older generations.
        tracing::warn!("Deletion candidate is >= latest generation, this is a bug!");
        return;
    } else if candidate_generation.next() == latest_gen {
        // Skip deleting the latest-1th generation's index.
        return;
    }

    if !is_old_enough(s3_client, bucket_config, min_age, key, summary).await {
        return;
    }

    if matches!(mode, GcMode::DryRun) {
        tracing::info!("Dry run: would delete this key");
        return;
    }

    // All validations passed: erase the object
    match s3_client
        .delete_object()
        .bucket(&bucket_config.bucket)
        .key(key)
        .send()
        .await
    {
        Ok(_) => {
            tracing::info!("Successfully deleted index");
            summary.indices_deleted += 1;
        }
        Err(e) => {
            tracing::warn!("Failed to delete index: {e}");
            summary.remote_storage_errors += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn gc_ancestor(
    s3_client: &Client,
    bucket_config: &BucketConfig,
    root_target: &RootTarget,
    min_age: &Duration,
    ancestor: TenantShardId,
    refs: &AncestorRefs,
    mode: GcMode,
    summary: &mut GcSummary,
) -> anyhow::Result<()> {
    // Scan timelines in the ancestor
    let timelines = stream_tenant_timelines(s3_client, root_target, ancestor).await?;
    let mut timelines = std::pin::pin!(timelines);

    // Build a list of keys to retain

    while let Some(ttid) = timelines.next().await {
        let ttid = ttid?;

        let data = list_timeline_blobs(s3_client, ttid, root_target).await?;

        let s3_layers = match data.blob_data {
            BlobDataParseResult::Parsed {
                index_part: _,
                index_part_generation: _,
                s3_layers,
            } => s3_layers,
            BlobDataParseResult::Relic => {
                // Post-deletion tenant location: don't try and GC it.
                continue;
            }
            BlobDataParseResult::Incorrect(reasons) => {
                // Our primary purpose isn't to report on bad data, but log this rather than skipping silently
                tracing::warn!(
                    "Skipping ancestor GC for timeline {ttid}, bad metadata: {reasons:?}"
                );
                continue;
            }
        };

        let ttid_refs = refs.get(&ttid.as_tenant_timeline_id());
        let ancestor_shard_index = ttid.tenant_shard_id.to_index();

        for (layer_name, layer_gen) in s3_layers {
            let ref_count = ttid_refs
                .and_then(|m| m.get(&(ancestor_shard_index, layer_name.clone())))
                .copied()
                .unwrap_or(0);

            if ref_count > 0 {
                tracing::debug!(%ttid, "Ancestor layer {layer_name}  has {ref_count} refs");
                continue;
            }

            tracing::info!(%ttid, "Ancestor layer {layer_name} is not referenced");

            // Build the key for the layer we are considering deleting
            let key = root_target.absolute_key(&remote_layer_path(
                &ttid.tenant_shard_id.tenant_id,
                &ttid.timeline_id,
                ancestor_shard_index,
                &layer_name,
                layer_gen,
            ));

            // We apply a time threshold to GCing objects that are un-referenced: this preserves our ability
            // to roll back a shard split if we have to, by avoiding deleting ancestor layers right away
            if !is_old_enough(s3_client, bucket_config, min_age, &key, summary).await {
                continue;
            }

            if !matches!(mode, GcMode::Full) {
                tracing::info!("Dry run: would delete key {key}");
                continue;
            }

            // All validations passed: erase the object
            match s3_client
                .delete_object()
                .bucket(&bucket_config.bucket)
                .key(&key)
                .send()
                .await
            {
                Ok(_) => {
                    tracing::info!("Successfully deleted unreferenced ancestor layer {key}");
                    summary.ancestor_layers_deleted += 1;
                }
                Err(e) => {
                    tracing::warn!("Failed to delete layer {key}: {e}");
                    summary.remote_storage_errors += 1;
                }
            }
        }

        // TODO: if all the layers are gone, clean up the whole timeline dir (remove index)
    }

    Ok(())
}

/// Physical garbage collection: removing unused S3 objects.  This is distinct from the garbage collection
/// done inside the pageserver, which operates at a higher level (keys, layers).  This type of garbage collection
/// is about removing:
/// - Objects that were uploaded but never referenced in the remote index (e.g. because of a shutdown between
///   uploading a layer and uploading an index)
/// - Index objects from historic generations
///
/// This type of GC is not necessary for correctness: rather it serves to reduce wasted storage capacity, and
/// make sure that object listings don't get slowed down by large numbers of garbage objects.
pub async fn pageserver_physical_gc(
    bucket_config: BucketConfig,
    tenant_ids: Vec<TenantShardId>,
    min_age: Duration,
    mode: GcMode,
) -> anyhow::Result<GcSummary> {
    let (s3_client, target) = init_remote(bucket_config.clone(), NodeKind::Pageserver).await?;

    let tenants = if tenant_ids.is_empty() {
        futures::future::Either::Left(stream_tenants(&s3_client, &target))
    } else {
        futures::future::Either::Right(futures::stream::iter(tenant_ids.into_iter().map(Ok)))
    };

    // How many tenants to process in parallel.  We need to be mindful of pageservers
    // accessing the same per tenant prefixes, so use a lower setting than pageservers.
    const CONCURRENCY: usize = 32;

    // Accumulate information about each tenant for cross-shard GC step we'll do at the end
    let accumulator = Arc::new(std::sync::Mutex::new(TenantRefAccumulator::default()));

    // Generate a stream of TenantTimelineId
    let timelines = tenants.map_ok(|t| stream_tenant_timelines(&s3_client, &target, t));
    let timelines = timelines.try_buffered(CONCURRENCY);
    let timelines = timelines.try_flatten();

    // Generate a stream of S3TimelineBlobData
    async fn gc_timeline(
        s3_client: &Client,
        bucket_config: &BucketConfig,
        min_age: &Duration,
        target: &RootTarget,
        mode: GcMode,
        ttid: TenantShardTimelineId,
        accumulator: &Arc<std::sync::Mutex<TenantRefAccumulator>>,
    ) -> anyhow::Result<GcSummary> {
        let mut summary = GcSummary::default();
        let data = list_timeline_blobs(s3_client, ttid, target).await?;

        let (index_part, latest_gen, candidates) = match &data.blob_data {
            BlobDataParseResult::Parsed {
                index_part,
                index_part_generation,
                s3_layers: _s3_layers,
            } => (index_part, *index_part_generation, data.unused_index_keys),
            BlobDataParseResult::Relic => {
                // Post-deletion tenant location: don't try and GC it.
                return Ok(summary);
            }
            BlobDataParseResult::Incorrect(reasons) => {
                // Our primary purpose isn't to report on bad data, but log this rather than skipping silently
                tracing::warn!("Skipping timeline {ttid}, bad metadata: {reasons:?}");
                return Ok(summary);
            }
        };

        accumulator.lock().unwrap().update(ttid, index_part);

        for key in candidates {
            maybe_delete_index(
                s3_client,
                bucket_config,
                min_age,
                latest_gen,
                &key,
                mode,
                &mut summary,
            )
            .instrument(info_span!("maybe_delete_index", %ttid, ?latest_gen, key))
            .await;
        }

        Ok(summary)
    }

    let mut summary = GcSummary::default();

    // Drain futures for per-shard GC, populating accumulator as a side effect
    {
        let timelines = timelines.map_ok(|ttid| {
            gc_timeline(
                &s3_client,
                &bucket_config,
                &min_age,
                &target,
                mode,
                ttid,
                &accumulator,
            )
        });
        let mut timelines = std::pin::pin!(timelines.try_buffered(CONCURRENCY));

        while let Some(i) = timelines.next().await {
            summary.merge(i?);
        }
    }

    // Execute cross-shard GC, using the accumulator's full view of all the shards built in the per-shard GC
    let (ancestor_shards, ancestor_refs) = Arc::into_inner(accumulator)
        .unwrap()
        .into_inner()
        .unwrap()
        .into_gc_ancestors();
    for ancestor_shard in ancestor_shards {
        gc_ancestor(
            &s3_client,
            &bucket_config,
            &target,
            &min_age,
            ancestor_shard,
            &ancestor_refs,
            mode,
            &mut summary,
        )
        .instrument(info_span!("gc_ancestor", %ancestor_shard))
        .await?;
    }

    Ok(summary)
}
