use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::{
    init_remote, list_objects_with_retries, metadata_stream::stream_tenants, BucketConfig, NodeKind,
};

#[derive(Serialize, Deserialize)]
pub struct LargeObject {
    pub key: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize)]
pub struct LargeObjectListing {
    pub objects: Vec<LargeObject>,
}

pub async fn find_large_objects(
    bucket_config: BucketConfig,
    min_size: u64,
) -> anyhow::Result<LargeObjectListing> {
    let (s3_client, target) = init_remote(bucket_config.clone(), NodeKind::Pageserver)?;
    let mut tenants = std::pin::pin!(stream_tenants(&s3_client, &target));
    let mut objects = Vec::new();
    let mut tenant_ctr = 0u64;
    let mut object_ctr = 0u64;
    while let Some(tenant_shard_id) = tenants.next().await {
        let tenant_shard_id = tenant_shard_id?;
        let mut tenant_root = target.tenant_root(&tenant_shard_id);
        // We want the objects and not just common prefixes
        tenant_root.delimiter.clear();
        let mut continuation_token = None;
        loop {
            let fetch_response =
                list_objects_with_retries(&s3_client, &tenant_root, continuation_token.clone())
                    .await?;
            for obj in fetch_response.contents().iter().filter(|o| {
                if let Some(obj_size) = o.size {
                    min_size as i64 <= obj_size
                } else {
                    false
                }
            }) {
                objects.push(LargeObject {
                    key: obj
                        .key()
                        .map(|k| k.to_owned())
                        .unwrap_or_else(|| "<unknown key>".to_owned()),
                    size: obj.size.unwrap() as u64,
                })
            }
            object_ctr += fetch_response.contents().len() as u64;
            match fetch_response.next_continuation_token {
                Some(new_token) => continuation_token = Some(new_token),
                None => break,
            }
        }

        tenant_ctr += 1;
        if tenant_ctr % 10 == 0 {
            tracing::info!(
                "Scanned {tenant_ctr} shards, {object_ctr} objects. current {tenant_shard_id}."
            );
        }
    }
    //let objects = Vec::new();

    // TODO
    Ok(LargeObjectListing { objects })
}
