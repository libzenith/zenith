use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::{
    init_remote, list_objects_with_retries,
    metadata_stream::{stream_tenant_timelines, stream_tenants},
    BucketConfig, NodeKind,
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
    while let Some(tenant_shard_id) = tenants.next().await {
        let tenant_shard_id = tenant_shard_id?;
        let mut timelines =
            std::pin::pin!(stream_tenant_timelines(&s3_client, &target, tenant_shard_id).await?);
        while let Some(timeline) = timelines.next().await {
            let timeline = timeline?;
            let target = target.timeline_root(&timeline);
            let mut continuation_token = None;
            loop {
                let fetch_response =
                    list_objects_with_retries(&s3_client, &target, continuation_token.clone())
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
                    // TODO
                }
                match fetch_response.next_continuation_token {
                    Some(new_token) => continuation_token = Some(new_token),
                    None => break,
                }
            }
        }
        //objects.push();
    }
    //let objects = Vec::new();

    // TODO
    Ok(LargeObjectListing { objects })
}
