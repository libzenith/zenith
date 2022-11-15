//! Helper functions to delete files from remote storage with a RemoteStorage
use anyhow::Context;
use std::path::Path;
use tracing::debug;

use remote_storage::GenericRemoteStorage;

pub(super) async fn delete_layer(
    storage: &GenericRemoteStorage,
    local_layer_path: &Path,
) -> anyhow::Result<()> {
    async {
        debug!(
            "Deleting layer from remote storage: {:?}",
            local_layer_path.display()
        );

        let storage_path = storage
            .remote_object_id(local_layer_path)
            .with_context(|| {
                format!(
                    "Failed to get the layer storage path for local path '{}'",
                    local_layer_path.display()
                )
            })?;

        // FIXME: If the deletion fails because the object already didn't exist,
        // it would be good to just issue a warning but consider it success.
        storage.delete(&storage_path).await.with_context(|| {
            format!(
                "Failed to delete remote layer from storage at '{:?}'",
                storage_path
            )
        })
    }
    .await
}
