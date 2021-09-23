//! Abstractions for the page server to store its relish layer data in the external storage.
//!
//! Main purpose of this module subtree is to provide a set of abstractions to manage the storage state
//! in a way, optimal for page server.
//!
//! The abstractions hide multiple custom external storage API implementations,
//! such as AWS S3, local filesystem, etc., located in the submodules.

mod local_fs;
mod rust_s3;
/// A queue-based storage with the background machinery behind it to synchronize
/// local page server layer files with external storage.
pub mod synced_storage;

use std::path::Path;

use anyhow::Context;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use super::filename::{DeltaFileName, ImageFileName};

/// Storage (potentially remote) API to manage its state.
#[async_trait::async_trait]
pub trait RelishStorage: Send + Sync {
    type RelishStoragePath: std::fmt::Debug;

    fn derive_destination(
        page_server_workdir: &Path,
        relish_local_path: &Path,
    ) -> anyhow::Result<Self::RelishStoragePath>;

    fn relish_info(relish: &Self::RelishStoragePath) -> anyhow::Result<RelishInfo>;

    async fn list_relishes(&self) -> anyhow::Result<Vec<Self::RelishStoragePath>>;

    async fn download_relish(
        &self,
        from: &Self::RelishStoragePath,
        to: &Path,
    ) -> anyhow::Result<()>;

    async fn delete_relish(&self, path: &Self::RelishStoragePath) -> anyhow::Result<()>;

    async fn upload_relish(&self, from: &Path, to: &Self::RelishStoragePath) -> anyhow::Result<()>;
}

pub struct RelishInfo {
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    kind: RelishKind,
}

#[derive(Debug)]
pub enum RelishKind {
    Metadata,
    DeltaRelish(DeltaFileName),
    ImageRelish(ImageFileName),
}

fn strip_workspace_prefix<'a>(
    page_server_workdir: &'a Path,
    relish_local_path: &'a Path,
) -> anyhow::Result<&'a Path> {
    relish_local_path
        .strip_prefix(page_server_workdir)
        .with_context(|| {
            format!(
                "Unexpected: relish local path '{}' is not relevant to server workdir",
                relish_local_path.display(),
            )
        })
}
