//! A wrapper around AWS S3 client library `rust_s3` to be used a relish storage.

use std::path::Path;

use anyhow::Context;
use s3::{bucket::Bucket, creds::Credentials, region::Region};

use crate::{relish_storage::strip_workspace_prefix, S3Config};

use super::RelishStorage;

const S3_FILE_SEPARATOR: char = '/';

#[derive(Debug)]
pub struct S3ObjectKey(String);

impl S3ObjectKey {
    fn key(&self) -> &str {
        &self.0
    }
}

/// AWS S3 relish storage.
pub struct RustS3 {
    bucket: Bucket,
}

impl RustS3 {
    // TODO kb add prefix to allow sharing the same bucket with different pageservers
    /// Creates the relish storage, errors if incorrect AWS S3 configuration provided.
    pub fn new(aws_config: &S3Config) -> anyhow::Result<Self> {
        let region = aws_config
            .bucket_region
            .parse::<Region>()
            .context("Failed to parse the s3 region from config")?;
        let credentials = Credentials::new(
            aws_config.access_key_id.as_deref(),
            aws_config.secret_access_key.as_deref(),
            None,
            None,
            None,
        )
        .context("Failed to create the s3 credentials")?;
        Ok(Self {
            bucket: Bucket::new_with_path_style(
                aws_config.bucket_name.as_str(),
                region,
                credentials,
            )
            .context("Failed to create the s3 bucket")?,
        })
    }
}

#[async_trait::async_trait]
impl RelishStorage for RustS3 {
    type RelishStoragePath = S3ObjectKey;

    fn derive_destination(
        page_server_workdir: &Path,
        relish_local_path: &Path,
    ) -> anyhow::Result<Self::RelishStoragePath> {
        let relative_path = strip_workspace_prefix(page_server_workdir, relish_local_path)?;
        let mut key = String::new();
        for segment in relative_path {
            key.push(S3_FILE_SEPARATOR);
            key.push_str(&segment.to_string_lossy());
        }
        Ok(S3ObjectKey(key))
    }

    async fn list_relishes(&self) -> anyhow::Result<Vec<Self::RelishStoragePath>> {
        let list_response = self
            .bucket
            .list(String::new(), None)
            .await
            .context("Failed to list s3 objects")?;

        Ok(list_response
            .into_iter()
            .flat_map(|response| response.contents)
            .map(|s3_object| S3ObjectKey(s3_object.key))
            .collect())
    }

    async fn download_relish(
        &self,
        from: &Self::RelishStoragePath,
        to: &Path,
    ) -> anyhow::Result<()> {
        let mut target_file = std::fs::OpenOptions::new()
            .write(true)
            .open(to)
            .with_context(|| format!("Failed to open target s3 destination at {}", to.display()))?;
        let code = self
            .bucket
            .get_object_stream(from.key(), &mut target_file)
            .await
            .with_context(|| format!("Failed to download s3 object with key {}", from.key()))?;
        if code != 200 {
            Err(anyhow::format_err!(
                "Received non-200 exit code during downloading object from directory, code: {}",
                code
            ))
        } else {
            Ok(())
        }
    }

    async fn delete_relish(&self, path: &Self::RelishStoragePath) -> anyhow::Result<()> {
        let (_, code) = self
            .bucket
            .delete_object(path.key())
            .await
            .with_context(|| format!("Failed to delete s3 object with key {}", path.key()))?;
        if code != 200 {
            Err(anyhow::format_err!(
                "Received non-200 exit code during deleting object with key '{}', code: {}",
                path.key(),
                code
            ))
        } else {
            Ok(())
        }
    }

    async fn upload_relish(&self, from: &Path, to: &Self::RelishStoragePath) -> anyhow::Result<()> {
        let mut local_file = tokio::fs::OpenOptions::new().read(true).open(from).await?;

        let code = self
            .bucket
            .put_object_stream(&mut local_file, to.key())
            .await
            .with_context(|| format!("Failed to create s3 object with key {}", to.key()))?;
        if code != 200 {
            Err(anyhow::format_err!(
                "Received non-200 exit code during creating object with key '{}', code: {}",
                to.key(),
                code
            ))
        } else {
            Ok(())
        }
    }
}
