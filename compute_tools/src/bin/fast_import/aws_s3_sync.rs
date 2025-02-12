use camino::{Utf8Path, Utf8PathBuf};
use tokio::task::JoinSet;
use walkdir::WalkDir;

use super::s3_uri::S3Uri;

use tracing::{info, warn};

const MAX_PARALLEL_UPLOADS: usize = 10;

/// Upload all files from 'local' to 'remote'
pub(crate) async fn upload_dir_recursive(
    s3_client: &aws_sdk_s3::Client,
    local: &Utf8Path,
    remote: &S3Uri,
) -> anyhow::Result<()> {
    // Recursively scan directory
    let mut dirwalker = WalkDir::new(local)
        .into_iter()
        .map(|entry| {
            let entry = entry?;
            let file_type = entry.file_type();
            let path = <&Utf8Path>::try_from(entry.path())?.to_path_buf();
            Ok((file_type, path))
        })
        .filter_map(|e: anyhow::Result<(std::fs::FileType, Utf8PathBuf)>| {
            match e {
                Ok((file_type, path)) if file_type.is_file() => Some(Ok(path)),
                Ok((file_type, _path)) if file_type.is_dir() => {
                    // The WalkDir iterator will recurse into directories, but we don't want
                    // to do anything with directories as such. There's no concept of uploading
                    // an empty directory to S3.
                    None
                }
                Ok((file_type, path)) if file_type.is_symlink() => {
                    // huh, didn't expect a symlink. Can't upload that to S3. Warn and skip.
                    warn!("cannot upload symlink ({})", path);
                    None
                }
                Ok((_file_type, path)) => {
                    // should not happen
                    warn!("directory entry has unexpected type ({})", path);
                    None
                }
                Err(e) => Some(Err(e)),
            }
        });

    // Spawn upload tasks for each file, keeping MAX_PARALLEL_UPLOADS active in
    // parallel.
    let mut joinset = JoinSet::new();
    loop {
        // Could we upload more?
        while joinset.len() < MAX_PARALLEL_UPLOADS {
            if let Some(full_local_path) = dirwalker.next() {
                let full_local_path = full_local_path?;
                let relative_local_path = full_local_path
                    .strip_prefix(local)
                    .expect("all paths start from the walkdir root");
                let remote_path = remote.append(relative_local_path.as_str());
                info!(
                    "starting upload of {} to {}",
                    &full_local_path, &remote_path
                );
                let upload_task = upload_file(s3_client.clone(), full_local_path, remote_path);
                joinset.spawn(upload_task);
            } else {
                info!("draining upload tasks");
                break;
            }
        }

        // Wait for an upload to complete
        if let Some(res) = joinset.join_next().await {
            let _ = res?;
        } else {
            // all done!
            break;
        }
    }
    Ok(())
}

pub(crate) async fn upload_file(
    s3_client: aws_sdk_s3::Client,
    local_path: Utf8PathBuf,
    remote: S3Uri,
) -> anyhow::Result<()> {
    use aws_smithy_types::byte_stream::ByteStream;
    let stream = ByteStream::from_path(&local_path).await?;

    let _result = s3_client
        .put_object()
        .bucket(remote.bucket)
        .key(&remote.key)
        .body(stream)
        .send()
        .await?;
    info!("upload of {} to {} finished", &local_path, &remote.key);

    Ok(())
}
