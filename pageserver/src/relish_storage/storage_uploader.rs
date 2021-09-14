use std::{
    collections::BinaryHeap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use zenith_utils::{lsn::Lsn, zid::ZTimelineId};

use crate::{relish_storage::RelishStorage, RelishStorageConfig};

use super::{local_fs::LocalFs, rust_s3::RustS3};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SyncTask {
    // TODO kb also have tenant_id here?
    UrgentDownload(ZTimelineId),
    Upload(LayerUpload),
    Download(ZTimelineId),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LayerUpload {
    pub timeline_id: ZTimelineId,
    pub disk_consistent_lsn: Lsn,
    pub metadata_path: PathBuf,
    pub disk_relishes: Vec<PathBuf>,
}

pub struct RelishStorageWithBackgroundSync {
    sync_tasks_queue: Arc<Mutex<BinaryHeap<SyncTask>>>,
}

impl RelishStorageWithBackgroundSync {
    pub fn new(
        config: &RelishStorageConfig,
        page_server_workdir: &'static Path,
    ) -> anyhow::Result<Self> {
        let sync_tasks_queue = Arc::new(Mutex::new(BinaryHeap::new()));
        let _handle = match config {
            RelishStorageConfig::LocalFs(root) => {
                let relish_storage = LocalFs::new(root.clone())?;
                create_task_processing_thread(
                    Arc::clone(&sync_tasks_queue),
                    relish_storage,
                    page_server_workdir,
                )?
            }
            RelishStorageConfig::AwsS3(s3_config) => {
                let relish_storage = RustS3::new(s3_config)?;
                create_task_processing_thread(
                    Arc::clone(&sync_tasks_queue),
                    relish_storage,
                    page_server_workdir,
                )?
            }
        };
        Ok(Self { sync_tasks_queue })
    }

    pub fn upload_layer(&self, layer_upload: LayerUpload) {
        self.sync_tasks_queue
            .lock()
            .unwrap()
            .push(SyncTask::Upload(layer_upload));
    }
}

fn create_task_processing_thread<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    sync_tasks_queue: Arc<Mutex<BinaryHeap<SyncTask>>>,
    relish_storage: S,
    page_server_workdir: &'static Path,
) -> std::io::Result<thread::JoinHandle<()>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    thread::Builder::new()
        .name("Queue based relish storage sync".to_string())
        .spawn(move || loop {
            let mut queue_accessor = sync_tasks_queue.lock().unwrap();
            log::debug!("current storage queue length: {}", queue_accessor.len());
            let sync_task = queue_accessor.pop();
            drop(queue_accessor);

            match sync_task {
                Some(task) => runtime.block_on(async {
                    match task {
                        SyncTask::Download(_timeline) | SyncTask::UrgentDownload(_timeline) => {
                            todo!("TODO kb");
                        }
                        SyncTask::Upload(layer_upload) => {
                            upload_layer(
                                &sync_tasks_queue,
                                &relish_storage,
                                page_server_workdir,
                                layer_upload,
                            )
                            .await
                        }
                    }
                }),
                None => {
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
            };
        })
}

async fn upload_layer<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    sync_tasks_queue: &Mutex<BinaryHeap<SyncTask>>,
    relish_storage: &S,
    page_server_workdir: &Path,
    layer_upload: LayerUpload,
) {
    log::debug!("Uploading layers for timeline {}", layer_upload.timeline_id);
    let mut failed_relish_uploads = Vec::new();
    let mut relish_uploads = FuturesUnordered::new();

    // TODO kb put into config
    let concurrent_upload_limit = 10;

    for chunk in layer_upload.disk_relishes.chunks(concurrent_upload_limit) {
        for relish_local_path in chunk {
            relish_uploads.push(async move {
                let upload_result =
                    upload_file(relish_storage, page_server_workdir, relish_local_path).await;
                (relish_local_path, upload_result)
            });
        }
        while let Some((relish_local_path, relish_upload_result)) = relish_uploads.next().await {
            match relish_upload_result {
                Ok(()) => log::trace!(
                    "Successfully uploaded relish '{}'",
                    relish_local_path.display()
                ),
                Err(e) => {
                    log::error!(
                        "Failed to upload file '{}', reason: {}",
                        relish_local_path.display(),
                        e
                    );
                    failed_relish_uploads.push(relish_local_path.clone());
                }
            }
        }
    }

    if failed_relish_uploads.is_empty() {
        log::debug!("Successfully uploaded all relishes");

        match upload_file(
            relish_storage,
            page_server_workdir,
            &layer_upload.metadata_path,
        )
        .await
        {
            Ok(()) => log::debug!("Successfully uploaded the metadata file"),
            Err(e) => {
                log::error!(
                    "Failed to upload metadata file '{}', reason: {}",
                    layer_upload.metadata_path.display(),
                    e
                );
                sync_tasks_queue
                    .lock()
                    .unwrap()
                    .push(SyncTask::Upload(LayerUpload {
                        disk_relishes: Vec::new(),
                        ..layer_upload
                    }));
            }
        }
    } else {
        log::error!(
            "Failed to upload {} files, rescheduling the job",
            failed_relish_uploads.len()
        );
        sync_tasks_queue
            .lock()
            .unwrap()
            .push(SyncTask::Upload(LayerUpload {
                disk_relishes: failed_relish_uploads,
                ..layer_upload
            }));
    }
}

async fn upload_file<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    relish_storage: &S,
    page_server_workdir: &Path,
    local_file: &Path,
) -> anyhow::Result<()> {
    let destination =
        S::derive_destination(page_server_workdir, &local_file).with_context(|| {
            format!(
                "Failed to derive storage destination out of metadata path {}",
                local_file.display()
            )
        })?;
    relish_storage
        .upload_relish(&local_file, &destination)
        .await
}
