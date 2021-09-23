use std::{
    collections::BinaryHeap,
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc, Mutex},
    thread,
    time::Duration,
};

use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;
use zenith_utils::{
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

use crate::{PageServerConf, RelishStorageConfig};

use super::{local_fs::LocalFs, rust_s3::RustS3, RelishStorage};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SyncTask {
    UrgentDownload(ZTimelineId),
    Upload(TimelineUpload),
    Download(ZTimelineId),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimelineUpload {
    pub tenant_id: ZTenantId,
    pub timeline_id: ZTimelineId,
    pub disk_consistent_lsn: Lsn,
    pub metadata_path: PathBuf,
    pub disk_relishes: Vec<PathBuf>,
}

lazy_static::lazy_static! {
    pub static ref RELISH_STORAGE_WITH_BACKGROUND_SYNC: Arc<RelishStorageWithBackgroundSync> = Arc::new(RelishStorageWithBackgroundSync::new());
}

pub struct RelishStorageWithBackgroundSync {
    enabled: AtomicBool,
    queue: Mutex<BinaryHeap<SyncTask>>,
}

impl RelishStorageWithBackgroundSync {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            queue: Mutex::new(BinaryHeap::new()),
        }
    }

    pub fn schedule_timeline_upload(&self, timeline_upload: TimelineUpload) {
        if self.is_enabled() {
            self.queue
                .lock()
                .unwrap()
                .push(SyncTask::Upload(timeline_upload));
        }
    }

    fn disable(&self) {
        self.enabled
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.queue.lock().unwrap().clear();
    }

    fn is_enabled(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn next(&self) -> Option<SyncTask> {
        if self.is_enabled() {
            let mut queue_accessor = self.queue.lock().unwrap();
            let new_task = queue_accessor.pop();
            log::debug!("current storage queue length: {}", queue_accessor.len());
            new_task
        } else {
            None
        }
    }
}

pub fn create_storage_sync_thread(
    config: &'static PageServerConf,
) -> anyhow::Result<Option<thread::JoinHandle<()>>> {
    // TODO kb revert
    // match &config.relish_storage_config {
    //     Some(RelishStorageConfig::LocalFs(root)) => {
    //         let relish_storage = LocalFs::new(root.clone())?;
    //         Ok(Some(run_thread(
    //             Arc::clone(&RELISH_STORAGE_WITH_BACKGROUND_SYNC),
    //             relish_storage,
    //             &config.workdir,
    //         )?))
    //     }
    //     Some(RelishStorageConfig::AwsS3(s3_config)) => {
    //         let relish_storage = RustS3::new(s3_config)?;
    //         Ok(Some(run_thread(
    //             Arc::clone(&RELISH_STORAGE_WITH_BACKGROUND_SYNC),
    //             relish_storage,
    //             &config.workdir,
    //         )?))
    //     }
    //     None => {
    //         RELISH_STORAGE_WITH_BACKGROUND_SYNC.disable();
    //         Ok(None)
    //     }
    // }
    let relish_storage = LocalFs::new(PathBuf::from("/Users/someonetoignore/Downloads/tmp_dir"))?;
    Ok(Some(run_thread(
        Arc::clone(&RELISH_STORAGE_WITH_BACKGROUND_SYNC),
        relish_storage,
        &config.workdir,
    )?))
}

fn run_thread<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    sync_tasks_queue: Arc<RelishStorageWithBackgroundSync>,
    relish_storage: S,
    page_server_workdir: &'static Path,
) -> std::io::Result<thread::JoinHandle<()>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    thread::Builder::new()
        .name("Queue based relish storage sync".to_string())
        .spawn(move || {
            let uploaded_relishes = runtime
                .block_on(relish_storage.list_relishes())
                .expect("TODO kb");
            // Now think of how Vec<P> is mapped against TimelineUpload data (we need to determine that the upload happened)
            // (need to parse the uploaded paths at least)
            // let mut uploads: HashMap<ZTimelineId, BTreeSet<Lsn>>
            // downloads should go straight to queue
            // let mut files_to_download: Vec<P>
            loop {
                match sync_tasks_queue.next() {
                    Some(task) => runtime.block_on(async {
                        match task {
                            SyncTask::Download(_timeline) | SyncTask::UrgentDownload(_timeline) => {
                                todo!("TODO kb");
                            }
                            SyncTask::Upload(layer_upload) => {
                                upload_timeline(
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
            }
        })
}

async fn upload_timeline<P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    sync_tasks_queue: &RelishStorageWithBackgroundSync,
    relish_storage: &S,
    page_server_workdir: &Path,
    timeline_upload: TimelineUpload,
) {
    log::debug!(
        "Uploading layers for timeline {}",
        timeline_upload.timeline_id
    );
    let mut failed_relish_uploads = Vec::new();
    let mut relish_uploads = FuturesUnordered::new();

    // TODO kb put into config
    let concurrent_upload_limit = Arc::new(Semaphore::new(10));
    for relish_local_path in timeline_upload.disk_relishes {
        let upload_limit = Arc::clone(&concurrent_upload_limit);
        relish_uploads.push(async move {
            let permit = upload_limit
                .acquire()
                .await
                .expect("Semaphore is not closed yet");
            let upload_result =
                upload_file(relish_storage, page_server_workdir, &relish_local_path).await;
            drop(permit);
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

    if failed_relish_uploads.is_empty() {
        log::debug!("Successfully uploaded all relishes");

        match upload_file(
            relish_storage,
            page_server_workdir,
            &timeline_upload.metadata_path,
        )
        .await
        {
            Ok(()) => log::debug!("Successfully uploaded the metadata file"),
            Err(e) => {
                log::error!(
                    "Failed to upload metadata file '{}', reason: {}",
                    timeline_upload.metadata_path.display(),
                    e
                );
                sync_tasks_queue.schedule_timeline_upload(TimelineUpload {
                    disk_relishes: Vec::new(),
                    ..timeline_upload
                });
            }
        }
    } else {
        log::error!(
            "Failed to upload {} files, rescheduling the job",
            failed_relish_uploads.len()
        );
        sync_tasks_queue.schedule_timeline_upload(TimelineUpload {
            disk_relishes: failed_relish_uploads,
            ..timeline_upload
        });
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
