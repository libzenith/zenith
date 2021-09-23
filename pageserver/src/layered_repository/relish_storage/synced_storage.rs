use std::{
    collections::{BTreeSet, BinaryHeap, HashMap},
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

use crate::{
    layered_repository::{
        delta_layer::DeltaLayer,
        filename::{DeltaFileName, ImageFileName, PathOrConf, TimelineFiles},
        image_layer::ImageLayer,
        metadata_path,
        relish_storage::RelishKind,
    },
    PageServerConf,
};

use super::{local_fs::LocalFs, RelishStorage};

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
    pub image_layers: BTreeSet<ImageFileName>,
    pub delta_layers: BTreeSet<DeltaFileName>,
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
        config,
        Arc::clone(&RELISH_STORAGE_WITH_BACKGROUND_SYNC),
        relish_storage,
    )?))
}

fn run_thread<P: std::fmt::Debug, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    sync_tasks_queue: Arc<RelishStorageWithBackgroundSync>,
    relish_storage: S,
) -> std::io::Result<thread::JoinHandle<()>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    thread::Builder::new()
        .name("Queue based relish storage sync".to_string())
        .spawn(move || {
            let mut timeline_uploads = categorize_relish_uploads::<P, S>(
                config,
                runtime
                    .block_on(relish_storage.list_relishes())
                    .expect("Failed to list relish uploads"),
            );
            // Now think of how Vec<P> is mapped against TimelineUpload data (we need to determine that the upload happened)
            // (need to parse the uploaded paths at least)
            // let mut uploads: HashMap<(ZTenantId, ZTimelineId), BTreeSet<Lsn>>
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
                                    config,
                                    &mut timeline_uploads,
                                    &sync_tasks_queue,
                                    &relish_storage,
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

fn categorize_relish_uploads<
    P: std::fmt::Debug,
    S: 'static + RelishStorage<RelishStoragePath = P>,
>(
    config: &'static PageServerConf,
    uploaded_relishes: Vec<P>,
) -> HashMap<(ZTenantId, ZTimelineId), TimelineFiles> {
    let mut timelines = HashMap::new();

    for upload in uploaded_relishes {
        match S::relish_info(&upload) {
            Ok(relish_info) => {
                let timeline_files = timelines
                    .entry((relish_info.tenant_id, relish_info.timeline_id))
                    .or_insert_with(|| TimelineFiles {
                        image_layers: BTreeSet::new(),
                        delta_layers: BTreeSet::new(),
                        metadata: None,
                    });

                match relish_info.kind {
                    RelishKind::Metadata => {
                        timeline_files.metadata = Some(metadata_path(
                            config,
                            relish_info.timeline_id,
                            relish_info.tenant_id,
                        ))
                    }
                    RelishKind::DeltaRelish(delta_relish) => {
                        timeline_files.delta_layers.insert(delta_relish);
                    }
                    RelishKind::ImageRelish(image_relish) => {
                        timeline_files.image_layers.insert(image_relish);
                    }
                }
            }
            Err(e) => {
                log::error!(
                    "Failed to get relish info from the path '{:?}', reason: {}",
                    upload,
                    e
                );
                continue;
            }
        }
    }

    timelines
}

enum Upload {
    Image(ImageFileName),
    Delta(DeltaFileName),
}

async fn upload_timeline<'a, P, S: 'static + RelishStorage<RelishStoragePath = P>>(
    config: &'static PageServerConf,
    existing_uploads: &'a mut HashMap<(ZTenantId, ZTimelineId), TimelineFiles>,
    sync_tasks_queue: &'a RelishStorageWithBackgroundSync,
    relish_storage: &'a S,
    mut new_upload: TimelineUpload,
) {
    let tenant_id = new_upload.tenant_id;
    let timeline_id = new_upload.timeline_id;
    log::debug!("Uploading layers for timeline {}", timeline_id);

    let uploaded_files = existing_uploads.get(&(tenant_id, timeline_id));
    if let Some(uploaded_timeline_files) = uploaded_files {
        new_upload.image_layers.retain(|path_to_upload| {
            !uploaded_timeline_files
                .image_layers
                .contains(path_to_upload)
        });
        new_upload.delta_layers.retain(|path_to_upload| {
            !uploaded_timeline_files
                .delta_layers
                .contains(path_to_upload)
        });
        if new_upload.image_layers.is_empty()
            && new_upload.delta_layers.is_empty()
            && uploaded_timeline_files.metadata.is_some()
        {
            log::debug!("All layers are uploaded already");
            return;
        }
    }

    // TODO kb put into config
    let concurrent_upload_limit = Arc::new(Semaphore::new(10));
    let mut relish_uploads = FuturesUnordered::new();

    for upload in new_upload
        .image_layers
        .into_iter()
        .map(Upload::Image)
        .chain(new_upload.delta_layers.into_iter().map(Upload::Delta))
    {
        let upload_limit = Arc::clone(&concurrent_upload_limit);
        relish_uploads.push(async move {
            let conf = PathOrConf::Conf(config);
            let relish_local_path = match &upload {
                Upload::Image(image_name) => {
                    ImageLayer::path_for(&conf, timeline_id, tenant_id, image_name)
                }
                Upload::Delta(delta_name) => {
                    DeltaLayer::path_for(&conf, timeline_id, tenant_id, delta_name)
                }
            };
            let permit = upload_limit
                .acquire()
                .await
                .expect("Semaphore is not closed yet");
            let upload_result =
                upload_file(relish_storage, &config.workdir, &relish_local_path).await;
            drop(permit);
            (upload, relish_local_path, upload_result)
        });
    }

    let mut failed_image_uploads = BTreeSet::new();
    let mut failed_delta_uploads = BTreeSet::new();
    let mut successful_image_uploads = BTreeSet::new();
    let mut successful_delta_uploads = BTreeSet::new();
    while let Some((upload, relish_local_path, relish_upload_result)) = relish_uploads.next().await
    {
        match relish_upload_result {
            Ok(()) => {
                log::trace!(
                    "Successfully uploaded relish '{}'",
                    relish_local_path.display()
                );
                match upload {
                    Upload::Image(image_name) => {
                        successful_image_uploads.insert(image_name);
                    }
                    Upload::Delta(delta_name) => {
                        successful_delta_uploads.insert(delta_name);
                    }
                }
            }
            Err(e) => {
                log::error!(
                    "Failed to upload file '{}', reason: {}",
                    relish_local_path.display(),
                    e
                );
                match upload {
                    Upload::Image(image_name) => {
                        failed_image_uploads.insert(image_name);
                    }
                    Upload::Delta(delta_name) => {
                        failed_delta_uploads.insert(delta_name);
                    }
                }
            }
        }
    }

    if failed_image_uploads.is_empty() && failed_delta_uploads.is_empty() {
        log::debug!("Successfully uploaded all relishes");

        match upload_file(relish_storage, &config.workdir, &new_upload.metadata_path).await {
            Ok(()) => {
                log::debug!("Successfully uploaded the metadata file");
                let entry_to_update = existing_uploads
                    .entry((tenant_id, timeline_id))
                    .or_insert_with(|| TimelineFiles {
                        image_layers: BTreeSet::new(),
                        delta_layers: BTreeSet::new(),
                        metadata: None,
                    });

                entry_to_update
                    .image_layers
                    .extend(successful_image_uploads.into_iter());
                entry_to_update
                    .delta_layers
                    .extend(successful_delta_uploads.into_iter());
                entry_to_update.metadata = Some(new_upload.metadata_path);
            }
            Err(e) => {
                log::error!(
                    "Failed to upload metadata file '{}', reason: {}",
                    new_upload.metadata_path.display(),
                    e
                );
                sync_tasks_queue.schedule_timeline_upload(TimelineUpload {
                    image_layers: BTreeSet::new(),
                    delta_layers: BTreeSet::new(),
                    ..new_upload
                });
            }
        }
    } else {
        log::error!(
            "Failed to upload {} image layers and {} delta layers, rescheduling the job",
            failed_image_uploads.len(),
            failed_delta_uploads.len(),
        );
        sync_tasks_queue.schedule_timeline_upload(TimelineUpload {
            image_layers: failed_image_uploads,
            delta_layers: failed_delta_uploads,
            ..new_upload
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
