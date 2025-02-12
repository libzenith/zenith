//! Timeline utility module to hydrate everything from the current heatmap.
//!
//! Provides utilities to spawn and abort a background task where the downloads happen.
//! See /v1/tenant/:tenant_shard_id/timeline/:timeline_id/download_heatmap_layers.

use futures::StreamExt;
use std::sync::{Arc, Mutex};
use utils::http::error::ApiError;

use super::Timeline;

// This status is not strictly necessary now, but gives us a nice place
// to store progress information if we ever wish to expose it.
pub(super) enum HeatmapLayersDownloadStatus {
    InProgress,
    Complete,
}

pub(super) struct HeatmapLayersDownloader {
    handle: tokio::task::JoinHandle<()>,
    status: Arc<Mutex<HeatmapLayersDownloadStatus>>,
}

impl HeatmapLayersDownloader {
    fn new(
        timeline: Arc<Timeline>,
        concurrency: usize,
    ) -> Result<HeatmapLayersDownloader, ApiError> {
        let tl_guard = timeline.gate.enter().map_err(|_| ApiError::Cancelled)?;
        let status = Arc::new(Mutex::new(HeatmapLayersDownloadStatus::InProgress));

        let handle = tokio::task::spawn({
            let status = status.clone();
            async move {
                let _guard = tl_guard;

                scopeguard::defer! {
                    *status.lock().unwrap() = HeatmapLayersDownloadStatus::Complete;
                }

                let Some(heatmap) = timeline.generate_heatmap().await else {
                    tracing::info!("Heatmap layers download failed to generate heatmap");
                    return;
                };

                tracing::info!(
                    resident_size=%timeline.resident_physical_size(),
                    heatmap_leayrs=%heatmap.layers.len(),
                    "Starting heatmap layers download"
                );

                let stream = futures::stream::iter(heatmap.layers.into_iter().map(
                    |layer| {
                        let tl = timeline.clone();
                        async move {
                            let res = tl.download_layer(&layer.name).await;
                            if let Err(err) = res {
                                tracing::warn!(layer=%layer.name,"Failed to download heatmap layer: {err}")
                            }
                        }
                    }
                )).buffered(concurrency);

                tokio::select! {
                    _ = stream.collect::<()>() => {
                        tracing::info!(
                            resident_size=%timeline.resident_physical_size(),
                            "Heatmap layers download completed"
                        );
                    },
                    _ = timeline.cancel.cancelled() => {
                        tracing::info!("Heatmap layers download cancelled. Timeline shutting down.");
                    }
                }
            }
        });

        Ok(Self { status, handle })
    }

    fn is_complete(&self) -> bool {
        matches!(
            *self.status.lock().unwrap(),
            HeatmapLayersDownloadStatus::Complete
        )
    }

    fn abort(&self) {
        self.handle.abort();
        *self.status.lock().unwrap() = HeatmapLayersDownloadStatus::Complete;
    }
}

impl Timeline {
    pub(crate) async fn start_heatmap_layers_download(
        self: &Arc<Self>,
        concurrency: usize,
    ) -> Result<(), ApiError> {
        let mut locked = self.heatmap_layers_downloader.lock().unwrap();
        if locked.as_ref().map(|dl| dl.is_complete()).unwrap_or(true) {
            let dl = HeatmapLayersDownloader::new(self.clone(), concurrency)?;
            *locked = Some(dl);
            Ok(())
        } else {
            Err(ApiError::Conflict("Already running".to_string()))
        }
    }

    pub(crate) fn abort_heatmap_layers_download(&self) {
        let locked = self.heatmap_layers_downloader.lock().unwrap();
        if let Some(ref dl) = *locked {
            dl.abort();
        }
    }
}
