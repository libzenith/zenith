//! Azure Blob Storage wrapper

use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::io;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use super::REMOTE_STORAGE_PREFIX_SEPARATOR;
use anyhow::Result;
use azure_core::request_options::{MaxResults, Metadata, Range};
use azure_core::RetryOptions;
use azure_identity::DefaultAzureCredential;
use azure_storage::StorageCredentials;
use azure_storage_blobs::blob::CopyStatus;
use azure_storage_blobs::prelude::ClientBuilder;
use azure_storage_blobs::{blob::operations::GetBlobBuilder, prelude::ContainerClient};
use bytes::Bytes;
use futures::future::Either;
use futures::stream::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use http_types::{StatusCode, Url};
use scopeguard::ScopeGuard;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::metrics::{start_measuring_requests, AttemptOutcome, RequestKind};
use crate::{
    error::Cancelled, AzureConfig, ConcurrencyLimiter, Download,
    DownloadError, Listing, ListingMode, RemotePath, RemoteStorage, StorageMetadata,
    TimeTravelError, TimeoutOrCancel,
};

pub struct AzureBlobStorage {
    client: ContainerClient,
    prefix_in_container: Option<String>,
    max_keys_per_list_response: Option<NonZeroU32>,
    concurrency_limiter: ConcurrencyLimiter,
    // Per-request timeout. Accessible for tests.
    pub timeout: Duration,
}

impl AzureBlobStorage {
    pub fn new(azure_config: &AzureConfig, timeout: Duration) -> Result<Self> {
        debug!(
            "Creating azure remote storage for azure container {}",
            azure_config.container_name
        );

        let account = env::var("AZURE_STORAGE_ACCOUNT").expect("missing AZURE_STORAGE_ACCOUNT");

        // If the `AZURE_STORAGE_ACCESS_KEY` env var has an access key, use that,
        // otherwise try the token based credentials.
        let credentials = if let Ok(access_key) = env::var("AZURE_STORAGE_ACCESS_KEY") {
            StorageCredentials::access_key(account.clone(), access_key)
        } else {
            let token_credential = DefaultAzureCredential::default();
            StorageCredentials::token_credential(Arc::new(token_credential))
        };

        // we have an outer retry
        let builder = ClientBuilder::new(account, credentials).retry(RetryOptions::none());

        let client = builder.container_client(azure_config.container_name.to_owned());

        let max_keys_per_list_response =
            if let Some(limit) = azure_config.max_keys_per_list_response {
                Some(
                    NonZeroU32::new(limit as u32)
                        .ok_or_else(|| anyhow::anyhow!("max_keys_per_list_response can't be 0"))?,
                )
            } else {
                None
            };

        Ok(AzureBlobStorage {
            client,
            prefix_in_container: azure_config.prefix_in_container.to_owned(),
            max_keys_per_list_response,
            concurrency_limiter: ConcurrencyLimiter::new(azure_config.concurrency_limit.get()),
            timeout,
        })
    }

    pub fn relative_path_to_name(&self, path: &RemotePath) -> String {
        assert_eq!(std::path::MAIN_SEPARATOR, REMOTE_STORAGE_PREFIX_SEPARATOR);
        let path_string = path
            .get_path()
            .as_str()
            .trim_end_matches(REMOTE_STORAGE_PREFIX_SEPARATOR);
        match &self.prefix_in_container {
            Some(prefix) => {
                if prefix.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                    prefix.clone() + path_string
                } else {
                    format!("{prefix}{REMOTE_STORAGE_PREFIX_SEPARATOR}{path_string}")
                }
            }
            None => path_string.to_string(),
        }
    }

    fn name_to_relative_path(&self, key: &str) -> RemotePath {
        let relative_path =
            match key.strip_prefix(self.prefix_in_container.as_deref().unwrap_or_default()) {
                Some(stripped) => stripped,
                // we rely on Azure to return properly prefixed paths
                // for requests with a certain prefix
                None => panic!(
                    "Key {key} does not start with container prefix {:?}",
                    self.prefix_in_container
                ),
            };
        RemotePath(
            relative_path
                .split(REMOTE_STORAGE_PREFIX_SEPARATOR)
                .collect(),
        )
    }

    async fn download_for_builder(
        &self,
        builder: GetBlobBuilder,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        let kind = RequestKind::Get;

        let _permit = self.permit(kind, cancel).await?;
        let cancel_or_timeout = crate::support::cancel_or_timeout(self.timeout, cancel.clone());
        let cancel_or_timeout_ = crate::support::cancel_or_timeout(self.timeout, cancel.clone());

        let mut etag = None;
        let mut last_modified = None;
        let mut metadata = HashMap::new();

        let started_at = start_measuring_requests(kind);

        let download = async {
            let response = builder
                // convert to concrete Pageable
                .into_stream()
                // convert to TryStream
                .into_stream()
                .map_err(to_download_error);

            // apply per request timeout
            let response = tokio_stream::StreamExt::timeout(response, self.timeout);

            // flatten
            let response = response.map(|res| match res {
                Ok(res) => res,
                Err(_elapsed) => Err(DownloadError::Timeout),
            });

            let mut response = Box::pin(response);

            let Some(part) = response.next().await else {
                return Err(DownloadError::Other(anyhow::anyhow!(
                    "Azure GET response contained no response body"
                )));
            };
            let part = part?;
            if etag.is_none() {
                etag = Some(part.blob.properties.etag);
            }
            if last_modified.is_none() {
                last_modified = Some(part.blob.properties.last_modified.into());
            }
            if let Some(blob_meta) = part.blob.metadata {
                metadata.extend(blob_meta.iter().map(|(k, v)| (k.to_owned(), v.to_owned())));
            }

            // unwrap safety: if these were None, bufs would be empty and we would have returned an error already
            let etag = etag.unwrap();
            let last_modified = last_modified.unwrap();

            let tail_stream = response
                .map(|part| match part {
                    Ok(part) => Either::Left(part.data.map(|r| r.map_err(io::Error::other))),
                    Err(e) => {
                        Either::Right(futures::stream::once(async { Err(io::Error::other(e)) }))
                    }
                })
                .flatten();
            let stream = part
                .data
                .map(|r| r.map_err(io::Error::other))
                .chain(sync_wrapper::SyncStream::new(tail_stream));
            //.chain(SyncStream::from_pin(Box::pin(tail_stream)));

            let download_stream = crate::support::DownloadStream::new(cancel_or_timeout_, stream);

            Ok(Download {
                download_stream: Box::pin(download_stream),
                etag,
                last_modified,
                metadata: Some(StorageMetadata(metadata)),
            })
        };

        let download = tokio::select! {
            bufs = download => bufs,
            cancel_or_timeout = cancel_or_timeout => match cancel_or_timeout {
                TimeoutOrCancel::Timeout => return Err(DownloadError::Timeout),
                TimeoutOrCancel::Cancel => return Err(DownloadError::Cancelled),
            },
        };
        let started_at = ScopeGuard::into_inner(started_at);
        let outcome = match &download {
            Ok(_) => AttemptOutcome::Ok,
            Err(_) => AttemptOutcome::Err,
        };
        crate::metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, outcome, started_at);
        download
    }

    async fn permit(
        &self,
        kind: RequestKind,
        cancel: &CancellationToken,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, Cancelled> {
        let acquire = self.concurrency_limiter.acquire(kind);

        tokio::select! {
            permit = acquire => Ok(permit.expect("never closed")),
            _ = cancel.cancelled() => Err(Cancelled),
        }
    }
}

fn to_azure_metadata(metadata: StorageMetadata) -> Metadata {
    let mut res = Metadata::new();
    for (k, v) in metadata.0.into_iter() {
        res.insert(k, v);
    }
    res
}

fn to_download_error(error: azure_core::Error) -> DownloadError {
    if let Some(http_err) = error.as_http_error() {
        match http_err.status() {
            StatusCode::NotFound => DownloadError::NotFound,
            StatusCode::BadRequest => DownloadError::BadInput(anyhow::Error::new(error)),
            _ => DownloadError::Other(anyhow::Error::new(error)),
        }
    } else {
        DownloadError::Other(error.into())
    }
}

impl RemoteStorage for AzureBlobStorage {
    async fn list(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Listing, DownloadError> {
        let _permit = self.permit(RequestKind::List, cancel).await?;

        let op = async {
            // get the passed prefix or if it is not set use prefix_in_bucket value
            let list_prefix = prefix
                .map(|p| self.relative_path_to_name(p))
                .or_else(|| self.prefix_in_container.clone())
                .map(|mut p| {
                    // required to end with a separator
                    // otherwise request will return only the entry of a prefix
                    if matches!(mode, ListingMode::WithDelimiter)
                        && !p.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR)
                    {
                        p.push(REMOTE_STORAGE_PREFIX_SEPARATOR);
                    }
                    p
                });

            let mut builder = self.client.list_blobs();

            if let ListingMode::WithDelimiter = mode {
                builder = builder.delimiter(REMOTE_STORAGE_PREFIX_SEPARATOR.to_string());
            }

            if let Some(prefix) = list_prefix {
                builder = builder.prefix(Cow::from(prefix.to_owned()));
            }

            if let Some(limit) = self.max_keys_per_list_response {
                builder = builder.max_results(MaxResults::new(limit));
            }

            let response = builder.into_stream();
            let response = response.into_stream().map_err(to_download_error);
            let response = tokio_stream::StreamExt::timeout(response, self.timeout);
            let response = response.map(|res| match res {
                Ok(res) => res,
                Err(_elapsed) => Err(DownloadError::Timeout),
            });

            let mut response = std::pin::pin!(response);

            let mut res = Listing::default();

            let mut max_keys = max_keys.map(|mk| mk.get());
            while let Some(entry) = response.next().await {
                let entry = entry?;
                let prefix_iter = entry
                    .blobs
                    .prefixes()
                    .map(|prefix| self.name_to_relative_path(&prefix.name));
                res.prefixes.extend(prefix_iter);

                let blob_iter = entry
                    .blobs
                    .blobs()
                    .map(|k| self.name_to_relative_path(&k.name));

                for key in blob_iter {
                    res.keys.push(key);

                    if let Some(mut mk) = max_keys {
                        assert!(mk > 0);
                        mk -= 1;
                        if mk == 0 {
                            return Ok(res); // limit reached
                        }
                        max_keys = Some(mk);
                    }
                }
            }

            Ok(res)
        };

        tokio::select! {
            res = op => res,
            _ = cancel.cancelled() => Err(DownloadError::Cancelled),
        }
    }

    async fn upload(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Put;
        let _permit = self.permit(kind, cancel).await?;

        let started_at = start_measuring_requests(kind);

        let op = async {
            let blob_client = self.client.blob_client(self.relative_path_to_name(to));

            let from: Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>> =
                Box::pin(from);

            let from = NonSeekableStream::new(from, data_size_bytes);

            let body = azure_core::Body::SeekableStream(Box::new(from));

            let mut builder = blob_client.put_block_blob(body);

            if let Some(metadata) = metadata {
                builder = builder.metadata(to_azure_metadata(metadata));
            }

            let fut = builder.into_future();
            let fut = tokio::time::timeout(self.timeout, fut);

            match fut.await {
                Ok(Ok(_response)) => Ok(()),
                Ok(Err(azure)) => Err(azure.into()),
                Err(_timeout) => Err(TimeoutOrCancel::Timeout.into()),
            }
        };

        let res = tokio::select! {
            res = op => res,
            _ = cancel.cancelled() => return Err(TimeoutOrCancel::Cancel.into()),
        };

        let outcome = match res {
            Ok(_) => AttemptOutcome::Ok,
            Err(_) => AttemptOutcome::Err,
        };
        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, outcome, started_at);

        res
    }

    async fn download(
        &self,
        from: &RemotePath,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        let blob_client = self.client.blob_client(self.relative_path_to_name(from));

        let builder = blob_client.get();

        self.download_for_builder(builder, cancel).await
    }

    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        let blob_client = self.client.blob_client(self.relative_path_to_name(from));

        let mut builder = blob_client.get();

        let range: Range = if let Some(end_exclusive) = end_exclusive {
            (start_inclusive..end_exclusive).into()
        } else {
            (start_inclusive..).into()
        };
        builder = builder.range(range);

        self.download_for_builder(builder, cancel).await
    }

    async fn delete(&self, path: &RemotePath, cancel: &CancellationToken) -> anyhow::Result<()> {
        self.delete_objects(std::array::from_ref(path), cancel)
            .await
    }

    async fn delete_objects<'a>(
        &self,
        paths: &'a [RemotePath],
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Delete;
        let _permit = self.permit(kind, cancel).await?;
        let started_at = start_measuring_requests(kind);

        let op = async {
            // TODO batch requests are not supported by the SDK
            // https://github.com/Azure/azure-sdk-for-rust/issues/1068
            for path in paths {
                let blob_client = self.client.blob_client(self.relative_path_to_name(path));

                let request = blob_client.delete().into_future();

                let res = tokio::time::timeout(self.timeout, request).await;

                match res {
                    Ok(Ok(_response)) => continue,
                    Ok(Err(e)) => {
                        if let Some(http_err) = e.as_http_error() {
                            if http_err.status() == StatusCode::NotFound {
                                continue;
                            }
                        }
                        return Err(e.into());
                    }
                    Err(_elapsed) => return Err(TimeoutOrCancel::Timeout.into()),
                }
            }

            Ok(())
        };

        let res = tokio::select! {
            res = op => res,
            _ = cancel.cancelled() => return Err(TimeoutOrCancel::Cancel.into()),
        };

        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, &res, started_at);
        res
    }

    async fn copy(
        &self,
        from: &RemotePath,
        to: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let _permit = self.permit(RequestKind::Copy, cancel).await?;

        let timeout = tokio::time::sleep(self.timeout);

        let mut copy_status = None;

        let op = async {
            let blob_client = self.client.blob_client(self.relative_path_to_name(to));

            let source_url = format!(
                "{}/{}",
                self.client.url()?,
                self.relative_path_to_name(from)
            );

            let builder = blob_client.copy(Url::from_str(&source_url)?);
            let copy = builder.into_future();

            let result = copy.await?;

            copy_status = Some(result.copy_status);
            loop {
                match copy_status.as_ref().expect("we always set it to Some") {
                    CopyStatus::Aborted => {
                        anyhow::bail!("Received abort for copy from {from} to {to}.");
                    }
                    CopyStatus::Failed => {
                        anyhow::bail!("Received failure response for copy from {from} to {to}.");
                    }
                    CopyStatus::Success => return Ok(()),
                    CopyStatus::Pending => (),
                }
                // The copy is taking longer. Waiting a second and then re-trying.
                // TODO estimate time based on copy_progress and adjust time based on that
                tokio::time::sleep(Duration::from_millis(1000)).await;
                let properties = blob_client.get_properties().into_future().await?;
                let Some(status) = properties.blob.properties.copy_status else {
                    tracing::warn!("copy_status for copy is None!, from={from}, to={to}");
                    return Ok(());
                };
                copy_status = Some(status);
            }
        };

        tokio::select! {
            res = op => res,
            _ = cancel.cancelled() => Err(anyhow::Error::new(TimeoutOrCancel::Cancel)),
            _ = timeout => {
                let e = anyhow::Error::new(TimeoutOrCancel::Timeout);
                let e = e.context(format!("Timeout, last status: {copy_status:?}"));
                Err(e)
            },
        }
    }

    async fn time_travel_recover(
        &self,
        _prefix: Option<&RemotePath>,
        _timestamp: SystemTime,
        _done_if_after: SystemTime,
        _cancel: &CancellationToken,
    ) -> Result<(), TimeTravelError> {
        // TODO use Azure point in time recovery feature for this
        // https://learn.microsoft.com/en-us/azure/storage/blobs/point-in-time-restore-overview
        Err(TimeTravelError::Unimplemented)
    }
}

pin_project_lite::pin_project! {
    /// Hack to work around not being able to stream once with azure sdk.
    ///
    /// Azure sdk clones streams around with the assumption that they are like
    /// `Arc<tokio::fs::File>` (except not supporting tokio), however our streams are not like
    /// that. For example for an `index_part.json` we just have a single chunk of [`Bytes`]
    /// representing the whole serialized vec. It could be trivially cloneable and "semi-trivially"
    /// seekable, but we can also just re-try the request easier.
    #[project = NonSeekableStreamProj]
    enum NonSeekableStream<S> {
        /// A stream wrappers initial form.
        ///
        /// Mutex exists to allow moving when cloning. If the sdk changes to do less than 1
        /// clone before first request, then this must be changed.
        Initial {
            inner: std::sync::Mutex<Option<tokio_util::compat::Compat<tokio_util::io::StreamReader<S, Bytes>>>>,
            len: usize,
        },
        /// The actually readable variant, produced by cloning the Initial variant.
        ///
        /// The sdk currently always clones once, even without retry policy.
        Actual {
            #[pin]
            inner: tokio_util::compat::Compat<tokio_util::io::StreamReader<S, Bytes>>,
            len: usize,
            read_any: bool,
        },
        /// Most likely unneeded, but left to make life easier, in case more clones are added.
        Cloned {
            len_was: usize,
        }
    }
}

impl<S> NonSeekableStream<S>
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
{
    fn new(inner: S, len: usize) -> NonSeekableStream<S> {
        use tokio_util::compat::TokioAsyncReadCompatExt;

        let inner = tokio_util::io::StreamReader::new(inner).compat();
        let inner = Some(inner);
        let inner = std::sync::Mutex::new(inner);
        NonSeekableStream::Initial { inner, len }
    }
}

impl<S> std::fmt::Debug for NonSeekableStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initial { len, .. } => f.debug_struct("Initial").field("len", len).finish(),
            Self::Actual { len, .. } => f.debug_struct("Actual").field("len", len).finish(),
            Self::Cloned { len_was, .. } => f.debug_struct("Cloned").field("len", len_was).finish(),
        }
    }
}

impl<S> futures::io::AsyncRead for NonSeekableStream<S>
where
    S: Stream<Item = std::io::Result<Bytes>>,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.project() {
            NonSeekableStreamProj::Actual {
                inner, read_any, ..
            } => {
                *read_any = true;
                inner.poll_read(cx, buf)
            }
            // NonSeekableStream::Initial does not support reading because it is just much easier
            // to have the mutex in place where one does not poll the contents, or that's how it
            // seemed originally. If there is a version upgrade which changes the cloning, then
            // that support needs to be hacked in.
            //
            // including {self:?} into the message would be useful, but unsure how to unproject.
            _ => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "cloned or initial values cannot be read",
            ))),
        }
    }
}

impl<S> Clone for NonSeekableStream<S> {
    /// Weird clone implementation exists to support the sdk doing cloning before issuing the first
    /// request, see type documentation.
    fn clone(&self) -> Self {
        use NonSeekableStream::*;

        match self {
            Initial { inner, len } => {
                if let Some(inner) = inner.lock().unwrap().take() {
                    Actual {
                        inner,
                        len: *len,
                        read_any: false,
                    }
                } else {
                    Self::Cloned { len_was: *len }
                }
            }
            Actual { len, .. } => Cloned { len_was: *len },
            Cloned { len_was } => Cloned { len_was: *len_was },
        }
    }
}

#[async_trait::async_trait]
impl<S> azure_core::SeekableStream for NonSeekableStream<S>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin + Send + Sync + 'static,
{
    async fn reset(&mut self) -> azure_core::error::Result<()> {
        use NonSeekableStream::*;

        let msg = match self {
            Initial { inner, .. } => {
                if inner.get_mut().unwrap().is_some() {
                    return Ok(());
                } else {
                    "reset after first clone is not supported"
                }
            }
            Actual { read_any, .. } if !*read_any => return Ok(()),
            Actual { .. } => "reset after reading is not supported",
            Cloned { .. } => "reset after second clone is not supported",
        };
        Err(azure_core::error::Error::new(
            azure_core::error::ErrorKind::Io,
            std::io::Error::new(std::io::ErrorKind::Other, msg),
        ))
    }

    // Note: it is not documented if this should be the total or remaining length, total passes the
    // tests.
    fn len(&self) -> usize {
        use NonSeekableStream::*;
        match self {
            Initial { len, .. } => *len,
            Actual { len, .. } => *len,
            Cloned { len_was, .. } => *len_was,
        }
    }
}
