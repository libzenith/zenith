use std::error::Error as _;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use consumption_metrics::{Event, EventChunk, IdempotencyKey, CHUNK_SIZE};
use remote_storage::{GenericRemoteStorage, RemotePath};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::{metrics::Name, Cache, MetricsKey, NewRawMetric, RawMetric};
use utils::id::{TenantId, TimelineId};

/// How the metrics from pageserver are identified.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq)]
struct Ids {
    pub(super) tenant_id: TenantId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) timeline_id: Option<TimelineId>,
}

/// Serialize and write metrics to an HTTP endpoint
#[tracing::instrument(skip_all, fields(metrics_total = %metrics.len()))]
pub(super) async fn upload_metrics_http(
    client: &reqwest::Client,
    metric_collection_endpoint: &reqwest::Url,
    cancel: &CancellationToken,
    metrics: &[NewRawMetric],
    cached_metrics: &mut Cache,
    idempotency_keys: &[IdempotencyKey<'_>],
) -> anyhow::Result<()> {
    let mut uploaded = 0;
    let mut failed = 0;

    let started_at = std::time::Instant::now();

    let mut iter = serialize_in_chunks(CHUNK_SIZE, metrics, idempotency_keys);

    while let Some(res) = iter.next() {
        let (chunk, body) = res?;

        let event_bytes = body.len();

        let is_last = iter.len() == 0;

        let res = upload(client, metric_collection_endpoint, body, cancel, is_last)
            .instrument(tracing::info_span!(
                "upload",
                %event_bytes,
                uploaded,
                total = metrics.len(),
            ))
            .await;

        match res {
            Ok(()) => {
                for item in chunk {
                    cached_metrics.insert(item.key, item.clone());
                }
                uploaded += chunk.len();
            }
            Err(_) => {
                // failure(s) have already been logged
                //
                // however this is an inconsistency: if we crash here, we will start with the
                // values as uploaded. in practice, the rejections no longer happen.
                failed += chunk.len();
            }
        }
    }

    let elapsed = started_at.elapsed();

    tracing::info!(
        uploaded,
        failed,
        elapsed_ms = elapsed.as_millis(),
        "done sending metrics"
    );

    Ok(())
}

/// Serialize and write metrics to a remote storage object
#[tracing::instrument(skip_all, fields(metrics_total = %metrics.len()))]
pub(super) async fn upload_metrics_bucket(
    client: &GenericRemoteStorage,
    cancel: &CancellationToken,
    node_id: &str,
    metrics: &[NewRawMetric],
    idempotency_keys: &[IdempotencyKey<'_>],
) -> anyhow::Result<()> {
    if metrics.is_empty() {
        // Skip uploads if we have no metrics, so that readers don't have to handle the edge case
        // of an empty object.
        return Ok(());
    }

    // Compose object path
    let datetime: DateTime<Utc> = SystemTime::now().into();
    let ts_prefix = datetime.format("year=%Y/month=%m/day=%d/%H:%M:%SZ");
    let path = RemotePath::from_string(&format!("{ts_prefix}_{node_id}.ndjson.gz"))?;

    // Set up a gzip writer into a buffer
    let mut compressed_bytes: Vec<u8> = Vec::new();
    let compressed_writer = std::io::Cursor::new(&mut compressed_bytes);
    let mut gzip_writer = async_compression::tokio::write::GzipEncoder::new(compressed_writer);

    // Serialize and write into compressed buffer
    let started_at = std::time::Instant::now();
    for res in serialize_in_chunks(CHUNK_SIZE, metrics, idempotency_keys) {
        let (_chunk, body) = res?;
        gzip_writer.write_all(&body).await?;
    }
    gzip_writer.flush().await?;
    gzip_writer.shutdown().await?;
    let compressed_length = compressed_bytes.len();

    // Write to remote storage
    client
        .upload_storage_object(
            futures::stream::once(futures::future::ready(Ok(compressed_bytes.into()))),
            compressed_length,
            &path,
            cancel,
        )
        .await?;
    let elapsed = started_at.elapsed();

    tracing::info!(
        compressed_length,
        elapsed_ms = elapsed.as_millis(),
        "write metrics bucket at {path}",
    );

    Ok(())
}

/// Serializes the input metrics as JSON in chunks of chunk_size. The provided
/// idempotency keys are injected into the corresponding metric events (reused
/// across different metrics sinks), and must have the same length as input.
fn serialize_in_chunks<'a>(
    chunk_size: usize,
    input: &'a [NewRawMetric],
    idempotency_keys: &'a [IdempotencyKey<'a>],
) -> impl ExactSizeIterator<Item = Result<(&'a [NewRawMetric], bytes::Bytes), serde_json::Error>> + 'a
{
    use bytes::BufMut;

    assert_eq!(input.len(), idempotency_keys.len());

    struct Iter<'a> {
        inner: std::slice::Chunks<'a, NewRawMetric>,
        idempotency_keys: std::slice::Iter<'a, IdempotencyKey<'a>>,
        chunk_size: usize,

        // write to a BytesMut so that we can cheaply clone the frozen Bytes for retries
        buffer: bytes::BytesMut,
        // chunk amount of events are reused to produce the serialized document
        scratch: Vec<Event<Ids, Name>>,
    }

    impl<'a> Iterator for Iter<'a> {
        type Item = Result<(&'a [NewRawMetric], bytes::Bytes), serde_json::Error>;

        fn next(&mut self) -> Option<Self::Item> {
            let chunk = self.inner.next()?;

            if self.scratch.is_empty() {
                // first round: create events with N strings
                self.scratch.extend(
                    chunk
                        .iter()
                        .zip(&mut self.idempotency_keys)
                        .map(|(raw_metric, key)| raw_metric.as_event(key)),
                );
            } else {
                // next rounds: update_in_place to reuse allocations
                assert_eq!(self.scratch.len(), self.chunk_size);
                itertools::izip!(self.scratch.iter_mut(), chunk, &mut self.idempotency_keys)
                    .for_each(|(slot, raw_metric, key)| raw_metric.update_in_place(slot, key));
            }

            let res = serde_json::to_writer(
                (&mut self.buffer).writer(),
                &EventChunk {
                    events: (&self.scratch[..chunk.len()]).into(),
                },
            );

            match res {
                Ok(()) => Some(Ok((chunk, self.buffer.split().freeze()))),
                Err(e) => Some(Err(e)),
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.inner.size_hint()
        }
    }

    impl ExactSizeIterator for Iter<'_> {}

    let buffer = bytes::BytesMut::new();
    let inner = input.chunks(chunk_size);
    let idempotency_keys = idempotency_keys.iter();
    let scratch = Vec::new();

    Iter {
        inner,
        idempotency_keys,
        chunk_size,
        buffer,
        scratch,
    }
}

trait RawMetricExt {
    fn as_event(&self, key: &IdempotencyKey<'_>) -> Event<Ids, Name>;
    fn update_in_place(&self, event: &mut Event<Ids, Name>, key: &IdempotencyKey<'_>);
}

impl RawMetricExt for RawMetric {
    fn as_event(&self, key: &IdempotencyKey<'_>) -> Event<Ids, Name> {
        let MetricsKey {
            metric,
            tenant_id,
            timeline_id,
        } = self.0;

        let (kind, value) = self.1;

        Event {
            kind,
            metric,
            idempotency_key: key.to_string(),
            value,
            extra: Ids {
                tenant_id,
                timeline_id,
            },
        }
    }

    fn update_in_place(&self, event: &mut Event<Ids, Name>, key: &IdempotencyKey<'_>) {
        use std::fmt::Write;

        let MetricsKey {
            metric,
            tenant_id,
            timeline_id,
        } = self.0;

        let (kind, value) = self.1;

        *event = Event {
            kind,
            metric,
            idempotency_key: {
                event.idempotency_key.clear();
                write!(event.idempotency_key, "{key}").unwrap();
                std::mem::take(&mut event.idempotency_key)
            },
            value,
            extra: Ids {
                tenant_id,
                timeline_id,
            },
        };
    }
}

impl RawMetricExt for NewRawMetric {
    fn as_event(&self, key: &IdempotencyKey<'_>) -> Event<Ids, Name> {
        let MetricsKey {
            metric,
            tenant_id,
            timeline_id,
        } = self.key;

        let kind = self.kind;
        let value = self.value;

        Event {
            kind,
            metric,
            idempotency_key: key.to_string(),
            value,
            extra: Ids {
                tenant_id,
                timeline_id,
            },
        }
    }

    fn update_in_place(&self, event: &mut Event<Ids, Name>, key: &IdempotencyKey<'_>) {
        use std::fmt::Write;

        let MetricsKey {
            metric,
            tenant_id,
            timeline_id,
        } = self.key;

        let kind = self.kind;
        let value = self.value;

        *event = Event {
            kind,
            metric,
            idempotency_key: {
                event.idempotency_key.clear();
                write!(event.idempotency_key, "{key}").unwrap();
                std::mem::take(&mut event.idempotency_key)
            },
            value,
            extra: Ids {
                tenant_id,
                timeline_id,
            },
        };
    }
}

pub(crate) trait KeyGen<'a> {
    fn generate(&self) -> IdempotencyKey<'a>;
}

impl<'a> KeyGen<'a> for &'a str {
    fn generate(&self) -> IdempotencyKey<'a> {
        IdempotencyKey::generate(self)
    }
}

enum UploadError {
    Rejected(reqwest::StatusCode),
    Reqwest(reqwest::Error),
    Cancelled,
}

impl std::fmt::Debug for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // use same impl because backoff::retry will log this using both
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use UploadError::*;

        match self {
            Rejected(code) => write!(f, "server rejected the metrics with {code}"),
            Reqwest(e) => write!(
                f,
                "request failed: {e}{}",
                e.source().map(|e| format!(": {e}")).unwrap_or_default()
            ),
            Cancelled => write!(f, "cancelled"),
        }
    }
}

impl UploadError {
    fn is_reject(&self) -> bool {
        matches!(self, UploadError::Rejected(_))
    }
}

// this is consumed by the test verifiers
static LAST_IN_BATCH: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("pageserver-metrics-last-upload-in-batch");

async fn upload(
    client: &reqwest::Client,
    metric_collection_endpoint: &reqwest::Url,
    body: bytes::Bytes,
    cancel: &CancellationToken,
    is_last: bool,
) -> Result<(), UploadError> {
    let warn_after = 3;
    let max_attempts = 10;

    // this is used only with tests so far
    let last_value = if is_last { "true" } else { "false" };

    let res = utils::backoff::retry(
        || async {
            let res = client
                .post(metric_collection_endpoint.clone())
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header(LAST_IN_BATCH.clone(), last_value)
                .body(body.clone())
                .send()
                .await;

            let res = res.and_then(|res| res.error_for_status());

            // 10 redirects are normally allowed, so we don't need worry about 3xx
            match res {
                Ok(_response) => Ok(()),
                Err(e) => {
                    let status = e.status().filter(|s| s.is_client_error());
                    if let Some(status) = status {
                        // rejection used to be a thing when the server could reject a
                        // whole batch of metrics if one metric was bad.
                        Err(UploadError::Rejected(status))
                    } else {
                        Err(UploadError::Reqwest(e))
                    }
                }
            }
        },
        UploadError::is_reject,
        warn_after,
        max_attempts,
        "upload consumption_metrics",
        cancel,
    )
    .await
    .ok_or_else(|| UploadError::Cancelled)
    .and_then(|x| x);

    match &res {
        Ok(_) => {}
        Err(e) if e.is_reject() => {
            // permanent errors currently do not get logged by backoff::retry
            // display alternate has no effect, but keeping it here for easier pattern matching.
            tracing::error!("failed to upload metrics: {e:#}");
        }
        Err(_) => {
            // these have been logged already
        }
    }

    res
}

#[cfg(test)]
mod tests {
    use crate::consumption_metrics::{
        disk_cache::read_metrics_from_serde_value, NewMetricsRefRoot,
    };

    use super::*;
    use chrono::{DateTime, Utc};
    use once_cell::sync::Lazy;

    #[test]
    fn chunked_serialization() {
        let examples = metric_samples();
        assert!(examples.len() > 1);

        let now = Utc::now();
        let idempotency_keys = (0..examples.len())
            .map(|i| FixedGen::new(now, "1", i as u16).generate())
            .collect::<Vec<_>>();

        // need to use Event here because serde_json::Value uses default hashmap, not linked
        // hashmap
        #[derive(serde::Deserialize)]
        struct EventChunk {
            events: Vec<Event<Ids, Name>>,
        }

        let correct = serialize_in_chunks(examples.len(), &examples, &idempotency_keys)
            .map(|res| res.unwrap().1)
            .flat_map(|body| serde_json::from_slice::<EventChunk>(&body).unwrap().events)
            .collect::<Vec<_>>();

        for chunk_size in 1..examples.len() {
            let actual = serialize_in_chunks(chunk_size, &examples, &idempotency_keys)
                .map(|res| res.unwrap().1)
                .flat_map(|body| serde_json::from_slice::<EventChunk>(&body).unwrap().events)
                .collect::<Vec<_>>();

            // if these are equal, it means that multi-chunking version works as well
            assert_eq!(correct, actual);
        }
    }

    #[derive(Clone, Copy)]
    struct FixedGen<'a>(chrono::DateTime<chrono::Utc>, &'a str, u16);

    impl<'a> FixedGen<'a> {
        fn new(now: chrono::DateTime<chrono::Utc>, node_id: &'a str, nonce: u16) -> Self {
            FixedGen(now, node_id, nonce)
        }
    }

    impl<'a> KeyGen<'a> for FixedGen<'a> {
        fn generate(&self) -> IdempotencyKey<'a> {
            IdempotencyKey::for_tests(self.0, self.1, self.2)
        }
    }

    static SAMPLES_NOW: Lazy<DateTime<Utc>> = Lazy::new(|| {
        DateTime::parse_from_rfc3339("2023-09-15T00:00:00.123456789Z")
            .unwrap()
            .into()
    });

    #[test]
    fn metric_image_stability() {
        // it is important that these strings stay as they are

        let examples = [
            (
                line!(),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"written_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000","timeline_id":"ffffffffffffffffffffffffffffffff"}"#,
            ),
            (
                line!(),
                r#"{"type":"incremental","start_time":"2023-09-14T00:00:00.123456789Z","stop_time":"2023-09-15T00:00:00.123456789Z","metric":"written_data_bytes_delta","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000","timeline_id":"ffffffffffffffffffffffffffffffff"}"#,
            ),
            (
                line!(),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"timeline_logical_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000","timeline_id":"ffffffffffffffffffffffffffffffff"}"#,
            ),
            (
                line!(),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"remote_storage_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":0,"tenant_id":"00000000000000000000000000000000"}"#,
            ),
            (
                line!(),
                r#"{"type":"absolute","time":"2023-09-15T00:00:00.123456789Z","metric":"synthetic_storage_size","idempotency_key":"2023-09-15 00:00:00.123456789 UTC-1-0000","value":1,"tenant_id":"00000000000000000000000000000000"}"#,
            ),
        ];

        let idempotency_key = consumption_metrics::IdempotencyKey::for_tests(*SAMPLES_NOW, "1", 0);
        let examples = examples.into_iter().zip(metric_samples());

        for ((line, expected), item) in examples {
            let e = consumption_metrics::Event {
                kind: item.kind,
                metric: item.key.metric,
                idempotency_key: idempotency_key.to_string(),
                value: item.value,
                extra: Ids {
                    tenant_id: item.key.tenant_id,
                    timeline_id: item.key.timeline_id,
                },
            };
            let actual = serde_json::to_string(&e).unwrap();
            assert_eq!(
                expected, actual,
                "example for {:?} from line {line}",
                item.kind
            );
        }
    }

    #[test]
    fn disk_format_upgrade() {
        let old_samples_json = serde_json::to_value(metric_samples_old()).unwrap();
        let new_samples =
            serde_json::to_value(NewMetricsRefRoot::new(metric_samples().as_ref())).unwrap();
        let upgraded_samples = read_metrics_from_serde_value(old_samples_json).unwrap();
        let new_samples = read_metrics_from_serde_value(new_samples).unwrap();
        assert_eq!(upgraded_samples, new_samples);
    }

    fn metric_samples_old() -> [RawMetric; 5] {
        let tenant_id = TenantId::from_array([0; 16]);
        let timeline_id = TimelineId::from_array([0xff; 16]);

        let before = DateTime::parse_from_rfc3339("2023-09-14T00:00:00.123456789Z")
            .unwrap()
            .into();
        let [now, before] = [*SAMPLES_NOW, before];

        super::super::metrics::metric_examples_old(tenant_id, timeline_id, now, before)
    }

    fn metric_samples() -> [NewRawMetric; 5] {
        let tenant_id = TenantId::from_array([0; 16]);
        let timeline_id = TimelineId::from_array([0xff; 16]);

        let before = DateTime::parse_from_rfc3339("2023-09-14T00:00:00.123456789Z")
            .unwrap()
            .into();
        let [now, before] = [*SAMPLES_NOW, before];

        super::super::metrics::metric_examples(tenant_id, timeline_id, now, before)
    }
}
