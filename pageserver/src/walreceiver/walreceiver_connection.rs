//! Actual Postgres connection handler to stream WAL to the server.

use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, ensure, Context};
use bytes::BytesMut;
use fail::fail_point;
use postgres::{SimpleQueryMessage, SimpleQueryRow};
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use tokio::{pin, select, sync::watch, time};
use tokio_postgres::{replication::ReplicationStream, Client};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, info_span, trace, warn, Instrument};

use super::TaskEvent;
use crate::{
    http::models::WalReceiverEntry,
    repository::{Repository, Timeline},
    tenant_mgr,
    walingest::WalIngest,
};
use postgres_ffi::waldecoder::WalStreamDecoder;
use utils::{lsn::Lsn, pq_proto::ReplicationFeedback, zid::ZTenantTimelineId};

/// Opens a conneciton to the given wal producer and streams the WAL, sending progress messages during streaming.
pub async fn handle_walreceiver_connection(
    id: ZTenantTimelineId,
    wal_producer_connstr: &str,
    events_sender: &watch::Sender<TaskEvent<ReplicationFeedback>>,
    mut cancellation: watch::Receiver<()>,
    connect_timeout: Duration,
) -> anyhow::Result<()> {
    // Connect to the database in replication mode.
    info!("connecting to {wal_producer_connstr}");
    let connect_cfg =
        format!("{wal_producer_connstr} application_name=pageserver replication=true");

    let (mut replication_client, connection) = time::timeout(
        connect_timeout,
        tokio_postgres::connect(&connect_cfg, postgres::NoTls),
    )
    .await
    .context("Timed out while waiting for walreceiver connection to open")?
    .context("Failed to open walreceiver conection")?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    let mut connection_cancellation = cancellation.clone();
    tokio::spawn(
        async move {
            info!("connected!");
            select! {
                    connection_result = connection => match connection_result{
                            Ok(()) => info!("Walreceiver db connection closed"),
                            Err(connection_error) => {
                                if connection_error.is_closed() {
                                    info!("Connection closed regularly: {connection_error}")
                                } else {
                                    warn!("Connection aborted: {connection_error}")
                                }
                            }
                        },

                    _ = connection_cancellation.changed() => info!("Connection cancelled"),
            }
        }
        .instrument(info_span!("safekeeper_handle_db")),
    );

    // Immediately increment the gauge, then create a job to decrement it on task exit.
    // One of the pros of `defer!` is that this will *most probably*
    // get called, even in presence of panics.
    let gauge = crate::LIVE_CONNECTIONS_COUNT.with_label_values(&["wal_receiver"]);
    gauge.inc();
    scopeguard::defer! {
        gauge.dec();
    }

    let identify = identify_system(&mut replication_client).await?;
    info!("{identify:?}");
    let end_of_wal = Lsn::from(u64::from(identify.xlogpos));
    let mut caught_up = false;
    let ZTenantTimelineId {
        tenant_id,
        timeline_id,
    } = id;

    let (repo, timeline) = tokio::task::spawn_blocking(move || {
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)
            .with_context(|| format!("no repository found for tenant {tenant_id}"))?;
        let timeline = tenant_mgr::get_local_timeline_with_load(tenant_id, timeline_id)
            .with_context(|| {
                format!("local timeline {timeline_id} not found for tenant {tenant_id}")
            })?;
        Ok::<_, anyhow::Error>((repo, timeline))
    })
    .await
    .with_context(|| format!("Failed to spawn blocking task to get repository and timeline for tenant {tenant_id} timeline {timeline_id}"))??;

    //
    // Start streaming the WAL, from where we left off previously.
    //
    // If we had previously received WAL up to some point in the middle of a WAL record, we
    // better start from the end of last full WAL record, not in the middle of one.
    let mut last_rec_lsn = timeline.get_last_record_lsn();
    let mut startpoint = last_rec_lsn;

    if startpoint == Lsn(0) {
        bail!("No previous WAL position");
    }

    // There might be some padding after the last full record, skip it.
    startpoint += startpoint.calc_padding(8u32);

    info!("last_record_lsn {last_rec_lsn} starting replication from {startpoint}, server is at {end_of_wal}...");

    let query = format!("START_REPLICATION PHYSICAL {startpoint}");

    let copy_stream = replication_client.copy_both_simple(&query).await?;
    let physical_stream = ReplicationStream::new(copy_stream);
    pin!(physical_stream);

    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let mut walingest = WalIngest::new(timeline.as_ref(), startpoint)?;

    while let Some(replication_message) = {
        select! {
            _ = cancellation.changed() => {
                info!("walreceiver interrupted");
                None
            }
            replication_message = physical_stream.next() => replication_message,
        }
    } {
        let replication_message = replication_message?;
        let status_update = match replication_message {
            ReplicationMessage::XLogData(xlog_data) => {
                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                let data = xlog_data.data();
                let startlsn = Lsn::from(xlog_data.wal_start());
                let endlsn = startlsn + data.len() as u64;

                trace!("received XLogData between {startlsn} and {endlsn}");

                waldecoder.feed_bytes(data);

                while let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                    let _enter = info_span!("processing record", lsn = %lsn).entered();

                    // It is important to deal with the aligned records as lsn in getPage@LSN is
                    // aligned and can be several bytes bigger. Without this alignment we are
                    // at risk of hitting a deadlock.
                    ensure!(lsn.is_aligned());

                    walingest.ingest_record(&timeline, recdata, lsn)?;

                    fail_point!("walreceiver-after-ingest");

                    last_rec_lsn = lsn;
                }

                if !caught_up && endlsn >= end_of_wal {
                    info!("caught up at LSN {endlsn}");
                    caught_up = true;
                }

                let timeline_to_check = Arc::clone(&timeline.tline);
                tokio::task::spawn_blocking(move || timeline_to_check.check_checkpoint_distance())
                    .await
                    .with_context(|| {
                        format!("Spawned checkpoint check task panicked for timeline {id}")
                    })?
                    .with_context(|| {
                        format!("Failed to check checkpoint distance for timeline {id}")
                    })?;

                Some(endlsn)
            }

            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = keepalive.wal_end();
                let timestamp = keepalive.timestamp();
                let reply_requested = keepalive.reply() != 0;

                trace!("received PrimaryKeepAlive(wal_end: {wal_end}, timestamp: {timestamp:?} reply: {reply_requested})");

                if reply_requested {
                    Some(last_rec_lsn)
                } else {
                    None
                }
            }

            _ => None,
        };

        if let Some(last_lsn) = status_update {
            let remote_index = repo.get_remote_index();
            let timeline_remote_consistent_lsn = remote_index
                .read()
                .await
                // here we either do not have this timeline in remote index
                // or there were no checkpoints for it yet
                .timeline_entry(&ZTenantTimelineId {
                    tenant_id,
                    timeline_id,
                })
                .map(|remote_timeline| remote_timeline.metadata.disk_consistent_lsn())
                // no checkpoint was uploaded
                .unwrap_or(Lsn(0));

            // The last LSN we processed. It is not guaranteed to survive pageserver crash.
            let write_lsn = u64::from(last_lsn);
            // `disk_consistent_lsn` is the LSN at which page server guarantees local persistence of all received data
            let flush_lsn = u64::from(timeline.tline.get_disk_consistent_lsn());
            // The last LSN that is synced to remote storage and is guaranteed to survive pageserver crash
            // Used by safekeepers to remove WAL preceding `remote_consistent_lsn`.
            let apply_lsn = u64::from(timeline_remote_consistent_lsn);
            let ts = SystemTime::now();

            // Update the current WAL receiver's data stored inside the global hash table `WAL_RECEIVERS`
            {
                super::WAL_RECEIVER_ENTRIES.write().await.insert(
                    id,
                    WalReceiverEntry {
                        wal_producer_connstr: Some(wal_producer_connstr.to_owned()),
                        last_received_msg_lsn: Some(last_lsn),
                        last_received_msg_ts: Some(
                            ts.duration_since(SystemTime::UNIX_EPOCH)
                                .expect("Received message time should be before UNIX EPOCH!")
                                .as_micros(),
                        ),
                    },
                );
            }

            // Send zenith feedback message.
            // Regular standby_status_update fields are put into this message.
            let zenith_status_update = ReplicationFeedback {
                current_timeline_size: timeline.get_current_logical_size() as u64,
                ps_writelsn: write_lsn,
                ps_flushlsn: flush_lsn,
                ps_applylsn: apply_lsn,
                ps_replytime: ts,
            };

            debug!("zenith_status_update {zenith_status_update:?}");

            let mut data = BytesMut::new();
            zenith_status_update.serialize(&mut data)?;
            physical_stream
                .as_mut()
                .zenith_status_update(data.len() as u64, &data)
                .await?;
            if let Err(e) = events_sender.send(TaskEvent::NewEvent(zenith_status_update)) {
                warn!("Wal connection event listener dropped, aborting the connection: {e}");
                return Ok(());
            }
        }
    }

    Ok(())
}

/// Data returned from the postgres `IDENTIFY_SYSTEM` command
///
/// See the [postgres docs] for more details.
///
/// [postgres docs]: https://www.postgresql.org/docs/current/protocol-replication.html
#[derive(Debug)]
// As of nightly 2021-09-11, fields that are only read by the type's `Debug` impl still count as
// unused. Relevant issue: https://github.com/rust-lang/rust/issues/88900
#[allow(dead_code)]
struct IdentifySystem {
    systemid: u64,
    timeline: u32,
    xlogpos: PgLsn,
    dbname: Option<String>,
}

/// There was a problem parsing the response to
/// a postgres IDENTIFY_SYSTEM command.
#[derive(Debug, thiserror::Error)]
#[error("IDENTIFY_SYSTEM parse error")]
struct IdentifyError;

/// Run the postgres `IDENTIFY_SYSTEM` command
async fn identify_system(client: &mut Client) -> anyhow::Result<IdentifySystem> {
    let query_str = "IDENTIFY_SYSTEM";
    let response = client.simple_query(query_str).await?;

    // get(N) from row, then parse it as some destination type.
    fn get_parse<T>(row: &SimpleQueryRow, idx: usize) -> Result<T, IdentifyError>
    where
        T: FromStr,
    {
        let val = row.get(idx).ok_or(IdentifyError)?;
        val.parse::<T>().or(Err(IdentifyError))
    }

    // extract the row contents into an IdentifySystem struct.
    // written as a closure so I can use ? for Option here.
    if let Some(SimpleQueryMessage::Row(first_row)) = response.get(0) {
        Ok(IdentifySystem {
            systemid: get_parse(first_row, 0)?,
            timeline: get_parse(first_row, 1)?,
            xlogpos: get_parse(first_row, 2)?,
            dbname: get_parse(first_row, 3).ok(),
        })
    } else {
        Err(IdentifyError.into())
    }
}
