//! WAL receiver manages an open connection to safekeeper, to get the WAL it streams into.
//! To do so, a current implementation needs to do the following:
//!
//! * acknowledge the timelines that it needs to stream WAL into.
//! Pageserver is able to dynamically (un)load tenants on attach and detach,
//! hence WAL receiver needs to react on such events.
//!
//! * get a broker subscription, stream data from it to determine that a timeline needs WAL streaming.
//! For that, it watches specific keys in storage_broker and pulls the relevant data periodically.
//! The data is produced by safekeepers, that push it periodically and pull it to synchronize between each other.
//! Without this data, no WAL streaming is possible currently.
//!
//! Only one active WAL streaming connection is allowed at a time.
//! The connection is supposed to be updated periodically, based on safekeeper timeline data.
//!
//! * handle the actual connection and WAL streaming
//!
//! Handling happens dynamically, by portions of WAL being processed and registered in the server.
//! Along with the registration, certain metadata is written to show WAL streaming progress and rely on that when considering safekeepers for connection.
//!
//! The current module contains high-level primitives used in the submodules; general synchronization, timeline acknowledgement and shutdown logic.

mod connection_manager;
mod walreceiver_connection;

use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind, WALRECEIVER_RUNTIME};
use crate::tenant::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::timeline::walreceiver::connection_manager::{
    connection_manager_loop_step, ConnectionManagerState,
};

use std::future::Future;
use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;
use storage_broker::BrokerClientChannel;
use tokio::select;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::*;

use utils::id::TenantTimelineId;

use self::connection_manager::ConnectionManagerStatus;

use super::Timeline;

#[derive(Clone)]
pub struct WalReceiverConf {
    /// The timeout on the connection to safekeeper for WAL streaming.
    pub wal_connect_timeout: Duration,
    /// The timeout to use to determine when the current connection is "stale" and reconnect to the other one.
    pub lagging_wal_timeout: Duration,
    /// The Lsn lag to use to determine when the current connection is lagging to much behind and reconnect to the other one.
    pub max_lsn_wal_lag: NonZeroU64,
    pub auth_token: Option<Arc<String>>,
    pub availability_zone: Option<String>,
}

pub struct WalReceiver {
    timeline: TenantTimelineId,
    manager_status: Arc<std::sync::RwLock<Option<ConnectionManagerStatus>>>,
}

impl WalReceiver {
    pub fn start(
        timeline: Arc<Timeline>,
        conf: WalReceiverConf,
        mut broker_client: BrokerClientChannel,
        ctx: &RequestContext,
    ) -> Self {
        let tenant_id = timeline.tenant_id;
        let timeline_id = timeline.timeline_id;
        let walreceiver_ctx =
            ctx.detached_child(TaskKind::WalReceiverManager, DownloadBehavior::Error);

        let loop_status = Arc::new(std::sync::RwLock::new(None));
        let manager_status = Arc::clone(&loop_status);
        task_mgr::spawn(
            WALRECEIVER_RUNTIME.handle(),
            TaskKind::WalReceiverManager,
            Some(tenant_id),
            Some(timeline_id),
            &format!("walreceiver for timeline {tenant_id}/{timeline_id}"),
            false,
            async move {
                debug_assert_current_span_has_tenant_and_timeline_id();
                debug!("WAL receiver manager started, connecting to broker");
                let mut connection_manager_state = ConnectionManagerState::new(
                    timeline,
                    conf,
                );
                loop {
                    select! {
                        _ = task_mgr::shutdown_watcher() => {
                            trace!("WAL receiver shutdown requested, shutting down");
                            break;
                        },
                        loop_step_result = connection_manager_loop_step(
                            &mut broker_client,
                            &mut connection_manager_state,
                            &walreceiver_ctx,
                            &loop_status,
                        ) => match loop_step_result {
                            ControlFlow::Continue(()) => continue,
                            ControlFlow::Break(()) => {
                                trace!("Connection manager loop ended, shutting down");
                                break;
                            }
                        },
                    }
                }

                connection_manager_state.shutdown().await;
                *loop_status.write().unwrap() = None;
                Ok(())
            }
            .instrument(info_span!(parent: None, "wal_connection_manager", tenant_id = %tenant_id, timeline_id = %timeline_id))
        );

        Self {
            timeline: TenantTimelineId::new(tenant_id, timeline_id),
            manager_status,
        }
    }

    pub async fn stop(self) {
        task_mgr::shutdown_tasks(
            Some(TaskKind::WalReceiverManager),
            Some(self.timeline.tenant_id),
            Some(self.timeline.timeline_id),
        )
        .await;
    }

    pub(super) fn status(&self) -> Option<ConnectionManagerStatus> {
        self.manager_status.read().unwrap().clone()
    }
}

/// A handle of an asynchronous task.
/// The task has a channel that it can use to communicate its lifecycle events in a certain form, see [`TaskEvent`]
/// and a cancellation token that it can listen to for earlier interrupts.
///
/// Note that the communication happens via the `watch` channel, that does not accumulate the events, replacing the old one with the never one on submission.
/// That may lead to certain events not being observed by the listener.
#[derive(Debug)]
struct TaskHandle<E> {
    join_handle: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    events_receiver: watch::Receiver<TaskStateUpdate<E>>,
    cancellation: CancellationToken,
}

enum TaskEvent<E> {
    Update(TaskStateUpdate<E>),
    End(anyhow::Result<()>),
}

#[derive(Debug, Clone)]
enum TaskStateUpdate<E> {
    Started,
    Progress(E),
}

impl<E: Clone> TaskHandle<E> {
    /// Initializes the task, starting it immediately after the creation.
    fn spawn<Fut>(
        task: impl FnOnce(watch::Sender<TaskStateUpdate<E>>, CancellationToken) -> Fut + Send + 'static,
    ) -> Self
    where
        Fut: Future<Output = anyhow::Result<()>> + Send,
        E: Send + Sync + 'static,
    {
        let cancellation = CancellationToken::new();
        let (events_sender, events_receiver) = watch::channel(TaskStateUpdate::Started);

        let cancellation_clone = cancellation.clone();
        let join_handle = WALRECEIVER_RUNTIME.spawn(async move {
            events_sender.send(TaskStateUpdate::Started).ok();
            task(events_sender, cancellation_clone).await
            // events_sender is dropped at some point during the .await above.
            // But the task is still running on WALRECEIVER_RUNTIME.
            // That is the window when `!jh.is_finished()`
            // is true inside `fn next_task_event()` below.
        });

        TaskHandle {
            join_handle: Some(join_handle),
            events_receiver,
            cancellation,
        }
    }

    async fn next_task_event(&mut self) -> TaskEvent<E> {
        match self.events_receiver.changed().await {
            Ok(()) => TaskEvent::Update((self.events_receiver.borrow()).clone()),
            Err(_task_channel_part_dropped) => {
                TaskEvent::End(match self.join_handle.as_mut() {
                    Some(jh) => {
                        if !jh.is_finished() {
                            // See: https://github.com/neondatabase/neon/issues/2885
                            trace!("sender is dropped while join handle is still alive");
                        }

                        let res = match jh.await {
                            Ok(res) => res,
                            Err(je) if je.is_cancelled() => {
                                unreachable!("we don't cancel via tokio")
                            }
                            Err(je) if je.is_panic() => {
                                // panic has already been reported; do not double report
                                Ok(())
                            }
                            Err(je) => {
                                Err(anyhow::Error::new(je).context("join task walreceiver task"))
                            }
                        };

                        // For cancellation-safety, drop join_handle only after successful .await.
                        self.join_handle = None;

                        res
                    }
                    None => {
                        // Another option is to have an enum, join handle or result and give away the reference to it
                        Err(anyhow::anyhow!("Task was joined more than once"))
                    }
                })
            }
        }
    }

    /// Aborts current task, waiting for it to finish.
    async fn shutdown(self) {
        if let Some(jh) = self.join_handle {
            self.cancellation.cancel();
            match jh.await {
                Ok(Ok(())) => debug!("Shutdown success"),
                Ok(Err(e)) => error!("Shutdown task error: {e:?}"),
                Err(join_error) => {
                    if join_error.is_cancelled() {
                        error!("Shutdown task was cancelled");
                    } else {
                        error!("Shutdown task join error: {join_error}")
                    }
                }
            }
        }
    }
}
