use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use pageserver_api::models::PageserverUtilization;

use thiserror::Error;
use utils::id::NodeId;

use crate::node::Node;

struct Heartbeater {
    receiver: tokio::sync::mpsc::UnboundedReceiver<HeartbeatRequest>,
    cancel: CancellationToken,

    state: Arc<HashMap<NodeId, PageserverState>>,

    max_unavailable_interval: Duration,
    jwt_token: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum PageserverState {
    Available {
        last_seen_at: Instant,
        utilization: PageserverUtilization,
    },
    Offline,
}

#[derive(Debug)]
pub(crate) struct AvailablityDeltas(pub Vec<(NodeId, PageserverState)>);

#[derive(Debug, Error)]
pub(crate) enum HeartbeaterError {
    #[error("Cancelled")]
    Cancel,
}

struct HeartbeatRequest {
    pageservers: Arc<HashMap<NodeId, Node>>,
    reply: tokio::sync::oneshot::Sender<Result<AvailablityDeltas, HeartbeaterError>>,
}

pub(crate) struct HeartbeaterHandler {
    sender: tokio::sync::mpsc::UnboundedSender<HeartbeatRequest>,
}

impl HeartbeaterHandler {
    pub(crate) fn new(
        jwt_token: Option<String>,
        max_unavailable_interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<HeartbeatRequest>();
        let mut heartbeater =
            Heartbeater::new(receiver, jwt_token, max_unavailable_interval, cancel);
        tokio::task::spawn(async move { heartbeater.run().await });

        Self { sender }
    }

    pub(crate) async fn heartbeat(
        &self,
        pageservers: Arc<HashMap<NodeId, Node>>,
    ) -> Result<AvailablityDeltas, HeartbeaterError> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.sender
            .send(HeartbeatRequest {
                pageservers,
                reply: sender,
            })
            .unwrap();

        receiver.await.unwrap()
    }
}

impl Heartbeater {
    fn new(
        receiver: tokio::sync::mpsc::UnboundedReceiver<HeartbeatRequest>,
        jwt_token: Option<String>,
        max_unavailable_interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            receiver,
            cancel,
            state: Arc::new(HashMap::new()),
            max_unavailable_interval,
            jwt_token,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                request = self.receiver.recv() => {
                    match request {
                        Some(req) => {
                            let res = self.heartbeat(req.pageservers).await;
                            req.reply.send(res).unwrap();
                        },
                        None => { return; }
                    }
                },
                _ = self.cancel.cancelled() => return
            }
        }
    }

    async fn heartbeat(
        &mut self,
        pageservers: Arc<HashMap<NodeId, Node>>,
    ) -> Result<AvailablityDeltas, HeartbeaterError> {
        let current_state = (*self.state).clone();
        let mut new_state = HashMap::new();

        let mut heartbeat_futs = FuturesUnordered::new();
        for (node_id, node) in &*pageservers {
            heartbeat_futs.push({
                let jwt_token = self.jwt_token.clone();
                let cancel = self.cancel.clone();
                async move {
                    let response = node
                        .with_client_retries(
                            |client| async move { client.get_utilization().await },
                            &jwt_token,
                            2,
                            3,
                            Duration::from_secs(1),
                            &cancel,
                        )
                        .await;

                    let response = match response {
                        Some(r) => r,
                        None => {
                            // This indicates cancellation of the request.
                            // We ignore the node in this case.
                            return None;
                        }
                    };

                    let status = if let Ok(utilization) = response {
                        PageserverState::Available {
                            last_seen_at: Instant::now(),
                            utilization,
                        }
                    } else {
                        PageserverState::Offline
                    };

                    Some((*node_id, status))
                }
            });

            loop {
                let maybe_status = tokio::select! {
                    next = heartbeat_futs.next() => {
                        match next {
                            Some(result) => result,
                            None => { break; }
                        }
                    },
                    _ = self.cancel.cancelled() => { return Err(HeartbeaterError::Cancel); }
                };

                if let Some((node_id, status)) = maybe_status {
                    new_state.insert(node_id, status);
                }
            }
        }

        let mut deltas = Vec::new();
        let now = Instant::now();
        for (node_id, ps_state) in &new_state {
            match current_state.get(node_id) {
                Some(current_ps_state) => match (current_ps_state, ps_state) {
                    (PageserverState::Offline, PageserverState::Offline) => {}
                    (PageserverState::Available { last_seen_at, .. }, PageserverState::Offline) => {
                        if now - *last_seen_at >= self.max_unavailable_interval {
                            deltas.push((*node_id, ps_state.clone()));
                        }
                    }
                    _ => {
                        deltas.push((*node_id, ps_state.clone()));
                    }
                },
                None => {
                    deltas.push((*node_id, ps_state.clone()));
                }
            }
        }

        self.state = Arc::new(new_state);

        Ok(AvailablityDeltas(deltas))
    }
}
