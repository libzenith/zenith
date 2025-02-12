use std::{str::FromStr, time::Duration};

use pageserver_api::{
    controller_api::{
        AvailabilityZone, NodeAvailability, NodeDescribeResponse, NodeRegisterRequest,
        NodeSchedulingPolicy, TenantLocateResponseShard,
    },
    shard::TenantShardId,
};
use pageserver_client::mgmt_api;
use reqwest::StatusCode;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use utils::{backoff, id::NodeId};

use crate::{
    pageserver_client::PageserverClient, persistence::NodePersistence, scheduler::MaySchedule,
};

/// Represents the in-memory description of a Node.
///
/// Scheduling statistics are maintened separately in [`crate::scheduler`].
///
/// The persistent subset of the Node is defined in [`crate::persistence::NodePersistence`]: the
/// implementation of serialization on this type is only for debug dumps.
#[derive(Clone, Serialize)]
pub(crate) struct Node {
    id: NodeId,

    availability: NodeAvailability,
    scheduling: NodeSchedulingPolicy,

    listen_http_addr: String,
    listen_http_port: u16,
    use_https: bool,

    listen_pg_addr: String,
    listen_pg_port: u16,

    availability_zone_id: AvailabilityZone,

    // This cancellation token means "stop any RPCs in flight to this node, and don't start
    // any more". It is not related to process shutdown.
    #[serde(skip)]
    cancel: CancellationToken,
}

/// When updating [`Node::availability`] we use this type to indicate to the caller
/// whether/how they changed it.
pub(crate) enum AvailabilityTransition {
    ToActive,
    ToWarmingUpFromActive,
    ToWarmingUpFromOffline,
    ToOffline,
    Unchanged,
}

impl Node {
    pub(crate) fn base_url(&self) -> String {
        format!("{}://{}:{}", if self.use_https { "https" } else {"http"}, self.listen_http_addr, self.listen_http_port)
    }

    pub(crate) fn get_id(&self) -> NodeId {
        self.id
    }

    #[allow(unused)]
    pub(crate) fn get_availability_zone_id(&self) -> &AvailabilityZone {
        &self.availability_zone_id
    }

    pub(crate) fn get_scheduling(&self) -> NodeSchedulingPolicy {
        self.scheduling
    }

    pub(crate) fn set_scheduling(&mut self, scheduling: NodeSchedulingPolicy) {
        self.scheduling = scheduling
    }

    /// Does this registration request match `self`?  This is used when deciding whether a registration
    /// request should be allowed to update an existing record with the same node ID.
    pub(crate) fn registration_match(&self, register_req: &NodeRegisterRequest) -> bool {
        self.id == register_req.node_id
            && self.listen_http_addr == register_req.listen_http_addr
            && self.listen_http_port == register_req.listen_http_port
            && self.use_https == register_req.use_https
            && self.listen_pg_addr == register_req.listen_pg_addr
            && self.listen_pg_port == register_req.listen_pg_port
            && self.availability_zone_id == register_req.availability_zone_id
    }

    /// For a shard located on this node, populate a response object
    /// with this node's address information.
    pub(crate) fn shard_location(&self, shard_id: TenantShardId) -> TenantLocateResponseShard {
        TenantLocateResponseShard {
            shard_id,
            node_id: self.id,
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port,
            use_https: self.use_https,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port,
        }
    }

    pub(crate) fn get_availability(&self) -> &NodeAvailability {
        &self.availability
    }

    pub(crate) fn set_availability(&mut self, availability: NodeAvailability) {
        use AvailabilityTransition::*;
        use NodeAvailability::WarmingUp;

        match self.get_availability_transition(&availability) {
            ToActive => {
                // Give the node a new cancellation token, effectively resetting it to un-cancelled.  Any
                // users of previously-cloned copies of the node will still see the old cancellation
                // state.  For example, Reconcilers in flight will have to complete and be spawned
                // again to realize that the node has become available.
                self.cancel = CancellationToken::new();
            }
            ToOffline | ToWarmingUpFromActive => {
                // Fire the node's cancellation token to cancel any in-flight API requests to it
                self.cancel.cancel();
            }
            Unchanged | ToWarmingUpFromOffline => {}
        }

        if let (WarmingUp(crnt), WarmingUp(proposed)) = (&self.availability, &availability) {
            self.availability = WarmingUp(std::cmp::max(*crnt, *proposed));
        } else {
            self.availability = availability;
        }
    }

    /// Without modifying the availability of the node, convert the intended availability
    /// into a description of the transition.
    pub(crate) fn get_availability_transition(
        &self,
        availability: &NodeAvailability,
    ) -> AvailabilityTransition {
        use AvailabilityTransition::*;
        use NodeAvailability::*;

        match (&self.availability, availability) {
            (Offline, Active(_)) => ToActive,
            (Active(_), Offline) => ToOffline,
            (Active(_), WarmingUp(_)) => ToWarmingUpFromActive,
            (WarmingUp(_), Offline) => ToOffline,
            (WarmingUp(_), Active(_)) => ToActive,
            (Offline, WarmingUp(_)) => ToWarmingUpFromOffline,
            _ => Unchanged,
        }
    }

    /// Whether we may send API requests to this node.
    pub(crate) fn is_available(&self) -> bool {
        // When we clone a node, [`Self::availability`] is a snapshot, but [`Self::cancel`] holds
        // a reference to the original Node's cancellation status.  Checking both of these results
        // in a "pessimistic" check where we will consider a Node instance unavailable if it was unavailable
        // when we cloned it, or if the original Node instance's cancellation token was fired.
        matches!(self.availability, NodeAvailability::Active(_)) && !self.cancel.is_cancelled()
    }

    /// Is this node elegible to have work scheduled onto it?
    pub(crate) fn may_schedule(&self) -> MaySchedule {
        let utilization = match &self.availability {
            NodeAvailability::Active(u) => u.clone(),
            NodeAvailability::Offline | NodeAvailability::WarmingUp(_) => return MaySchedule::No,
        };

        match self.scheduling {
            NodeSchedulingPolicy::Active => MaySchedule::Yes(utilization),
            NodeSchedulingPolicy::Draining => MaySchedule::No,
            NodeSchedulingPolicy::Filling => MaySchedule::Yes(utilization),
            NodeSchedulingPolicy::Pause => MaySchedule::No,
            NodeSchedulingPolicy::PauseForRestart => MaySchedule::No,
        }
    }

    pub(crate) fn new(
        id: NodeId,
        listen_http_addr: String,
        listen_http_port: u16,
        use_https: bool,
        listen_pg_addr: String,
        listen_pg_port: u16,
        availability_zone_id: AvailabilityZone,
    ) -> Self {
        Self {
            id,
            listen_http_addr,
            listen_http_port,
            use_https,
            listen_pg_addr,
            listen_pg_port,
            scheduling: NodeSchedulingPolicy::Active,
            availability: NodeAvailability::Offline,
            availability_zone_id,
            cancel: CancellationToken::new(),
        }
    }

    pub(crate) fn to_persistent(&self) -> NodePersistence {
        NodePersistence {
            node_id: self.id.0 as i64,
            scheduling_policy: self.scheduling.into(),
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port as i32,
            use_https: self.use_https,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port as i32,
            availability_zone_id: self.availability_zone_id.0.clone(),
        }
    }

    pub(crate) fn from_persistent(np: NodePersistence) -> Self {
        Self {
            id: NodeId(np.node_id as u64),
            // At startup we consider a node offline until proven otherwise.
            availability: NodeAvailability::Offline,
            scheduling: NodeSchedulingPolicy::from_str(&np.scheduling_policy)
                .expect("Bad scheduling policy in DB"),
            listen_http_addr: np.listen_http_addr,
            listen_http_port: np.listen_http_port as u16,
            use_https: np.use_https,
            listen_pg_addr: np.listen_pg_addr,
            listen_pg_port: np.listen_pg_port as u16,
            availability_zone_id: AvailabilityZone(np.availability_zone_id),
            cancel: CancellationToken::new(),
        }
    }

    /// Wrapper for issuing requests to pageserver management API: takes care of generic
    /// retry/backoff for retryable HTTP status codes.
    ///
    /// This will return None to indicate cancellation.  Cancellation may happen from
    /// the cancellation token passed in, or from Self's cancellation token (i.e. node
    /// going offline).
    pub(crate) async fn with_client_retries<T, O, F>(
        &self,
        mut op: O,
        jwt: &Option<String>,
        warn_threshold: u32,
        max_retries: u32,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Option<mgmt_api::Result<T>>
    where
        O: FnMut(PageserverClient) -> F,
        F: std::future::Future<Output = mgmt_api::Result<T>>,
    {
        fn is_fatal(e: &mgmt_api::Error) -> bool {
            use mgmt_api::Error::*;
            match e {
                SendRequest(_) | ReceiveBody(_) | ReceiveErrorBody(_) => false,
                ApiError(StatusCode::SERVICE_UNAVAILABLE, _)
                | ApiError(StatusCode::GATEWAY_TIMEOUT, _)
                | ApiError(StatusCode::REQUEST_TIMEOUT, _) => false,
                ApiError(_, _) => true,
                Cancelled => true,
            }
        }

        backoff::retry(
            || {
                let http_client = reqwest::ClientBuilder::new()
                    .timeout(timeout)
                    .build()
                    .expect("Failed to construct HTTP client");

                let client = PageserverClient::from_client(
                    self.get_id(),
                    http_client,
                    self.base_url(),
                    jwt.as_deref(),
                );

                let node_cancel_fut = self.cancel.cancelled();

                let op_fut = op(client);

                async {
                    tokio::select! {
                        r = op_fut=> {r},
                        _ = node_cancel_fut => {
                        Err(mgmt_api::Error::Cancelled)
                    }}
                }
            },
            is_fatal,
            warn_threshold,
            max_retries,
            &format!(
                "Call to node {} ({}:{}) management API",
                self.id, self.listen_http_addr, self.listen_http_port
            ),
            cancel,
        )
        .await
    }

    /// Generate the simplified API-friendly description of a node's state
    pub(crate) fn describe(&self) -> NodeDescribeResponse {
        NodeDescribeResponse {
            id: self.id,
            availability: self.availability.clone().into(),
            scheduling: self.scheduling,
            availability_zone_id: self.availability_zone_id.0.clone(),
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port,
            use_https: self.use_https,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port,
        }
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.id, self.listen_http_addr)
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.id, self.listen_http_addr)
    }
}
