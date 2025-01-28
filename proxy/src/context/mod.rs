//! Connection request monitoring contexts

use std::net::IpAddr;

use chrono::Utc;
use once_cell::sync::OnceCell;
use pq_proto::StartupMessageParams;
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tracing::field::display;
use tracing::{debug, error, info_span, Span};
use try_lock::TryLock;
use uuid::Uuid;

use self::parquet::RequestData;
use crate::control_plane::messages::{ColdStartInfo, MetricsAuxInfo};
use crate::error::ErrorKind;
use crate::intern::{BranchIdInt, ProjectIdInt};
use crate::metrics::{
    ConnectOutcome, InvalidEndpointsGroup, LatencyTimer, Metrics, Protocol, Waiting,
};
use crate::protocol2::{ConnectionInfo, ConnectionInfoExtra};
use crate::types::{DbName, EndpointId, RoleName};

pub mod parquet;

pub(crate) static LOG_CHAN: OnceCell<mpsc::WeakUnboundedSender<RequestData>> = OnceCell::new();
pub(crate) static LOG_CHAN_DISCONNECT: OnceCell<mpsc::WeakUnboundedSender<RequestData>> =
    OnceCell::new();

/// Context data for a single request to connect to a database.
///
/// This data should **not** be used for connection logic, only for observability and limiting purposes.
/// All connection logic should instead use strongly typed state machines, not a bunch of Options.
pub struct RequestContext(
    /// To allow easier use of the ctx object, we have interior mutability.
    /// I would typically use a RefCell but that would break the `Send` requirements
    /// so we need something with thread-safety. `TryLock` is a cheap alternative
    /// that offers similar semantics to a `RefCell` but with synchronisation.
    TryLock<RequestContextInner>,
);

struct RequestContextInner {
    pub(crate) conn_info: ConnectionInfo,
    pub(crate) session_id: Uuid,
    pub(crate) protocol: Protocol,
    first_packet: chrono::DateTime<Utc>,
    region: &'static str,
    pub(crate) span: Span,

    // filled in as they are discovered
    project: Option<ProjectIdInt>,
    branch: Option<BranchIdInt>,
    endpoint_id: Option<EndpointId>,
    dbname: Option<DbName>,
    user: Option<RoleName>,
    application: Option<SmolStr>,
    error_kind: Option<ErrorKind>,
    pub(crate) auth_method: Option<AuthMethod>,
    jwt_issuer: Option<String>,
    success: bool,
    pub(crate) cold_start_info: ColdStartInfo,
    pg_options: Option<StartupMessageParams>,

    // extra
    // This sender is here to keep the request monitoring channel open while requests are taking place.
    sender: Option<mpsc::UnboundedSender<RequestData>>,
    // This sender is only used to log the length of session in case of success.
    disconnect_sender: Option<mpsc::UnboundedSender<RequestData>>,
    pub(crate) latency_timer: LatencyTimer,
    // Whether proxy decided that it's not a valid endpoint end rejected it before going to cplane.
    rejected: Option<bool>,
    disconnect_timestamp: Option<chrono::DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub(crate) enum AuthMethod {
    // aka passwordless, fka link
    ConsoleRedirect,
    ScramSha256,
    ScramSha256Plus,
    Cleartext,
    Jwt,
}

impl Clone for RequestContext {
    fn clone(&self) -> Self {
        let inner = self.0.try_lock().expect("should not deadlock");
        let new = RequestContextInner {
            conn_info: inner.conn_info.clone(),
            session_id: inner.session_id,
            protocol: inner.protocol,
            first_packet: inner.first_packet,
            region: inner.region,
            span: info_span!("background_task"),

            project: inner.project,
            branch: inner.branch,
            endpoint_id: inner.endpoint_id.clone(),
            dbname: inner.dbname.clone(),
            user: inner.user.clone(),
            application: inner.application.clone(),
            error_kind: inner.error_kind,
            auth_method: inner.auth_method.clone(),
            jwt_issuer: inner.jwt_issuer.clone(),
            success: inner.success,
            rejected: inner.rejected,
            cold_start_info: inner.cold_start_info,
            pg_options: inner.pg_options.clone(),

            sender: None,
            disconnect_sender: None,
            latency_timer: LatencyTimer::noop(inner.protocol),
            disconnect_timestamp: inner.disconnect_timestamp,
        };

        Self(TryLock::new(new))
    }
}

impl RequestContext {
    pub fn new(
        session_id: Uuid,
        conn_info: ConnectionInfo,
        protocol: Protocol,
        region: &'static str,
    ) -> Self {
        // TODO: be careful with long lived spans
        let span = info_span!(
            "connect_request",
            %protocol,
            ?session_id,
            %conn_info,
            ep = tracing::field::Empty,
            role = tracing::field::Empty,
        );

        let inner = RequestContextInner {
            conn_info,
            session_id,
            protocol,
            first_packet: Utc::now(),
            region,
            span,

            project: None,
            branch: None,
            endpoint_id: None,
            dbname: None,
            user: None,
            application: None,
            error_kind: None,
            auth_method: None,
            jwt_issuer: None,
            success: false,
            rejected: None,
            cold_start_info: ColdStartInfo::Unknown,
            pg_options: None,

            sender: LOG_CHAN.get().and_then(|tx| tx.upgrade()),
            disconnect_sender: LOG_CHAN_DISCONNECT.get().and_then(|tx| tx.upgrade()),
            latency_timer: LatencyTimer::new(protocol),
            disconnect_timestamp: None,
        };

        Self(TryLock::new(inner))
    }

    #[cfg(test)]
    pub(crate) fn test() -> Self {
        use std::net::SocketAddr;
        let ip = IpAddr::from([127, 0, 0, 1]);
        let addr = SocketAddr::new(ip, 5432);
        let conn_info = ConnectionInfo { addr, extra: None };
        RequestContext::new(Uuid::now_v7(), conn_info, Protocol::Tcp, "test")
    }

    pub(crate) fn console_application_name(&self) -> String {
        let this = self.0.try_lock().expect("should not deadlock");
        format!(
            "{}/{}",
            this.application.as_deref().unwrap_or_default(),
            this.protocol
        )
    }

    pub(crate) fn set_rejected(&self, rejected: bool) {
        let mut this = self.0.try_lock().expect("should not deadlock");
        this.rejected = Some(rejected);
    }

    pub(crate) fn set_cold_start_info(&self, info: ColdStartInfo) {
        self.0
            .try_lock()
            .expect("should not deadlock")
            .set_cold_start_info(info);
    }

    pub(crate) fn set_db_options(&self, options: StartupMessageParams) {
        let mut this = self.0.try_lock().expect("should not deadlock");
        this.set_application(options.get("application_name").map(SmolStr::from));
        if let Some(user) = options.get("user") {
            this.set_user(user.into());
        }
        if let Some(dbname) = options.get("database") {
            this.set_dbname(dbname.into());
        }

        this.pg_options = Some(options);
    }

    pub(crate) fn set_project(&self, x: MetricsAuxInfo) {
        let mut this = self.0.try_lock().expect("should not deadlock");
        if this.endpoint_id.is_none() {
            this.set_endpoint_id(x.endpoint_id.as_str().into());
        }
        this.branch = Some(x.branch_id);
        this.project = Some(x.project_id);
        this.set_cold_start_info(x.cold_start_info);
    }

    pub(crate) fn set_project_id(&self, project_id: ProjectIdInt) {
        let mut this = self.0.try_lock().expect("should not deadlock");
        this.project = Some(project_id);
    }

    pub(crate) fn set_endpoint_id(&self, endpoint_id: EndpointId) {
        self.0
            .try_lock()
            .expect("should not deadlock")
            .set_endpoint_id(endpoint_id);
    }

    pub(crate) fn set_dbname(&self, dbname: DbName) {
        self.0
            .try_lock()
            .expect("should not deadlock")
            .set_dbname(dbname);
    }

    pub(crate) fn set_user(&self, user: RoleName) {
        self.0
            .try_lock()
            .expect("should not deadlock")
            .set_user(user);
    }

    pub(crate) fn set_auth_method(&self, auth_method: AuthMethod) {
        let mut this = self.0.try_lock().expect("should not deadlock");
        this.auth_method = Some(auth_method);
    }

    pub(crate) fn set_jwt_issuer(&self, jwt_issuer: String) {
        let mut this = self.0.try_lock().expect("should not deadlock");
        this.jwt_issuer = Some(jwt_issuer);
    }

    pub fn has_private_peer_addr(&self) -> bool {
        self.0
            .try_lock()
            .expect("should not deadlock")
            .has_private_peer_addr()
    }

    pub(crate) fn set_error_kind(&self, kind: ErrorKind) {
        let mut this = self.0.try_lock().expect("should not deadlock");
        // Do not record errors from the private address to metrics.
        if !this.has_private_peer_addr() {
            Metrics::get().proxy.errors_total.inc(kind);
        }
        if let Some(ep) = &this.endpoint_id {
            let metric = &Metrics::get().proxy.endpoints_affected_by_errors;
            let label = metric.with_labels(kind);
            metric.get_metric(label).measure(ep);
        }
        this.error_kind = Some(kind);
    }

    pub fn set_success(&self) {
        let mut this = self.0.try_lock().expect("should not deadlock");
        this.success = true;
    }

    pub fn log_connect(self) -> DisconnectLogger {
        let mut this = self.0.into_inner();
        this.log_connect();

        // close current span.
        this.span = Span::none();

        DisconnectLogger(this)
    }

    pub(crate) fn protocol(&self) -> Protocol {
        self.0.try_lock().expect("should not deadlock").protocol
    }

    pub(crate) fn span(&self) -> Span {
        self.0.try_lock().expect("should not deadlock").span.clone()
    }

    pub(crate) fn session_id(&self) -> Uuid {
        self.0.try_lock().expect("should not deadlock").session_id
    }

    pub(crate) fn peer_addr(&self) -> IpAddr {
        self.0
            .try_lock()
            .expect("should not deadlock")
            .conn_info
            .addr
            .ip()
    }

    pub(crate) fn extra(&self) -> Option<ConnectionInfoExtra> {
        self.0
            .try_lock()
            .expect("should not deadlock")
            .conn_info
            .extra
            .clone()
    }

    pub(crate) fn cold_start_info(&self) -> ColdStartInfo {
        self.0
            .try_lock()
            .expect("should not deadlock")
            .cold_start_info
    }

    pub(crate) fn latency_timer_pause(&self, waiting_for: Waiting) -> LatencyTimerPause<'_> {
        LatencyTimerPause {
            ctx: self,
            start: tokio::time::Instant::now(),
            waiting_for,
        }
    }

    pub(crate) fn success(&self) {
        self.0
            .try_lock()
            .expect("should not deadlock")
            .latency_timer
            .success();
    }
}

pub(crate) struct LatencyTimerPause<'a> {
    ctx: &'a RequestContext,
    start: tokio::time::Instant,
    waiting_for: Waiting,
}

impl Drop for LatencyTimerPause<'_> {
    fn drop(&mut self) {
        self.ctx
            .0
            .try_lock()
            .expect("should not deadlock")
            .latency_timer
            .unpause(self.start, self.waiting_for);
    }
}

impl RequestContextInner {
    fn set_cold_start_info(&mut self, info: ColdStartInfo) {
        self.cold_start_info = info;
        self.latency_timer.cold_start_info(info);
    }

    fn set_endpoint_id(&mut self, endpoint_id: EndpointId) {
        if self.endpoint_id.is_none() {
            self.span.record("ep", display(&endpoint_id));
            let metric = &Metrics::get().proxy.connecting_endpoints;
            let label = metric.with_labels(self.protocol);
            metric.get_metric(label).measure(&endpoint_id);
            self.endpoint_id = Some(endpoint_id);
        }
    }

    fn set_application(&mut self, app: Option<SmolStr>) {
        if let Some(app) = app {
            self.application = Some(app);
        }
    }

    fn set_dbname(&mut self, dbname: DbName) {
        self.dbname = Some(dbname);
    }

    fn set_user(&mut self, user: RoleName) {
        self.span.record("role", display(&user));
        self.user = Some(user);
    }

    fn has_private_peer_addr(&self) -> bool {
        match self.conn_info.addr.ip() {
            IpAddr::V4(ip) => ip.is_private(),
            IpAddr::V6(_) => false,
        }
    }

    fn log_connect(&mut self) {
        let outcome = if self.success {
            ConnectOutcome::Success
        } else {
            ConnectOutcome::Failed
        };

        // TODO: get rid of entirely/refactor
        // check for false positives
        // AND false negatives
        if let Some(rejected) = self.rejected {
            let ep = self
                .endpoint_id
                .as_ref()
                .map(|x| x.as_str())
                .unwrap_or_default();
            // This makes sense only if cache is disabled
            debug!(
                ?outcome,
                ?rejected,
                ?ep,
                "check endpoint is valid with outcome"
            );
            Metrics::get()
                .proxy
                .invalid_endpoints_total
                .inc(InvalidEndpointsGroup {
                    protocol: self.protocol,
                    rejected: rejected.into(),
                    outcome,
                });
        }

        if let Some(tx) = self.sender.take() {
            // If type changes, this error handling needs to be updated.
            let tx: mpsc::UnboundedSender<RequestData> = tx;
            if let Err(e) = tx.send(RequestData::from(&*self)) {
                error!("log_connect channel send failed: {e}");
            }
        }
    }

    fn log_disconnect(&mut self) {
        // If we are here, it's guaranteed that the user successfully connected to the endpoint.
        // Here we log the length of the session.
        self.disconnect_timestamp = Some(Utc::now());
        if let Some(tx) = self.disconnect_sender.take() {
            // If type changes, this error handling needs to be updated.
            let tx: mpsc::UnboundedSender<RequestData> = tx;
            if let Err(e) = tx.send(RequestData::from(&*self)) {
                error!("log_disconnect channel send failed: {e}");
            }
        }
    }
}

impl Drop for RequestContextInner {
    fn drop(&mut self) {
        if self.sender.is_some() {
            self.log_connect();
        }
    }
}

pub struct DisconnectLogger(RequestContextInner);

impl Drop for DisconnectLogger {
    fn drop(&mut self) {
        self.0.log_disconnect();
    }
}
