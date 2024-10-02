use hyper_1 as hyper;
use std::time::Duration;
use tonic::codegen::StdError;
use tonic::transport::{ClientTlsConfig, Endpoint};
use tonic::{transport::Channel, Status};
use utils::id::{TenantId, TenantTimelineId, TimelineId};

use proto::{
    broker_service_client::BrokerServiceClient, TenantTimelineId as ProtoTenantTimelineId,
};

// Code generated by protobuf.
pub mod proto {
    // Tonic does derives as `#[derive(Clone, PartialEq, ::prost::Message)]`
    // we don't use these types for anything but broker data transmission,
    // so it's ok to ignore this one.
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("storage_broker");
}

pub mod metrics;

// Re-exports to avoid direct tonic dependency in user crates.
pub use tonic::Code;
pub use tonic::Request;
pub use tonic::Streaming;

pub use hyper::Uri;

pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:50051";
pub const DEFAULT_ENDPOINT: &str = const_format::formatcp!("http://{DEFAULT_LISTEN_ADDR}");

pub const DEFAULT_KEEPALIVE_INTERVAL: &str = "5000 ms";
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(5000);

// BrokerServiceClient charged with tonic provided Channel transport; helps to
// avoid depending on tonic directly in user crates.
pub type BrokerClientChannel = BrokerServiceClient<Channel>;

// Create connection object configured to run TLS if schema starts with https://
// and plain text otherwise. Connection is lazy, only endpoint sanity is
// validated here.
//
// NB: this function is not async, but still must be run on a tokio runtime thread
// because that's a requirement of tonic_endpoint.connect_lazy()'s Channel::new call.
pub fn connect<U>(endpoint: U, keepalive_interval: Duration) -> anyhow::Result<BrokerClientChannel>
where
    U: std::convert::TryInto<Uri>,
    U::Error: std::error::Error + Send + Sync + 'static,
{
    let uri: Uri = endpoint.try_into()?;
    let mut tonic_endpoint: Endpoint = uri.into();
    // If schema starts with https, start encrypted connection; do plain text
    // otherwise.
    if let Some("https") = tonic_endpoint.uri().scheme_str() {
        let tls = ClientTlsConfig::new();
        tonic_endpoint = tonic_endpoint.tls_config(tls)?;
    }
    tonic_endpoint = tonic_endpoint
        .http2_keep_alive_interval(keepalive_interval)
        .keep_alive_while_idle(true)
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT);
    //  keep_alive_timeout is 20s by default on both client and server side
    let channel = tonic_endpoint.connect_lazy();
    Ok(BrokerClientChannel::new(channel))
}

impl BrokerClientChannel {
    /// Create a new client to the given endpoint, but don't actually connect until the first request.
    pub async fn connect_lazy<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let conn = tonic::transport::Endpoint::new(dst)?.connect_lazy();
        Ok(Self::new(conn))
    }
}

// parse variable length bytes from protobuf
pub fn parse_proto_ttid(proto_ttid: &ProtoTenantTimelineId) -> Result<TenantTimelineId, Status> {
    let tenant_id = TenantId::from_slice(&proto_ttid.tenant_id)
        .map_err(|e| Status::new(Code::InvalidArgument, format!("malformed tenant_id: {}", e)))?;
    let timeline_id = TimelineId::from_slice(&proto_ttid.timeline_id).map_err(|e| {
        Status::new(
            Code::InvalidArgument,
            format!("malformed timeline_id: {}", e),
        )
    })?;
    Ok(TenantTimelineId {
        tenant_id,
        timeline_id,
    })
}