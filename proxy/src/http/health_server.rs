use anyhow::{anyhow, bail};
use hyper::{Body, Request, Response, StatusCode};
use std::{convert::Infallible, net::TcpListener};
use tracing::info;
use utils::http::{
    endpoint::{self, prometheus_metrics_handler, request_span},
    error::ApiError,
    json::json_response,
    RouterBuilder, RouterService,
};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    endpoint::make_router()
        .get("/metrics", |r| request_span(r, prometheus_metrics_handler))
        .get("/v1/status", status_handler)
}

pub async fn task_main(http_listener: TcpListener) -> anyhow::Result<Infallible> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService::new(make_router().build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    bail!("hyper server without shutdown handling cannot shutdown successfully");
}
