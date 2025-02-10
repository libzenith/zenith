use anyhow::Context;
use hyper::client::conn;
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tracing::warn;

const NEONVM_DAEMON_CONTROL_SOCKET_PATH: &str = "/neonvm/run/neonvm-daemon-socket";

/// Open a connection to neonvm-daemon's control socket, prepare to send
/// requests to it with hyper.
async fn connect_neonvm_daemon<B>() -> anyhow::Result<SendRequest<B>>
where
    B: hyper::body::Body + 'static + Send,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let mut attempts = 0;
    let stream = loop {
        match tokio::net::UnixStream::connect(NEONVM_DAEMON_CONTROL_SOCKET_PATH).await {
            Ok(stream) => break stream,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && attempts < 50 => {
                // Retry
                warn!("neonvm-daemon control socket not found, retrying...");
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(err) => Err(err).context("opening neonvm-daemon control socket")?,
        }
    };
    let io = TokioIo::new(stream);
    let (request_sender, connection) = conn::http1::handshake(io).await.unwrap();

    // spawn a task to poll the connection and drive the HTTP state
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Error in connection: {}", e);
        }
    });

    Ok(request_sender)
}

pub fn resize_swap(size_bytes: u64) -> anyhow::Result<()> {
    let rt = tokio::runtime::Handle::current();
    rt.block_on(resize_swap_async(size_bytes))
}

pub async fn resize_swap_async(size_bytes: u64) -> anyhow::Result<()> {
    let mut neonvmd = connect_neonvm_daemon().await?;

    // Passing 'once' causes neonvm-daemon to reject any future resize requests
    let request = Request::builder()
        .method("POST")
        .uri("/resize-swap-once")
        .header("Host", "localhost") // hyper requires Host, even though the server won't care
        .body(format!("{}", size_bytes))
        .unwrap();

    let resp = neonvmd.send_request(request).await?;
    let status = resp.status();
    match status {
        StatusCode::OK => Ok(()),
        StatusCode::CONFLICT => {
            // 409 Conflict means that the swap was already resized. That happens if the
            // compute_ctl restarts within the VM. That's considered OK.
            warn!("Swap was already resized");
            Ok(())
        }
        _ => Err(anyhow::anyhow!(
            "error resizing swap: {}",
            status.to_string()
        )),
    }
}

pub fn set_disk_quota(size_bytes: u64) -> anyhow::Result<()> {
    let rt = tokio::runtime::Handle::current();
    rt.block_on(set_disk_quota_async(size_bytes))
}

/// If size_bytes is 0, it disables the quota. Otherwise, it sets filesystem quota to size_bytes.
pub async fn set_disk_quota_async(size_bytes: u64) -> anyhow::Result<()> {
    let mut neonvmd = connect_neonvm_daemon().await?;

    let request = Request::builder()
        .method("POST")
        .uri("/set-disk-quota")
        .header("Host", "localhost") // hyper requires Host, even though the server won't care
        .body(format!("{}", size_bytes))
        .unwrap();

    let resp = neonvmd.send_request(request).await?;
    let status = resp.status();
    match status {
        StatusCode::OK => Ok(()),
        _ => Err(anyhow::anyhow!(
            "error setting disk quota: {}",
            status.to_string()
        )),
    }
}
