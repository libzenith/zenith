use tokio::net::TcpStream;

use crate::client::SocketConfig;
use crate::config::{Host, SslMode};
use crate::tls::MakeTlsConnect;
use crate::{cancel_query_raw, connect_socket, Error};
use std::io;

pub(crate) async fn cancel_query<T>(
    config: Option<SocketConfig>,
    ssl_mode: SslMode,
    mut tls: T,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    T: MakeTlsConnect<TcpStream>,
{
    let config = match config {
        Some(config) => config,
        None => {
            return Err(Error::connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown host",
            )))
        }
    };

    let hostname = match &config.host {
        Host::Tcp(host) => &**host,
    };
    let tls = tls
        .make_tls_connect(hostname)
        .map_err(|e| Error::tls(e.into()))?;

    let socket =
        connect_socket::connect_socket(config.host_addr, &config.host, config.port, config.connect_timeout).await?;

    cancel_query_raw::cancel_query_raw(socket, ssl_mode, tls, process_id, secret_key).await
}
