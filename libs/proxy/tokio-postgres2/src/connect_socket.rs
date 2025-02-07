use crate::config::Host;
use crate::Error;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::{self, TcpStream};
use tokio::time;

pub(crate) async fn connect_socket(
    host_addr: Option<IpAddr>,
    host: &Host,
    port: u16,
    connect_timeout: Option<Duration>,
) -> Result<TcpStream, Error> {
    if let Some(addr) = host_addr {
        let addr = SocketAddr::new(addr, port);

        let stream = connect_with_timeout(TcpStream::connect(addr), connect_timeout).await?;

        stream.set_nodelay(true).map_err(Error::connect)?;

        return Ok(stream);
    }

    match host {
        Host::Tcp(host) => {
            let addrs = net::lookup_host((&**host, port))
                .await
                .map_err(Error::connect)?;

            let mut last_err = None;

            for addr in addrs {
                let stream =
                    match connect_with_timeout(TcpStream::connect(addr), connect_timeout).await {
                        Ok(stream) => stream,
                        Err(e) => {
                            last_err = Some(e);
                            continue;
                        }
                    };

                stream.set_nodelay(true).map_err(Error::connect)?;

                return Ok(stream);
            }

            Err(last_err.unwrap_or_else(|| {
                Error::connect(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve any addresses",
                ))
            }))
        }
    }
}

async fn connect_with_timeout<F, T>(connect: F, timeout: Option<Duration>) -> Result<T, Error>
where
    F: Future<Output = io::Result<T>>,
{
    match timeout {
        Some(timeout) => match time::timeout(timeout, connect).await {
            Ok(Ok(socket)) => Ok(socket),
            Ok(Err(e)) => Err(Error::connect(e)),
            Err(_) => Err(Error::connect(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection timed out",
            ))),
        },
        None => match connect.await {
            Ok(socket) => Ok(socket),
            Err(e) => Err(Error::connect(e)),
        },
    }
}
