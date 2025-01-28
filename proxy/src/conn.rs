use std::future::{poll_fn, Future};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

pub trait Acceptor {
    type Connection: AsyncRead + AsyncWrite + Send + Unpin + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let _ = cx;
        Poll::Ready(Ok(()))
    }

    fn accept(
        &self,
    ) -> impl Future<Output = Result<(Self::Connection, SocketAddr), Self::Error>> + Send;
}

pub trait Connector {
    type Connection: AsyncRead + AsyncWrite + Send + Unpin + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let _ = cx;
        Poll::Ready(Ok(()))
    }

    fn connect(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;
}

pub struct TokioTcpAcceptor {
    listener: TcpListener,
    tcp_nodelay: Option<bool>,
    tcp_keepalive: Option<bool>,
}

impl TokioTcpAcceptor {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        // When set for the server socket, the keepalive setting
        // will be inherited by all accepted client sockets.
        socket2::SockRef::from(&listener).set_keepalive(true)?;
        Ok(Self {
            listener,
            tcp_nodelay: Some(true),
            tcp_keepalive: None,
        })
    }

    pub fn into_std(self) -> io::Result<std::net::TcpListener> {
        self.listener.into_std()
    }
}

impl Acceptor for TokioTcpAcceptor {
    type Connection = TcpStream;
    type Error = io::Error;

    fn accept(&self) -> impl Future<Output = Result<(Self::Connection, SocketAddr), Self::Error>> {
        async move {
            let (stream, addr) = self.listener.accept().await?;

            let socket = socket2::SockRef::from(&stream);
            if let Some(nodelay) = self.tcp_nodelay {
                socket.set_nodelay(nodelay)?;
            }
            if let Some(keepalive) = self.tcp_keepalive {
                socket.set_keepalive(keepalive)?;
            }

            Ok((stream, addr))
        }
    }
}

pub struct TokioTcpConnector;

impl Connector for TokioTcpConnector {
    type Connection = TcpStream;
    type Error = io::Error;

    fn connect(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = Result<Self::Connection, Self::Error>> {
        async move {
            let socket = TcpStream::connect(addr).await?;
            socket.set_nodelay(true)?;
            Ok(socket)
        }
    }
}

pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Stream for T {}

pub trait AsyncRead {
    fn readable(&self) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Send + Sync,
    {
        poll_fn(move |cx| self.poll_read_ready(cx))
    }

    fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>;

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>>;
}

pub trait AsyncWrite {
    fn writable(&self) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Send + Sync,
    {
        poll_fn(move |cx| self.poll_write_ready(cx))
    }

    fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>;

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>>;

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

impl AsyncRead for tokio::net::TcpStream {
    fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        tokio::net::TcpStream::poll_read_ready(self, cx)
    }

    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match tokio::net::TcpStream::try_read(Pin::new(&mut *self).get_mut(), buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        match tokio::net::TcpStream::try_read_vectored(Pin::new(&mut *self).get_mut(), bufs) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for tokio::net::TcpStream {
    fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        tokio::net::TcpStream::poll_write_ready(self, cx)
    }

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        <Self as tokio::io::AsyncWrite>::poll_write(self, cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        <Self as tokio::io::AsyncWrite>::poll_write_vectored(self, cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <Self as tokio::io::AsyncWrite>::poll_flush(self, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <Self as tokio::io::AsyncWrite>::poll_shutdown(self, cx)
    }
}
