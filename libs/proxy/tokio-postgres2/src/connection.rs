use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::error::DbError;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::{AsyncMessage, Error, Notification};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures_util::{ready, Sink, Stream};
use log::{info, trace};
use postgres_protocol2::message::backend::Message;
use postgres_protocol2::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tokio_util::sync::PollSender;

pub enum RequestMessages {
    Single(FrontendMessage),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<BackendMessages>,
}

pub struct Response {
    sender: PollSender<BackendMessages>,
}

#[derive(PartialEq, Debug)]
enum State {
    Active,
    Closing,
}

/// A connection to a PostgreSQL database.
///
/// This is one half of what is returned when a new connection is established. It performs the actual IO with the
/// server, and should generally be spawned off onto an executor to run in the background.
///
/// `Connection` implements `Future`, and only resolves when the connection is closed, either because a fatal error has
/// occurred, or because its associated `Client` has dropped and all outstanding work has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Connection<S, T> {
    /// HACK: we need this in the Neon Proxy.
    pub stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    /// HACK: we need this in the Neon Proxy to forward params.
    pub parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,
    pending_responses: VecDeque<BackendMessage>,
    responses: VecDeque<Response>,
    state: State,
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        pending_responses: VecDeque<BackendMessage>,
        parameters: HashMap<String, String>,
        receiver: mpsc::UnboundedReceiver<Request>,
    ) -> Connection<S, T> {
        Connection {
            stream,
            parameters,
            receiver,
            pending_responses,
            responses: VecDeque::new(),
            state: State::Active,
        }
    }

    fn poll_response(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<BackendMessage, Error>>> {
        if let Some(message) = self.pending_responses.pop_front() {
            trace!("retrying pending response");
            return Poll::Ready(Some(Ok(message)));
        }

        Pin::new(&mut self.stream)
            .poll_next(cx)
            .map(|o| o.map(|r| r.map_err(Error::io)))
    }

    /// Read and process messages from the connection to postgres.
    /// client <- postgres
    fn poll_read(&mut self, cx: &mut Context<'_>) -> Result<Option<AsyncMessage>, Error> {
        if self.state == State::Closing {
            trace!("poll_read: done");
            return Ok(None);
        }

        loop {
            let message = match self.poll_response(cx)? {
                Poll::Ready(Some(message)) => message,
                Poll::Ready(None) => return Err(Error::closed()),
                Poll::Pending => {
                    trace!("poll_read: waiting on response");
                    return Ok(None);
                }
            };

            let (mut messages, request_complete) = match message {
                BackendMessage::Async(Message::NoticeResponse(body)) => {
                    let error = DbError::parse(&mut body.fields()).map_err(Error::parse)?;
                    return Ok(Some(AsyncMessage::Notice(error)));
                }
                BackendMessage::Async(Message::NotificationResponse(body)) => {
                    let notification = Notification {
                        process_id: body.process_id(),
                        channel: body.channel().map_err(Error::parse)?.to_string(),
                        payload: body.message().map_err(Error::parse)?.to_string(),
                    };
                    return Ok(Some(AsyncMessage::Notification(notification)));
                }
                BackendMessage::Async(Message::ParameterStatus(body)) => {
                    self.parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                    continue;
                }
                BackendMessage::Async(_) => unreachable!(),
                BackendMessage::Normal {
                    messages,
                    request_complete,
                } => (messages, request_complete),
            };

            let mut response = match self.responses.pop_front() {
                Some(response) => response,
                None => match messages.next().map_err(Error::parse)? {
                    Some(Message::ErrorResponse(error)) => return Err(Error::db(error)),
                    _ => return Err(Error::unexpected_message()),
                },
            };

            match response.sender.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    let _ = response.sender.send_item(messages);
                    if !request_complete {
                        self.responses.push_front(response);
                    }
                }
                Poll::Ready(Err(_)) => {
                    // we need to keep paging through the rest of the messages even if the receiver's hung up
                    if !request_complete {
                        self.responses.push_front(response);
                    }
                }
                Poll::Pending => {
                    self.responses.push_front(response);
                    self.pending_responses.push_back(BackendMessage::Normal {
                        messages,
                        request_complete,
                    });
                    trace!("poll_read: waiting on sender");
                    return Ok(None);
                }
            }
        }
    }

    /// Fetch the next client request and enqueue the response sender.
    fn poll_request(&mut self, cx: &mut Context<'_>) -> Poll<Option<RequestMessages>> {
        if self.receiver.is_closed() {
            return Poll::Ready(None);
        }

        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(request)) => {
                trace!("polled new request");
                self.responses.push_back(Response {
                    sender: PollSender::new(request.sender),
                });
                Poll::Ready(Some(request.messages))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Process client requests and write them to the postgres connection, flushing if necessary.
    /// client -> postgres
    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        loop {
            if self.state == State::Closing {
                trace!("poll_write: done");
                return Ok(());
            }

            if Pin::new(&mut self.stream)
                .poll_ready(cx)
                .map_err(Error::io)?
                .is_pending()
            {
                trace!("poll_write: waiting on socket");

                // poll_ready is self-flushing.
                return Ok(());
            }

            match self.poll_request(cx) {
                Poll::Ready(Some(RequestMessages::Single(request))) => {
                    Pin::new(&mut self.stream)
                        .start_send(request)
                        .map_err(Error::io)?;
                }
                Poll::Ready(None) if self.responses.is_empty() => {
                    trace!("poll_write: at eof, terminating");
                    let mut request = BytesMut::new();
                    frontend::terminate(&mut request);
                    let request = FrontendMessage::Raw(request.freeze());

                    Pin::new(&mut self.stream)
                        .start_send(request)
                        .map_err(Error::io)?;

                    trace!("poll_write: sent eof, closing");
                    self.state = State::Closing;

                    trace!("poll_write: done");
                    return Ok(());
                }
                Poll::Ready(None) => {
                    trace!(
                        "poll_write: at eof, pending responses {}",
                        self.responses.len()
                    );
                    self.poll_flush(cx)?;
                    return Ok(());
                }
                Poll::Pending => {
                    trace!("poll_write: waiting on request");
                    self.poll_flush(cx)?;
                    return Ok(());
                }
            }
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        match Pin::new(&mut self.stream)
            .poll_flush(cx)
            .map_err(Error::io)?
        {
            Poll::Ready(()) => trace!("poll_flush: flushed"),
            Poll::Pending => trace!("poll_flush: waiting on socket"),
        }
        Ok(())
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match Pin::new(&mut self.stream)
            .poll_close(cx)
            .map_err(Error::io)?
        {
            Poll::Ready(()) => {
                trace!("poll_shutdown: complete");
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                trace!("poll_shutdown: waiting on socket");
                Poll::Pending
            }
        }
    }

    /// Returns the value of a runtime parameter for this connection.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }

    /// Polls for asynchronous messages from the server.
    ///
    /// The server can send notices as well as notifications asynchronously to the client. Applications that wish to
    /// examine those messages should use this method to drive the connection rather than its `Future` implementation.
    pub fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AsyncMessage, Error>>> {
        let message = self.poll_read(cx)?;
        self.poll_write(cx)?;

        match message {
            Some(message) => Poll::Ready(Some(Ok(message))),
            // if state is not closing, and there's no message from postgres, then:
            //
            // poll_read():
            // 1. There were no pending responses and we're waiting for postgres to send a response.
            // 2. There are pending responses but the Client cannot yet receive it.
            //
            // poll_write():
            // 3. The buffer to postgres is too full, so flushing.
            // 4. There's no more client requests, but still pending responses, and there is data to flush.
            // 5. There's no more client requests, but still pending responses, but there is no data to flush.
            // 6. We're still waiting for client requests.
            //
            // In both cases 1 and 2, the cx.waker() is registered.
            // In cases 3, 4, and 6 the cx.waker() is regestered.
            //
            // In case 5 no waker is registered, but the only process can be made by waiting for data from postgres, but we only
            // from cases 1 or 2 that a waker is registered for reading.
            None if self.state != State::Closing => Poll::Pending,
            None => match self.poll_shutdown(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(None),
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl<S, T> Future for Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        while let Some(message) = ready!(self.poll_message(cx)?) {
            if let AsyncMessage::Notice(notice) = message {
                info!("{}: {}", notice.severity(), notice.message());
            }
        }
        Poll::Ready(Ok(()))
    }
}
