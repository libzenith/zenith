//! Postgres protocol messages serialization-deserialization. See
//! <https://www.postgresql.org/docs/devel/protocol-message-formats.html>
//! on message formats.

// Tools for calling certain async methods in sync contexts.
pub mod sync;

use anyhow::{ensure, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_protocol::PG_EPOCH;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    future::Future,
    io::{self, Cursor},
    str,
    time::{Duration, SystemTime},
};
use sync::{AsyncishRead, SyncFuture};
use tokio::io::AsyncReadExt;
use tracing::{trace, warn};

pub type Oid = u32;
pub type SystemId = u64;

pub const INT8_OID: Oid = 20;
pub const INT4_OID: Oid = 23;
pub const TEXT_OID: Oid = 25;

#[derive(Debug)]
pub enum FeMessage {
    StartupPacket(FeStartupPacket),
    // Simple query.
    Query(Bytes),
    // Extended query protocol.
    Parse(FeParseMessage),
    Describe(FeDescribeMessage),
    Bind(FeBindMessage),
    Execute(FeExecuteMessage),
    Close(FeCloseMessage),
    Sync,
    Terminate,
    CopyData(Bytes),
    CopyDone,
    CopyFail,
    PasswordMessage(Bytes),
}

#[derive(Debug)]
pub enum FeStartupPacket {
    CancelRequest(CancelKeyData),
    SslRequest,
    GssEncRequest,
    StartupMessage {
        major_version: u32,
        minor_version: u32,
        params: StartupMessageParams,
    },
}

#[derive(Debug)]
pub struct StartupMessageParams {
    params: HashMap<String, String>,
}

impl StartupMessageParams {
    /// Get parameter's value by its name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.params.get(name).map(|s| s.as_str())
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// taking into account all escape sequences but leaving them as-is.
    /// [`None`] means that there's no `options` in [`Self`].
    pub fn options_raw(&self) -> Option<impl Iterator<Item = &str>> {
        self.get("options").map(Self::parse_options_raw)
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// applying all escape sequences (using owned strings as needed).
    /// [`None`] means that there's no `options` in [`Self`].
    pub fn options_escaped(&self) -> Option<impl Iterator<Item = Cow<'_, str>>> {
        self.get("options").map(Self::parse_options_escaped)
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// taking into account all escape sequences but leaving them as-is.
    pub fn parse_options_raw(input: &str) -> impl Iterator<Item = &str> {
        // See `postgres: pg_split_opts`.
        let mut last_was_escape = false;
        input
            .split(move |c: char| {
                // We split by non-escaped whitespace symbols.
                let should_split = c.is_ascii_whitespace() && !last_was_escape;
                last_was_escape = c == '\\' && !last_was_escape;
                should_split
            })
            .filter(|s| !s.is_empty())
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// applying all escape sequences (using owned strings as needed).
    pub fn parse_options_escaped(input: &str) -> impl Iterator<Item = Cow<'_, str>> {
        // See `postgres: pg_split_opts`.
        Self::parse_options_raw(input).map(|s| {
            let mut preserve_next_escape = false;
            let escape = |c| {
                // We should remove '\\' unless it's preceded by '\\'.
                let should_remove = c == '\\' && !preserve_next_escape;
                preserve_next_escape = should_remove;
                should_remove
            };

            match s.contains('\\') {
                true => Cow::Owned(s.replace(escape, "")),
                false => Cow::Borrowed(s),
            }
        })
    }

    /// Iterate through key-value pairs in an arbitrary order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.params.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    // This function is mostly useful in tests.
    #[doc(hidden)]
    pub fn new<'a, const N: usize>(pairs: [(&'a str, &'a str); N]) -> Self {
        Self {
            params: pairs.map(|(k, v)| (k.to_owned(), v.to_owned())).into(),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct CancelKeyData {
    pub backend_pid: i32,
    pub cancel_key: i32,
}

impl fmt::Display for CancelKeyData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hi = (self.backend_pid as u64) << 32;
        let lo = self.cancel_key as u64;
        let id = hi | lo;

        // This format is more compact and might work better for logs.
        f.debug_tuple("CancelKeyData")
            .field(&format_args!("{:x}", id))
            .finish()
    }
}

use rand::distributions::{Distribution, Standard};
impl Distribution<CancelKeyData> for Standard {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> CancelKeyData {
        CancelKeyData {
            backend_pid: rng.gen(),
            cancel_key: rng.gen(),
        }
    }
}

// We only support the simple case of Parse on unnamed prepared statement and
// no params
#[derive(Debug)]
pub struct FeParseMessage {
    pub query_string: Bytes,
}

#[derive(Debug)]
pub struct FeDescribeMessage {
    pub kind: u8, // 'S' to describe a prepared statement; or 'P' to describe a portal.
                  // we only support unnamed prepared stmt or portal
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
pub struct FeBindMessage;

// we only support unnamed prepared stmt or portal
#[derive(Debug)]
pub struct FeExecuteMessage {
    /// max # of rows
    pub maxrows: i32,
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
pub struct FeCloseMessage;

/// Retry a read on EINTR
///
/// This runs the enclosed expression, and if it returns
/// Err(io::ErrorKind::Interrupted), retries it.
macro_rules! retry_read {
    ( $x:expr ) => {
        loop {
            match $x {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                res => break res,
            }
        }
    };
}

/// An error occured during connection being open.
#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    /// IO error during writing to or reading from the connection socket.
    #[error("Socket IO error: {0}")]
    Socket(std::io::Error),
    /// Invalid packet was received from client
    #[error("Protocol error: {0}")]
    Protocol(String),
    /// Failed to parse a protocol mesage
    #[error("Message parse error: {0}")]
    MessageParse(anyhow::Error),
}

impl From<anyhow::Error> for ConnectionError {
    fn from(e: anyhow::Error) -> Self {
        Self::MessageParse(e)
    }
}

impl ConnectionError {
    pub fn into_io_error(self) -> io::Error {
        match self {
            ConnectionError::Socket(io) => io,
            other => io::Error::new(io::ErrorKind::Other, other.to_string()),
        }
    }
}

impl FeMessage {
    /// Read one message from the stream.
    /// This function returns `Ok(None)` in case of EOF.
    /// One way to handle this properly:
    ///
    /// ```
    /// # use std::io;
    /// # use pq_proto::FeMessage;
    /// #
    /// # fn process_message(msg: FeMessage) -> anyhow::Result<()> {
    /// #     Ok(())
    /// # };
    /// #
    /// fn do_the_job(stream: &mut (impl io::Read + Unpin)) -> anyhow::Result<()> {
    ///     while let Some(msg) = FeMessage::read(stream)? {
    ///         process_message(msg)?;
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline(never)]
    pub fn read(
        stream: &mut (impl io::Read + Unpin),
    ) -> Result<Option<FeMessage>, ConnectionError> {
        Self::read_fut(&mut AsyncishRead(stream)).wait()
    }

    /// Read one message from the stream.
    /// See documentation for `Self::read`.
    pub fn read_fut<Reader>(
        stream: &mut Reader,
    ) -> SyncFuture<Reader, impl Future<Output = Result<Option<FeMessage>, ConnectionError>> + '_>
    where
        Reader: tokio::io::AsyncRead + Unpin,
    {
        // We return a Future that's sync (has a `wait` method) if and only if the provided stream is SyncProof.
        // SyncFuture contract: we are only allowed to await on sync-proof futures, the AsyncRead and
        // AsyncReadExt methods of the stream.
        SyncFuture::new(async move {
            // Each libpq message begins with a message type byte, followed by message length
            // If the client closes the connection, return None. But if the client closes the
            // connection in the middle of a message, we will return an error.
            let tag = match retry_read!(stream.read_u8().await) {
                Ok(b) => b,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(ConnectionError::Socket(e)),
            };

            // The message length includes itself, so it better be at least 4.
            let len = retry_read!(stream.read_u32().await)
                .map_err(ConnectionError::Socket)?
                .checked_sub(4)
                .ok_or_else(|| ConnectionError::Protocol("invalid message length".to_string()))?;

            let body = {
                let mut buffer = vec![0u8; len as usize];
                stream
                    .read_exact(&mut buffer)
                    .await
                    .map_err(ConnectionError::Socket)?;
                Bytes::from(buffer)
            };

            match tag {
                b'Q' => Ok(Some(FeMessage::Query(body))),
                b'P' => Ok(Some(FeParseMessage::parse(body)?)),
                b'D' => Ok(Some(FeDescribeMessage::parse(body)?)),
                b'E' => Ok(Some(FeExecuteMessage::parse(body)?)),
                b'B' => Ok(Some(FeBindMessage::parse(body)?)),
                b'C' => Ok(Some(FeCloseMessage::parse(body)?)),
                b'S' => Ok(Some(FeMessage::Sync)),
                b'X' => Ok(Some(FeMessage::Terminate)),
                b'd' => Ok(Some(FeMessage::CopyData(body))),
                b'c' => Ok(Some(FeMessage::CopyDone)),
                b'f' => Ok(Some(FeMessage::CopyFail)),
                b'p' => Ok(Some(FeMessage::PasswordMessage(body))),
                tag => {
                    return Err(ConnectionError::Protocol(format!(
                        "unknown message tag: {tag},'{body:?}'"
                    )))
                }
            }
        })
    }
}

impl FeStartupPacket {
    /// Read startup message from the stream.
    // XXX: It's tempting yet undesirable to accept `stream` by value,
    // since such a change will cause user-supplied &mut references to be consumed
    pub fn read(
        stream: &mut (impl io::Read + Unpin),
    ) -> Result<Option<FeMessage>, ConnectionError> {
        Self::read_fut(&mut AsyncishRead(stream)).wait()
    }

    /// Read startup message from the stream.
    // XXX: It's tempting yet undesirable to accept `stream` by value,
    // since such a change will cause user-supplied &mut references to be consumed
    pub fn read_fut<Reader>(
        stream: &mut Reader,
    ) -> SyncFuture<Reader, impl Future<Output = Result<Option<FeMessage>, ConnectionError>> + '_>
    where
        Reader: tokio::io::AsyncRead + Unpin,
    {
        const MAX_STARTUP_PACKET_LENGTH: usize = 10000;
        const RESERVED_INVALID_MAJOR_VERSION: u32 = 1234;
        const CANCEL_REQUEST_CODE: u32 = 5678;
        const NEGOTIATE_SSL_CODE: u32 = 5679;
        const NEGOTIATE_GSS_CODE: u32 = 5680;

        SyncFuture::new(async move {
            // Read length. If the connection is closed before reading anything (or before
            // reading 4 bytes, to be precise), return None to indicate that the connection
            // was closed. This matches the PostgreSQL server's behavior, which avoids noise
            // in the log if the client opens connection but closes it immediately.
            let len = match retry_read!(stream.read_u32().await) {
                Ok(len) => len as usize,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(ConnectionError::Socket(e)),
            };

            #[allow(clippy::manual_range_contains)]
            if len < 4 || len > MAX_STARTUP_PACKET_LENGTH {
                return Err(ConnectionError::Protocol(format!(
                    "invalid message length {len}"
                )));
            }

            let request_code =
                retry_read!(stream.read_u32().await).map_err(ConnectionError::Socket)?;

            // the rest of startup packet are params
            let params_len = len - 8;
            let mut params_bytes = vec![0u8; params_len];
            stream
                .read_exact(params_bytes.as_mut())
                .await
                .map_err(ConnectionError::Socket)?;

            // Parse params depending on request code
            let req_hi = request_code >> 16;
            let req_lo = request_code & ((1 << 16) - 1);
            let message = match (req_hi, req_lo) {
                (RESERVED_INVALID_MAJOR_VERSION, CANCEL_REQUEST_CODE) => {
                    if params_len != 8 {
                        return Err(ConnectionError::Protocol(
                            "expected 8 bytes for CancelRequest params".to_string(),
                        ));
                    }
                    let mut cursor = Cursor::new(params_bytes);
                    FeStartupPacket::CancelRequest(CancelKeyData {
                        backend_pid: cursor.read_i32().await.map_err(ConnectionError::Socket)?,
                        cancel_key: cursor.read_i32().await.map_err(ConnectionError::Socket)?,
                    })
                }
                (RESERVED_INVALID_MAJOR_VERSION, NEGOTIATE_SSL_CODE) => {
                    // Requested upgrade to SSL (aka TLS)
                    FeStartupPacket::SslRequest
                }
                (RESERVED_INVALID_MAJOR_VERSION, NEGOTIATE_GSS_CODE) => {
                    // Requested upgrade to GSSAPI
                    FeStartupPacket::GssEncRequest
                }
                (RESERVED_INVALID_MAJOR_VERSION, unrecognized_code) => {
                    return Err(ConnectionError::Protocol(format!(
                        "Unrecognized request code {unrecognized_code}"
                    )));
                }
                // TODO bail if protocol major_version is not 3?
                (major_version, minor_version) => {
                    // Parse pairs of null-terminated strings (key, value).
                    // See `postgres: ProcessStartupPacket, build_startup_packet`.
                    let mut tokens = str::from_utf8(&params_bytes)
                        .context("StartupMessage params: invalid utf-8")?
                        .strip_suffix('\0') // drop packet's own null
                        .ok_or_else(|| {
                            ConnectionError::Protocol(
                                "StartupMessage params: missing null terminator".to_string(),
                            )
                        })?
                        .split_terminator('\0');

                    let mut params = HashMap::new();
                    while let Some(name) = tokens.next() {
                        let value = tokens.next().ok_or_else(|| {
                            ConnectionError::Protocol(
                                "StartupMessage params: key without value".to_string(),
                            )
                        })?;

                        params.insert(name.to_owned(), value.to_owned());
                    }

                    FeStartupPacket::StartupMessage {
                        major_version,
                        minor_version,
                        params: StartupMessageParams { params },
                    }
                }
            };

            Ok(Some(FeMessage::StartupPacket(message)))
        })
    }
}

impl FeParseMessage {
    fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        // FIXME: the rust-postgres driver uses a named prepared statement
        // for copy_out(). We're not prepared to handle that correctly. For
        // now, just ignore the statement name, assuming that the client never
        // uses more than one prepared statement at a time.

        let _pstmt_name = read_cstr(&mut buf)?;
        let query_string = read_cstr(&mut buf)?;
        let nparams = buf.get_i16();

        ensure!(nparams == 0, "query params not implemented");

        Ok(FeMessage::Parse(FeParseMessage { query_string }))
    }
}

impl FeDescribeMessage {
    fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let kind = buf.get_u8();
        let _pstmt_name = read_cstr(&mut buf)?;

        // FIXME: see FeParseMessage::parse
        ensure!(
            kind == b'S',
            "only prepared statemement Describe is implemented"
        );

        Ok(FeMessage::Describe(FeDescribeMessage { kind }))
    }
}

impl FeExecuteMessage {
    fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let portal_name = read_cstr(&mut buf)?;
        let maxrows = buf.get_i32();

        ensure!(portal_name.is_empty(), "named portals not implemented");
        ensure!(maxrows == 0, "row limit in Execute message not implemented");

        Ok(FeMessage::Execute(FeExecuteMessage { maxrows }))
    }
}

impl FeBindMessage {
    fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let portal_name = read_cstr(&mut buf)?;
        let _pstmt_name = read_cstr(&mut buf)?;

        // FIXME: see FeParseMessage::parse
        ensure!(portal_name.is_empty(), "named portals not implemented");

        Ok(FeMessage::Bind(FeBindMessage))
    }
}

impl FeCloseMessage {
    fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let _kind = buf.get_u8();
        let _pstmt_or_portal_name = read_cstr(&mut buf)?;

        // FIXME: we do nothing with Close
        Ok(FeMessage::Close(FeCloseMessage))
    }
}

// Backend

#[derive(Debug)]
pub enum BeMessage<'a> {
    AuthenticationOk,
    AuthenticationMD5Password([u8; 4]),
    AuthenticationSasl(BeAuthenticationSaslMessage<'a>),
    AuthenticationCleartextPassword,
    BackendKeyData(CancelKeyData),
    BindComplete,
    CommandComplete(&'a [u8]),
    CopyData(&'a [u8]),
    CopyDone,
    CopyFail,
    CopyInResponse,
    CopyOutResponse,
    CopyBothResponse,
    CloseComplete,
    // None means column is NULL
    DataRow(&'a [Option<&'a [u8]>]),
    ErrorResponse(&'a str, Option<&'a [u8; 5]>),
    /// Single byte - used in response to SSLRequest/GSSENCRequest.
    EncryptionResponse(bool),
    NoData,
    ParameterDescription,
    ParameterStatus {
        name: &'a [u8],
        value: &'a [u8],
    },
    ParseComplete,
    ReadyForQuery,
    RowDescription(&'a [RowDescriptor<'a>]),
    XLogData(XLogDataBody<'a>),
    NoticeResponse(&'a str),
    KeepAlive(WalSndKeepAlive),
}

/// Common shorthands.
impl<'a> BeMessage<'a> {
    /// A [`BeMessage::ParameterStatus`] holding the client encoding, i.e. UTF-8.
    /// This is a sensible default, given that:
    ///  * rust strings only support this encoding out of the box.
    ///  * tokio-postgres, postgres-jdbc (and probably more) mandate it.
    ///
    /// TODO: do we need to report `server_encoding` as well?
    pub const CLIENT_ENCODING: Self = Self::ParameterStatus {
        name: b"client_encoding",
        value: b"UTF8",
    };

    /// Build a [`BeMessage::ParameterStatus`] holding the server version.
    pub fn server_version(version: &'a str) -> Self {
        Self::ParameterStatus {
            name: b"server_version",
            value: version.as_bytes(),
        }
    }
}

#[derive(Debug)]
pub enum BeAuthenticationSaslMessage<'a> {
    Methods(&'a [&'a str]),
    Continue(&'a [u8]),
    Final(&'a [u8]),
}

#[derive(Debug)]
pub enum BeParameterStatusMessage<'a> {
    Encoding(&'a str),
    ServerVersion(&'a str),
}

// One row description in RowDescription packet.
#[derive(Debug)]
pub struct RowDescriptor<'a> {
    pub name: &'a [u8],
    pub tableoid: Oid,
    pub attnum: i16,
    pub typoid: Oid,
    pub typlen: i16,
    pub typmod: i32,
    pub formatcode: i16,
}

impl Default for RowDescriptor<'_> {
    fn default() -> RowDescriptor<'static> {
        RowDescriptor {
            name: b"",
            tableoid: 0,
            attnum: 0,
            typoid: 0,
            typlen: 0,
            typmod: 0,
            formatcode: 0,
        }
    }
}

impl RowDescriptor<'_> {
    /// Convenience function to create a RowDescriptor message for an int8 column
    pub const fn int8_col(name: &[u8]) -> RowDescriptor {
        RowDescriptor {
            name,
            tableoid: 0,
            attnum: 0,
            typoid: INT8_OID,
            typlen: 8,
            typmod: 0,
            formatcode: 0,
        }
    }

    pub const fn text_col(name: &[u8]) -> RowDescriptor {
        RowDescriptor {
            name,
            tableoid: 0,
            attnum: 0,
            typoid: TEXT_OID,
            typlen: -1,
            typmod: 0,
            formatcode: 0,
        }
    }
}

#[derive(Debug)]
pub struct XLogDataBody<'a> {
    pub wal_start: u64,
    pub wal_end: u64,
    pub timestamp: i64,
    pub data: &'a [u8],
}

#[derive(Debug)]
pub struct WalSndKeepAlive {
    pub sent_ptr: u64,
    pub timestamp: i64,
    pub request_reply: bool,
}

pub static HELLO_WORLD_ROW: BeMessage = BeMessage::DataRow(&[Some(b"hello world")]);

// single text column
pub static SINGLE_COL_ROWDESC: BeMessage = BeMessage::RowDescription(&[RowDescriptor {
    name: b"data",
    tableoid: 0,
    attnum: 0,
    typoid: TEXT_OID,
    typlen: -1,
    typmod: 0,
    formatcode: 0,
}]);

/// Call f() to write body of the message and prepend it with 4-byte len as
/// prescribed by the protocol.
fn write_body<R>(buf: &mut BytesMut, f: impl FnOnce(&mut BytesMut) -> R) -> R {
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    let res = f(buf);

    let size = i32::try_from(buf.len() - base).expect("message too big to transmit");
    (&mut buf[base..]).put_slice(&size.to_be_bytes());

    res
}

/// Safe write of s into buf as cstring (String in the protocol).
fn write_cstr(s: impl AsRef<[u8]>, buf: &mut BytesMut) -> io::Result<()> {
    let bytes = s.as_ref();
    if bytes.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "string contains embedded null",
        ));
    }
    buf.put_slice(bytes);
    buf.put_u8(0);
    Ok(())
}

fn read_cstr(buf: &mut Bytes) -> anyhow::Result<Bytes> {
    let pos = buf.iter().position(|x| *x == 0);
    let result = buf.split_to(pos.context("missing terminator")?);
    buf.advance(1); // drop the null terminator
    Ok(result)
}

pub const SQLSTATE_INTERNAL_ERROR: &[u8; 5] = b"XX000";

impl<'a> BeMessage<'a> {
    /// Write message to the given buf.
    // Unlike the reading side, we use BytesMut
    // here as msg len precedes its body and it is handy to write it down first
    // and then fill the length. With Write we would have to either calc it
    // manually or have one more buffer.
    pub fn write(buf: &mut BytesMut, message: &BeMessage) -> io::Result<()> {
        match message {
            BeMessage::AuthenticationOk => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    buf.put_i32(0); // Specifies that the authentication was successful.
                });
            }

            BeMessage::AuthenticationCleartextPassword => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    buf.put_i32(3); // Specifies that clear text password is required.
                });
            }

            BeMessage::AuthenticationMD5Password(salt) => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    buf.put_i32(5); // Specifies that an MD5-encrypted password is required.
                    buf.put_slice(&salt[..]);
                });
            }

            BeMessage::AuthenticationSasl(msg) => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    use BeAuthenticationSaslMessage::*;
                    match msg {
                        Methods(methods) => {
                            buf.put_i32(10); // Specifies that SASL auth method is used.
                            for method in methods.iter() {
                                write_cstr(method, buf)?;
                            }
                            buf.put_u8(0); // zero terminator for the list
                        }
                        Continue(extra) => {
                            buf.put_i32(11); // Continue SASL auth.
                            buf.put_slice(extra);
                        }
                        Final(extra) => {
                            buf.put_i32(12); // Send final SASL message.
                            buf.put_slice(extra);
                        }
                    }
                    Ok::<_, io::Error>(())
                })?;
            }

            BeMessage::BackendKeyData(key_data) => {
                buf.put_u8(b'K');
                write_body(buf, |buf| {
                    buf.put_i32(key_data.backend_pid);
                    buf.put_i32(key_data.cancel_key);
                });
            }

            BeMessage::BindComplete => {
                buf.put_u8(b'2');
                write_body(buf, |_| {});
            }

            BeMessage::CloseComplete => {
                buf.put_u8(b'3');
                write_body(buf, |_| {});
            }

            BeMessage::CommandComplete(cmd) => {
                buf.put_u8(b'C');
                write_body(buf, |buf| write_cstr(cmd, buf))?;
            }

            BeMessage::CopyData(data) => {
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_slice(data);
                });
            }

            BeMessage::CopyDone => {
                buf.put_u8(b'c');
                write_body(buf, |_| {});
            }

            BeMessage::CopyFail => {
                buf.put_u8(b'f');
                write_body(buf, |_| {});
            }

            BeMessage::CopyInResponse => {
                buf.put_u8(b'G');
                write_body(buf, |buf| {
                    buf.put_u8(1); // copy_is_binary
                    buf.put_i16(0); // numAttributes
                });
            }

            BeMessage::CopyOutResponse => {
                buf.put_u8(b'H');
                write_body(buf, |buf| {
                    buf.put_u8(0); // copy_is_binary
                    buf.put_i16(0); // numAttributes
                });
            }

            BeMessage::CopyBothResponse => {
                buf.put_u8(b'W');
                write_body(buf, |buf| {
                    // doesn't matter, used only for replication
                    buf.put_u8(0); // copy_is_binary
                    buf.put_i16(0); // numAttributes
                });
            }

            BeMessage::DataRow(vals) => {
                buf.put_u8(b'D');
                write_body(buf, |buf| {
                    buf.put_u16(vals.len() as u16); // num of cols
                    for val_opt in vals.iter() {
                        if let Some(val) = val_opt {
                            buf.put_u32(val.len() as u32);
                            buf.put_slice(val);
                        } else {
                            buf.put_i32(-1);
                        }
                    }
                });
            }

            // ErrorResponse is a zero-terminated array of zero-terminated fields.
            // First byte of each field represents type of this field. Set just enough fields
            // to satisfy rust-postgres client: 'S' -- severity, 'C' -- error, 'M' -- error
            // message text.
            BeMessage::ErrorResponse(error_msg, pg_error_code) => {
                // 'E' signalizes ErrorResponse messages
                buf.put_u8(b'E');
                write_body(buf, |buf| {
                    buf.put_u8(b'S'); // severity
                    buf.put_slice(b"ERROR\0");

                    buf.put_u8(b'C'); // SQLSTATE error code
                    buf.put_slice(&terminate_code(
                        pg_error_code.unwrap_or(SQLSTATE_INTERNAL_ERROR),
                    ));

                    buf.put_u8(b'M'); // the message
                    write_cstr(error_msg, buf)?;

                    buf.put_u8(0); // terminator
                    Ok::<_, io::Error>(())
                })?;
            }

            // NoticeResponse has the same format as ErrorResponse. From doc: "The frontend should display the
            // message but continue listening for ReadyForQuery or ErrorResponse"
            BeMessage::NoticeResponse(error_msg) => {
                // For all the errors set Severity to Error and error code to
                // 'internal error'.

                // 'N' signalizes NoticeResponse messages
                buf.put_u8(b'N');
                write_body(buf, |buf| {
                    buf.put_u8(b'S'); // severity
                    buf.put_slice(b"NOTICE\0");

                    buf.put_u8(b'C'); // SQLSTATE error code
                    buf.put_slice(&terminate_code(SQLSTATE_INTERNAL_ERROR));

                    buf.put_u8(b'M'); // the message
                    write_cstr(error_msg.as_bytes(), buf)?;

                    buf.put_u8(0); // terminator
                    Ok::<_, io::Error>(())
                })?;
            }

            BeMessage::NoData => {
                buf.put_u8(b'n');
                write_body(buf, |_| {});
            }

            BeMessage::EncryptionResponse(should_negotiate) => {
                let response = if *should_negotiate { b'S' } else { b'N' };
                buf.put_u8(response);
            }

            BeMessage::ParameterStatus { name, value } => {
                buf.put_u8(b'S');
                write_body(buf, |buf| {
                    write_cstr(name, buf)?;
                    write_cstr(value, buf)
                })?;
            }

            BeMessage::ParameterDescription => {
                buf.put_u8(b't');
                write_body(buf, |buf| {
                    // we don't support params, so always 0
                    buf.put_i16(0);
                });
            }

            BeMessage::ParseComplete => {
                buf.put_u8(b'1');
                write_body(buf, |_| {});
            }

            BeMessage::ReadyForQuery => {
                buf.put_u8(b'Z');
                write_body(buf, |buf| {
                    buf.put_u8(b'I');
                });
            }

            BeMessage::RowDescription(rows) => {
                buf.put_u8(b'T');
                write_body(buf, |buf| {
                    buf.put_i16(rows.len() as i16); // # of fields
                    for row in rows.iter() {
                        write_cstr(row.name, buf)?;
                        buf.put_i32(0); /* table oid */
                        buf.put_i16(0); /* attnum */
                        buf.put_u32(row.typoid);
                        buf.put_i16(row.typlen);
                        buf.put_i32(-1); /* typmod */
                        buf.put_i16(0); /* format code */
                    }
                    Ok::<_, io::Error>(())
                })?;
            }

            BeMessage::XLogData(body) => {
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_u8(b'w');
                    buf.put_u64(body.wal_start);
                    buf.put_u64(body.wal_end);
                    buf.put_i64(body.timestamp);
                    buf.put_slice(body.data);
                });
            }

            BeMessage::KeepAlive(req) => {
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_u8(b'k');
                    buf.put_u64(req.sent_ptr);
                    buf.put_i64(req.timestamp);
                    buf.put_u8(u8::from(req.request_reply));
                });
            }
        }
        Ok(())
    }
}

// Neon extension of postgres replication protocol
// See NEON_STATUS_UPDATE_TAG_BYTE
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationFeedback {
    // Last known size of the timeline. Used to enforce timeline size limit.
    pub current_timeline_size: u64,
    // Parts of StandbyStatusUpdate we resend to compute via safekeeper
    pub ps_writelsn: u64,
    pub ps_applylsn: u64,
    pub ps_flushlsn: u64,
    pub ps_replytime: SystemTime,
}

// NOTE: Do not forget to increment this number when adding new fields to ReplicationFeedback.
// Do not remove previously available fields because this might be backwards incompatible.
pub const REPLICATION_FEEDBACK_FIELDS_NUMBER: u8 = 5;

impl ReplicationFeedback {
    pub fn empty() -> ReplicationFeedback {
        ReplicationFeedback {
            current_timeline_size: 0,
            ps_writelsn: 0,
            ps_applylsn: 0,
            ps_flushlsn: 0,
            ps_replytime: SystemTime::now(),
        }
    }

    // Serialize ReplicationFeedback using custom format
    // to support protocol extensibility.
    //
    // Following layout is used:
    // char - number of key-value pairs that follow.
    //
    // key-value pairs:
    // null-terminated string - key,
    // uint32 - value length in bytes
    // value itself
    pub fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        buf.put_u8(REPLICATION_FEEDBACK_FIELDS_NUMBER); // # of keys
        buf.put_slice(b"current_timeline_size\0");
        buf.put_i32(8);
        buf.put_u64(self.current_timeline_size);

        buf.put_slice(b"ps_writelsn\0");
        buf.put_i32(8);
        buf.put_u64(self.ps_writelsn);
        buf.put_slice(b"ps_flushlsn\0");
        buf.put_i32(8);
        buf.put_u64(self.ps_flushlsn);
        buf.put_slice(b"ps_applylsn\0");
        buf.put_i32(8);
        buf.put_u64(self.ps_applylsn);

        let timestamp = self
            .ps_replytime
            .duration_since(*PG_EPOCH)
            .expect("failed to serialize pg_replytime earlier than PG_EPOCH")
            .as_micros() as i64;

        buf.put_slice(b"ps_replytime\0");
        buf.put_i32(8);
        buf.put_i64(timestamp);
        Ok(())
    }

    // Deserialize ReplicationFeedback message
    pub fn parse(mut buf: Bytes) -> ReplicationFeedback {
        let mut rf = ReplicationFeedback::empty();
        let nfields = buf.get_u8();
        for _ in 0..nfields {
            let key = read_cstr(&mut buf).unwrap();
            match key.as_ref() {
                b"current_timeline_size" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    rf.current_timeline_size = buf.get_u64();
                }
                b"ps_writelsn" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    rf.ps_writelsn = buf.get_u64();
                }
                b"ps_flushlsn" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    rf.ps_flushlsn = buf.get_u64();
                }
                b"ps_applylsn" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    rf.ps_applylsn = buf.get_u64();
                }
                b"ps_replytime" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    let raw_time = buf.get_i64();
                    if raw_time > 0 {
                        rf.ps_replytime = *PG_EPOCH + Duration::from_micros(raw_time as u64);
                    } else {
                        rf.ps_replytime = *PG_EPOCH - Duration::from_micros(-raw_time as u64);
                    }
                }
                _ => {
                    let len = buf.get_i32();
                    warn!(
                        "ReplicationFeedback parse. unknown key {} of len {len}. Skip it.",
                        String::from_utf8_lossy(key.as_ref())
                    );
                    buf.advance(len as usize);
                }
            }
        }
        trace!("ReplicationFeedback parsed is {:?}", rf);
        rf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_feedback_serialization() {
        let mut rf = ReplicationFeedback::empty();
        // Fill rf with some values
        rf.current_timeline_size = 12345678;
        // Set rounded time to be able to compare it with deserialized value,
        // because it is rounded up to microseconds during serialization.
        rf.ps_replytime = *PG_EPOCH + Duration::from_secs(100_000_000);
        let mut data = BytesMut::new();
        rf.serialize(&mut data).unwrap();

        let rf_parsed = ReplicationFeedback::parse(data.freeze());
        assert_eq!(rf, rf_parsed);
    }

    #[test]
    fn test_replication_feedback_unknown_key() {
        let mut rf = ReplicationFeedback::empty();
        // Fill rf with some values
        rf.current_timeline_size = 12345678;
        // Set rounded time to be able to compare it with deserialized value,
        // because it is rounded up to microseconds during serialization.
        rf.ps_replytime = *PG_EPOCH + Duration::from_secs(100_000_000);
        let mut data = BytesMut::new();
        rf.serialize(&mut data).unwrap();

        // Add an extra field to the buffer and adjust number of keys
        if let Some(first) = data.first_mut() {
            *first = REPLICATION_FEEDBACK_FIELDS_NUMBER + 1;
        }

        data.put_slice(b"new_field_one\0");
        data.put_i32(8);
        data.put_u64(42);

        // Parse serialized data and check that new field is not parsed
        let rf_parsed = ReplicationFeedback::parse(data.freeze());
        assert_eq!(rf, rf_parsed);
    }

    #[test]
    fn test_startup_message_params_options_escaped() {
        fn split_options(params: &StartupMessageParams) -> Vec<Cow<'_, str>> {
            params
                .options_escaped()
                .expect("options are None")
                .collect()
        }

        let make_params = |options| StartupMessageParams::new([("options", options)]);

        let params = StartupMessageParams::new([]);
        assert!(matches!(params.options_escaped(), None));

        let params = make_params("");
        assert!(split_options(&params).is_empty());

        let params = make_params("foo");
        assert_eq!(split_options(&params), ["foo"]);

        let params = make_params(" foo  bar ");
        assert_eq!(split_options(&params), ["foo", "bar"]);

        let params = make_params("foo\\ bar \\ \\\\ baz\\  lol");
        assert_eq!(split_options(&params), ["foo bar", " \\", "baz ", "lol"]);
    }

    // Make sure that `read` is sync/async callable
    async fn _assert(stream: &mut (impl tokio::io::AsyncRead + Unpin)) {
        let _ = FeMessage::read(&mut [].as_ref());
        let _ = FeMessage::read_fut(stream).await;

        let _ = FeStartupPacket::read(&mut [].as_ref());
        let _ = FeStartupPacket::read_fut(stream).await;
    }
}

fn terminate_code(code: &[u8; 5]) -> [u8; 6] {
    let mut terminated = [0; 6];
    for (i, &elem) in code.iter().enumerate() {
        terminated[i] = elem;
    }

    terminated
}
