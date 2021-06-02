//
//! The Page Service listens for client connections and serves their GetPage@LSN
//! requests.
//
//   It is possible to connect here using usual psql/pgbench/libpq. Following
// commands are supported now:
//     *status* -- show actual info about this pageserver,
//     *pagestream* -- enter mode where smgr and pageserver talk with their
//  custom protocol.
//     *callmemaybe <zenith timelineid> $url* -- ask pageserver to start walreceiver on $url
//

use anyhow::{anyhow, bail};
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use regex::Regex;
use std::io;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str::FromStr;
use std::thread;
use zenith_utils::{bin_ser::BeSer, lsn::Lsn};

use crate::basebackup;
use crate::branches;
use crate::page_cache;
use crate::repository::{BufferTag, RelTag, RelationUpdate, Update};
use crate::restore_local_repo;
use crate::walreceiver;
use crate::PageServerConf;
use crate::ZTimelineId;

#[derive(Debug)]
enum FeMessage {
    StartupMessage(FeStartupMessage),
    Query(FeQueryMessage), // Simple query
    Parse(FeParseMessage), // Extended query protocol
    Describe(FeDescribeMessage),
    Bind(FeBindMessage),
    Execute(FeExecuteMessage),
    Close(FeCloseMessage),
    Sync,
    Terminate,
    CopyData(Bytes),
    CopyDone,
}

#[derive(Debug)]
enum BeMessage {
    AuthenticationOk,
    ParameterStatus,
    ReadyForQuery,
    RowDescription,
    ParseComplete,
    ParameterDescription,
    NoData,
    BindComplete,
    CloseComplete,
    DataRow(Bytes),
    CommandComplete,
    ControlFile,
    CopyData(Bytes),
    ErrorResponse(String),
    CopyInResponse,
}

// Wrapped in libpq CopyData
enum PagestreamFeMessage {
    Exists(PagestreamRequest),
    Nblocks(PagestreamRequest),
    Read(PagestreamRequest),
}

// Wrapped in libpq CopyData
enum PagestreamBeMessage {
    Status(PagestreamStatusResponse),
    Nblocks(PagestreamStatusResponse),
    Read(PagestreamReadResponse),
}

static HELLO_WORLD_ROW: BeMessage = BeMessage::DataRow(Bytes::from_static(b"hello world"));

#[derive(Debug)]
struct PagestreamRequest {
    spcnode: u32,
    dbnode: u32,
    relnode: u32,
    forknum: u8,
    blkno: u32,
    lsn: Lsn,
}

#[derive(Debug)]
struct PagestreamStatusResponse {
    ok: bool,
    n_blocks: u32,
}

#[derive(Debug)]
struct PagestreamReadResponse {
    ok: bool,
    n_blocks: u32,
    page: Bytes,
}

#[derive(Debug)]
struct FeStartupMessage {
    version: u32,
    kind: StartupRequestCode,
}

#[derive(Debug)]
enum StartupRequestCode {
    Cancel,
    NegotiateSsl,
    NegotiateGss,
    Normal,
}

impl FeStartupMessage {
    pub fn read(stream: &mut dyn std::io::Read) -> anyhow::Result<Option<FeMessage>> {
        const MAX_STARTUP_PACKET_LENGTH: u32 = 10000;
        const CANCEL_REQUEST_CODE: u32 = (1234 << 16) | 5678;
        const NEGOTIATE_SSL_CODE: u32 = (1234 << 16) | 5679;
        const NEGOTIATE_GSS_CODE: u32 = (1234 << 16) | 5680;

        // Read length. If the connection is closed before reading anything (or before
        // reading 4 bytes, to be precise), return None to indicate that the connection
        // was closed. This matches the PostgreSQL server's behavior, which avoids noise
        // in the log if the client opens connection but closes it immediately.
        let len = match stream.read_u32::<BE>() {
            Ok(len) => len,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if len < 4 || len > MAX_STARTUP_PACKET_LENGTH {
            bail!("invalid message length");
        }
        let bodylen = len - 4;

        // Read the rest of the startup packet
        let mut body_buf: Vec<u8> = vec![0; bodylen as usize];
        stream.read_exact(&mut body_buf)?;
        let mut body = Bytes::from(body_buf);

        // Parse the first field, which indicates what kind of a packet it is
        let version = body.get_u32();
        let kind = match version {
            CANCEL_REQUEST_CODE => StartupRequestCode::Cancel,
            NEGOTIATE_SSL_CODE => StartupRequestCode::NegotiateSsl,
            NEGOTIATE_GSS_CODE => StartupRequestCode::NegotiateGss,
            _ => StartupRequestCode::Normal,
        };

        // Ignore the rest of the packet

        Ok(Some(FeMessage::StartupMessage(FeStartupMessage {
            version,
            kind,
        })))
    }
}

#[derive(Debug)]
struct Buffer {
    bytes: Bytes,
    idx: usize,
}

#[derive(Debug)]
struct FeQueryMessage {
    body: Bytes,
}

// We only support the simple case of Parse on unnamed prepared statement and
// no params
#[derive(Debug)]
struct FeParseMessage {
    query_string: Bytes,
}

fn read_null_terminated(buf: &mut Bytes) -> anyhow::Result<Bytes> {
    let mut result = BytesMut::new();

    loop {
        if !buf.has_remaining() {
            bail!("no null-terminator in string");
        }

        let byte = buf.get_u8();

        if byte == 0 {
            break;
        }
        result.put_u8(byte);
    }
    Ok(result.freeze())
}

impl FeParseMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let _pstmt_name = read_null_terminated(&mut buf)?;
        let query_string = read_null_terminated(&mut buf)?;
        let nparams = buf.get_i16();

        // FIXME: the rust-postgres driver uses a named prepared statement
        // for copy_out(). We're not prepared to handle that correctly. For
        // now, just ignore the statement name, assuming that the client never
        // uses more than one prepared statement at a time.
        /*
        if !pstmt_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named prepared statements not implemented in Parse",
            ));
        }
         */

        if nparams != 0 {
            bail!("query params not implemented");
        }

        Ok(FeMessage::Parse(FeParseMessage { query_string }))
    }
}

#[derive(Debug)]
struct FeDescribeMessage {
    kind: u8, // 'S' to describe a prepared statement; or 'P' to describe a portal.
              // we only support unnamed prepared stmt or portal
}

impl FeDescribeMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let kind = buf.get_u8();
        let _pstmt_name = read_null_terminated(&mut buf)?;

        // FIXME: see FeParseMessage::parse
        /*
        if !pstmt_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named prepared statements not implemented in Describe",
            ));
        }
        */

        if kind != b'S' {
            bail!("only prepared statmement Describe is implemented");
        }

        Ok(FeMessage::Describe(FeDescribeMessage { kind }))
    }
}

// we only support unnamed prepared stmt or portal
#[derive(Debug)]
struct FeExecuteMessage {
    /// max # of rows
    maxrows: i32,
}

impl FeExecuteMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let maxrows = buf.get_i32();

        if !portal_name.is_empty() {
            bail!("named portals not implemented");
        }

        if maxrows != 0 {
            bail!("row limit in Execute message not supported");
        }

        Ok(FeMessage::Execute(FeExecuteMessage { maxrows }))
    }
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
struct FeBindMessage {}

impl FeBindMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let _pstmt_name = read_null_terminated(&mut buf)?;

        if !portal_name.is_empty() {
            bail!("named portals not implemented");
        }

        // FIXME: see FeParseMessage::parse
        /*
        if !pstmt_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named prepared statements not implemented",
            ));
        }
        */

        Ok(FeMessage::Bind(FeBindMessage {}))
    }
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
struct FeCloseMessage {}

impl FeCloseMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let _kind = buf.get_u8();
        let _pstmt_or_portal_name = read_null_terminated(&mut buf)?;

        // FIXME: we do nothing with Close

        Ok(FeMessage::Close(FeCloseMessage {}))
    }
}

impl FeMessage {
    pub fn read(stream: &mut dyn Read) -> anyhow::Result<Option<FeMessage>> {
        // Each libpq message begins with a message type byte, followed by message length
        // If the client closes the connection, return None. But if the client closes the
        // connection in the middle of a message, we will return an error.
        let tag = match stream.read_u8() {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let len = stream.read_u32::<BE>()?;

        // The message length includes itself, so it better be at least 4
        if len < 4 {
            bail!("invalid message length: parsing u32");
        }
        let bodylen = len - 4;

        // Read message body
        let mut body_buf: Vec<u8> = vec![0; bodylen as usize];
        stream.read_exact(&mut body_buf)?;

        let body = Bytes::from(body_buf);

        // Parse it
        match tag {
            b'Q' => Ok(Some(FeMessage::Query(FeQueryMessage { body }))),
            b'P' => Ok(Some(FeParseMessage::parse(body)?)),
            b'D' => Ok(Some(FeDescribeMessage::parse(body)?)),
            b'E' => Ok(Some(FeExecuteMessage::parse(body)?)),
            b'B' => Ok(Some(FeBindMessage::parse(body)?)),
            b'C' => Ok(Some(FeCloseMessage::parse(body)?)),
            b'S' => Ok(Some(FeMessage::Sync)),
            b'X' => Ok(Some(FeMessage::Terminate)),
            b'd' => Ok(Some(FeMessage::CopyData(body))),
            b'c' => Ok(Some(FeMessage::CopyDone)),
            tag => Err(anyhow!("unknown message tag: {},'{:?}'", tag, body)),
        }
    }
}

impl PagestreamFeMessage {
    fn parse(mut body: Bytes) -> anyhow::Result<PagestreamFeMessage> {
        // TODO these gets can fail

        let smgr_tag = body.get_u8();
        let zreq = PagestreamRequest {
            spcnode: body.get_u32(),
            dbnode: body.get_u32(),
            relnode: body.get_u32(),
            forknum: body.get_u8(),
            blkno: body.get_u32(),
            lsn: Lsn::from(body.get_u64()),
        };

        // TODO: consider using protobuf or serde bincode for less error prone
        // serialization.
        match smgr_tag {
            0 => Ok(PagestreamFeMessage::Exists(zreq)),
            1 => Ok(PagestreamFeMessage::Nblocks(zreq)),
            2 => Ok(PagestreamFeMessage::Read(zreq)),
            _ => Err(anyhow!(
                "unknown smgr message tag: {},'{:?}'",
                smgr_tag,
                body
            )),
        }
    }
}

impl PagestreamBeMessage {
    fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        match self {
            Self::Status(resp) => {
                bytes.put_u8(100); /* tag from pagestore_client.h */
                bytes.put_u8(resp.ok as u8);
                bytes.put_u32(resp.n_blocks);
            }

            Self::Nblocks(resp) => {
                bytes.put_u8(101); /* tag from pagestore_client.h */
                bytes.put_u8(resp.ok as u8);
                bytes.put_u32(resp.n_blocks);
            }

            Self::Read(resp) => {
                bytes.put_u8(102); /* tag from pagestore_client.h */
                bytes.put_u8(resp.ok as u8);
                bytes.put_u32(resp.n_blocks);
                bytes.put(&resp.page[..]);
            }
        }

        bytes.into()
    }
}

///////////////////////////////////////////////////////////////////////////////

///
/// Main loop of the page service.
///
/// Listens for connections, and launches a new handler thread for each.
///
pub fn thread_main(conf: &'static PageServerConf, listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (socket, peer_addr) = listener.accept()?;
        debug!("accepted connection from {}", peer_addr);
        socket.set_nodelay(true).unwrap();
        let mut conn_handler = Connection::new(conf, socket);

        thread::spawn(move || {
            if let Err(err) = conn_handler.run() {
                error!("error: {}", err);
            }
        });
    }
}

#[derive(Debug)]
struct Connection {
    stream_in: BufReader<TcpStream>,
    stream: BufWriter<TcpStream>,
    init_done: bool,
    conf: &'static PageServerConf,
}

impl Connection {
    pub fn new(conf: &'static PageServerConf, socket: TcpStream) -> Connection {
        Connection {
            stream_in: BufReader::new(socket.try_clone().unwrap()),
            stream: BufWriter::new(socket),
            init_done: false,
            conf,
        }
    }

    //
    // Read full message or return None if connection is closed
    //
    fn read_message(&mut self) -> anyhow::Result<Option<FeMessage>> {
        if !self.init_done {
            FeStartupMessage::read(&mut self.stream_in)
        } else {
            FeMessage::read(&mut self.stream_in)
        }
    }

    fn write_message_noflush(&mut self, message: &BeMessage) -> io::Result<()> {
        match message {
            BeMessage::AuthenticationOk => {
                self.stream.write_u8(b'R')?;
                self.stream.write_i32::<BE>(4 + 4)?;
                self.stream.write_i32::<BE>(0)?;
            }

            BeMessage::ParameterStatus => {
                self.stream.write_u8(b'S')?;
                // parameter names and values are specified by null terminated strings
                const PARAM_NAME_VALUE: &[u8] = b"client_encoding\0UTF8\0";
                // length of this i32 + rest of data in message
                self.stream
                    .write_i32::<BE>(4 + PARAM_NAME_VALUE.len() as i32)?;
                self.stream.write_all(PARAM_NAME_VALUE)?;
            }

            BeMessage::ReadyForQuery => {
                self.stream.write_u8(b'Z')?;
                self.stream.write_i32::<BE>(4 + 1)?;
                self.stream.write_u8(b'I')?;
            }

            BeMessage::ParseComplete => {
                self.stream.write_u8(b'1')?;
                self.stream.write_i32::<BE>(4)?;
            }

            BeMessage::BindComplete => {
                self.stream.write_u8(b'2')?;
                self.stream.write_i32::<BE>(4)?;
            }

            BeMessage::CloseComplete => {
                self.stream.write_u8(b'3')?;
                self.stream.write_i32::<BE>(4)?;
            }

            BeMessage::NoData => {
                self.stream.write_u8(b'n')?;
                self.stream.write_i32::<BE>(4)?;
            }

            BeMessage::ParameterDescription => {
                self.stream.write_u8(b't')?;
                self.stream.write_i32::<BE>(6)?;
                // we don't support params, so always 0
                self.stream.write_i16::<BE>(0)?;
            }

            BeMessage::RowDescription => {
                // XXX
                let b = Bytes::from("data\0");

                self.stream.write_u8(b'T')?;
                self.stream
                    .write_i32::<BE>(4 + 2 + b.len() as i32 + 3 * (4 + 2))?;

                self.stream.write_i16::<BE>(1)?;
                self.stream.write_all(&b)?;
                self.stream.write_i32::<BE>(0)?; /* table oid */
                self.stream.write_i16::<BE>(0)?; /* attnum */
                self.stream.write_i32::<BE>(25)?; /* TEXTOID */
                self.stream.write_i16::<BE>(-1)?; /* typlen */
                self.stream.write_i32::<BE>(0)?; /* typmod */
                self.stream.write_i16::<BE>(0)?; /* format code */
            }

            // XXX: accept some text data
            BeMessage::DataRow(b) => {
                self.stream.write_u8(b'D')?;
                self.stream.write_i32::<BE>(4 + 2 + 4 + b.len() as i32)?;

                self.stream.write_i16::<BE>(1)?;
                self.stream.write_i32::<BE>(b.len() as i32)?;
                self.stream.write_all(&b)?;
            }

            BeMessage::ControlFile => {
                // TODO pass checkpoint and xid info in this message
                let b = Bytes::from("hello pg_control");

                self.stream.write_u8(b'D')?;
                self.stream.write_i32::<BE>(4 + 2 + 4 + b.len() as i32)?;

                self.stream.write_i16::<BE>(1)?;
                self.stream.write_i32::<BE>(b.len() as i32)?;
                self.stream.write_all(&b)?;
            }

            BeMessage::CommandComplete => {
                let b = Bytes::from("SELECT 1\0");

                self.stream.write_u8(b'C')?;
                self.stream.write_i32::<BE>(4 + b.len() as i32)?;
                self.stream.write_all(&b)?;
            }

            BeMessage::CopyData(data) => {
                self.stream.write_u8(b'd')?;
                self.stream.write_u32::<BE>(4 + data.len() as u32)?;
                self.stream.write_all(&data)?;
            }

            // ErrorResponse is a zero-terminated array of zero-terminated fields.
            // First byte of each field represents type of this field. Set just enough fields
            // to satisfy rust-postgres client: 'S' -- severity, 'C' -- error, 'M' -- error
            // message text.
            BeMessage::ErrorResponse(error_msg) => {
                // For all the errors set Severity to Error and error code to
                // 'internal error'.
                let severity = Bytes::from("SERROR\0");
                let code = Bytes::from("CXX000\0");

                // 'E' signalizes ErrorResponse messages
                self.stream.write_u8(b'E')?;
                self.stream.write_u32::<BE>(
                    4 + severity.len() as u32
                        + code.len() as u32
                        + (1 + error_msg.len() as u32 + 1)
                        + 1,
                )?;

                // Send severity and code fields
                self.stream.write_all(&severity)?;
                self.stream.write_all(&code)?;

                // Send error message field
                self.stream.write_u8(b'M')?;
                self.stream.write_all(error_msg.as_bytes())?;
                self.stream.write_u8(0)?;

                // Terminate fields
                self.stream.write_u8(0)?;
            }

            BeMessage::CopyInResponse => {
                self.stream.write_u8(b'G')?;
                self.stream.write_u32::<BE>(4 + 1 + 2)?;
                self.stream.write_u8(1)?; // binary
                self.stream.write_u16::<BE>(0)?; // no columns
            }
        }

        Ok(())
    }

    fn write_message(&mut self, message: &BeMessage) -> io::Result<()> {
        self.write_message_noflush(message)?;
        self.stream.flush()
    }

    fn run(&mut self) -> anyhow::Result<()> {
        let mut unnamed_query_string = Bytes::new();
        loop {
            let msg = self.read_message()?;
            trace!("got message {:?}", msg);
            match msg {
                Some(FeMessage::StartupMessage(m)) => {
                    trace!("got message {:?}", m);

                    match m.kind {
                        StartupRequestCode::NegotiateGss | StartupRequestCode::NegotiateSsl => {
                            let b = Bytes::from("N");
                            self.stream.write_all(&b)?;
                            self.stream.flush()?;
                        }
                        StartupRequestCode::Normal => {
                            self.write_message_noflush(&BeMessage::AuthenticationOk)?;
                            // psycopg2 will not connect if client_encoding is not
                            // specified by the server
                            self.write_message_noflush(&BeMessage::ParameterStatus)?;
                            self.write_message(&BeMessage::ReadyForQuery)?;
                            self.init_done = true;
                        }
                        StartupRequestCode::Cancel => return Ok(()),
                    }
                }
                Some(FeMessage::Query(m)) => {
                    if let Err(e) = self.process_query(m.body) {
                        let errmsg = format!("{}", e);
                        self.write_message_noflush(&BeMessage::ErrorResponse(errmsg))?;
                    }
                    self.write_message(&BeMessage::ReadyForQuery)?;
                }
                Some(FeMessage::Parse(m)) => {
                    unnamed_query_string = m.query_string;
                    self.write_message(&BeMessage::ParseComplete)?;
                }
                Some(FeMessage::Describe(_)) => {
                    self.write_message_noflush(&BeMessage::ParameterDescription)?;
                    self.write_message(&BeMessage::NoData)?;
                }
                Some(FeMessage::Bind(_)) => {
                    self.write_message(&BeMessage::BindComplete)?;
                }
                Some(FeMessage::Close(_)) => {
                    self.write_message(&BeMessage::CloseComplete)?;
                }
                Some(FeMessage::Execute(_)) => {
                    self.process_query(unnamed_query_string.clone())?;
                    self.stream.flush()?;
                }
                Some(FeMessage::Sync) => {
                    self.write_message(&BeMessage::ReadyForQuery)?;
                }
                Some(FeMessage::Terminate) => {
                    break;
                }
                None => {
                    info!("connection closed");
                    break;
                }
                x => {
                    bail!("unexpected message type : {:?}", x);
                }
            }
        }

        Ok(())
    }

    fn process_query(&mut self, query_string: Bytes) -> anyhow::Result<()> {
        debug!("process query {:?}", query_string);

        // remove null terminator, if any
        let mut query_string = query_string;
        if query_string.last() == Some(&0) {
            query_string.truncate(query_string.len() - 1);
        }

        if query_string.starts_with(b"controlfile") {
            self.handle_controlfile()?;
        } else if query_string.starts_with(b"pagestream ") {
            let (_l, r) = query_string.split_at("pagestream ".len());
            let timelineid_str = String::from_utf8(r.to_vec())?;
            let timelineid = ZTimelineId::from_str(&timelineid_str)?;

            self.handle_pagerequests(timelineid)?;
        } else if query_string.starts_with(b"basebackup ") {
            let (_l, r) = query_string.split_at("basebackup ".len());
            let r = r.to_vec();
            let basebackup_args = String::from(String::from_utf8(r)?.trim_end());
            let args: Vec<&str> = basebackup_args.rsplit(' ').collect();
            let timelineid_str = args[0];
            info!("got basebackup command: \"{}\"", timelineid_str);
            let timelineid = ZTimelineId::from_str(&timelineid_str)?;
            let lsn = if args.len() > 1 {
                Some(Lsn::from_str(args[1])?)
            } else {
                None
            };
            // Check that the timeline exists
            self.handle_basebackup_request(timelineid, lsn)?;
            self.write_message_noflush(&BeMessage::CommandComplete)?;
        } else if query_string.starts_with(b"callmemaybe ") {
            let query_str = String::from_utf8(query_string.to_vec())?;

            // callmemaybe <zenith timelineid as hex string> <connstr>
            // TODO lazy static
            let re = Regex::new(r"^callmemaybe ([[:xdigit:]]+) (.*)$").unwrap();
            let caps = re
                .captures(&query_str)
                .ok_or_else(|| anyhow!("invalid callmemaybe: '{}'", query_str))?;

            let timelineid = ZTimelineId::from_str(caps.get(1).unwrap().as_str())?;
            let connstr: String = String::from(caps.get(2).unwrap().as_str());

            // Check that the timeline exists
            let repository = page_cache::get_repository();
            if repository.get_timeline(timelineid).is_err() {
                bail!("client requested callmemaybe on timeline {} which does not exist in page server", timelineid);
            }

            walreceiver::launch_wal_receiver(&self.conf, timelineid, &connstr);

            self.write_message_noflush(&BeMessage::CommandComplete)?;
        } else if query_string.starts_with(b"branch_create ") {
            let query_str = String::from_utf8(query_string.to_vec())?;
            let err = || anyhow!("invalid branch_create: '{}'", query_str);

            // branch_create <branchname> <startpoint>
            // TODO lazy static
            // TOOD: escaping, to allow branch names with spaces
            let re = Regex::new(r"^branch_create (\S+) ([^\r\n\s;]+)[\r\n\s;]*;?$").unwrap();
            let caps = re.captures(&query_str).ok_or_else(err)?;

            let branchname: String = String::from(caps.get(1).ok_or_else(err)?.as_str());
            let startpoint_str: String = String::from(caps.get(2).ok_or_else(err)?.as_str());

            let branch = branches::create_branch(&self.conf, &branchname, &startpoint_str)?;
            let branch = serde_json::to_vec(&branch)?;

            self.write_message_noflush(&BeMessage::RowDescription)?;
            self.write_message_noflush(&BeMessage::DataRow(Bytes::from(branch)))?;
            self.write_message_noflush(&BeMessage::CommandComplete)?;
        } else if query_string.starts_with(b"push ") {
            let query_str = std::str::from_utf8(&query_string)?;
            let mut it = query_str.split(' ');
            it.next().unwrap();
            let timeline_id: ZTimelineId = it
                .next()
                .ok_or_else(|| anyhow!("missing timeline id"))?
                .parse()?;

            let start_lsn = Lsn(0); // TODO this needs to come from the repo
            let timeline =
                page_cache::get_repository().create_empty_timeline(timeline_id, start_lsn)?;

            self.write_message(&BeMessage::CopyInResponse)?;

            let mut last_lsn = Lsn(0);

            while let Some(msg) = self.read_message()? {
                match msg {
                    FeMessage::CopyData(bytes) => {
                        let relation_update = RelationUpdate::des(&bytes)?;

                        last_lsn = relation_update.lsn;

                        match relation_update.update {
                            Update::Page { blknum, img } => {
                                let tag = BufferTag {
                                    rel: relation_update.rel,
                                    blknum,
                                };

                                timeline.put_page_image(tag, relation_update.lsn, img)?;
                            }
                            Update::WALRecord { blknum, rec } => {
                                let tag = BufferTag {
                                    rel: relation_update.rel,
                                    blknum,
                                };

                                timeline.put_wal_record(tag, rec)?;
                            }
                            Update::Truncate { n_blocks } => {
                                timeline.put_truncation(
                                    relation_update.rel,
                                    relation_update.lsn,
                                    n_blocks,
                                )?;
                            }
                            Update::Unlink => {
                                todo!()
                            }
                        }
                    }
                    FeMessage::CopyDone => {
                        timeline.advance_last_valid_lsn(last_lsn);
                        break;
                    }
                    FeMessage::Sync => {}
                    _ => bail!("unexpected message {:?}", msg),
                }
            }

            self.write_message(&BeMessage::CommandComplete)?;
        } else if query_string.starts_with(b"request_push ") {
            let query_str = std::str::from_utf8(&query_string)?;
            let mut it = query_str.split(' ');
            it.next().unwrap();

            let timeline_id: ZTimelineId = it
                .next()
                .ok_or_else(|| anyhow!("missing timeline id"))?
                .parse()?;
            let timeline = page_cache::get_repository().get_timeline(timeline_id)?;

            let postgres_connection_uri = it.next().ok_or(anyhow!("missing postgres uri"))?;

            let mut conn = postgres::Client::connect(postgres_connection_uri, postgres::NoTls)?;
            let mut copy_in = conn.copy_in(format!("push {}", timeline_id.to_string()).as_str())?;

            let history = timeline.history()?;
            for update_res in history {
                let update = update_res?;
                let update_bytes = update.ser()?;
                copy_in.write_all(&update_bytes)?;
                copy_in.flush()?; // ensure that messages are sent inside individual CopyData packets
            }

            copy_in.finish()?;

            self.write_message(&BeMessage::CommandComplete)?;
        } else if query_string.starts_with(b"branch_list") {
            let branches = crate::branches::get_branches(&self.conf)?;
            let branches_buf = serde_json::to_vec(&branches)?;

            self.write_message_noflush(&BeMessage::RowDescription)?;
            self.write_message_noflush(&BeMessage::DataRow(Bytes::from(branches_buf)))?;
            self.write_message_noflush(&BeMessage::CommandComplete)?;
        } else if query_string.starts_with(b"status") {
            self.write_message_noflush(&BeMessage::RowDescription)?;
            self.write_message_noflush(&HELLO_WORLD_ROW)?;
            self.write_message_noflush(&BeMessage::CommandComplete)?;
        } else if query_string.to_ascii_lowercase().starts_with(b"set ") {
            // important because psycopg2 executes "SET datestyle TO 'ISO'"
            // on connect
            self.write_message_noflush(&BeMessage::CommandComplete)?;
        } else if query_string
            .to_ascii_lowercase()
            .starts_with(b"identify_system")
        {
            // TODO: match postgres response formarmat for 'identify_system'
            let system_id = crate::branches::get_system_id(&self.conf)?.to_string();

            self.write_message_noflush(&BeMessage::RowDescription)?;
            self.write_message_noflush(&BeMessage::DataRow(Bytes::from(system_id)))?;
            self.write_message_noflush(&BeMessage::CommandComplete)?;
        } else {
            bail!("unknown command");
        }

        Ok(())
    }

    fn handle_controlfile(&mut self) -> io::Result<()> {
        self.write_message_noflush(&BeMessage::RowDescription)?;
        self.write_message_noflush(&BeMessage::ControlFile)?;
        self.write_message(&BeMessage::CommandComplete)?;

        Ok(())
    }

    fn handle_pagerequests(&mut self, timelineid: ZTimelineId) -> anyhow::Result<()> {
        // Check that the timeline exists
        let repository = page_cache::get_repository();
        let timeline = repository.get_timeline(timelineid).map_err(|_| {
            anyhow!(
                "client requested pagestream on timeline {} which does not exist in page server",
                timelineid
            )
        })?;

        /* switch client to COPYBOTH */
        self.stream.write_u8(b'W')?;
        self.stream.write_i32::<BE>(4 + 1 + 2)?;
        self.stream.write_u8(0)?; /* copy_is_binary */
        self.stream.write_i16::<BE>(0)?; /* numAttributes */
        self.stream.flush()?;

        while let Some(message) = self.read_message()? {
            trace!("query({:?}): {:?}", timelineid, message);

            let copy_data_bytes = match message {
                FeMessage::CopyData(bytes) => bytes,
                _ => continue,
            };

            let zenith_fe_msg = PagestreamFeMessage::parse(copy_data_bytes)?;

            let response = match zenith_fe_msg {
                PagestreamFeMessage::Exists(req) => {
                    let tag = RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    let exist = timeline.get_rel_exists(tag, req.lsn).unwrap_or(false);

                    PagestreamBeMessage::Status(PagestreamStatusResponse {
                        ok: exist,
                        n_blocks: 0,
                    })
                }
                PagestreamFeMessage::Nblocks(req) => {
                    let tag = RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    let n_blocks = timeline.get_rel_size(tag, req.lsn).unwrap_or(0);

                    PagestreamBeMessage::Nblocks(PagestreamStatusResponse { ok: true, n_blocks })
                }
                PagestreamFeMessage::Read(req) => {
                    let buf_tag = BufferTag {
                        rel: RelTag {
                            spcnode: req.spcnode,
                            dbnode: req.dbnode,
                            relnode: req.relnode,
                            forknum: req.forknum,
                        },
                        blknum: req.blkno,
                    };

                    let read_response = match timeline.get_page_at_lsn(buf_tag, req.lsn) {
                        Ok(p) => PagestreamReadResponse {
                            ok: true,
                            n_blocks: 0,
                            page: p,
                        },
                        Err(e) => {
                            const ZERO_PAGE: [u8; 8192] = [0; 8192];
                            error!("get_page_at_lsn: {}", e);
                            PagestreamReadResponse {
                                ok: false,
                                n_blocks: 0,
                                page: Bytes::from_static(&ZERO_PAGE),
                            }
                        }
                    };

                    PagestreamBeMessage::Read(read_response)
                }
            };

            self.write_message(&BeMessage::CopyData(response.serialize()))?;
        }

        Ok(())
    }

    fn handle_basebackup_request(
        &mut self,
        timelineid: ZTimelineId,
        lsn: Option<Lsn>,
    ) -> anyhow::Result<()> {
        // check that the timeline exists
        let repository = page_cache::get_repository();
        let timeline = repository.get_timeline(timelineid).map_err(|e| {
            error!("error fetching timeline: {:?}", e);
            anyhow!(
                "client requested basebackup on timeline {} which does not exist in page server",
                timelineid
            )
        })?;
        /* switch client to COPYOUT */
        let stream = &mut self.stream;
        stream.write_u8(b'H')?;
        stream.write_i32::<BE>(4 + 1 + 2)?;
        stream.write_u8(0)?; /* copy_is_binary */
        stream.write_i16::<BE>(0)?; /* numAttributes */
        stream.flush()?;
        info!("sent CopyOut");

        /* Send a tarball of the latest snapshot on the timeline */

        // find latest snapshot
        let snapshot_lsn =
            restore_local_repo::find_latest_snapshot(&self.conf, timelineid).unwrap();
        let req_lsn = lsn.unwrap_or(snapshot_lsn);
        basebackup::send_tarball_at_lsn(
            &mut CopyDataSink { stream },
            timelineid,
            &timeline,
            req_lsn,
            snapshot_lsn,
        )?;
        // CopyDone
        self.stream.write_u8(b'c')?;
        self.stream.write_u32::<BE>(4)?;
        self.stream.flush()?;
        debug!("CopyDone sent!");

        Ok(())
    }
}

///
/// A std::io::Write implementation that wraps all data written to it in CopyData
/// messages.
///
struct CopyDataSink<'a> {
    stream: &'a mut BufWriter<TcpStream>,
}

impl<'a> io::Write for CopyDataSink<'a> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        // CopyData
        // FIXME: if the input is large, we should split it into multiple messages.
        // Not sure what the threshold should be, but the ultimate hard limit is that
        // the length cannot exceed u32.
        self.stream.write_u8(b'd')?;
        self.stream.write_u32::<BE>((4 + data.len()) as u32)?;
        self.stream.write_all(&data)?;
        trace!("CopyData sent for {} bytes!", data.len());

        // FIXME: flush isn't really required, but makes it easier
        // to view in wireshark
        self.stream.flush()?;

        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        // no-op
        Ok(())
    }
}
