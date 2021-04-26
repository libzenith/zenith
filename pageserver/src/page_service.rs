//
//   The Page Service listens for client connections and serves their GetPage@LSN
// requests.
//
//   It is possible to connect here using usual psql/pgbench/libpq. Following
// commands are supported now:
//     *status* -- show actual info about this pageserver,
//     *pagestream* -- enter mode where smgr and pageserver talk with their
//  custom protocol.
//     *callmemaybe <zenith timelineid> $url* -- ask pageserver to start walreceiver on $url
//

use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use regex::Regex;
use std::io;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str::FromStr;
use std::thread;
use std::time::Duration;
use zenith_utils::lsn::Lsn;

use crate::basebackup;
use crate::page_cache;
use crate::restore_local_repo;
use crate::walreceiver;
use crate::PageServerConf;
use crate::ZTimelineId;

type Result<T> = std::result::Result<T, io::Error>;

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

    //
    // All that messages are actually CopyData from libpq point of view.
    //
    ZenithExistsRequest(ZenithRequest),
    ZenithNblocksRequest(ZenithRequest),
    ZenithReadRequest(ZenithRequest),
}

#[derive(Debug)]
enum BeMessage {
    AuthenticationOk,
    ReadyForQuery,
    RowDescription,
    ParseComplete,
    ParameterDescription,
    NoData,
    BindComplete,
    CloseComplete,
    DataRow,
    CommandComplete,
    ControlFile,

    //
    // All that messages are actually CopyData from libpq point of view.
    //
    ZenithStatusResponse(ZenithStatusResponse),
    ZenithNblocksResponse(ZenithStatusResponse),
    ZenithReadResponse(ZenithReadResponse),
}

#[derive(Debug)]
struct ZenithRequest {
    spcnode: u32,
    dbnode: u32,
    relnode: u32,
    forknum: u8,
    blkno: u32,
    lsn: Lsn,
}

#[derive(Debug)]
struct ZenithStatusResponse {
    ok: bool,
    n_blocks: u32,
}

#[derive(Debug)]
struct ZenithReadResponse {
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
    pub fn read(stream: &mut dyn std::io::Read) -> Result<Option<FeMessage>> {
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
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                } else {
                    return Err(err);
                }
            }
        };
        if len < 4 || len as u32 > MAX_STARTUP_PACKET_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid message length",
            ));
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

fn read_null_terminated(buf: &mut Bytes) -> Result<Bytes> {
    let mut result = BytesMut::new();

    loop {
        if !buf.has_remaining() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no null-terminator in string",
            ));
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
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "query params not implemented",
            ));
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
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only prepared statmement Describe is implemented",
            ));
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
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let maxrows = buf.get_i32();

        if !portal_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named portals not implemented",
            ));
        }

        if maxrows != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "row limit in Execute message not supported",
            ));
        }

        Ok(FeMessage::Execute(FeExecuteMessage { maxrows }))
    }
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
struct FeBindMessage {}

impl FeBindMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let _pstmt_name = read_null_terminated(&mut buf)?;

        if !portal_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named portals not implemented",
            ));
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
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let _kind = buf.get_u8();
        let _pstmt_or_portal_name = read_null_terminated(&mut buf)?;

        // FIXME: we do nothing with Close

        Ok(FeMessage::Close(FeCloseMessage {}))
    }
}

impl FeMessage {
    pub fn read(stream: &mut dyn Read) -> Result<Option<FeMessage>> {
        // Each libpq message begins with a message type byte, followed by message length
        // If the client closes the connection, return None. But if the client closes the
        // connection in the middle of a message, we will return an error.
        let tag = match stream.read_u8() {
            Ok(b) => b,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                } else {
                    return Err(err);
                }
            }
        };
        let len = stream.read_u32::<BE>()?;

        // The message length includes itself, so it better be at least 4
        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message length: parsing u32",
            ));
        }
        let bodylen = len - 4;

        // Read message body
        let mut body_buf: Vec<u8> = vec![0; bodylen as usize];
        stream.read_exact(&mut body_buf)?;

        let mut body = Bytes::from(body_buf);

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
            b'd' => {
                let smgr_tag = body.get_u8();
                let zreq = ZenithRequest {
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
                    0 => Ok(Some(FeMessage::ZenithExistsRequest(zreq))),
                    1 => Ok(Some(FeMessage::ZenithNblocksRequest(zreq))),
                    2 => Ok(Some(FeMessage::ZenithReadRequest(zreq))),
                    _ => Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown smgr message tag: {},'{:?}'", smgr_tag, body),
                    )),
                }
            }
            tag => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown message tag: {},'{:?}'", tag, body),
            )),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

///
/// Main loop of the page service.
///
/// Listens for connections, and launches a new handler thread for each.
///
pub fn thread_main(conf: &PageServerConf) {
    info!("Starting page server on {}", conf.listen_addr);

    let listener = TcpListener::bind(conf.listen_addr).unwrap();

    loop {
        let (socket, peer_addr) = listener.accept().unwrap();
        debug!("accepted connection from {}", peer_addr);
        socket.set_nodelay(true).unwrap();
        let mut conn_handler = Connection::new(conf.clone(), socket);

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
    buffer: BytesMut,
    init_done: bool,
    conf: PageServerConf,
}

impl Connection {
    pub fn new(conf: PageServerConf, socket: TcpStream) -> Connection {
        Connection {
            stream_in: BufReader::new(socket.try_clone().unwrap()),
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(10 * 1024),
            init_done: false,
            conf,
        }
    }

    //
    // Read full message or return None if connection is closed
    //
    fn read_message(&mut self) -> Result<Option<FeMessage>> {
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
            BeMessage::DataRow => {
                // XXX
                let b = Bytes::from("hello world");

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

            BeMessage::ZenithStatusResponse(resp) => {
                self.stream.write_u8(b'd')?;
                self.stream.write_u32::<BE>(4 + 1 + 1 + 4)?;
                self.stream.write_u8(100)?; /* tag from pagestore_client.h */
                self.stream.write_u8(resp.ok as u8)?;
                self.stream.write_u32::<BE>(resp.n_blocks)?;
            }

            BeMessage::ZenithNblocksResponse(resp) => {
                self.stream.write_u8(b'd')?;
                self.stream.write_u32::<BE>(4 + 1 + 1 + 4)?;
                self.stream.write_u8(101)?; /* tag from pagestore_client.h */
                self.stream.write_u8(resp.ok as u8)?;
                self.stream.write_u32::<BE>(resp.n_blocks)?;
            }

            BeMessage::ZenithReadResponse(resp) => {
                self.stream.write_u8(b'd')?;
                self.stream
                    .write_u32::<BE>(4 + 1 + 1 + 4 + resp.page.len() as u32)?;
                self.stream.write_u8(102)?; /* tag from pagestore_client.h */
                self.stream.write_u8(resp.ok as u8)?;
                self.stream.write_u32::<BE>(resp.n_blocks)?;
                self.stream.write_all(&resp.page.clone())?;
            }
        }

        Ok(())
    }

    fn write_message(&mut self, message: &BeMessage) -> io::Result<()> {
        self.write_message_noflush(message)?;
        self.stream.flush()
    }

    fn run(&mut self) -> Result<()> {
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
                            self.write_message(&BeMessage::ReadyForQuery)?;
                            self.init_done = true;
                        }
                        StartupRequestCode::Cancel => return Ok(()),
                    }
                }
                Some(FeMessage::Query(m)) => {
                    self.process_query(m.body)?;
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
                    error!("unexpected message type : {:?}", x);
                    return Err(io::Error::new(io::ErrorKind::Other, "unexpected message"));
                }
            }
        }

        Ok(())
    }

    fn process_query(&mut self, query_string: Bytes) -> Result<()> {
        debug!("process query {:?}", query_string);

        // remove null terminator, if any
        let mut query_string = query_string.clone();
        if query_string.last() == Some(&0) {
            query_string.truncate(query_string.len() - 1);
        }

        if query_string.starts_with(b"controlfile") {
            self.handle_controlfile()
        } else if query_string.starts_with(b"pagestream ") {
            let (_l, r) = query_string.split_at("pagestream ".len());
            let timelineid_str = String::from_utf8(r.to_vec()).unwrap();
            let timelineid = ZTimelineId::from_str(&timelineid_str).unwrap();

            self.handle_pagerequests(timelineid)
        } else if query_string.starts_with(b"basebackup ") {
            let (_l, r) = query_string.split_at("basebackup ".len());
            let r = r.to_vec();
            let timelineid_str = String::from(String::from_utf8(r).unwrap().trim_end());
            info!("got basebackup command: \"{}\"", timelineid_str);
            let timelineid = ZTimelineId::from_str(&timelineid_str).unwrap();

            // Check that the timeline exists
            self.handle_basebackup_request(timelineid)?;
            self.write_message_noflush(&BeMessage::CommandComplete)?;
            self.write_message(&BeMessage::ReadyForQuery)
        } else if query_string.starts_with(b"callmemaybe ") {
            let query_str = String::from_utf8(query_string.to_vec()).unwrap();

            // callmemaybe <zenith timelineid as hex string> <connstr>
            let re = Regex::new(r"^callmemaybe ([[:xdigit:]]+) (.*)$").unwrap();
            let caps = re.captures(&query_str);
            let caps = caps.unwrap();

            let timelineid = ZTimelineId::from_str(caps.get(1).unwrap().as_str()).unwrap();
            let connstr: String = String::from(caps.get(2).unwrap().as_str());

            // Check that the timeline exists
            let pcache = page_cache::get_or_restore_pagecache(&self.conf, timelineid);
            if pcache.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("client requested callmemaybe on timeline {} which does not exist in page server", timelineid)));
            }

            walreceiver::launch_wal_receiver(&self.conf, timelineid, &connstr);

            self.write_message_noflush(&BeMessage::CommandComplete)?;
            self.write_message(&BeMessage::ReadyForQuery)
        } else if query_string.starts_with(b"status") {
            self.write_message_noflush(&BeMessage::RowDescription)?;
            self.write_message_noflush(&BeMessage::DataRow)?;
            self.write_message_noflush(&BeMessage::CommandComplete)?;
            self.write_message(&BeMessage::ReadyForQuery)
        } else {
            self.write_message_noflush(&BeMessage::RowDescription)?;
            self.write_message_noflush(&BeMessage::DataRow)?;
            self.write_message_noflush(&BeMessage::CommandComplete)?;
            self.write_message(&BeMessage::ReadyForQuery)
        }
    }

    fn handle_controlfile(&mut self) -> Result<()> {
        self.write_message_noflush(&BeMessage::RowDescription)?;
        self.write_message_noflush(&BeMessage::ControlFile)?;
        self.write_message_noflush(&BeMessage::CommandComplete)?;
        self.write_message(&BeMessage::ReadyForQuery)
    }

    fn handle_pagerequests(&mut self, timelineid: ZTimelineId) -> Result<()> {
        // Check that the timeline exists
        let pcache = page_cache::get_or_restore_pagecache(&self.conf, timelineid);
        if pcache.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("client requested pagestream on timeline {} which does not exist in page server", timelineid)));
        }
        let pcache = pcache.unwrap();

        /* switch client to COPYBOTH */
        self.stream.write_u8(b'W')?;
        self.stream.write_i32::<BE>(4 + 1 + 2)?;
        self.stream.write_u8(0)?; /* copy_is_binary */
        self.stream.write_i16::<BE>(0)?; /* numAttributes */
        self.stream.flush()?;

        loop {
            let message = self.read_message()?;

            if let Some(m) = &message {
                trace!("query({:?}): {:?}", timelineid, m);
            };

            if message.is_none() {
                // connection was closed
                return Ok(());
            }

            match message {
                Some(FeMessage::ZenithExistsRequest(req)) => {
                    let tag = page_cache::RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    let exist = pcache.relsize_exist(&tag, req.lsn).unwrap_or(false);

                    self.write_message(&BeMessage::ZenithStatusResponse(ZenithStatusResponse {
                        ok: exist,
                        n_blocks: 0,
                    }))?
                }
                Some(FeMessage::ZenithNblocksRequest(req)) => {
                    let tag = page_cache::RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    let n_blocks = pcache.relsize_get(&tag, req.lsn).unwrap_or(0);

                    self.write_message(&BeMessage::ZenithNblocksResponse(ZenithStatusResponse {
                        ok: true,
                        n_blocks,
                    }))?
                }
                Some(FeMessage::ZenithReadRequest(req)) => {
                    let buf_tag = page_cache::BufferTag {
                        rel: page_cache::RelTag {
                            spcnode: req.spcnode,
                            dbnode: req.dbnode,
                            relnode: req.relnode,
                            forknum: req.forknum,
                        },
                        blknum: req.blkno,
                    };

                    let msg = match pcache.get_page_at_lsn(buf_tag, req.lsn) {
                        Ok(p) => BeMessage::ZenithReadResponse(ZenithReadResponse {
                            ok: true,
                            n_blocks: 0,
                            page: p,
                        }),
                        Err(e) => {
                            const ZERO_PAGE: [u8; 8192] = [0; 8192];
                            error!("get_page_at_lsn: {}", e);
                            BeMessage::ZenithReadResponse(ZenithReadResponse {
                                ok: false,
                                n_blocks: 0,
                                page: Bytes::from_static(&ZERO_PAGE),
                            })
                        }
                    };

                    self.write_message(&msg)?
                }
                _ => {}
            }
        }
    }

    fn handle_basebackup_request(&mut self, timelineid: ZTimelineId) -> Result<()> {
        // check that the timeline exists
        let pcache = page_cache::get_or_restore_pagecache(&self.conf, timelineid);
        if pcache.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("client requested basebackup on timeline {} which does not exist in page server", timelineid)));
        }

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
        let snapshotlsn = restore_local_repo::find_latest_snapshot(&self.conf, timelineid).unwrap();

        basebackup::send_snapshot_tarball(&mut CopyDataSink { stream }, timelineid, snapshotlsn)?;

        // CopyDone
        self.stream.write_u8(b'c')?;
        self.stream.write_u32::<BE>(4)?;
        self.stream.flush()?;
        debug!("CopyDone sent!");

        // FIXME: I'm getting an error from the tokio copyout driver without this.
        // I think it happens when the CommandComplete, CloseComplete and ReadyForQuery
        // are sent in the same TCP packet as the CopyDone. I don't understand why.
        thread::sleep(Duration::from_secs(1));

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

impl<'a> std::io::Write for CopyDataSink<'a> {
    fn write(&mut self, data: &[u8]) -> std::result::Result<usize, std::io::Error> {
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
    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        // no-op
        Ok(())
    }
}
