use std::{
    net::{TcpListener, TcpStream},
    thread,
};

use anyhow::bail;
use bytes::Bytes;
use serde::Deserialize;
use zenith_utils::{
    postgres_backend::{self, query_from_cstring, AuthType, PostgresBackend},
    pq_proto::{BeMessage, SINGLE_COL_ROWDESC},
};

use crate::{cplane_api::DatabaseInfo, ProxyState};

///
/// Main proxy listener loop.
///
/// Listens for connections, and launches a new handler thread for each.
///
pub fn thread_main(state: &'static ProxyState, listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (socket, peer_addr) = listener.accept()?;
        println!("accepted connection from {}", peer_addr);
        socket.set_nodelay(true).unwrap();

        thread::spawn(move || {
            if let Err(err) = mgmt_conn_main(state, socket) {
                println!("error: {}", err);
            }
        });
    }
}

pub fn mgmt_conn_main(state: &'static ProxyState, socket: TcpStream) -> anyhow::Result<()> {
    let mut conn_handler = MgmtHandler { state };
    let pgbackend = PostgresBackend::new(socket, AuthType::Trust, None)?;
    pgbackend.run(&mut conn_handler)
}

struct MgmtHandler {
    state: &'static ProxyState,
}
/// Serialized examples:
// {
//     "session_id": "71d6d03e6d93d99a",
//     "result": {
//         "Success": {
//             "host": "127.0.0.1",
//             "port": 5432,
//             "dbname": "stas",
//             "user": "stas"
//             "password": "mypass"
//         }
//     }
// }
// {
//     "session_id": "71d6d03e6d93d99a",
//     "result": {
//         "Failure": "oops"
//     }
// }
#[derive(Deserialize)]
pub struct PsqlSessionResponse {
    session_id: String,
    result: PsqlSessionResult,
}

#[derive(Deserialize)]
pub enum PsqlSessionResult {
    Success(DatabaseInfo),
    Failure(String),
}

impl postgres_backend::Handler for MgmtHandler {
    fn process_query(
        &mut self,
        pgb: &mut PostgresBackend,
        query_string: Bytes,
    ) -> anyhow::Result<()> {
        let query_string = query_from_cstring(query_string);

        println!("Got mgmt query: '{}'", std::str::from_utf8(&query_string)?);

        let resp: PsqlSessionResponse = serde_json::from_slice(&query_string)?;

        let waiters = self.state.waiters.lock().unwrap();

        let sender = waiters
            .get(&resp.session_id)
            .ok_or_else(|| anyhow::Error::msg("psql_session_id is not found"))?;

        match resp.result {
            PsqlSessionResult::Success(db_info) => {
                sender.send(Ok(db_info))?;

                pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                    .write_message_noflush(&BeMessage::DataRow(&[Some(b"ok")]))?
                    .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
                pgb.flush()?;
                Ok(())
            }

            PsqlSessionResult::Failure(message) => {
                sender.send(Err(anyhow::Error::msg(message.clone())))?;

                bail!("psql session request failed: {}", message)
            }
        }
    }
}
