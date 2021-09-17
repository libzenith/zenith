//! Safekeeper communication endpoint to wal proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::*;

use std::net::SocketAddr;

use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;

use crate::replication::PushReplicationConn;
use crate::send_wal::SendWalHandler;
use crate::timeline::TimelineTools;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage};
use zenith_utils::zid::ZTenantId;

pub struct ReceiveWalConn<'pg> {
    /// Postgres connection
    pg_backend: &'pg mut PostgresBackend,
    /// The cached result of `pg_backend.socket().peer_addr()` (roughly)
    peer_addr: SocketAddr,
}

impl<'pg> ReceiveWalConn<'pg> {
    pub fn new(pg: &'pg mut PostgresBackend) -> Result<ReceiveWalConn<'pg>> {
        let peer_addr = *pg.get_peer_addr();
        Ok(ReceiveWalConn {
            pg_backend: pg,
            peer_addr,
        })
    }

    // Read and extract the bytes of a `CopyData` message from the postgres instance
    fn read_msg_bytes(&mut self) -> Result<Bytes> {
        match self.pg_backend.read_message()? {
            Some(FeMessage::CopyData(bytes)) => Ok(bytes),
            Some(msg) => bail!("expected `CopyData` message, found {:?}", msg),
            None => bail!("connection closed unexpectedly"),
        }
    }

    // Read and parse message sent from the postgres instance
    fn read_msg(&mut self) -> Result<ProposerAcceptorMessage> {
        let data = self.read_msg_bytes()?;
        ProposerAcceptorMessage::parse(data)
    }

    // Send message to the postgres
    fn write_msg(&mut self, msg: &AcceptorProposerMessage) -> Result<()> {
        let mut buf = Vec::new();
        msg.serialize(&mut buf)?;
        self.pg_backend.write_message(&BeMessage::CopyData(&buf))?;
        Ok(())
    }

    /// Receive WAL from wal_proposer
    pub fn run(&mut self, swh: &mut SendWalHandler) -> Result<()> {
        // Notify the libpq client that it's allowed to send `CopyData` messages
        self.pg_backend
            .write_message(&BeMessage::CopyBothResponse)?;

        // Receive information about server
        let mut msg = self
            .read_msg()
            .context("failed to receive proposer greeting")?;

        let tenant_id: ZTenantId;
        let wal_seg_size: u32;

        match msg {
            ProposerAcceptorMessage::Greeting(ref greeting) => {
                info!(
                    "start handshake with wal proposer {} sysid {} timeline {}",
                    self.peer_addr, greeting.system_id, greeting.tli,
                );
                tenant_id = greeting.tenant_id;
                wal_seg_size = greeting.wal_seg_size;
            }
            _ => bail!("unexpected message {:?} instead of greeting", msg),
        }

        // if requested, ask pageserver to fetch wal from us
        // xxx: this place seems not really fitting
        if let Some(ps_addr) = swh.conf.pageserver_addr.clone() {
            let timeline = swh.timeline.get().clone();

            PushReplicationConn::start(
                ps_addr,
                swh.conf.clone(),
                timeline,
                tenant_id,
                wal_seg_size as usize,
            )
            .context("failed to start push replication to page server")?;
        }

        loop {
            let reply = swh.timeline.get().process_msg(&msg)?;
            self.write_msg(&reply)?;
            msg = self.read_msg()?;
        }
    }
}
