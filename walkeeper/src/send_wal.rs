//! Part of WAL acceptor pretending to be Postgres, streaming xlog to
//! pageserver/any other consumer.
//!

use crate::receive_wal::ReceiveWalConn;
use crate::replication::StandardReplicationConn;
use crate::timeline::{Timeline, TimelineTools};
use crate::WalAcceptorConf;
use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use std::str::FromStr;
use std::sync::Arc;
use zenith_utils::postgres_backend;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeStartupMessage, RowDescriptor};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use crate::timeline::CreateControlFile;

/// Handler for streaming WAL from acceptor
pub struct SendWalHandler {
    /// wal acceptor configuration
    pub conf: WalAcceptorConf,
    /// assigned application name
    pub appname: Option<String>,
    pub tenantid: Option<ZTenantId>,
    pub timelineid: Option<ZTimelineId>,
    pub timeline: Option<Arc<Timeline>>,
}

impl postgres_backend::Handler for SendWalHandler {
    fn startup(&mut self, _pgb: &mut PostgresBackend, sm: &FeStartupMessage) -> Result<()> {
        let ztimelineid = sm
            .params
            .get("ztimelineid")
            .ok_or_else(|| anyhow!("timelineid is required"))?;
        self.timelineid = Some(ZTimelineId::from_str(ztimelineid)?);

        let ztenantid = sm
            .params
            .get("ztenantid")
            .ok_or_else(|| anyhow!("tenantid is required"))?;
        self.tenantid = Some(ZTenantId::from_str(ztenantid)?);

        if let Some(app_name) = sm.params.get("application_name") {
            self.appname = Some(app_name.clone());
        }
        Ok(())
    }

    fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: Bytes) -> Result<()> {
        // START_WAL_PUSH is the only command that initializes the timeline
        if self.timeline.is_none() {
            if query_string.starts_with(b"START_WAL_PUSH") {
                self.timeline.set(
                    &self.conf,
                    self.tenantid.unwrap(),
                    self.timelineid.unwrap(),
                    CreateControlFile::True,
                )?;
            } else {
                self.timeline.set(
                    &self.conf,
                    self.tenantid.unwrap(),
                    self.timelineid.unwrap(),
                    CreateControlFile::False,
                )?;
            }
        }
        if query_string.starts_with(b"IDENTIFY_SYSTEM") {
            self.handle_identify_system(pgb)?;
            Ok(())
        } else if query_string.starts_with(b"START_REPLICATION") {
            StandardReplicationConn::new(pgb).run(self, &query_string)?;
            Ok(())
        } else if query_string.starts_with(b"START_WAL_PUSH") {
            ReceiveWalConn::new(pgb)?.run(self)?;
            Ok(())
        } else {
            bail!("Unexpected command {:?}", query_string);
        }
    }
}

impl SendWalHandler {
    pub fn new(conf: WalAcceptorConf) -> Self {
        SendWalHandler {
            conf,
            appname: None,
            tenantid: None,
            timelineid: None,
            timeline: None,
        }
    }

    ///
    /// Handle IDENTIFY_SYSTEM replication command
    ///
    fn handle_identify_system(&mut self, pgb: &mut PostgresBackend) -> Result<()> {
        let (start_pos, timeline) = self.timeline.get().get_end_of_wal();
        let lsn = start_pos.to_string();
        let tli = timeline.to_string();
        let sysid = self.timeline.get().get_info().server.system_id.to_string();
        let lsn_bytes = lsn.as_bytes();
        let tli_bytes = tli.as_bytes();
        let sysid_bytes = sysid.as_bytes();

        pgb.write_message_noflush(&BeMessage::RowDescription(&[
            RowDescriptor {
                name: b"systemid",
                typoid: 25,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"timeline",
                typoid: 23,
                typlen: 4,
                ..Default::default()
            },
            RowDescriptor {
                name: b"xlogpos",
                typoid: 25,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"dbname",
                typoid: 25,
                typlen: -1,
                ..Default::default()
            },
        ]))?
        .write_message_noflush(&BeMessage::DataRow(&[
            Some(sysid_bytes),
            Some(tli_bytes),
            Some(lsn_bytes),
            None,
        ]))?
        .write_message(&BeMessage::CommandComplete(b"IDENTIFY_SYSTEM"))?;
        Ok(())
    }
}
