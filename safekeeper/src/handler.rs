//! Part of Safekeeper pretending to be Postgres, i.e. handling Postgres
//! protocol commands.

use crate::auth::check_permission;
use crate::json_ctrl::{handle_json_ctrl, AppendLogicalMessage};
use crate::receive_wal::ReceiveWalConn;

use crate::send_wal::ReplicationConn;

use crate::{GlobalTimelines, SafeKeeperConf};
use anyhow::Context;

use postgres_ffi::PG_TLI;
use regex::Regex;

use pq_proto::{BeMessage, FeStartupPacket, RowDescriptor, INT4_OID, TEXT_OID};
use std::str;
use tracing::info;
use utils::auth::{Claims, Scope};
use utils::postgres_backend_async::QueryError;
use utils::{
    id::{TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    postgres_backend::{self, PostgresBackend},
};

/// Safekeeper handler of postgres commands
pub struct SafekeeperPostgresHandler {
    pub conf: SafeKeeperConf,
    /// assigned application name
    pub appname: Option<String>,
    pub tenant_id: Option<TenantId>,
    pub timeline_id: Option<TimelineId>,
    pub ttid: TenantTimelineId,
    claims: Option<Claims>,
}

/// Parsed Postgres command.
enum SafekeeperPostgresCommand {
    StartWalPush,
    StartReplication { start_lsn: Lsn },
    IdentifySystem,
    JSONCtrl { cmd: AppendLogicalMessage },
}

fn parse_cmd(cmd: &str) -> anyhow::Result<SafekeeperPostgresCommand> {
    if cmd.starts_with("START_WAL_PUSH") {
        Ok(SafekeeperPostgresCommand::StartWalPush)
    } else if cmd.starts_with("START_REPLICATION") {
        let re =
            Regex::new(r"START_REPLICATION(?: PHYSICAL)? ([[:xdigit:]]+/[[:xdigit:]]+)").unwrap();
        let mut caps = re.captures_iter(cmd);
        let start_lsn = caps
            .next()
            .map(|cap| cap[1].parse::<Lsn>())
            .context("failed to parse start LSN from START_REPLICATION command")??;
        Ok(SafekeeperPostgresCommand::StartReplication { start_lsn })
    } else if cmd.starts_with("IDENTIFY_SYSTEM") {
        Ok(SafekeeperPostgresCommand::IdentifySystem)
    } else if cmd.starts_with("JSON_CTRL") {
        let cmd = cmd.strip_prefix("JSON_CTRL").context("invalid prefix")?;
        Ok(SafekeeperPostgresCommand::JSONCtrl {
            cmd: serde_json::from_str(cmd)?,
        })
    } else {
        anyhow::bail!("unsupported command {cmd}");
    }
}

impl postgres_backend::Handler for SafekeeperPostgresHandler {
    // tenant_id and timeline_id are passed in connection string params
    fn startup(
        &mut self,
        _pgb: &mut PostgresBackend,
        sm: &FeStartupPacket,
    ) -> Result<(), QueryError> {
        if let FeStartupPacket::StartupMessage { params, .. } = sm {
            if let Some(options) = params.options_raw() {
                for opt in options {
                    // FIXME `ztenantid` and `ztimelineid` left for compatibility during deploy,
                    // remove these after the PR gets deployed:
                    // https://github.com/neondatabase/neon/pull/2433#discussion_r970005064
                    match opt.split_once('=') {
                        Some(("ztenantid", value)) | Some(("tenant_id", value)) => {
                            self.tenant_id = Some(value.parse().with_context(|| {
                                format!("Failed to parse {value} as tenant id")
                            })?);
                        }
                        Some(("ztimelineid", value)) | Some(("timeline_id", value)) => {
                            self.timeline_id = Some(value.parse().with_context(|| {
                                format!("Failed to parse {value} as timeline id")
                            })?);
                        }
                        _ => continue,
                    }
                }
            }

            if let Some(app_name) = params.get("application_name") {
                self.appname = Some(app_name.to_owned());
            }

            Ok(())
        } else {
            Err(QueryError::Other(anyhow::anyhow!(
                "Safekeeper received unexpected initial message: {sm:?}"
            )))
        }
    }

    fn check_auth_jwt(
        &mut self,
        _pgb: &mut PostgresBackend,
        jwt_response: &[u8],
    ) -> Result<(), QueryError> {
        // this unwrap is never triggered, because check_auth_jwt only called when auth_type is NeonJWT
        // which requires auth to be present
        let data = self
            .conf
            .auth
            .as_ref()
            .unwrap()
            .decode(str::from_utf8(jwt_response).context("jwt response is not UTF-8")?)?;

        if matches!(data.claims.scope, Scope::Tenant) && data.claims.tenant_id.is_none() {
            return Err(QueryError::Other(anyhow::anyhow!(
                "jwt token scope is Tenant, but tenant id is missing"
            )));
        }

        info!(
            "jwt auth succeeded for scope: {:#?} by tenant id: {:?}",
            data.claims.scope, data.claims.tenant_id,
        );

        self.claims = Some(data.claims);
        Ok(())
    }

    fn process_query(
        &mut self,
        pgb: &mut PostgresBackend,
        query_string: &str,
    ) -> Result<(), QueryError> {
        if query_string
            .to_ascii_lowercase()
            .starts_with("set datestyle to ")
        {
            // important for debug because psycopg2 executes "SET datestyle TO 'ISO'" on connect
            pgb.write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
            return Ok(());
        }
        let cmd = parse_cmd(query_string)?;

        info!(
            "got query {:?} in timeline {:?}",
            query_string, self.timeline_id
        );

        let tenant_id = self.tenant_id.context("tenantid is required")?;
        let timeline_id = self.timeline_id.context("timelineid is required")?;
        self.check_permission(Some(tenant_id))?;
        self.ttid = TenantTimelineId::new(tenant_id, timeline_id);

        let res = match cmd {
            SafekeeperPostgresCommand::StartWalPush => ReceiveWalConn::new(pgb).run(self),
            SafekeeperPostgresCommand::StartReplication { start_lsn } => {
                ReplicationConn::new(pgb).run(self, pgb, start_lsn)
            }
            SafekeeperPostgresCommand::IdentifySystem => self.handle_identify_system(pgb),
            SafekeeperPostgresCommand::JSONCtrl { ref cmd } => handle_json_ctrl(self, pgb, cmd),
        };

        match res {
            Ok(()) => Ok(()),
            Err(QueryError::Disconnected(connection_error)) => {
                info!("Timeline {tenant_id}/{timeline_id} query failed with connection error: {connection_error}");
                Err(QueryError::Disconnected(connection_error))
            }
            Err(QueryError::Other(e)) => Err(QueryError::Other(e.context(format!(
                "Failed to process query for timeline {}",
                self.ttid
            )))),
        }
    }
}

impl SafekeeperPostgresHandler {
    pub fn new(conf: SafeKeeperConf) -> Self {
        SafekeeperPostgresHandler {
            conf,
            appname: None,
            tenant_id: None,
            timeline_id: None,
            ttid: TenantTimelineId::empty(),
            claims: None,
        }
    }

    // when accessing management api supply None as an argument
    // when using to authorize tenant pass corresponding tenant id
    fn check_permission(&self, tenant_id: Option<TenantId>) -> anyhow::Result<()> {
        if self.conf.auth.is_none() {
            // auth is set to Trust, nothing to check so just return ok
            return Ok(());
        }
        // auth is some, just checked above, when auth is some
        // then claims are always present because of checks during connection init
        // so this expect won't trigger
        let claims = self
            .claims
            .as_ref()
            .expect("claims presence already checked");
        check_permission(claims, tenant_id)
    }

    ///
    /// Handle IDENTIFY_SYSTEM replication command
    ///
    fn handle_identify_system(&mut self, pgb: &mut PostgresBackend) -> Result<(), QueryError> {
        let tli = GlobalTimelines::get(self.ttid)?;

        let lsn = if self.is_walproposer_recovery() {
            // walproposer should get all local WAL until flush_lsn
            tli.get_flush_lsn()
        } else {
            // other clients shouldn't get any uncommitted WAL
            tli.get_state().0.commit_lsn
        }
        .to_string();

        let sysid = tli.get_state().1.server.system_id.to_string();
        let lsn_bytes = lsn.as_bytes();
        let tli = PG_TLI.to_string();
        let tli_bytes = tli.as_bytes();
        let sysid_bytes = sysid.as_bytes();

        pgb.write_message_noflush(&BeMessage::RowDescription(&[
            RowDescriptor {
                name: b"systemid",
                typoid: TEXT_OID,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"timeline",
                typoid: INT4_OID,
                typlen: 4,
                ..Default::default()
            },
            RowDescriptor {
                name: b"xlogpos",
                typoid: TEXT_OID,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"dbname",
                typoid: TEXT_OID,
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

    /// Returns true if current connection is a replication connection, originating
    /// from a walproposer recovery function. This connection gets a special handling:
    /// safekeeper must stream all local WAL till the flush_lsn, whether committed or not.
    pub fn is_walproposer_recovery(&self) -> bool {
        self.appname == Some("wal_proposer_recovery".to_string())
    }
}
