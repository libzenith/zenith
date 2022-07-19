//! User credentials used in authentication.

use crate::error::UserFacingError;
use thiserror::Error;
use utils::pq_proto::StartupMessageParams;

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum ClientCredsParseError {
    #[error("Parameter '{0}' is missing in startup packet.")]
    MissingKey(&'static str),

    #[error("Inconsistent project name inferred from SNI ('{0}') and project option ('{1}').")]
    InconsistentProjectNames(String, String),

    #[error(
        "SNI ('{1}') inconsistently formatted with respect to common name ('{0}'). \
        SNI should be formatted as '<project-name>.{0}'."
    )]
    InconsistentSni(String, String),

    #[error("Project name ('{0}') must contain only alphanumeric characters and hyphen.")]
    MalformedProjectName(String),
}

impl UserFacingError for ClientCredsParseError {}

/// Various client credentials which we use for authentication.
/// Note that we don't store any kind of client key or password here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCredentials {
    pub user: String,
    pub dbname: String,
    pub project: Option<String>,
}

impl ClientCredentials {
    pub fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }
}

impl ClientCredentials {
    pub fn parse(
        mut options: StartupMessageParams,
        sni: Option<&str>,
        common_name: Option<&str>,
    ) -> Result<Self, ClientCredsParseError> {
        use ClientCredsParseError::*;

        // Some parameters are absolutely necessary, others not so much.
        let mut get_param = |key| options.remove(key).ok_or(MissingKey(key));

        // Some parameters are stored in the startup message.
        let user = get_param("user")?;
        let dbname = get_param("database")?;
        let project_a = get_param("project").ok();

        // Alternative project name is in fact a subdomain from SNI.
        // NOTE: we do not consider SNI if `common_name` is missing.
        let project_b = sni
            .zip(common_name)
            .map(|(sni, cn)| {
                // TODO: what if SNI is present but just a common name?
                subdomain_from_sni(sni, cn)
                    .ok_or_else(|| InconsistentSni(sni.to_owned(), cn.to_owned()))
            })
            .transpose()?;

        let project = match (project_a, project_b) {
            // Invariant: if we have both project name variants, they should match.
            (Some(a), Some(b)) if a != b => Some(Err(InconsistentProjectNames(a, b))),
            (a, b) => a.or(b).map(|name| {
                // Invariant: project name may not contain certain characters.
                check_project_name(name).map_err(MalformedProjectName)
            }),
        }
        .transpose()?;

        Ok(Self {
            user,
            dbname,
            project,
        })
    }
}

fn check_project_name(name: String) -> Result<String, String> {
    if name.chars().all(|c| c.is_alphanumeric() || c == '-') {
        Ok(name)
    } else {
        Err(name)
    }
}

fn subdomain_from_sni(sni: &str, common_name: &str) -> Option<String> {
    sni.strip_suffix(common_name)?
        .strip_suffix('.')
        .map(str::to_owned)
}
