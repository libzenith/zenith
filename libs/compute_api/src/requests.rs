//! Structs representing the JSON formats used in the compute_ctl's HTTP API.
use crate::{
    privilege::Privilege,
    responses::ComputeCtlConfig,
    spec::{ComputeSpec, ExtVersion, PgIdent},
};
use serde::Deserialize;

/// Request of the /configure API
///
/// We now pass only `spec` in the configuration request, but later we can
/// extend it and something like `restart: bool` or something else. So put
/// `spec` into a struct initially to be more flexible in the future.
#[derive(Deserialize, Debug)]
pub struct ConfigurationRequest {
    pub spec: ComputeSpec,
    pub compute_ctl_config: ComputeCtlConfig,
}

#[derive(Deserialize, Debug)]
pub struct ExtensionInstallRequest {
    pub extension: PgIdent,
    pub database: PgIdent,
    pub version: ExtVersion,
}

#[derive(Deserialize, Debug)]
pub struct SetRoleGrantsRequest {
    pub database: PgIdent,
    pub schema: PgIdent,
    pub privileges: Vec<Privilege>,
    pub role: PgIdent,
}
