//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::branches;
use crate::object_repository::ObjectRepository;
use crate::repository::Repository;
use crate::rocksdb_storage::RocksObjectStore;
use crate::walredo::PostgresRedoManager;
use crate::PageServerConf;
use anyhow::{anyhow, bail, Result};
use lazy_static::lazy_static;
use log::info;
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use zenith_utils::zid::ZTenantId;

lazy_static! {
    pub static ref REPOSITORY: Mutex<HashMap<ZTenantId, Arc<dyn Repository>>> =
        Mutex::new(HashMap::new());
}

pub fn init(conf: &'static PageServerConf) {
    let mut m = REPOSITORY.lock().unwrap();

    for dir_entry in fs::read_dir(conf.tenants_path()).unwrap() {
        let tenantid =
            ZTenantId::from_str(dir_entry.unwrap().file_name().to_str().unwrap()).unwrap();
        let obj_store = RocksObjectStore::open(conf, &tenantid).unwrap();

        // Set up a WAL redo manager, for applying WAL records.
        let walredo_mgr = PostgresRedoManager::new(conf, tenantid);

        // Set up an object repository, for actual data storage.
        let repo =
            ObjectRepository::new(conf, Arc::new(obj_store), Arc::new(walredo_mgr), tenantid);
        info!("initialized storage for tenant: {}", &tenantid);
        m.insert(tenantid, Arc::new(repo));
    }
}

pub fn create_repository_for_tenant(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
) -> Result<()> {
    let mut m = REPOSITORY.lock().unwrap();

    // First check that the tenant doesn't exist already
    if m.get(&tenantid).is_some() {
        bail!("tenant {} already exists", tenantid);
    }
    let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenantid));
    let repo = branches::create_repo(conf, tenantid, wal_redo_manager)?;

    m.insert(tenantid, Arc::new(repo));

    Ok(())
}

pub fn insert_repository_for_tenant(tenantid: ZTenantId, repo: Arc<dyn Repository>) {
    let o = &mut REPOSITORY.lock().unwrap();
    o.insert(tenantid, repo);
}

pub fn get_repository_for_tenant(tenantid: &ZTenantId) -> Result<Arc<dyn Repository>> {
    let o = &REPOSITORY.lock().unwrap();
    o.get(tenantid)
        .map(|repo| Arc::clone(repo))
        .ok_or_else(|| anyhow!("repository not found for tenant name {}", tenantid))
}
