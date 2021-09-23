//!
//! Branch management code
//!
// TODO: move all paths construction to conf impl
//

use anyhow::{bail, ensure, Context, Result};
use postgres_ffi::ControlFileData;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    str::FromStr,
    sync::Arc,
};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use log::*;
use zenith_utils::logging;
use zenith_utils::lsn::Lsn;

use crate::tenant_mgr;
use crate::walredo::WalRedoManager;
use crate::{repository::Repository, PageServerConf};
use crate::{restore_local_repo, LOG_FILE_NAME};

#[derive(Serialize, Deserialize, Clone)]
pub struct BranchInfo {
    pub name: String,
    #[serde(with = "hex")]
    pub timeline_id: ZTimelineId,
    pub latest_valid_lsn: Lsn,
    pub ancestor_id: Option<String>,
    pub ancestor_lsn: Option<String>,
    pub current_logical_size: usize,
    pub current_logical_size_non_incremental: usize,
}

impl BranchInfo {
    pub fn from_path<T: AsRef<Path>>(
        path: T,
        conf: &PageServerConf,
        tenantid: &ZTenantId,
        repo: &Arc<dyn Repository>,
    ) -> Result<Self> {
        let name = path
            .as_ref()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let timeline_id = std::fs::read_to_string(path)?.parse::<ZTimelineId>()?;

        let timeline = repo.get_timeline(timeline_id)?;

        let ancestor_path = conf.ancestor_path(&timeline_id, tenantid);
        let mut ancestor_id: Option<String> = None;
        let mut ancestor_lsn: Option<String> = None;

        if ancestor_path.exists() {
            let ancestor = std::fs::read_to_string(ancestor_path)?;
            let mut strings = ancestor.split('@');

            ancestor_id = Some(
                strings
                    .next()
                    .with_context(|| "wrong branch ancestor point in time format")?
                    .to_owned(),
            );
            ancestor_lsn = Some(
                strings
                    .next()
                    .with_context(|| "wrong branch ancestor point in time format")?
                    .to_owned(),
            );
        }

        Ok(BranchInfo {
            name,
            timeline_id,
            latest_valid_lsn: timeline.get_last_record_lsn(),
            ancestor_id,
            ancestor_lsn,
            current_logical_size: timeline.get_current_logical_size(),
            current_logical_size_non_incremental: timeline
                .get_current_logical_size_non_incremental(timeline.get_last_record_lsn())?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PointInTime {
    pub timelineid: ZTimelineId,
    pub lsn: Lsn,
}

pub fn init_pageserver(conf: &'static PageServerConf, create_tenant: Option<&str>) -> Result<()> {
    // Initialize logger
    // use true as daemonize parameter because otherwise we pollute zenith cli output with a few pages long output of info messages
    let (_scope_guard, _log_file) = logging::init(LOG_FILE_NAME, true)?;

    // We don't use the real WAL redo manager, because we don't want to spawn the WAL redo
    // process during repository initialization.
    //
    // FIXME: That caused trouble, because the WAL redo manager spawned a thread that launched
    // initdb in the background, and it kept running even after the "zenith init" had exited.
    // In tests, we started the  page server immediately after that, so that initdb was still
    // running in the background, and we failed to run initdb again in the same directory. This
    // has been solved for the rapid init+start case now, but the general race condition remains
    // if you restart the server quickly. The WAL redo manager doesn't use a separate thread
    // anymore, but I think that could still happen.
    let dummy_redo_mgr = Arc::new(crate::walredo::DummyRedoManager {});

    if let Some(tenantid) = create_tenant {
        let tenantid = ZTenantId::from_str(tenantid)?;
        println!("initializing tenantid {}", tenantid);
        create_repo(conf, tenantid, dummy_redo_mgr).with_context(|| "failed to create repo")?;
    }
    fs::create_dir_all(conf.tenants_path())?;

    println!("pageserver init succeeded");
    Ok(())
}

pub fn create_repo(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    wal_redo_manager: Arc<dyn WalRedoManager + Send + Sync>,
) -> Result<Arc<dyn Repository>> {
    let repo_dir = conf.tenant_path(&tenantid);
    if repo_dir.exists() {
        bail!("repo for {} already exists", tenantid)
    }

    // top-level dir may exist if we are creating it through CLI
    fs::create_dir_all(&repo_dir)
        .with_context(|| format!("could not create directory {}", repo_dir.display()))?;

    fs::create_dir(conf.timelines_path(&tenantid))?;
    fs::create_dir_all(conf.branches_path(&tenantid))?;
    fs::create_dir_all(conf.tags_path(&tenantid))?;

    info!("created directory structure in {}", repo_dir.display());

    let tli = create_timeline(conf, None, &tenantid)?;

    let repo = Arc::new(crate::layered_repository::LayeredRepository::new(
        conf,
        wal_redo_manager,
        tenantid,
    ));

    // Load data into pageserver
    // TODO To implement zenith import we need to
    //      move data loading out of create_repo()
    bootstrap_timeline(conf, tenantid, tli, &*repo)?;

    Ok(repo)
}

// Returns checkpoint LSN from controlfile
fn get_lsn_from_controlfile(path: &Path) -> Result<Lsn> {
    // Read control file to extract the LSN
    let controlfile_path = path.join("global").join("pg_control");
    let controlfile = ControlFileData::decode(&fs::read(controlfile_path)?)?;
    let lsn = controlfile.checkPoint;

    Ok(Lsn(lsn))
}

// Create the cluster temporarily in 'initdbpath' directory inside the repository
// to get bootstrap data for timeline initialization.
//
fn run_initdb(conf: &'static PageServerConf, initdbpath: &Path) -> Result<()> {
    info!("running initdb... ");

    let initdb_path = conf.pg_bin_dir().join("initdb");
    let initdb_output = Command::new(initdb_path)
        .args(&["-D", initdbpath.to_str().unwrap()])
        .args(&["-U", &conf.superuser])
        .arg("--no-instructions")
        .env_clear()
        .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
        .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
        .stdout(Stdio::null())
        .output()
        .with_context(|| "failed to execute initdb")?;
    if !initdb_output.status.success() {
        anyhow::bail!(
            "initdb failed: '{}'",
            String::from_utf8_lossy(&initdb_output.stderr)
        );
    }
    info!("initdb succeeded");

    Ok(())
}

//
// - run initdb to init temporary instance and get bootstrap data
// - after initialization complete, remove the temp dir.
//
fn bootstrap_timeline(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    tli: ZTimelineId,
    repo: &dyn Repository,
) -> Result<()> {
    let initdb_path = conf.tenant_path(&tenantid).join("tmp");

    // Init temporarily repo to get bootstrap data
    run_initdb(conf, &initdb_path)?;
    let pgdata_path = initdb_path;

    let lsn = get_lsn_from_controlfile(&pgdata_path)?.align();

    info!("bootstrap_timeline {:?} at lsn {}", pgdata_path, lsn);

    // Import the contents of the data directory at the initial checkpoint
    // LSN, and any WAL after that.
    let timeline = repo.create_empty_timeline(tli)?;
    restore_local_repo::import_timeline_from_postgres_datadir(&pgdata_path, &*timeline, lsn)?;
    timeline.checkpoint()?;

    println!(
        "created initial timeline {} timeline.lsn {}",
        tli,
        timeline.get_last_record_lsn()
    );

    let data = tli.to_string();
    fs::write(conf.branch_path("main", &tenantid), data)?;
    println!("created main branch");

    // Remove temp dir. We don't need it anymore
    fs::remove_dir_all(pgdata_path)?;

    Ok(())
}

pub(crate) fn get_tenants(conf: &PageServerConf) -> Result<Vec<String>> {
    let tenants_dir = conf.tenants_path();

    std::fs::read_dir(&tenants_dir)?
        .map(|dir_entry_res| {
            let dir_entry = dir_entry_res?;
            ensure!(dir_entry.file_type()?.is_dir());
            Ok(dir_entry.file_name().to_str().unwrap().to_owned())
        })
        .collect()
}

pub(crate) fn get_branches(conf: &PageServerConf, tenantid: &ZTenantId) -> Result<Vec<BranchInfo>> {
    let repo = tenant_mgr::get_repository_for_tenant(*tenantid)?;

    // Each branch has a corresponding record (text file) in the refs/branches
    // with timeline_id.
    let branches_dir = conf.branches_path(tenantid);

    std::fs::read_dir(&branches_dir)?
        .map(|dir_entry_res| {
            let dir_entry = dir_entry_res?;
            BranchInfo::from_path(dir_entry.path(), conf, tenantid, &repo)
        })
        .collect()
}

pub(crate) fn create_branch(
    conf: &PageServerConf,
    branchname: &str,
    startpoint_str: &str,
    tenantid: &ZTenantId,
) -> Result<BranchInfo> {
    let repo = tenant_mgr::get_repository_for_tenant(*tenantid)?;

    if conf.branch_path(branchname, tenantid).exists() {
        anyhow::bail!("branch {} already exists", branchname);
    }

    let mut startpoint = parse_point_in_time(conf, startpoint_str, tenantid)?;
    let timeline = repo.get_timeline(startpoint.timelineid)?;
    if startpoint.lsn == Lsn(0) {
        // Find end of WAL on the old timeline
        let end_of_wal = timeline.get_last_record_lsn();
        info!("branching at end of WAL: {}", end_of_wal);
        startpoint.lsn = end_of_wal;
    } else {
        // Wait for the WAL to arrive and be processed on the parent branch up
        // to the requested branch point. The repository code itself doesn't
        // require it, but if we start to receive WAL on the new timeline,
        // decoding the new WAL might need to look up previous pages, relation
        // sizes etc. and that would get confused if the previous page versions
        // are not in the repository yet.
        timeline.wait_lsn(startpoint.lsn)?;
    }
    startpoint.lsn = startpoint.lsn.align();
    if timeline.get_start_lsn() > startpoint.lsn {
        anyhow::bail!(
            "invalid startpoint {} for the branch {}: less than timeline start {}",
            startpoint.lsn,
            branchname,
            timeline.get_start_lsn()
        );
    }

    // create a new timeline directory for it
    let newtli = create_timeline(conf, Some(startpoint), tenantid)?;

    // Let the Repository backend do its initialization
    repo.branch_timeline(startpoint.timelineid, newtli, startpoint.lsn)?;

    // Remember the human-readable branch name for the new timeline.
    // FIXME: there's a race condition, if you create a branch with the same
    // name concurrently.
    let data = newtli.to_string();
    fs::write(conf.branch_path(branchname, tenantid), data)?;

    Ok(BranchInfo {
        name: branchname.to_string(),
        timeline_id: newtli,
        latest_valid_lsn: startpoint.lsn,
        ancestor_id: None,
        ancestor_lsn: None,
        current_logical_size: 0,
        current_logical_size_non_incremental: 0,
    })
}

//
// Parse user-given string that represents a point-in-time.
//
// We support multiple variants:
//
// Raw timeline id in hex, meaning the end of that timeline:
//    bc62e7d612d0e6fe8f99a6dd2f281f9d
//
// A specific LSN on a timeline:
//    bc62e7d612d0e6fe8f99a6dd2f281f9d@2/15D3DD8
//
// Same, with a human-friendly branch name:
//    main
//    main@2/15D3DD8
//
// Human-friendly tag name:
//    mytag
//
//
fn parse_point_in_time(
    conf: &PageServerConf,
    s: &str,
    tenantid: &ZTenantId,
) -> Result<PointInTime> {
    let mut strings = s.split('@');
    let name = strings.next().unwrap();

    let lsn: Option<Lsn>;
    if let Some(lsnstr) = strings.next() {
        lsn = Some(
            Lsn::from_str(lsnstr).with_context(|| "invalid LSN in point-in-time specification")?,
        );
    } else {
        lsn = None
    }

    // Check if it's a tag
    if lsn.is_none() {
        let tagpath = conf.tag_path(name, tenantid);
        if tagpath.exists() {
            let pointstr = fs::read_to_string(tagpath)?;

            return parse_point_in_time(conf, &pointstr, tenantid);
        }
    }

    // Check if it's a branch
    // Check if it's branch @ LSN
    let branchpath = conf.branch_path(name, tenantid);
    if branchpath.exists() {
        let pointstr = fs::read_to_string(branchpath)?;

        let mut result = parse_point_in_time(conf, &pointstr, tenantid)?;

        result.lsn = lsn.unwrap_or(Lsn(0));
        return Ok(result);
    }

    // Check if it's a timelineid
    // Check if it's timelineid @ LSN
    if let Ok(timelineid) = ZTimelineId::from_str(name) {
        let tlipath = conf.timeline_path(&timelineid, tenantid);
        if tlipath.exists() {
            return Ok(PointInTime {
                timelineid,
                lsn: lsn.unwrap_or(Lsn(0)),
            });
        }
    }

    bail!("could not parse point-in-time {}", s);
}

fn create_timeline(
    conf: &PageServerConf,
    ancestor: Option<PointInTime>,
    tenantid: &ZTenantId,
) -> Result<ZTimelineId> {
    // Create initial timeline

    let timelineid = ZTimelineId::generate();

    let timelinedir = conf.timeline_path(&timelineid, tenantid);

    fs::create_dir(&timelinedir)?;
    fs::create_dir(&timelinedir.join("wal"))?;

    if let Some(ancestor) = ancestor {
        let data = format!("{}@{}", ancestor.timelineid, ancestor.lsn);
        fs::write(timelinedir.join("ancestor"), data)?;
    }

    Ok(timelineid)
}
