use crate::object_key::*;
use crate::waldecoder::TransactionId;
use crate::ZTimelineId;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_ffi::nonrelfile_utils::transaction_id_get_status;
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::forknumber_to_name;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::iter::Iterator;
use std::sync::Arc;
use std::time::Duration;
use zenith_utils::lsn::Lsn;

///
/// A repository corresponds to one .zenith directory. One repository holds multiple
/// timelines, forked off from the same initial call to 'initdb'.
pub trait Repository: Send + Sync {
    /// Get Timeline handle for given zenith timeline ID.
    fn get_timeline(&self, timelineid: ZTimelineId) -> Result<Arc<dyn Timeline>>;

    /// Create a new, empty timeline. The caller is responsible for loading data into it
    fn create_empty_timeline(
        &self,
        timelineid: ZTimelineId,
        start_lsn: Lsn,
    ) -> Result<Arc<dyn Timeline>>;

    /// Branch a timeline
    fn branch_timeline(&self, src: ZTimelineId, dst: ZTimelineId, start_lsn: Lsn) -> Result<()>;

    //fn get_stats(&self) -> RepositoryStats;
}

///
/// Result of performing GC
///
#[derive(Default)]
pub struct GcResult {
    pub n_relations: u64,
    pub inspected: u64,
    pub truncated: u64,
    pub deleted: u64,
    pub prep_deleted: u64, // 2PC prepare
    pub slru_deleted: u64, // SLRU (clog, multixact)
    pub chkp_deleted: u64, // Checkpoints
    pub dropped: u64,
    pub elapsed: Duration,
}

pub trait Timeline: Send + Sync {
    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page in the cache.
    fn get_page_at_lsn(&self, tag: ObjectTag, lsn: Lsn) -> Result<Bytes>;

    /// Look up given page in the cache.
    fn get_page_at_lsn_nowait(&self, tag: ObjectTag, lsn: Lsn) -> Result<Bytes>;

    /// Get size of relation
    fn get_rel_size(&self, tag: RelTag, lsn: Lsn) -> Result<u32>;

    /// Does relation exist?
    fn get_rel_exists(&self, tag: RelTag, lsn: Lsn) -> Result<bool>;

    /// Get a list of all distinct relations in given tablespace and database.
    fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>>;

    /// Get a list of non-relational objects
    fn list_nonrels<'a>(&'a self, lsn: Lsn) -> Result<Box<dyn Iterator<Item = ObjectTag> + 'a>>;

    // Get total size of all relations in the timeline
    fn get_timeline_size(&self, lsn: Lsn) -> Result<u64>;

    //------------------------------------------------------------------------------
    // Public PUT functions, to update the repository with new page versions.
    //
    // These are called by the WAL receiver to digest WAL records.
    //------------------------------------------------------------------------------

    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    fn put_wal_record(&self, tag: ObjectTag, rec: WALRecord) -> Result<()>;

    /// Put raw data
    fn put_raw_data(&self, tag: ObjectTag, lsn: Lsn, data: &[u8]) -> Result<()>;

    /// Like put_wal_record, but with ready-made image of the page.
    fn put_page_image(&self, tag: ObjectTag, lsn: Lsn, img: Bytes, update_meta: bool)
        -> Result<()>;

    /// Truncate relation
    fn put_truncation(&self, rel: RelTag, lsn: Lsn, nblocks: u32) -> Result<()>;

    /// Unlink relation. This method is used for marking dropped relations.
    fn put_unlink(&self, tag: RelTag, lsn: Lsn) -> Result<()>;

    /// Truncate SLRU segment
    fn put_slru_truncate(&self, tag: ObjectTag, lsn: Lsn) -> Result<()>;

    // Get object tag greater or equal than specified
    fn get_next_tag(&self, tag: ObjectTag) -> Result<Option<ObjectTag>>;

    /// Remember the all WAL before the given LSN has been processed.
    ///
    /// The WAL receiver calls this after the put_* functions, to indicate that
    /// all WAL before this point has been digested. Before that, if you call
    /// GET on an earlier LSN, it will block.
    fn advance_last_valid_lsn(&self, lsn: Lsn);
    fn get_last_valid_lsn(&self) -> Lsn;
    fn init_valid_lsn(&self, lsn: Lsn);

    /// Like `advance_last_valid_lsn`, but this always points to the end of
    /// a WAL record, not in the middle of one.
    ///
    /// This must be <= last valid LSN. This is tracked separately from last
    /// valid LSN, so that the WAL receiver knows where to restart streaming.
    fn advance_last_record_lsn(&self, lsn: Lsn);
    fn get_last_record_lsn(&self) -> Lsn;

    // Like `advance_last_record_lsn`, but points to the start position of last record
    fn get_prev_record_lsn(&self) -> Lsn;

    ///
    /// Flush to disk all data that was written with the put_* functions
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    fn checkpoint(&self) -> Result<()>;

    /// Events for all relations in the timeline.
    /// Contains updates from start up to the last valid LSN
    /// at time of history() call. This lsn can be read via the lsn() function.
    ///
    /// Relation size is increased implicitly and decreased with Truncate updates.
    // TODO ordering guarantee?
    fn history<'a>(&'a self) -> Result<Box<dyn History + 'a>>;

    /// Perform one garbage collection iteration.
    /// Garbage collection is periodically performed by GC thread,
    /// but it can be explicitly requested through page server API.
    ///
    /// `horizon` specifies delta from last LSN to preserve all object versions (PITR interval).
    fn gc_iteration(&self, horizon: u64) -> Result<GcResult>;

    // Check transaction status
    fn get_tx_status(&self, xid: TransactionId, lsn: Lsn) -> anyhow::Result<u8> {
        let blknum = xid / pg_constants::CLOG_XACTS_PER_PAGE;
        let tag = ObjectTag::Clog(SlruBufferTag { blknum });
        let clog_page = self.get_page_at_lsn(tag, lsn)?;
        let status = transaction_id_get_status(xid, &clog_page[..]);
        Ok(status)
    }
}

pub trait History: Iterator<Item = Result<RelationUpdate>> {
    /// The last_valid_lsn at the time of history() call.
    fn lsn(&self) -> Lsn;
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationUpdate {
    pub rel: RelTag,
    pub lsn: Lsn,
    pub update: Update,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Update {
    Page { blknum: u32, img: Bytes },
    WALRecord { blknum: u32, rec: WALRecord },
    Truncate { n_blocks: u32 },
    Unlink,
}

#[derive(Clone)]
pub struct RepositoryStats {
    pub num_entries: Lsn,
    pub num_page_images: Lsn,
    pub num_wal_records: Lsn,
    pub num_getpage_requests: Lsn,
}

///
/// Relation data file segment id throughout the Postgres cluster.
///
/// Every data file in Postgres is uniquely identified by 4 numbers:
/// - relation id / node (`relnode`)
/// - database id (`dbnode`)
/// - tablespace id (`spcnode`), in short this is a unique id of a separate
///   directory to store data files.
/// - forknumber (`forknum`) is used to split different kinds of data of the same relation
///   between some set of files (`relnode`, `relnode_fsm`, `relnode_vm`).
///
/// In native Postgres code `RelFileNode` structure and individual `ForkNumber` value
/// are used for the same purpose.
/// [See more related comments here](https:///github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/relfilenode.h#L57).
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Ord, Clone, Copy, Serialize, Deserialize)]
pub struct RelTag {
    pub forknum: u8,
    pub spcnode: u32,
    pub dbnode: u32,
    pub relnode: u32,
}

impl RelTag {
    pub const ZEROED: Self = Self {
        forknum: 0,
        spcnode: 0,
        dbnode: 0,
        relnode: 0,
    };
}

/// Display RelTag in the same format that's used in most PostgreSQL debug messages:
///
/// <spcnode>/<dbnode>/<relnode>[_fsm|_vm|_init]
///
impl fmt::Display for RelTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(forkname) = forknumber_to_name(self.forknum) {
            write!(
                f,
                "{}/{}/{}_{}",
                self.spcnode, self.dbnode, self.relnode, forkname
            )
        } else {
            write!(f, "{}/{}/{}", self.spcnode, self.dbnode, self.relnode)
        }
    }
}

///
/// `RelTag` + block number (`blknum`) gives us a unique id of the page in the cluster.
/// This is used as a part of the key inside key-value storage (RocksDB currently).
///
/// In Postgres `BufferTag` structure is used for exactly the same purpose.
/// [See more related comments here](https://github.com/postgres/postgres/blob/99c5852e20a0987eca1c38ba0c09329d4076b6a0/src/include/storage/buf_internals.h#L91).
///
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
pub struct BufferTag {
    pub rel: RelTag,
    pub blknum: u32,
}

impl BufferTag {
    pub const ZEROED: Self = Self {
        rel: RelTag::ZEROED,
        blknum: 0,
    };
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WALRecord {
    pub lsn: Lsn, // LSN at the *end* of the record
    pub will_init: bool,
    pub rec: Bytes,
    // Remember the offset of main_data in rec,
    // so that we don't have to parse the record again.
    // If record has no main_data, this offset equals rec.len().
    pub main_data_offset: u32,
}

impl WALRecord {
    pub fn pack(&self, buf: &mut BytesMut) {
        buf.put_u64(self.lsn.0);
        buf.put_u8(self.will_init as u8);
        buf.put_u32(self.main_data_offset);
        buf.put_u32(self.rec.len() as u32);
        buf.put_slice(&self.rec[..]);
    }
    pub fn unpack(buf: &mut Bytes) -> WALRecord {
        let lsn = Lsn::from(buf.get_u64());
        let will_init = buf.get_u8() != 0;
        let main_data_offset = buf.get_u32();
        let mut dst = vec![0u8; buf.get_u32() as usize];
        buf.copy_to_slice(&mut dst);
        WALRecord {
            lsn,
            will_init,
            rec: Bytes::from(dst),
            main_data_offset,
        }
    }
}

///
/// Tests that should work the same with any Repository/Timeline implementation.
///
#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_repository::ObjectRepository;
    use crate::rocksdb_storage::RocksObjectStore;
    use crate::walredo::{WalRedoError, WalRedoManager};
    use crate::PageServerConf;
    use postgres_ffi::pg_constants;
    use std::fs;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Duration;

    /// Arbitrary relation tag, for testing.
    const TESTREL_A: RelTag = RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1000,
        forknum: 0,
    };

    /// Convenience function to create a BufferTag for testing.
    /// Helps to keeps the tests shorter.
    #[allow(non_snake_case)]
    fn TEST_BUF(blknum: u32) -> ObjectTag {
        ObjectTag::RelationBuffer(BufferTag {
            rel: TESTREL_A,
            blknum,
        })
    }

    /// Convenience function to create a page image with given string as the only content
    #[allow(non_snake_case)]
    fn TEST_IMG(s: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        buf.resize(8192, 0);

        buf.freeze()
    }

    fn get_test_repo(test_name: &str) -> Result<Box<dyn Repository>> {
        let repo_dir = PathBuf::from(format!("../tmp_check/test_{}", test_name));
        let _ = fs::remove_dir_all(&repo_dir);
        fs::create_dir_all(&repo_dir)?;

        let conf = PageServerConf {
            daemonize: false,
            interactive: false,
            gc_horizon: 64 * 1024 * 1024,
            gc_period: Duration::from_secs(10),
            listen_addr: "127.0.0.1:5430".to_string(),
            workdir: repo_dir,
            pg_distrib_dir: "".into(),
        };
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));

        let obj_store = RocksObjectStore::create(conf)?;

        let walredo_mgr = TestRedoManager {};

        let repo = ObjectRepository::new(conf, Arc::new(obj_store), Arc::new(walredo_mgr));

        Ok(Box::new(repo))
    }

    /// Test get_relsize() and truncation.
    #[test]
    fn test_relsize() -> Result<()> {
        // get_timeline() with non-existent timeline id should fail
        //repo.get_timeline("11223344556677881122334455667788");

        // Create timeline to work on
        let repo = get_test_repo("test_relsize")?;
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0))?;

        tline.init_valid_lsn(Lsn(1));
        tline.put_page_image(TEST_BUF(0), Lsn(2), TEST_IMG("foo blk 0 at 2"), true)?;
        tline.put_page_image(TEST_BUF(0), Lsn(2), TEST_IMG("foo blk 0 at 2"), true)?;
        tline.put_page_image(TEST_BUF(0), Lsn(3), TEST_IMG("foo blk 0 at 3"), true)?;
        tline.put_page_image(TEST_BUF(1), Lsn(4), TEST_IMG("foo blk 1 at 4"), true)?;
        tline.put_page_image(TEST_BUF(2), Lsn(5), TEST_IMG("foo blk 2 at 5"), true)?;

        tline.advance_last_valid_lsn(Lsn(5));

        // The relation was created at LSN 2, not visible at LSN 1 yet.
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(1))?, false);
        assert!(tline.get_rel_size(TESTREL_A, Lsn(1)).is_err());

        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(2))?, true);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(2))?, 1);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(5))?, 3);

        // Check page contents at each LSN
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(2))?,
            TEST_IMG("foo blk 0 at 2")
        );

        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(3))?,
            TEST_IMG("foo blk 0 at 3")
        );

        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(4))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(1), Lsn(4))?,
            TEST_IMG("foo blk 1 at 4")
        );

        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(5))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(1), Lsn(5))?,
            TEST_IMG("foo blk 1 at 4")
        );
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(2), Lsn(5))?,
            TEST_IMG("foo blk 2 at 5")
        );

        // Truncate last block
        tline.put_truncation(TESTREL_A, Lsn(6), 2)?;
        tline.advance_last_valid_lsn(Lsn(6));

        // Check reported size and contents after truncation
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(6))?, 2);
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(0), Lsn(6))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(1), Lsn(6))?,
            TEST_IMG("foo blk 1 at 4")
        );

        // should still see the truncated block with older LSN
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(5))?, 3);
        assert_eq!(
            tline.get_page_at_lsn(TEST_BUF(2), Lsn(5))?,
            TEST_IMG("foo blk 2 at 5")
        );

        Ok(())
    }

    /// Test get_relsize() and truncation with a file larger than 1 GB, so that it's
    /// split into multiple 1 GB segments in Postgres.
    ///
    /// This isn't very interesting with the RocksDb implementation, as we don't pay
    /// any attention to Postgres segment boundaries there.
    #[test]
    fn test_large_rel() -> Result<()> {
        let repo = get_test_repo("test_large_rel")?;
        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0))?;

        tline.init_valid_lsn(Lsn(1));

        let mut lsn = 0;
        for i in 0..pg_constants::RELSEG_SIZE + 1 {
            let img = TEST_IMG(&format!("foo blk {} at {}", i, Lsn(lsn)));
            lsn += 1;
            tline.put_page_image(TEST_BUF(i as u32), Lsn(lsn), img, true)?;
        }
        tline.advance_last_valid_lsn(Lsn(lsn));

        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE + 1
        );

        // Truncate one block
        lsn += 1;
        tline.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE)?;
        tline.advance_last_valid_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE
        );

        // Truncate another block
        lsn += 1;
        tline.put_truncation(TESTREL_A, Lsn(lsn), pg_constants::RELSEG_SIZE - 1)?;
        tline.advance_last_valid_lsn(Lsn(lsn));
        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE - 1
        );

        Ok(())
    }

    #[test]
    fn test_history() -> Result<()> {
        let repo = get_test_repo("test_snapshot")?;

        let timelineid = ZTimelineId::from_str("11223344556677881122334455667788").unwrap();
        let tline = repo.create_empty_timeline(timelineid, Lsn(0))?;

        let mut snapshot = tline.history()?;
        assert_eq!(snapshot.lsn(), Lsn(0));
        assert_eq!(None, snapshot.next().transpose()?);

        // add a page and advance the last valid LSN
        let rel = TESTREL_A;
        let tag = TEST_BUF(1);
        tline.put_page_image(tag, Lsn(1), TEST_IMG("blk 1 @ lsn 1"), true)?;
        tline.advance_last_valid_lsn(Lsn(1));
        let mut snapshot = tline.history()?;
        assert_eq!(snapshot.lsn(), Lsn(1));
        let expected_page = RelationUpdate {
            rel: rel,
            lsn: Lsn(1),
            update: Update::Page {
                blknum: 1,
                img: TEST_IMG("blk 1 @ lsn 1"),
            },
        };
        assert_eq!(Some(&expected_page), snapshot.next().transpose()?.as_ref());
        assert_eq!(None, snapshot.next().transpose()?);

        // truncate to zero, but don't advance the last valid LSN
        tline.put_truncation(rel, Lsn(2), 0)?;
        let mut snapshot = tline.history()?;
        assert_eq!(snapshot.lsn(), Lsn(1));
        assert_eq!(Some(&expected_page), snapshot.next().transpose()?.as_ref());
        assert_eq!(None, snapshot.next().transpose()?);

        // advance the last valid LSN and the truncation should be observable
        tline.advance_last_valid_lsn(Lsn(2));
        let mut snapshot = tline.history()?;
        assert_eq!(snapshot.lsn(), Lsn(2));

        // TODO ordering not guaranteed by API. But currently it returns the
        // truncation entry before the block data.
        let expected_truncate = RelationUpdate {
            rel: rel,
            lsn: Lsn(2),
            update: Update::Truncate { n_blocks: 0 },
        };
        assert_eq!(Some(expected_truncate), snapshot.next().transpose()?);
        assert_eq!(Some(&expected_page), snapshot.next().transpose()?.as_ref());
        assert_eq!(None, snapshot.next().transpose()?);

        Ok(())
    }

    // Mock WAL redo manager that doesn't do much
    struct TestRedoManager {}

    impl WalRedoManager for TestRedoManager {
        fn request_redo(
            &self,
            tag: ObjectTag,
            lsn: Lsn,
            base_img: Option<Bytes>,
            records: Vec<WALRecord>,
        ) -> Result<Bytes, WalRedoError> {
            let s = format!(
                "redo for {:?} to get to {}, with {} and {} records",
                tag,
                lsn,
                if base_img.is_some() {
                    "base image"
                } else {
                    "no base image"
                },
                records.len()
            );
            println!("{}", s);
            Ok(TEST_IMG(&s))
        }
    }
}
