//! Low-level key-value storage abstraction.
//!
use crate::object_key::*;
use crate::relish::*;
use anyhow::Result;
use std::collections::HashSet;
use std::iter::Iterator;
use zenith_utils::lsn::Lsn;
use zenith_utils::zid::ZTimelineId;

///
/// Low-level storage abstraction.
///
/// All the data in the repository is stored in a key-value store. This trait
/// abstracts the details of the key-value store.
///
/// A simple key-value store would support just GET and PUT operations with
/// a key, but the upper layer needs slightly complicated read operations
///
/// The most frequently used function is 'object_versions'. It is used
/// to look up a page version. It is LSN aware, in that the caller
/// specifies an LSN, and the function returns all values for that
/// block with the same or older LSN.
///
pub trait ObjectStore: Send + Sync {
    ///
    /// Store a value with given key.
    ///
    fn put(&self, key: &ObjectKey, lsn: Lsn, value: &[u8]) -> Result<()>;

    /// Read entry with the exact given key.
    ///
    /// This is used for retrieving metadata with special key that doesn't
    /// correspond to any real relation.
    fn get(&self, key: &ObjectKey, lsn: Lsn) -> Result<Vec<u8>>;

    /// Read key greater or equal than specified
    fn get_next_key(&self, key: &ObjectKey) -> Result<Option<ObjectKey>>;

    /// Iterate through all page versions of one object.
    ///
    /// Returns all page versions in descending LSN order, along with the LSN
    /// of each page version.
    fn object_versions<'a>(
        &'a self,
        key: &ObjectKey,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = (Lsn, Vec<u8>)> + 'a>>;

    /// Iterate through versions of all objects in a timeline.
    ///
    /// Returns objects in increasing key-version order.
    /// Returns all versions up to and including the specified LSN.
    fn objects<'a>(
        &'a self,
        timeline: ZTimelineId,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = Result<(ObjectTag, Lsn, Vec<u8>)>> + 'a>>;

    /// Iterate through all keys with given tablespace and database ID, and LSN <= 'lsn'.
    /// Both dbnode and spcnode can be InvalidId (0) which means get all relations in tablespace/cluster
    ///
    /// This is used to implement 'create database'
    fn list_rels(
        &self,
        timelineid: ZTimelineId,
        spcnode: u32,
        dbnode: u32,
        lsn: Lsn,
    ) -> Result<HashSet<RelTag>>;

    /// Iterate through non-rel relishes
    ///
    /// This is used to prepare tarball for new node startup.
    fn list_nonrels<'a>(&'a self, timelineid: ZTimelineId, lsn: Lsn) -> Result<HashSet<RelishTag>>;

    /// Iterate through objects tags. If nonrel_only, then only non-relationa data is iterated.
    ///
    /// This is used to implement GC and preparing tarball for new node startup
    /// Returns objects in increasing key-version order.
    fn list_objects<'a>(
        &'a self,
        timelineid: ZTimelineId,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = ObjectTag> + 'a>>;

    /// Unlink object (used by GC). This mehod may actually delete object or just mark it for deletion.
    fn unlink(&self, key: &ObjectKey, lsn: Lsn) -> Result<()>;

    // Compact storage and remove versions marged for deletion
    fn compact(&self);
}
