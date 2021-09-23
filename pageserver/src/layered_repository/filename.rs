//!
//! Helper functions for dealing with filenames of the image and delta layer files.
//!
use crate::layered_repository::storage_layer::SegmentTag;
use crate::relish::*;
use crate::PageServerConf;
use crate::{ZTenantId, ZTimelineId};
use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use log::*;
use zenith_utils::lsn::Lsn;

// Note: LayeredTimeline::load_layer_map() relies on this sort order
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct DeltaFileName {
    pub seg: SegmentTag,
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub dropped: bool,
}

/// Represents the filename of a DeltaLayer
///
///    <spcnode>_<dbnode>_<relnode>_<forknum>_<seg>_<start LSN>_<end LSN>
///
/// or if it was dropped:
///
///    <spcnode>_<dbnode>_<relnode>_<forknum>_<seg>_<start LSN>_<end LSN>_DROPPED
///
impl DeltaFileName {
    ///
    /// Parse a string as a delta file name. Returns None if the filename does not
    /// match the expected pattern.
    ///
    pub fn from_str(fname: &str) -> Option<Self> {
        let rel;
        let mut parts;
        if let Some(rest) = fname.strip_prefix("rel_") {
            parts = rest.split('_');
            rel = RelishTag::Relation(RelTag {
                spcnode: parts.next()?.parse::<u32>().ok()?,
                dbnode: parts.next()?.parse::<u32>().ok()?,
                relnode: parts.next()?.parse::<u32>().ok()?,
                forknum: parts.next()?.parse::<u8>().ok()?,
            });
        } else if let Some(rest) = fname.strip_prefix("pg_xact_") {
            parts = rest.split('_');
            rel = RelishTag::Slru {
                slru: SlruKind::Clog,
                segno: u32::from_str_radix(parts.next()?, 16).ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_multixact_members_") {
            parts = rest.split('_');
            rel = RelishTag::Slru {
                slru: SlruKind::MultiXactMembers,
                segno: u32::from_str_radix(parts.next()?, 16).ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_multixact_offsets_") {
            parts = rest.split('_');
            rel = RelishTag::Slru {
                slru: SlruKind::MultiXactOffsets,
                segno: u32::from_str_radix(parts.next()?, 16).ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_filenodemap_") {
            parts = rest.split('_');
            rel = RelishTag::FileNodeMap {
                spcnode: parts.next()?.parse::<u32>().ok()?,
                dbnode: parts.next()?.parse::<u32>().ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_twophase_") {
            parts = rest.split('_');
            rel = RelishTag::TwoPhase {
                xid: parts.next()?.parse::<u32>().ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_control_checkpoint_") {
            parts = rest.split('_');
            rel = RelishTag::Checkpoint;
        } else if let Some(rest) = fname.strip_prefix("pg_control_") {
            parts = rest.split('_');
            rel = RelishTag::ControlFile;
        } else {
            return None;
        }

        let segno = parts.next()?.parse::<u32>().ok()?;

        let seg = SegmentTag { rel, segno };

        let start_lsn = Lsn::from_hex(parts.next()?).ok()?;
        let end_lsn = Lsn::from_hex(parts.next()?).ok()?;

        let mut dropped = false;
        if let Some(suffix) = parts.next() {
            if suffix == "DROPPED" {
                dropped = true;
            } else {
                return None;
            }
        }
        if parts.next().is_some() {
            return None;
        }

        Some(DeltaFileName {
            seg,
            start_lsn,
            end_lsn,
            dropped,
        })
    }
}

impl fmt::Display for DeltaFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let basename = match self.seg.rel {
            RelishTag::Relation(reltag) => format!(
                "rel_{}_{}_{}_{}",
                reltag.spcnode, reltag.dbnode, reltag.relnode, reltag.forknum
            ),
            RelishTag::Slru {
                slru: SlruKind::Clog,
                segno,
            } => format!("pg_xact_{:04X}", segno),
            RelishTag::Slru {
                slru: SlruKind::MultiXactMembers,
                segno,
            } => format!("pg_multixact_members_{:04X}", segno),
            RelishTag::Slru {
                slru: SlruKind::MultiXactOffsets,
                segno,
            } => format!("pg_multixact_offsets_{:04X}", segno),
            RelishTag::FileNodeMap { spcnode, dbnode } => {
                format!("pg_filenodemap_{}_{}", spcnode, dbnode)
            }
            RelishTag::TwoPhase { xid } => format!("pg_twophase_{}", xid),
            RelishTag::Checkpoint => "pg_control_checkpoint".to_string(),
            RelishTag::ControlFile => "pg_control".to_string(),
        };

        write!(
            f,
            "{}_{}_{:016X}_{:016X}{}",
            basename,
            self.seg.segno,
            u64::from(self.start_lsn),
            u64::from(self.end_lsn),
            if self.dropped { "_DROPPED" } else { "" }
        )
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct ImageFileName {
    pub seg: SegmentTag,
    pub lsn: Lsn,
}

///
/// Represents the filename of an ImageLayer
///
///    <spcnode>_<dbnode>_<relnode>_<forknum>_<seg>_<LSN>
///
impl ImageFileName {
    ///
    /// Parse a string as an image file name. Returns None if the filename does not
    /// match the expected pattern.
    ///
    pub fn from_str(fname: &str) -> Option<Self> {
        let rel;
        let mut parts;
        if let Some(rest) = fname.strip_prefix("rel_") {
            parts = rest.split('_');
            rel = RelishTag::Relation(RelTag {
                spcnode: parts.next()?.parse::<u32>().ok()?,
                dbnode: parts.next()?.parse::<u32>().ok()?,
                relnode: parts.next()?.parse::<u32>().ok()?,
                forknum: parts.next()?.parse::<u8>().ok()?,
            });
        } else if let Some(rest) = fname.strip_prefix("pg_xact_") {
            parts = rest.split('_');
            rel = RelishTag::Slru {
                slru: SlruKind::Clog,
                segno: u32::from_str_radix(parts.next()?, 16).ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_multixact_members_") {
            parts = rest.split('_');
            rel = RelishTag::Slru {
                slru: SlruKind::MultiXactMembers,
                segno: u32::from_str_radix(parts.next()?, 16).ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_multixact_offsets_") {
            parts = rest.split('_');
            rel = RelishTag::Slru {
                slru: SlruKind::MultiXactOffsets,
                segno: u32::from_str_radix(parts.next()?, 16).ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_filenodemap_") {
            parts = rest.split('_');
            rel = RelishTag::FileNodeMap {
                spcnode: parts.next()?.parse::<u32>().ok()?,
                dbnode: parts.next()?.parse::<u32>().ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_twophase_") {
            parts = rest.split('_');
            rel = RelishTag::TwoPhase {
                xid: parts.next()?.parse::<u32>().ok()?,
            };
        } else if let Some(rest) = fname.strip_prefix("pg_control_checkpoint_") {
            parts = rest.split('_');
            rel = RelishTag::Checkpoint;
        } else if let Some(rest) = fname.strip_prefix("pg_control_") {
            parts = rest.split('_');
            rel = RelishTag::ControlFile;
        } else {
            return None;
        }

        let segno = parts.next()?.parse::<u32>().ok()?;

        let seg = SegmentTag { rel, segno };

        let lsn = Lsn::from_hex(parts.next()?).ok()?;

        if parts.next().is_some() {
            return None;
        }

        Some(ImageFileName { seg, lsn })
    }
}

impl fmt::Display for ImageFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let basename = match self.seg.rel {
            RelishTag::Relation(reltag) => format!(
                "rel_{}_{}_{}_{}",
                reltag.spcnode, reltag.dbnode, reltag.relnode, reltag.forknum
            ),
            RelishTag::Slru {
                slru: SlruKind::Clog,
                segno,
            } => format!("pg_xact_{:04X}", segno),
            RelishTag::Slru {
                slru: SlruKind::MultiXactMembers,
                segno,
            } => format!("pg_multixact_members_{:04X}", segno),
            RelishTag::Slru {
                slru: SlruKind::MultiXactOffsets,
                segno,
            } => format!("pg_multixact_offsets_{:04X}", segno),
            RelishTag::FileNodeMap { spcnode, dbnode } => {
                format!("pg_filenodemap_{}_{}", spcnode, dbnode)
            }
            RelishTag::TwoPhase { xid } => format!("pg_twophase_{}", xid),
            RelishTag::Checkpoint => "pg_control_checkpoint".to_string(),
            RelishTag::ControlFile => "pg_control".to_string(),
        };

        write!(
            f,
            "{}_{}_{:016X}",
            basename,
            self.seg.segno,
            u64::from(self.lsn),
        )
    }
}

#[derive(Debug)]
pub struct TimelineFiles {
    pub image_layers: BTreeSet<ImageFileName>,
    pub delta_layers: BTreeSet<DeltaFileName>,
    pub metadata: Option<PathBuf>,
}

/// Scan timeline directory and create ImageFileName and DeltaFilename
/// structs representing all files on disk
pub fn list_timeline_files(
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
) -> Result<TimelineFiles> {
    let path = conf.timeline_path(&timelineid, &tenantid);

    let mut image_layers = BTreeSet::new();
    let mut delta_layers = BTreeSet::new();
    let mut metadata = None::<PathBuf>;
    for direntry in fs::read_dir(path)? {
        let entry = direntry?;
        let fname = entry.file_name();
        let fname = fname.to_str().unwrap();

        if let Some(deltafilename) = DeltaFileName::from_str(fname) {
            delta_layers.insert(deltafilename);
        } else if let Some(imgfilename) = ImageFileName::from_str(fname) {
            image_layers.insert(imgfilename);
        } else if fname == "metadata" {
            if let Some(old_path) = metadata.replace(entry.path()) {
                anyhow::bail!(
                    "Found two metadata files for a single layer. First: '{}', second: '{}'",
                    old_path.display(),
                    entry.path().display()
                )
            }
        } else if fname == "wal" || fname == "ancestor" {
            // ignore these
        } else {
            warn!("unrecognized filename in timeline dir: {}", fname);
        }
    }
    Ok(TimelineFiles {
        image_layers,
        delta_layers,
        metadata,
    })
}

/// Helper enum to hold a PageServerConf, or a path
///
/// This is used by DeltaLayer and ImageLayer. Normally, this holds a reference to the
/// global config, and paths to layer files are constructed using the tenant/timeline
/// path from the config. But in the 'dump_layerfile' binary, we need to construct a Layer
/// struct for a file on disk, without having a page server running, so that we have no
/// config. In that case, we use the Path variant to hold the full path to the file on
/// disk.
pub enum PathOrConf {
    Path(PathBuf),
    Conf(&'static PageServerConf),
}
