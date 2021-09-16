//!
//! The layer map tracks what layers exist for all the relishes in a timeline.
//!
//! When the timeline is first accessed, the server lists of all layer files
//! in the timelines/<timelineid> directory, and populates this map with
//! ImageLayer and DeltaLayer structs corresponding to each file. When new WAL
//! is received, we create InMemoryLayers to hold the incoming records. Now and
//! then, in the checkpoint() function, the in-memory layers are frozen, forming
//! new image and delta layers and corresponding files are written to disk.
//!

use crate::layered_repository::storage_layer::{Layer, SegmentTag};
use crate::layered_repository::InMemoryLayer;
use crate::relish::*;
use anyhow::Result;
use lazy_static::lazy_static;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::sync::Arc;
use zenith_metrics::{register_int_gauge, IntGauge};
use zenith_utils::lsn::Lsn;

lazy_static! {
    static ref NUM_INMEMORY_LAYERS: IntGauge =
        register_int_gauge!("pageserver_inmemory_layers", "Number of layers in memory")
            .expect("failed to define a metric");
    static ref NUM_ONDISK_LAYERS: IntGauge =
        register_int_gauge!("pageserver_ondisk_layers", "Number of layers on-disk")
            .expect("failed to define a metric");
}

///
/// LayerMap tracks what layers exist on a timeline.
///
pub struct LayerMap {
    /// All the layers keyed by segment tag
    segs: HashMap<SegmentTag, SegEntry>,

    /// All in-memory layers, ordered by 'oldest_pending_lsn' and generation
    /// of each layer. This allows easy access to the in-memory layer that
    /// contains the oldest WAL record.
    open_layers: BinaryHeap<OpenLayerEntry>,

    /// Generation number, used to distinguish newly inserted entries in the
    /// binary heap from older entries during checkpoint.
    current_generation: u64,
}

impl Default for LayerMap {
    fn default() -> Self {
        LayerMap {
            segs: HashMap::new(),
            open_layers: BinaryHeap::new(),
            current_generation: 0,
        }
    }
}

impl LayerMap {
    ///
    /// Look up a layer using the given segment tag and LSN. This differs from a
    /// plain key-value lookup in that if there is any layer that covers the
    /// given LSN, or precedes the given LSN, it is returned. In other words,
    /// you don't need to know the exact start LSN of the layer.
    ///
    pub fn get(&self, tag: &SegmentTag, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        let segentry = self.segs.get(tag)?;

        segentry.get(lsn)
    }

    ///
    /// Get the open layer for given segment for writing. Or None if no open
    /// layer exists.
    ///
    pub fn get_open(&self, tag: &SegmentTag) -> Option<Arc<InMemoryLayer>> {
        let segentry = self.segs.get(tag)?;

        segentry.open.as_ref().map(Arc::clone)
    }

    ///
    /// Insert an open in-memory layer
    ///
    pub fn insert_open(&mut self, layer: Arc<InMemoryLayer>) {
        let segentry = self.segs.entry(layer.get_seg_tag()).or_default();

        segentry.insert_open(Arc::clone(&layer));

        // Also add it to the binary heap
        let open_layer_entry = OpenLayerEntry {
            oldest_pending_lsn: layer.get_oldest_pending_lsn(),
            layer,
            generation: self.current_generation,
        };
        self.open_layers.push(open_layer_entry);

        NUM_INMEMORY_LAYERS.inc();
    }

    /// Remove the oldest in-memory layer
    pub fn pop_oldest_open(&mut self) {
        // Pop it from the binary heap
        let oldest_entry = self.open_layers.pop().unwrap();
        let segtag = oldest_entry.layer.get_seg_tag();

        // Also remove it from the SegEntry of this segment
        let mut segentry = self.segs.get_mut(&segtag).unwrap();
        assert!(Arc::ptr_eq(
            segentry.open.as_ref().unwrap(),
            &oldest_entry.layer
        ));
        segentry.open = None;

        NUM_INMEMORY_LAYERS.dec();
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        let segentry = self.segs.entry(layer.get_seg_tag()).or_default();
        segentry.insert_historic(layer);

        NUM_ONDISK_LAYERS.inc();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: &dyn Layer) {
        let tag = layer.get_seg_tag();
        let start_lsn = layer.get_start_lsn();

        if let Some(segentry) = self.segs.get_mut(&tag) {
            segentry.historic.remove(&start_lsn);
        }
        NUM_ONDISK_LAYERS.dec();
    }

    // List relations along with a flag that marks if they exist at the given lsn.
    // spcnode 0 and dbnode 0 have special meanings and mean all tabespaces/databases.
    // Pass Tag if we're only interested in some relations.
    pub fn list_relishes(&self, tag: Option<RelTag>, lsn: Lsn) -> Result<HashMap<RelishTag, bool>> {
        let mut rels: HashMap<RelishTag, bool> = HashMap::new();

        for (seg, segentry) in self.segs.iter() {
            match seg.rel {
                RelishTag::Relation(reltag) => {
                    if let Some(request_rel) = tag {
                        if (request_rel.spcnode == 0 || reltag.spcnode == request_rel.spcnode)
                            && (request_rel.dbnode == 0 || reltag.dbnode == request_rel.dbnode)
                        {
                            if let Some(exists) = segentry.exists_at_lsn(lsn) {
                                rels.insert(seg.rel, exists);
                            }
                        }
                    }
                }
                _ => {
                    if tag == None {
                        if let Some(exists) = segentry.exists_at_lsn(lsn) {
                            rels.insert(seg.rel, exists);
                        }
                    }
                }
            }
        }
        Ok(rels)
    }

    /// Is there a newer image layer for given segment?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted.
    pub fn newer_image_layer_exists(&self, seg: SegmentTag, lsn: Lsn) -> bool {
        if let Some(segentry) = self.segs.get(&seg) {
            segentry.newer_image_layer_exists(lsn)
        } else {
            false
        }
    }

    /// Return the oldest in-memory layer, along with its generation number.
    pub fn peek_oldest_open(&self) -> Option<(Arc<InMemoryLayer>, u64)> {
        self.open_layers
            .peek()
            .map(|oldest_entry| (Arc::clone(&oldest_entry.layer), oldest_entry.generation))
    }

    /// Increment the generation number used to stamp open in-memory layers. Layers
    /// added with `insert_open` after this call will be associated with the new
    /// generation. Returns the new generation number.
    pub fn increment_generation(&mut self) -> u64 {
        self.current_generation += 1;
        self.current_generation
    }

    pub fn iter_historic_layers(&self) -> HistoricLayerIter {
        HistoricLayerIter {
            segiter: self.segs.iter(),
            iter: None,
        }
    }

    /// debugging function to print out the contents of the layer map
    #[allow(unused)]
    pub fn dump(&self) -> Result<()> {
        println!("Begin dump LayerMap");
        for (seg, segentry) in self.segs.iter() {
            if let Some(open) = &segentry.open {
                open.dump()?;
            }

            for (_, layer) in segentry.historic.iter() {
                layer.dump()?;
            }
        }
        println!("End dump LayerMap");
        Ok(())
    }
}

///
/// Per-segment entry in the LayerMap::segs hash map. Holds all the layers
/// associated with the segment.
///
/// The last layer that is open for writes is always an InMemoryLayer,
/// and is kept in a separate field, because there can be only one for
/// each segment. The older layers, stored on disk, are kept in a
/// BTreeMap keyed by the layer's start LSN.
struct SegEntry {
    pub open: Option<Arc<InMemoryLayer>>,
    pub historic: BTreeMap<Lsn, Arc<dyn Layer>>,
}

impl Default for SegEntry {
    fn default() -> Self {
        SegEntry {
            open: None,
            historic: BTreeMap::new(),
        }
    }
}

impl SegEntry {
    /// Does the segment exist at given LSN?
    /// Return None if object is not found in this SegEntry.
    fn exists_at_lsn(&self, lsn: Lsn) -> Option<bool> {
        if let Some(layer) = &self.open {
            if layer.get_start_lsn() <= lsn && lsn <= layer.get_end_lsn() {
                let exists = layer.get_seg_exists(lsn).ok()?;
                return Some(exists);
            }
        } else if let Some((_, layer)) = self.historic.range(..=lsn).next_back() {
            let exists = layer.get_seg_exists(lsn).ok()?;
            return Some(exists);
        }
        None
    }

    pub fn get(&self, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        if let Some(open) = &self.open {
            if open.get_start_lsn() <= lsn {
                let x: Arc<dyn Layer> = Arc::clone(open) as _;
                return Some(x);
            }
        }

        if let Some((_start_lsn, layer)) = self.historic.range(..=lsn).next_back() {
            Some(Arc::clone(layer))
        } else {
            None
        }
    }

    pub fn newer_image_layer_exists(&self, lsn: Lsn) -> bool {
        // We only check on-disk layers, because
        // in-memory layers are not durable

        for (_newer_lsn, layer) in self.historic.range(lsn..) {
            // Ignore incremental layers.
            if layer.is_incremental() {
                continue;
            }
            if layer.get_end_lsn() > lsn {
                return true;
            } else {
                continue;
            }
        }
        false
    }

    pub fn insert_open(&mut self, layer: Arc<InMemoryLayer>) {
        assert!(self.open.is_none());
        self.open = Some(layer);
    }

    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        let start_lsn = layer.get_start_lsn();

        self.historic.insert(start_lsn, layer);
    }
}

/// Entry held in LayerMap::open_layers, with boilerplate comparison routines
/// to implement a min-heap ordered by 'oldest_pending_lsn' and 'generation'
///
/// The generation number associated with each entry can be used to distinguish
/// recently-added entries (i.e after last call to increment_generation()) from older
/// entries with the same 'oldest_pending_lsn'.
struct OpenLayerEntry {
    pub oldest_pending_lsn: Lsn, // copy of layer.get_oldest_pending_lsn()
    pub generation: u64,
    pub layer: Arc<InMemoryLayer>,
}
impl Ord for OpenLayerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, and we want a min-heap. Reverse the ordering here
        // to get that. Entries with identical oldest_pending_lsn are ordered by generation
        other
            .oldest_pending_lsn
            .cmp(&self.oldest_pending_lsn)
            .then_with(|| other.generation.cmp(&self.generation))
    }
}
impl PartialOrd for OpenLayerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for OpenLayerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for OpenLayerEntry {}

/// Iterator returned by LayerMap::iter_historic_layers()
pub struct HistoricLayerIter<'a> {
    segiter: std::collections::hash_map::Iter<'a, SegmentTag, SegEntry>,
    iter: Option<std::collections::btree_map::Iter<'a, Lsn, Arc<dyn Layer>>>,
}

impl<'a> Iterator for HistoricLayerIter<'a> {
    type Item = Arc<dyn Layer>;

    fn next(&mut self) -> std::option::Option<<Self as std::iter::Iterator>::Item> {
        loop {
            if let Some(x) = &mut self.iter {
                if let Some(x) = x.next() {
                    return Some(Arc::clone(&*x.1));
                }
            }
            if let Some(seg) = self.segiter.next() {
                self.iter = Some(seg.1.historic.iter());
                continue;
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PageServerConf;
    use std::str::FromStr;
    use zenith_utils::zid::{ZTenantId, ZTimelineId};

    /// Arbitrary relation tag, for testing.
    const TESTREL_A: RelishTag = RelishTag::Relation(RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1000,
        forknum: 0,
    });

    /// Construct a dummy InMemoryLayer for testing
    fn dummy_inmem_layer(
        conf: &'static PageServerConf,
        segno: u32,
        start_lsn: Lsn,
        oldest_pending_lsn: Lsn,
    ) -> Arc<InMemoryLayer> {
        Arc::new(
            InMemoryLayer::create(
                conf,
                ZTimelineId::from_str("00000000000000000000000000000000").unwrap(),
                ZTenantId::from_str("00000000000000000000000000000000").unwrap(),
                SegmentTag {
                    rel: TESTREL_A,
                    segno,
                },
                start_lsn,
                oldest_pending_lsn,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_open_layers() -> Result<()> {
        let conf = PageServerConf::dummy_conf(PageServerConf::test_repo_dir("dummy_inmem_layer"));
        let conf = Box::leak(Box::new(conf));

        let mut layers = LayerMap::default();

        let gen1 = layers.increment_generation();
        layers.insert_open(dummy_inmem_layer(conf, 0, Lsn(100), Lsn(100)));
        layers.insert_open(dummy_inmem_layer(conf, 1, Lsn(100), Lsn(200)));
        layers.insert_open(dummy_inmem_layer(conf, 2, Lsn(100), Lsn(120)));
        layers.insert_open(dummy_inmem_layer(conf, 3, Lsn(100), Lsn(110)));

        let gen2 = layers.increment_generation();
        layers.insert_open(dummy_inmem_layer(conf, 4, Lsn(100), Lsn(110)));
        layers.insert_open(dummy_inmem_layer(conf, 5, Lsn(100), Lsn(100)));

        // A helper function (closure) to pop the next oldest open entry from the layer map,
        // and assert that it is what we'd expect
        let mut assert_pop_layer = |expected_segno: u32, expected_generation: u64| {
            let (l, generation) = layers.peek_oldest_open().unwrap();
            assert!(l.get_seg_tag().segno == expected_segno);
            assert!(generation == expected_generation);
            layers.pop_oldest_open();
        };

        assert_pop_layer(0, gen1); // 100
        assert_pop_layer(5, gen2); // 100
        assert_pop_layer(3, gen1); // 110
        assert_pop_layer(4, gen2); // 110
        assert_pop_layer(2, gen1); // 120
        assert_pop_layer(1, gen1); // 200

        Ok(())
    }
}
