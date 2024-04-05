use serde::Serialize;
use utils::seqwait::MonotonicCounter;

mod auth;
mod compute_hook;
mod heartbeater;
pub mod http;
mod id_lock_map;
pub mod metrics;
mod node;
mod pageserver_client;
pub mod persistence;
mod reconciler;
mod scheduler;
mod schema;
pub mod service;
mod tenant_shard;

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Serialize)]
struct Sequence(u64);

impl Sequence {
    fn initial() -> Self {
        Self(0)
    }
}

impl std::fmt::Display for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl MonotonicCounter<Sequence> for Sequence {
    fn cnt_advance(&mut self, v: Sequence) {
        assert!(*self <= v);
        *self = v;
    }
    fn cnt_value(&self) -> Sequence {
        *self
    }
}

impl Sequence {
    fn next(&self) -> Sequence {
        Sequence(self.0 + 1)
    }
}
