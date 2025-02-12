//! See `pageserver_api::shard` for description on sharding.

use std::{ops::RangeInclusive, str::FromStr};

use hex::FromHex;
use serde::{Deserialize, Serialize};

use crate::id::TenantId;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug, Hash)]
pub struct ShardNumber(pub u8);

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Debug, Hash)]
pub struct ShardCount(pub u8);

/// Combination of ShardNumber and ShardCount.
///
/// For use within the context of a particular tenant, when we need to know which shard we're
/// dealing with, but do not need to know the full ShardIdentity (because we won't be doing
/// any page->shard mapping), and do not need to know the fully qualified TenantShardId.
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct ShardIndex {
    pub shard_number: ShardNumber,
    pub shard_count: ShardCount,
}

/// Formatting helper, for generating the `shard_id` label in traces.
pub struct ShardSlug<'a>(&'a TenantShardId);

/// TenantShardId globally identifies a particular shard in a particular tenant.
///
/// These are written as `<TenantId>-<ShardSlug>`, for example:
///   # The second shard in a two-shard tenant
///   072f1291a5310026820b2fe4b2968934-0102
///
/// If the `ShardCount` is _unsharded_, the `TenantShardId` is written without
/// a shard suffix and is equivalent to the encoding of a `TenantId`: this enables
/// an unsharded [`TenantShardId`] to be used interchangably with a [`TenantId`].
///
/// The human-readable encoding of an unsharded TenantShardId, such as used in API URLs,
/// is both forward and backward compatible with TenantId: a legacy TenantId can be
/// decoded as a TenantShardId, and when re-encoded it will be parseable
/// as a TenantId.
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct TenantShardId {
    pub tenant_id: TenantId,
    pub shard_number: ShardNumber,
    pub shard_count: ShardCount,
}

impl ShardCount {
    pub const MAX: Self = Self(u8::MAX);
    pub const MIN: Self = Self(0);

    /// The internal value of a ShardCount may be zero, which means "1 shard, but use
    /// legacy format for TenantShardId that excludes the shard suffix", also known
    /// as [`TenantShardId::unsharded`].
    ///
    /// This method returns the actual number of shards, i.e. if our internal value is
    /// zero, we return 1 (unsharded tenants have 1 shard).
    pub fn count(&self) -> u8 {
        if self.0 > 0 {
            self.0
        } else {
            1
        }
    }

    /// The literal internal value: this is **not** the number of shards in the
    /// tenant, as we have a special zero value for legacy unsharded tenants.  Use
    /// [`Self::count`] if you want to know the cardinality of shards.
    pub fn literal(&self) -> u8 {
        self.0
    }

    /// Whether the `ShardCount` is for an unsharded tenant, so uses one shard but
    /// uses the legacy format for `TenantShardId`. See also the documentation for
    /// [`Self::count`].
    pub fn is_unsharded(&self) -> bool {
        self.0 == 0
    }

    /// `v` may be zero, or the number of shards in the tenant.  `v` is what
    /// [`Self::literal`] would return.
    pub const fn new(val: u8) -> Self {
        Self(val)
    }
}

impl ShardNumber {
    pub const MAX: Self = Self(u8::MAX);
}

impl TenantShardId {
    pub fn unsharded(tenant_id: TenantId) -> Self {
        Self {
            tenant_id,
            shard_number: ShardNumber(0),
            shard_count: ShardCount(0),
        }
    }

    /// The range of all TenantShardId that belong to a particular TenantId.  This is useful when
    /// you have a BTreeMap of TenantShardId, and are querying by TenantId.
    pub fn tenant_range(tenant_id: TenantId) -> RangeInclusive<Self> {
        RangeInclusive::new(
            Self {
                tenant_id,
                shard_number: ShardNumber(0),
                shard_count: ShardCount(0),
            },
            Self {
                tenant_id,
                shard_number: ShardNumber::MAX,
                shard_count: ShardCount::MAX,
            },
        )
    }

    pub fn range(&self) -> RangeInclusive<Self> {
        RangeInclusive::new(*self, *self)
    }

    pub fn shard_slug(&self) -> impl std::fmt::Display + '_ {
        ShardSlug(self)
    }

    /// Convenience for code that has special behavior on the 0th shard.
    pub fn is_shard_zero(&self) -> bool {
        self.shard_number == ShardNumber(0)
    }

    /// The "unsharded" value is distinct from simply having a single shard: it represents
    /// a tenant which is not shard-aware at all, and whose storage paths will not include
    /// a shard suffix.
    pub fn is_unsharded(&self) -> bool {
        self.shard_number == ShardNumber(0) && self.shard_count.is_unsharded()
    }

    /// Convenience for dropping the tenant_id and just getting the ShardIndex: this
    /// is useful when logging from code that is already in a span that includes tenant ID, to
    /// keep messages reasonably terse.
    pub fn to_index(&self) -> ShardIndex {
        ShardIndex {
            shard_number: self.shard_number,
            shard_count: self.shard_count,
        }
    }

    /// Calculate the children of this TenantShardId when splitting the overall tenant into
    /// the given number of shards.
    pub fn split(&self, new_shard_count: ShardCount) -> Vec<TenantShardId> {
        let effective_old_shard_count = std::cmp::max(self.shard_count.0, 1);
        let mut child_shards = Vec::new();
        for shard_number in 0..ShardNumber(new_shard_count.0).0 {
            // Key mapping is based on a round robin mapping of key hash modulo shard count,
            // so our child shards are the ones which the same keys would map to.
            if shard_number % effective_old_shard_count == self.shard_number.0 {
                child_shards.push(TenantShardId {
                    tenant_id: self.tenant_id,
                    shard_number: ShardNumber(shard_number),
                    shard_count: new_shard_count,
                })
            }
        }

        child_shards
    }
}

impl std::fmt::Display for ShardNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for ShardSlug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02x}{:02x}",
            self.0.shard_number.0, self.0.shard_count.0
        )
    }
}

impl std::fmt::Display for TenantShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.shard_count != ShardCount(0) {
            write!(f, "{}-{}", self.tenant_id, self.shard_slug())
        } else {
            // Legacy case (shard_count == 0) -- format as just the tenant id.  Note that this
            // is distinct from the normal single shard case (shard count == 1).
            self.tenant_id.fmt(f)
        }
    }
}

impl std::fmt::Debug for TenantShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Debug is the same as Display: the compact hex representation
        write!(f, "{}", self)
    }
}

impl std::str::FromStr for TenantShardId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expect format: 16 byte TenantId, '-', 1 byte shard number, 1 byte shard count
        if s.len() == 32 {
            // Legacy case: no shard specified
            Ok(Self {
                tenant_id: TenantId::from_str(s)?,
                shard_number: ShardNumber(0),
                shard_count: ShardCount(0),
            })
        } else if s.len() == 37 {
            let bytes = s.as_bytes();
            let tenant_id = TenantId::from_hex(&bytes[0..32])?;
            let mut shard_parts: [u8; 2] = [0u8; 2];
            hex::decode_to_slice(&bytes[33..37], &mut shard_parts)?;
            Ok(Self {
                tenant_id,
                shard_number: ShardNumber(shard_parts[0]),
                shard_count: ShardCount(shard_parts[1]),
            })
        } else {
            Err(hex::FromHexError::InvalidStringLength)
        }
    }
}

impl From<[u8; 18]> for TenantShardId {
    fn from(b: [u8; 18]) -> Self {
        let tenant_id_bytes: [u8; 16] = b[0..16].try_into().unwrap();

        Self {
            tenant_id: TenantId::from(tenant_id_bytes),
            shard_number: ShardNumber(b[16]),
            shard_count: ShardCount(b[17]),
        }
    }
}

impl ShardIndex {
    pub fn new(number: ShardNumber, count: ShardCount) -> Self {
        Self {
            shard_number: number,
            shard_count: count,
        }
    }
    pub fn unsharded() -> Self {
        Self {
            shard_number: ShardNumber(0),
            shard_count: ShardCount(0),
        }
    }

    /// The "unsharded" value is distinct from simply having a single shard: it represents
    /// a tenant which is not shard-aware at all, and whose storage paths will not include
    /// a shard suffix.
    pub fn is_unsharded(&self) -> bool {
        self.shard_number == ShardNumber(0) && self.shard_count == ShardCount(0)
    }

    /// For use in constructing remote storage paths: concatenate this with a TenantId
    /// to get a fully qualified TenantShardId.
    ///
    /// Backward compat: this function returns an empty string if Self::is_unsharded, such
    /// that the legacy pre-sharding remote key format is preserved.
    pub fn get_suffix(&self) -> String {
        if self.is_unsharded() {
            "".to_string()
        } else {
            format!("-{:02x}{:02x}", self.shard_number.0, self.shard_count.0)
        }
    }
}

impl std::fmt::Display for ShardIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:02x}{:02x}", self.shard_number.0, self.shard_count.0)
    }
}

impl std::fmt::Debug for ShardIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Debug is the same as Display: the compact hex representation
        write!(f, "{}", self)
    }
}

impl std::str::FromStr for ShardIndex {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expect format: 1 byte shard number, 1 byte shard count
        if s.len() == 4 {
            let bytes = s.as_bytes();
            let mut shard_parts: [u8; 2] = [0u8; 2];
            hex::decode_to_slice(bytes, &mut shard_parts)?;
            Ok(Self {
                shard_number: ShardNumber(shard_parts[0]),
                shard_count: ShardCount(shard_parts[1]),
            })
        } else {
            Err(hex::FromHexError::InvalidStringLength)
        }
    }
}

impl From<[u8; 2]> for ShardIndex {
    fn from(b: [u8; 2]) -> Self {
        Self {
            shard_number: ShardNumber(b[0]),
            shard_count: ShardCount(b[1]),
        }
    }
}

impl Serialize for TenantShardId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            // Note: while human encoding of [`TenantShardId`] is backward and forward
            // compatible, this binary encoding is not.
            let mut packed: [u8; 18] = [0; 18];
            packed[0..16].clone_from_slice(&self.tenant_id.as_arr());
            packed[16] = self.shard_number.0;
            packed[17] = self.shard_count.0;

            packed.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for TenantShardId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdVisitor {
            is_human_readable_deserializer: bool,
        }

        impl<'de> serde::de::Visitor<'de> for IdVisitor {
            type Value = TenantShardId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                if self.is_human_readable_deserializer {
                    formatter.write_str("value in form of hex string")
                } else {
                    formatter.write_str("value in form of integer array([u8; 18])")
                }
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let s = serde::de::value::SeqAccessDeserializer::new(seq);
                let id: [u8; 18] = Deserialize::deserialize(s)?;
                Ok(TenantShardId::from(id))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                TenantShardId::from_str(v).map_err(E::custom)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(IdVisitor {
                is_human_readable_deserializer: true,
            })
        } else {
            deserializer.deserialize_tuple(
                18,
                IdVisitor {
                    is_human_readable_deserializer: false,
                },
            )
        }
    }
}

impl Serialize for ShardIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            // Binary encoding is not used in index_part.json, but is included in anticipation of
            // switching various structures (e.g. inter-process communication, remote metadata) to more
            // compact binary encodings in future.
            let mut packed: [u8; 2] = [0; 2];
            packed[0] = self.shard_number.0;
            packed[1] = self.shard_count.0;
            packed.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ShardIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdVisitor {
            is_human_readable_deserializer: bool,
        }

        impl<'de> serde::de::Visitor<'de> for IdVisitor {
            type Value = ShardIndex;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                if self.is_human_readable_deserializer {
                    formatter.write_str("value in form of hex string")
                } else {
                    formatter.write_str("value in form of integer array([u8; 2])")
                }
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let s = serde::de::value::SeqAccessDeserializer::new(seq);
                let id: [u8; 2] = Deserialize::deserialize(s)?;
                Ok(ShardIndex::from(id))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                ShardIndex::from_str(v).map_err(E::custom)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(IdVisitor {
                is_human_readable_deserializer: true,
            })
        } else {
            deserializer.deserialize_tuple(
                2,
                IdVisitor {
                    is_human_readable_deserializer: false,
                },
            )
        }
    }
}
