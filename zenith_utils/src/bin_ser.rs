//! Utilities for binary serialization/deserialization.
//!
//! The [`BeSer`] trait allows us to define data structures
//! that can match data structures that are sent over the wire
//! in big-endian form with no packing.
//!
//! The [`LeSer`] trait does the same thing, in little-endian form.
//!
//! Note: you will get a compile error if you try to `use` both traits
//! in the same module or scope. This is intended to be a safety
//! mechanism: mixing big-endian and little-endian encoding in the same file
//! is error-prone.

#![warn(missing_docs)]

use bincode::Options;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Read, Write};
use thiserror::Error;

/// An error that occurred during a deserialize operation
///
/// This could happen because the input data was too short,
/// or because an invalid value was encountered.
#[derive(Debug, Error)]
pub enum DeserializeError {
    /// The deserializer isn't able to deserialize the supplied data.
    #[error("deserialize error")]
    BadInput,
    /// While deserializing from a `Read` source, an `io::Error` occurred.
    #[error("deserialize error: {0}")]
    Io(io::Error),
}

impl From<bincode::Error> for DeserializeError {
    fn from(e: bincode::Error) -> Self {
        match *e {
            bincode::ErrorKind::Io(io_err) => DeserializeError::Io(io_err),
            _ => DeserializeError::BadInput,
        }
    }
}

/// An error that occurred during a serialize operation
///
/// This probably means our [`Write`] failed, e.g. we tried
/// to write beyond the end of a buffer.
#[derive(Debug, Error)]
pub enum SerializeError {
    /// The serializer isn't able to serialize the supplied data.
    #[error("serialize error")]
    BadInput,
    /// While serializing into a `Write` sink, an `io::Error` occurred.
    #[error("serialize error: {0}")]
    Io(io::Error),
}

impl From<bincode::Error> for SerializeError {
    fn from(e: bincode::Error) -> Self {
        match *e {
            bincode::ErrorKind::Io(io_err) => SerializeError::Io(io_err),
            _ => SerializeError::BadInput,
        }
    }
}

/// A shortcut that configures big-endian binary serialization
///
/// Properties:
/// - Big endian
/// - Fixed integer encoding (i.e. 1u32 is 00000001 not 01)
///
/// Does not allow trailing bytes in deserialization. If this is desired, you
/// may set [`Options::allow_trailing_bytes`] to explicitly accomodate this.
pub fn be_coder() -> impl Options {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
}

/// A shortcut that configures little-ending binary serialization
///
/// Properties:
/// - Little endian
/// - Fixed integer encoding (i.e. 1u32 is 00000001 not 01)
///
/// Does not allow trailing bytes in deserialization. If this is desired, you
/// may set [`Options::allow_trailing_bytes`] to explicitly accomodate this.
pub fn le_coder() -> impl Options {
    bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
}

/// Binary serialize/deserialize helper functions (Big Endian)
///
pub trait BeSer {
    /// Serialize into a byte slice
    fn ser_into_slice(&self, mut b: &mut [u8]) -> Result<(), SerializeError>
    where
        Self: Serialize,
    {
        // &mut [u8] implements Write, but `ser_into` needs a mutable
        // reference to that. So we need the slightly awkward "mutable
        // reference to a mutable reference.
        self.ser_into(&mut b)
    }

    /// Serialize into a borrowed writer
    ///
    /// This is useful for most `Write` types except `&mut [u8]`, which
    /// can more easily use [`ser_into_slice`](Self::ser_into_slice).
    fn ser_into<W: Write>(&self, w: &mut W) -> Result<(), SerializeError>
    where
        Self: Serialize,
    {
        be_coder().serialize_into(w, &self).map_err(|e| e.into())
    }

    /// Serialize into a new heap-allocated buffer
    fn ser(&self) -> Result<Vec<u8>, SerializeError>
    where
        Self: Serialize,
    {
        be_coder().serialize(&self).map_err(|e| e.into())
    }

    /// Deserialize from the full contents of a byte slice
    ///
    /// See also: [`BeSer::des_prefix`]
    fn des(buf: &[u8]) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        be_coder()
            .deserialize(buf)
            .or(Err(DeserializeError::BadInput))
    }

    /// Deserialize from a prefix of the byte slice
    ///
    /// Uses as much of the byte slice as is necessary to deserialize the
    /// type, but does not guarantee that the entire slice is used.
    ///
    /// See also: [`BeSer::des`]
    fn des_prefix(buf: &[u8]) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        be_coder()
            .allow_trailing_bytes()
            .deserialize(buf)
            .or(Err(DeserializeError::BadInput))
    }

    /// Deserialize from a reader
    fn des_from<R: Read>(r: &mut R) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        be_coder().deserialize_from(r).map_err(|e| e.into())
    }

    /// Compute the serialized size of a data structure
    ///
    /// Note: it may be faster to serialize to a buffer and then measure the
    /// buffer length, than to call `serialized_size` and then `ser_into`.
    fn serialized_size(&self) -> Result<u64, SerializeError>
    where
        Self: Serialize,
    {
        be_coder().serialized_size(self).map_err(|e| e.into())
    }
}

/// Binary serialize/deserialize helper functions (Little Endian)
///
pub trait LeSer {
    /// Serialize into a byte slice
    fn ser_into_slice(&self, mut b: &mut [u8]) -> Result<(), SerializeError>
    where
        Self: Serialize,
    {
        // &mut [u8] implements Write, but `ser_into` needs a mutable
        // reference to that. So we need the slightly awkward "mutable
        // reference to a mutable reference.
        self.ser_into(&mut b)
    }

    /// Serialize into a borrowed writer
    ///
    /// This is useful for most `Write` types except `&mut [u8]`, which
    /// can more easily use [`ser_into_slice`](Self::ser_into_slice).
    fn ser_into<W: Write>(&self, w: &mut W) -> Result<(), SerializeError>
    where
        Self: Serialize,
    {
        le_coder().serialize_into(w, &self).map_err(|e| e.into())
    }

    /// Serialize into a new heap-allocated buffer
    fn ser(&self) -> Result<Vec<u8>, SerializeError>
    where
        Self: Serialize,
    {
        le_coder().serialize(&self).map_err(|e| e.into())
    }

    /// Deserialize from the full contents of a byte slice
    ///
    /// See also: [`LeSer::des_prefix`]
    fn des(buf: &[u8]) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        le_coder()
            .deserialize(buf)
            .or(Err(DeserializeError::BadInput))
    }

    /// Deserialize from a prefix of the byte slice
    ///
    /// Uses as much of the byte slice as is necessary to deserialize the
    /// type, but does not guarantee that the entire slice is used.
    ///
    /// See also: [`LeSer::des`]
    fn des_prefix(buf: &[u8]) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        le_coder()
            .allow_trailing_bytes()
            .deserialize(buf)
            .or(Err(DeserializeError::BadInput))
    }

    /// Deserialize from a reader
    fn des_from<R: Read>(r: &mut R) -> Result<Self, DeserializeError>
    where
        Self: DeserializeOwned,
    {
        le_coder().deserialize_from(r).map_err(|e| e.into())
    }

    /// Compute the serialized size of a data structure
    ///
    /// Note: it may be faster to serialize to a buffer and then measure the
    /// buffer length, than to call `serialized_size` and then `ser_into`.
    fn serialized_size(&self) -> Result<u64, SerializeError>
    where
        Self: Serialize,
    {
        le_coder().serialized_size(self).map_err(|e| e.into())
    }
}

// Because usage of `BeSer` or `LeSer` can be done with *either* a Serialize or
// DeserializeOwned implementation, the blanket implementation has to be for every type.
impl<T> BeSer for T {}
impl<T> LeSer for T {}

#[cfg(test)]
mod tests {
    use super::DeserializeError;
    use serde::{Deserialize, Serialize};
    use std::io::Cursor;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct ShortStruct {
        a: u8,
        b: u32,
    }

    const SHORT1: ShortStruct = ShortStruct { a: 7, b: 65536 };
    const SHORT1_ENC_BE: &[u8] = &[7, 0, 1, 0, 0];
    const SHORT1_ENC_BE_TRAILING: &[u8] = &[7, 0, 1, 0, 0, 255, 255, 255];
    const SHORT1_ENC_LE: &[u8] = &[7, 0, 0, 1, 0];
    const SHORT1_ENC_LE_TRAILING: &[u8] = &[7, 0, 0, 1, 0, 255, 255, 255];

    const SHORT2: ShortStruct = ShortStruct {
        a: 8,
        b: 0x07030000,
    };
    const SHORT2_ENC_BE: &[u8] = &[8, 7, 3, 0, 0];
    const SHORT2_ENC_BE_TRAILING: &[u8] = &[8, 7, 3, 0, 0, 0xff, 0xff, 0xff];
    const SHORT2_ENC_LE: &[u8] = &[8, 0, 0, 3, 7];
    const SHORT2_ENC_LE_TRAILING: &[u8] = &[8, 0, 0, 3, 7, 0xff, 0xff, 0xff];

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct LongMsg {
        pub tag: u8,
        pub blockpos: u32,
        pub last_flush_position: u64,
        pub apply: u64,
        pub timestamp: i64,
        pub reply_requested: u8,
    }

    const LONG1: LongMsg = LongMsg {
        tag: 42,
        blockpos: 0x1000_2000,
        last_flush_position: 0x1234_2345_3456_4567,
        apply: 0x9876_5432_10FE_DCBA,
        timestamp: 0x7788_99AA_BBCC_DDFF,
        reply_requested: 1,
    };

    #[test]
    fn be_short() {
        use super::BeSer;

        assert_eq!(SHORT1.serialized_size().unwrap(), 5);

        let encoded = SHORT1.ser().unwrap();
        assert_eq!(encoded, SHORT1_ENC_BE);

        let decoded = ShortStruct::des(SHORT2_ENC_BE).unwrap();
        assert_eq!(decoded, SHORT2);

        // with trailing data
        let decoded = ShortStruct::des_prefix(SHORT2_ENC_BE_TRAILING).unwrap();
        assert_eq!(decoded, SHORT2);
        let err = ShortStruct::des(SHORT2_ENC_BE_TRAILING).unwrap_err();
        assert!(matches!(err, DeserializeError::BadInput));

        // serialize into a `Write` sink.
        let mut buf = Cursor::new(vec![0xFF; 8]);
        SHORT1.ser_into(&mut buf).unwrap();
        assert_eq!(buf.into_inner(), SHORT1_ENC_BE_TRAILING);

        // deserialize from a `Write` sink.
        let mut buf = Cursor::new(SHORT2_ENC_BE);
        let decoded = ShortStruct::des_from(&mut buf).unwrap();
        assert_eq!(decoded, SHORT2);

        // deserialize from a `Write` sink that terminates early.
        let mut buf = Cursor::new([0u8; 4]);
        let err = ShortStruct::des_from(&mut buf).unwrap_err();
        assert!(matches!(err, DeserializeError::Io(_)));
    }

    #[test]
    fn le_short() {
        use super::LeSer;

        assert_eq!(SHORT1.serialized_size().unwrap(), 5);

        let encoded = SHORT1.ser().unwrap();
        assert_eq!(encoded, SHORT1_ENC_LE);

        let decoded = ShortStruct::des(SHORT2_ENC_LE).unwrap();
        assert_eq!(decoded, SHORT2);

        // with trailing data
        let decoded = ShortStruct::des_prefix(SHORT2_ENC_LE_TRAILING).unwrap();
        assert_eq!(decoded, SHORT2);
        let err = ShortStruct::des(SHORT2_ENC_LE_TRAILING).unwrap_err();
        assert!(matches!(err, DeserializeError::BadInput));

        // serialize into a `Write` sink.
        let mut buf = Cursor::new(vec![0xFF; 8]);
        SHORT1.ser_into(&mut buf).unwrap();
        assert_eq!(buf.into_inner(), SHORT1_ENC_LE_TRAILING);

        // deserialize from a `Write` sink.
        let mut buf = Cursor::new(SHORT2_ENC_LE);
        let decoded = ShortStruct::des_from(&mut buf).unwrap();
        assert_eq!(decoded, SHORT2);

        // deserialize from a `Write` sink that terminates early.
        let mut buf = Cursor::new([0u8; 4]);
        let err = ShortStruct::des_from(&mut buf).unwrap_err();
        assert!(matches!(err, DeserializeError::Io(_)));
    }

    #[test]
    fn be_long() {
        use super::BeSer;

        assert_eq!(LONG1.serialized_size().unwrap(), 30);

        let msg = LONG1;

        let encoded = msg.ser().unwrap();
        let expected = hex_literal::hex!(
            "2A 1000 2000 1234 2345 3456 4567 9876 5432 10FE DCBA 7788 99AA BBCC DDFF 01"
        );
        assert_eq!(encoded, expected);

        let msg2 = LongMsg::des(&encoded).unwrap();
        assert_eq!(msg, msg2);
    }

    #[test]
    fn le_long() {
        use super::LeSer;

        assert_eq!(LONG1.serialized_size().unwrap(), 30);

        let msg = LONG1;

        let encoded = msg.ser().unwrap();
        let expected = hex_literal::hex!(
            "2A 0020 0010 6745 5634 4523 3412 BADC FE10 3254 7698 FFDD CCBB AA99 8877 01"
        );
        assert_eq!(encoded, expected);

        let msg2 = LongMsg::des(&encoded).unwrap();
        assert_eq!(msg, msg2);
    }
}
