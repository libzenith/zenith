use pageserver_api::key::{Key, AUX_KEY_PREFIX, METADATA_KEY_SIZE};
use tracing::warn;

/// Create a metadata key from a hash, encoded as [AUX_KEY_PREFIX, 2B directory prefix, first 13B of 128b xxhash].
fn aux_hash_to_metadata_key(dir_level1: u8, dir_level2: u8, data: &[u8]) -> Key {
    let mut key = [0; METADATA_KEY_SIZE];
    let hash = twox_hash::xxh3::hash128(data).to_be_bytes();
    key[0] = AUX_KEY_PREFIX;
    key[1] = dir_level1;
    key[2] = dir_level2;
    key[3..16].copy_from_slice(&hash[0..13]);
    Key::from_metadata_key_fixed_size(&key)
}

const AUX_DIR_PG_LOGICAL: u8 = 0x01;
const AUX_DIR_PG_REPLSLOT: u8 = 0x02;
const AUX_DIR_PG_UNKNOWN: u8 = 0xFF;

/// Encode the aux file into a fixed-size key.
///
/// The first byte is the AUX key prefix. We use the next 2 bytes of the key for the directory / aux file type.
/// We have one-to-one mapping for each of the aux file that we support. We hash the remaining part of the path
/// (usually a single file name, or several components) into 13-byte hash. The way we determine the 2-byte prefix
/// is roughly based on the first two components of the path, one unique number for one component.
///
/// * pg_logical/mappings -> 0x0101
/// * pg_logical/snapshots -> 0x0102
/// * pg_logical/replorigin_checkpoint -> 0x0103
/// * pg_logical/others -> 0x01FF
/// * pg_replslot/ -> 0x0201
/// * others -> 0xFFFF
///
/// If you add new AUX files to this function, please also add a test case to `test_encoding_portable`.
/// The new file type must have never been written to the storage before. Otherwise, there could be data
/// corruptions as the new file belongs to a new prefix but it might have been stored under the `others` prefix.
pub fn encode_aux_file_key(path: &str) -> Key {
    if let Some(fname) = path.strip_prefix("pg_logical/mappings/") {
        aux_hash_to_metadata_key(AUX_DIR_PG_LOGICAL, 0x01, fname.as_bytes())
    } else if let Some(fname) = path.strip_prefix("pg_logical/snapshots/") {
        aux_hash_to_metadata_key(AUX_DIR_PG_LOGICAL, 0x02, fname.as_bytes())
    } else if path == "pg_logical/replorigin_checkpoint" {
        aux_hash_to_metadata_key(AUX_DIR_PG_LOGICAL, 0x03, b"")
    } else if let Some(fname) = path.strip_prefix("pg_logical/") {
        if cfg!(debug_assertions) {
            warn!(
                "unsupported pg_logical aux file type: {}, putting to 0x01FF, would affect path scanning",
                path
            );
        }
        aux_hash_to_metadata_key(AUX_DIR_PG_LOGICAL, 0xFF, fname.as_bytes())
    } else if let Some(fname) = path.strip_prefix("pg_replslot/") {
        aux_hash_to_metadata_key(AUX_DIR_PG_REPLSLOT, 0x01, fname.as_bytes())
    } else {
        if cfg!(debug_assertions) {
            warn!(
                "unsupported aux file type: {}, putting to 0xFFFF, would affect path scanning",
                path
            );
        }
        aux_hash_to_metadata_key(AUX_DIR_PG_UNKNOWN, 0xFF, path.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_portable() {
        // AUX file encoding requires the hash to be portable across all platforms. This test case checks
        // if the algorithm produces the same hash across different environments.
        assert_eq!(
            305317690835051308206966631765527126151,
            twox_hash::xxh3::hash128("test1".as_bytes())
        );
        assert_eq!(
            85104974691013376326742244813280798847,
            twox_hash::xxh3::hash128("test/test2".as_bytes())
        );
        assert_eq!(0, twox_hash::xxh3::hash128("".as_bytes()));
    }

    #[test]
    fn test_encoding_portable() {
        // To correct retrieve AUX files, the generated keys for the same file must be the same for all versions
        // of the page server.
        assert_eq!(
            "6200000101E5B20C5F8DD5AA3289D6D9EAFA",
            encode_aux_file_key("pg_logical/mappings/test1").to_string()
        );
        assert_eq!(
            "620000010239AAC544893139B26F501B97E6",
            encode_aux_file_key("pg_logical/snapshots/test2").to_string()
        );
        assert_eq!(
            "620000010300000000000000000000000000",
            encode_aux_file_key("pg_logical/replorigin_checkpoint").to_string()
        );
        assert_eq!(
            "62000001FF8635AF2134B7266EC5B4189FD6",
            encode_aux_file_key("pg_logical/unsupported").to_string()
        );
        assert_eq!(
            "6200000201772D0E5D71DE14DA86142A1619",
            encode_aux_file_key("pg_replslot/test3").to_string()
        );
        assert_eq!(
            "620000FFFF1866EBEB53B807B26A2416F317",
            encode_aux_file_key("other_file_not_supported").to_string()
        );
    }
}
