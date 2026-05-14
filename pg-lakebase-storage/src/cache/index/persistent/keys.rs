use crate::error::{StorageError, StorageResult};
use crate::object::ObjectLocation;

pub(super) fn parse_db_key(key: &str) -> StorageResult<ObjectLocation> {
    let (store_id, rest) = key
        .split_once('/')
        .ok_or_else(|| StorageError::cache(format!("invalid cache key {key:?}")))?;
    let (bucket, object_key) = rest
        .split_once('/')
        .ok_or_else(|| StorageError::cache(format!("invalid cache key {key:?}")))?;
    ObjectLocation::new(store_id, bucket, object_key)
}

/// Lexicographic sort by access time: fixed-width hex ns, then NUL so object keys cannot collide with the timestamp
/// field.
pub(super) fn lru_key(last_access_ns: u64, key: &ObjectLocation) -> String {
    format!("{last_access_ns:016x}\0{key}")
}

pub(super) fn lru_access_ns(key: &str) -> StorageResult<u64> {
    let Some((last_access_ns, _)) = key.split_once('\0') else {
        return Err(StorageError::cache(format!("invalid lru key {key:?}")));
    };
    u64::from_str_radix(last_access_ns, 16)
        .map_err(|error| StorageError::cache_source("invalid lru timestamp", error))
}
