//! Stable identifiers for configured storage volumes.

use std::fmt;
use std::num::NonZeroU64;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

const MAX_VOLUME_ID: u64 = i64::MAX as u64;
const CROCKFORD_BASE32: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Cluster-unique, monotonic storage-volume identity.
///
/// IDs are positive PostgreSQL `bigint` values and are never reused. The
/// compact Crockford Base32 representation is used only in database-visible
/// paths; the storage protocol uses the numeric volume identity at attach.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageVolumeId(NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StorageVolumeIdError {
    #[error("storage volume id must be in 1..=i64::MAX")]
    OutOfRange,
}

impl StorageVolumeId {
    pub const MAX: u64 = MAX_VOLUME_ID;

    pub const fn new(value: u64) -> Result<Self, StorageVolumeIdError> {
        if value == 0 || value > MAX_VOLUME_ID {
            return Err(StorageVolumeIdError::OutOfRange);
        }
        // SAFETY: the range check above excludes zero.
        Ok(Self(unsafe { NonZeroU64::new_unchecked(value) }))
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub const fn as_i64(self) -> i64 {
        self.get() as i64
    }

    /// Encode this ID once at a control-plane/context boundary.
    ///
    /// The output is at most 13 lower-case ASCII characters and is used only
    /// for database-visible diagnostics and derived object prefixes.
    pub fn to_compact_string(self) -> String {
        let mut value = self.get();
        let mut encoded = [0_u8; 13];
        let mut start = encoded.len();
        loop {
            start -= 1;
            encoded[start] = CROCKFORD_BASE32[(value & 31) as usize];
            value >>= 5;
            if value == 0 {
                break;
            }
        }
        let value = std::str::from_utf8(&encoded[start..])
            .expect("Crockford alphabet is valid UTF-8");
        value.to_owned()
    }
}

impl TryFrom<u64> for StorageVolumeId {
    type Error = StorageVolumeIdError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<i64> for StorageVolumeId {
    type Error = StorageVolumeIdError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        u64::try_from(value)
            .map_err(|_| StorageVolumeIdError::OutOfRange)
            .and_then(Self::new)
    }
}

impl From<StorageVolumeId> for u64 {
    fn from(value: StorageVolumeId) -> Self {
        value.get()
    }
}

impl fmt::Debug for StorageVolumeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StorageVolumeId")
            .field(&self.get())
            .finish()
    }
}

impl fmt::Display for StorageVolumeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

impl Serialize for StorageVolumeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.get())
    }
}

impl<'de> Deserialize<'de> for StorageVolumeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_known_crockford_values() {
        assert_eq!(StorageVolumeId::new(1).unwrap().to_compact_string(), "1");
        assert_eq!(StorageVolumeId::new(31).unwrap().to_compact_string(), "z");
        assert_eq!(StorageVolumeId::new(32).unwrap().to_compact_string(), "10");
        assert!(
            StorageVolumeId::new(StorageVolumeId::MAX)
                .unwrap()
                .to_compact_string()
                .len()
                <= 13
        );
    }

    #[test]
    fn rejects_values_outside_postgres_bigint_range() {
        assert!(StorageVolumeId::new(0).is_err());
        assert!(StorageVolumeId::new(StorageVolumeId::MAX + 1).is_err());
    }
}
