use std::fmt;

use serde::{Deserialize, Serialize};

use super::error::StorageVolumeError;

const MAX_NAME_BYTES: usize = 63;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct StorageVolumeName(String);

impl StorageVolumeName {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, StorageVolumeError> {
        let value = value.into();
        Self::validate_value(&value)?;
        Ok(Self(value))
    }

    fn validate_value(value: &str) -> Result<(), StorageVolumeError> {
        if value.is_empty()
            || value.len() > MAX_NAME_BYTES
            || value.as_bytes().contains(&0)
        {
            return Err(StorageVolumeError::InvalidName);
        }
        Ok(())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for StorageVolumeName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl fmt::Debug for StorageVolumeName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
