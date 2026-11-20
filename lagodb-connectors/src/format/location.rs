//! Format-specific path classification and file collection selection.

use std::num::NonZeroU64;

use crate::error::ConnectorError;
use crate::storage::{
    ObjectLocationKind, ResolvedStorageLocation, StorageInput, StorageOutput,
    StoragePath,
};

use super::FormatKind;

impl FormatKind {
    pub(crate) fn location_kind(
        self,
        path: &StoragePath,
    ) -> Result<ObjectLocationKind, ConnectorError> {
        match path {
            StoragePath::Local(path) => Ok(Self::local_location_kind(path)),
            StoragePath::Object(object) => self.object_location_kind(object.key()),
        }
    }

    pub(crate) fn resolved_location_kind(
        self,
        location: &ResolvedStorageLocation,
    ) -> Result<ObjectLocationKind, ConnectorError> {
        match location {
            ResolvedStorageLocation::Local(path) => {
                Ok(Self::local_location_kind(path))
            }
            ResolvedStorageLocation::Object(object) => {
                self.object_location_kind(object.key())
            }
        }
    }

    fn local_location_kind(path: &str) -> ObjectLocationKind {
        if path.ends_with('/') {
            ObjectLocationKind::Prefix
        } else {
            ObjectLocationKind::Exact
        }
    }

    fn object_location_kind(
        self,
        key: &str,
    ) -> Result<ObjectLocationKind, ConnectorError> {
        // Format resolution has already rejected stream-compression suffixes
        // on Parquet and Avro objects, for both FDW options and COPY.
        match Self::infer_from_key(key) {
            Some(found) if found != self => Err(ConnectorError::invalid_path(
                "object suffix conflicts with the selected format",
            )),
            Some(_) => Ok(ObjectLocationKind::Exact),
            None => Ok(ObjectLocationKind::Prefix),
        }
    }

    pub(crate) fn input(
        self,
        location: &ResolvedStorageLocation,
    ) -> Result<StorageInput, ConnectorError> {
        StorageInput::resolve(
            location,
            self.resolved_location_kind(location)?,
            |key| self.matches_object_key(key),
        )
    }

    pub(crate) fn output(
        self,
        location: &ResolvedStorageLocation,
        prefix_target_file_bytes: impl FnOnce() -> NonZeroU64,
    ) -> Result<StorageOutput, ConnectorError> {
        StorageOutput::resolve(
            location,
            self.resolved_location_kind(location)?,
            prefix_target_file_bytes,
        )
    }
}
