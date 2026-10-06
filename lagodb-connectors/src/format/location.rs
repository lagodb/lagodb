//! Format-specific path classification and file collection selection.

use std::num::NonZeroU64;

use crate::error::ConnectorError;
use crate::storage::{
    ObjectInput, ObjectLocationKind, ObjectOutput, ResolvedStorageLocation,
    StoragePath,
};

use super::FormatKind;

impl FormatKind {
    pub(crate) fn location_kind(
        self,
        path: &StoragePath,
    ) -> Result<ObjectLocationKind, ConnectorError> {
        let StoragePath::Object(object) = path else {
            return Ok(if path.key().ends_with('/') {
                ObjectLocationKind::Prefix
            } else {
                ObjectLocationKind::Exact
            });
        };
        let key = object.key();
        match Self::infer_from_key(key) {
            Some(found) if found != self => Err(ConnectorError::invalid_option(
                "path",
                "object suffix conflicts with the selected format",
            )),
            Some(_) if self.matches_object_key(key) && !key.ends_with('/') => {
                Ok(ObjectLocationKind::Exact)
            }
            Some(_) => Err(ConnectorError::invalid_option(
                "path",
                "stream compression suffixes are not valid for Parquet or Avro objects",
            )),
            _ => Ok(ObjectLocationKind::Prefix),
        }
    }

    pub(crate) fn input(
        self,
        location: &ResolvedStorageLocation,
    ) -> Result<ObjectInput, ConnectorError> {
        ObjectInput::resolve(location, self.location_kind(location.path())?, |key| {
            self.matches_object_key(key)
        })
    }

    pub(crate) fn output(
        self,
        location: &ResolvedStorageLocation,
        prefix_target_file_bytes: impl FnOnce() -> NonZeroU64,
    ) -> Result<ObjectOutput, ConnectorError> {
        ObjectOutput::resolve(
            location,
            self.location_kind(location.path())?,
            prefix_target_file_bytes,
        )
    }
}
