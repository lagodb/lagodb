//! Storage operations on a location already authorized by the connector adapter.

use std::path::Path;

use lagodb_core::storage::foreign::{
    ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use lagodb_core::storage::profile::StorageProfileConfig;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::{InputFile, ObjectUri};

/// A local server path, or an object URI bound to the storage server and
/// user whose profile authorizes it.
pub(crate) enum ResolvedStorageLocation {
    Local(Box<str>),
    Object(ObjectLocation),
}

pub(crate) struct ObjectLocation {
    uri: ObjectUri,
    server_oid: pg_sys::Oid,
    effective_user: pg_sys::Oid,
}

impl ResolvedStorageLocation {
    pub(crate) fn open_file(&self) -> Result<InputFile, ConnectorError> {
        match self {
            Self::Local(path) => {
                InputFile::local(Path::new(&**path)).map_err(ConnectorError::from)
            }
            Self::Object(object) => object
                .acquire_object_access_from_pg_gucs()?
                .open()
                .map(|file| InputFile::object(file, object.key()))
                .map_err(ConnectorError::from),
        }
    }
}

impl ObjectLocation {
    pub(crate) fn new(
        uri: ObjectUri,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Self {
        Self {
            uri,
            server_oid,
            effective_user,
        }
    }

    pub(crate) fn key(&self) -> &str {
        self.uri.key()
    }

    pub(crate) fn normalized_prefix(&self) -> String {
        let key = self.uri.key();
        if key.ends_with('/') {
            key.to_owned()
        } else {
            format!("{key}/")
        }
    }

    pub(crate) fn acquire_object_access(
        &self,
        manager: &StorageManager,
    ) -> Result<ObjectAccess, ConnectorError> {
        manager
            .acquire_object_access::<StorageProfileConfig>(
                self.server_oid,
                self.effective_user,
                self.uri.bucket(),
                self.uri.key(),
            )
            .map_err(ConnectorError::storage_acquire)
    }

    pub(crate) fn acquire_prefix_access(
        &self,
        manager: &StorageManager,
        prefix: &str,
    ) -> Result<ObjectPrefixAccess, ConnectorError> {
        manager
            .acquire_prefix_access::<StorageProfileConfig>(
                self.server_oid,
                self.effective_user,
                self.uri.bucket(),
                prefix,
            )
            .map_err(ConnectorError::storage_acquire)
    }

    pub(crate) fn acquire_object_access_from_pg_gucs(
        &self,
    ) -> Result<ObjectAccess, ConnectorError> {
        let manager = StorageManager::from_pg_gucs()?;
        self.acquire_object_access(&manager)
    }
}
