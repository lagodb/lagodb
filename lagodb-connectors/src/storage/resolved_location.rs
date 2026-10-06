//! Storage operations on a location already authorized by the connector adapter.

use std::path::Path;

use lagodb_core::storage::foreign::{
    ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use lagodb_core::storage::profile::StorageProfileConfig;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::{InputFile, StoragePath};

pub(crate) struct ResolvedStorageLocation {
    server_oid: pg_sys::Oid,
    effective_user: pg_sys::Oid,
    object: StoragePath,
}

impl ResolvedStorageLocation {
    pub(crate) fn new(
        object: StoragePath,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Self {
        Self {
            object,
            server_oid,
            effective_user,
        }
    }

    pub(crate) fn path(&self) -> &StoragePath {
        &self.object
    }

    pub(crate) fn acquire_object_access(
        &self,
        manager: &StorageManager,
    ) -> Result<ObjectAccess, ConnectorError> {
        let StoragePath::Object(object) = &self.object else {
            unreachable!("object-store access is requested only for remote locations")
        };
        manager
            .acquire_object_access::<StorageProfileConfig>(
                self.server_oid,
                self.effective_user,
                object.bucket(),
                object.key(),
            )
            .map_err(ConnectorError::storage_acquire)
    }

    pub(crate) fn acquire_prefix_access(
        &self,
        manager: &StorageManager,
        prefix: &str,
    ) -> Result<ObjectPrefixAccess, ConnectorError> {
        let StoragePath::Object(object) = &self.object else {
            unreachable!("object-store access is requested only for remote locations")
        };
        manager
            .acquire_prefix_access::<StorageProfileConfig>(
                self.server_oid,
                self.effective_user,
                object.bucket(),
                prefix,
            )
            .map_err(ConnectorError::storage_acquire)
    }

    pub(crate) fn local_path(&self) -> Option<&str> {
        match &self.object {
            StoragePath::Local(path) => Some(path),
            StoragePath::Object(_) => None,
        }
    }

    pub(crate) fn open_file(&self) -> Result<InputFile, ConnectorError> {
        if let Some(path) = self.local_path() {
            return InputFile::local(Path::new(path)).map_err(ConnectorError::from);
        }
        self.acquire_object_access_from_pg_gucs()?
            .open()
            .map(|file| InputFile::object(file, self.object_key()))
            .map_err(ConnectorError::from)
    }

    pub(crate) fn object_key(&self) -> &str {
        self.object.key()
    }

    pub(crate) fn normalized_prefix(&self) -> String {
        let key = self.object.key();
        if key.ends_with('/') {
            key.to_owned()
        } else {
            format!("{key}/")
        }
    }

    pub(crate) fn acquire_object_access_from_pg_gucs(
        &self,
    ) -> Result<ObjectAccess, ConnectorError> {
        let manager = StorageManager::from_pg_gucs()?;
        self.acquire_object_access(&manager)
    }
}
