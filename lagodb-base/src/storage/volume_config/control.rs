//! Storage-volume control-plane operations shared by SQL and DDL adapters.

use std::io::ErrorKind;

use lagodb_core::storage::volume::StorageVolumeId;
use pgrx::pg_sys;
use serde_json::Value;

use super::super::state;
use super::binding::TablespaceBindingAdmission;
use super::credential::CredentialConfig;
use super::domain::{
    StorageLocation, StorageVolumeConfig, StorageVolumeError, StorageVolumeName,
    StorageVolumeSnapshot,
};
use super::lifecycle::UnixMillis;
use super::store::StorageVolumeConfigStore;

/// Application service for machine-managed volume mutations.
///
/// Coordinates SQL read-modify-write operations. The store invalidates binding
/// caches and notifies the worker immediately after publishing a configuration.
pub(crate) struct StorageVolumeControl {
    store: StorageVolumeConfigStore,
}

impl StorageVolumeControl {
    pub(crate) fn current() -> Self {
        Self {
            store: StorageVolumeConfigStore::for_current_data_directory(),
        }
    }

    pub(crate) fn snapshot(
        &self,
    ) -> Result<StorageVolumeSnapshot, StorageVolumeError> {
        self.store.read()
    }

    pub(crate) fn create(
        &self,
        name: &StorageVolumeName,
        location: StorageLocation,
        credential: CredentialConfig,
        tablespace_name: &str,
    ) -> Result<bool, StorageVolumeError> {
        let tablespace = TablespaceBindingAdmission::resolve(tablespace_name)?;
        let (_, changed) = self.store.update(|snapshot| {
            // Existing identical creates remain idempotent even after tables are added.
            if !snapshot.volumes.contains_key(name) {
                tablespace.ensure_empty()?;
            }
            let (_, changed) = snapshot.create(
                name.clone(),
                location,
                credential,
                tablespace.oid().to_u32(),
            )?;
            Ok(((), changed))
        })?;
        Ok(changed)
    }

    pub(crate) fn rename(
        &self,
        old: &StorageVolumeName,
        new: StorageVolumeName,
    ) -> Result<bool, StorageVolumeError> {
        let (_, changed) = self.store.update(|snapshot| {
            let changed = snapshot.rename(old, new)?;
            Ok(((), changed))
        })?;
        Ok(changed)
    }

    pub(crate) fn update_credential(
        &self,
        name: &StorageVolumeName,
        credential: Value,
    ) -> Result<bool, StorageVolumeError> {
        let (_, changed) = self.store.update(|snapshot| {
            let credential =
                CredentialConfig::parse(credential, &snapshot.find(name)?.location)?;
            let changed = snapshot.update_credential(name, credential)?;
            Ok(((), changed))
        })?;
        Ok(changed)
    }

    /// Read the configuration for binding lookups, without caching it.
    ///
    /// PostgreSQL accepts connections without waiting for the storage worker to
    /// create the initial config. Ordinary user tablespaces also use this lookup,
    /// so NotFound means no binding: local operations must not depend on worker
    /// bootstrap completing. An existing config with no matching binding likewise
    /// selects local storage. Config publication invalidates cached local results.
    ///
    /// After initialization, preservation of the config and its identities is a
    /// program invariant. External file loss requires operator restoration and
    /// is outside this lookup's recovery contract. This lookup never creates or
    /// repairs configuration; other read errors propagate, and mutations and
    /// worker reloads continue to use strict snapshot reads.
    pub(crate) fn binding_snapshot(
        &self,
    ) -> Result<Option<StorageVolumeSnapshot>, StorageVolumeError> {
        match self.store.read() {
            Ok(snapshot) => Ok(Some(snapshot)),
            Err(StorageVolumeError::ConfigIo { source, .. })
                if source.kind() == ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Retirement admission reads current configuration, never the query cache.
    pub(crate) fn find_bound_tablespace(
        &self,
        tablespace_oid: pg_sys::Oid,
    ) -> Result<Option<StorageVolumeConfig>, StorageVolumeError> {
        let Some(snapshot) = self.binding_snapshot()? else {
            return Ok(None);
        };
        Ok(snapshot.volumes.into_values().find(|volume| {
            volume.lifecycle.bound_tablespace_oid() == Some(tablespace_oid.to_u32())
        }))
    }

    pub(crate) fn repair(
        &self,
        name: &StorageVolumeName,
        expected_id: StorageVolumeId,
        expected_tablespace_oid: u32,
        marked_at_ms: UnixMillis,
        retirement_grace_ms: u64,
    ) -> Result<bool, StorageVolumeError> {
        let (_, changed) = self.store.update(|snapshot| {
            let volume = snapshot.find(name)?;
            if volume.id != expected_id
                || volume.lifecycle.bound_tablespace_oid()
                    != Some(expected_tablespace_oid)
            {
                return Err(StorageVolumeError::Invariant(
                    "storage volume changed while retirement repair was pending",
                ));
            }
            let changed = snapshot.repair(name, marked_at_ms, retirement_grace_ms)?;
            Ok(((), changed))
        })?;
        Ok(changed)
    }

    pub(crate) fn request_reload(force_default_chain: bool) {
        state::request_reload(force_default_chain);
    }
}
