//! Storage-volume control-plane operations shared by SQL and DDL adapters.

use pg_lakebase_core::options::TablespaceBinding;
use pg_lakebase_core::storage::volume::StorageVolumeId;
use pgrx::pg_sys;
use serde_json::Value;

use super::super::state;
use super::credential::CredentialConfig;
use super::domain::{
    StorageLocation, StorageVolumeError, StorageVolumeName, StorageVolumeSnapshot,
};
use super::lifecycle::UnixMillis;
use super::store::StorageVolumeConfigStore;

/// Concrete application service for machine-managed Volume mutations.
///
/// SQL and ProcessUtility hooks remain adapters: this type owns the shared
/// read-modify-write and reload-notification orchestration without introducing
/// a repository trait or a second persistence abstraction.
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
        expires_after_seconds: Option<i64>,
    ) -> Result<bool, StorageVolumeError> {
        let (_, changed) = self.update(|snapshot| {
            let (_, changed) = snapshot.create(
                name.clone(),
                location,
                credential,
                expires_after_seconds,
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
        let (_, changed) = self.update(|snapshot| {
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
        let (_, changed) = self.update(|snapshot| {
            let credential =
                CredentialConfig::parse(credential, &snapshot.find(name)?.location)?;
            let changed = snapshot.update_credential(name, credential)?;
            Ok(((), changed))
        })?;
        Ok(changed)
    }

    pub(crate) fn remove(
        &self,
        name: &StorageVolumeName,
    ) -> Result<bool, StorageVolumeError> {
        let (_, changed) = self.update(|snapshot| {
            let changed = snapshot.remove(name)?;
            Ok(((), changed))
        })?;
        Ok(changed)
    }

    pub(crate) fn resolve_binding(
        &self,
        name: &str,
    ) -> Result<TablespaceBinding, StorageVolumeError> {
        let name = StorageVolumeName::new(name)?;
        Ok(self.store.read()?.find(&name)?.tablespace_binding())
    }

    pub(crate) fn ensure_unbound_name(
        &self,
        name: &str,
        id: StorageVolumeId,
    ) -> Result<(), StorageVolumeError> {
        let name = StorageVolumeName::new(name)?;
        let snapshot = self.store.read()?;
        let volume = snapshot.find(&name)?;
        if volume.id != id {
            return Err(StorageVolumeError::BindingConflict {
                expected_id: id,
                actual_id: volume.id,
            });
        }
        if let Some(oid) = volume.lifecycle.bound_tablespace_oid() {
            return Err(StorageVolumeError::AlreadyBound(oid));
        }
        if volume.lifecycle.is_retiring() {
            return Err(StorageVolumeError::LifecycleOperation {
                operation: "bound",
            });
        }
        let now = UnixMillis::now()?;
        if volume.lifecycle.is_expired_at(now) {
            return Err(StorageVolumeError::Expired);
        }
        Ok(())
    }

    pub(crate) fn bind(
        &self,
        id: StorageVolumeId,
        tablespace_oid: pg_sys::Oid,
    ) -> Result<bool, StorageVolumeError> {
        let (_, changed) = self.update(|snapshot| {
            let now = UnixMillis::now()?;
            let changed = snapshot.bind(id, tablespace_oid.to_u32(), now)?;
            Ok(((), changed))
        })?;
        Ok(changed)
    }

    pub(crate) fn find_bound_tablespace(
        &self,
        tablespace_oid: pg_sys::Oid,
    ) -> Result<Option<StorageVolumeId>, StorageVolumeError> {
        Ok(self
            .store
            .read()?
            .find_bound_by_tablespace_oid(tablespace_oid.to_u32()))
    }

    pub(crate) fn repair(
        &self,
        name: &StorageVolumeName,
        expected_id: StorageVolumeId,
        expected_tablespace_oid: u32,
        marked_at_ms: UnixMillis,
        retirement_grace_ms: u64,
    ) -> Result<bool, StorageVolumeError> {
        let (_, changed) = self.update(|snapshot| {
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

    fn update<T>(
        &self,
        mutation: impl FnOnce(
            &mut StorageVolumeSnapshot,
        ) -> Result<(T, bool), StorageVolumeError>,
    ) -> Result<(T, bool), StorageVolumeError> {
        match self.store.update(mutation) {
            Ok((value, changed)) => {
                if changed {
                    Self::request_reload(false);
                }
                Ok((value, changed))
            }
            Err(error) => {
                if error.was_published() {
                    Self::request_reload(false);
                }
                Err(error)
            }
        }
    }
}
