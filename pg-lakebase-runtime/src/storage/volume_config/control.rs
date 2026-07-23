//! Storage-volume control-plane operations shared by SQL and DDL adapters.

use pg_lakebase_core::options::TablespaceBinding;
use pg_lakebase_core::storage_volume::StorageVolumeId;
use pgrx::pg_sys;
use serde_json::Value;

use super::credential::CredentialConfig;
use super::domain::{
    StorageLocation, StorageVolumeError, StorageVolumeName, StorageVolumeSnapshot,
};
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
    ) -> Result<bool, StorageVolumeError> {
        let (_, changed) = self.store.update(|snapshot| {
            let (_, changed) = snapshot.create(name.clone(), location, credential)?;
            Ok(((), changed))
        })?;
        self.notify_if_changed(changed, false);
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
        self.notify_if_changed(changed, false);
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
        self.notify_if_changed(changed, false);
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
            return Err(StorageVolumeError::Invariant(
                "storage volume name resolved to a different id while binding",
            ));
        }
        if let Some(oid) = volume.bound_tablespace_oid {
            return Err(StorageVolumeError::AlreadyBound(oid));
        }
        Ok(())
    }

    pub(crate) fn bind(
        &self,
        id: StorageVolumeId,
        tablespace_oid: pg_sys::Oid,
    ) -> Result<bool, StorageVolumeError> {
        let (_, changed) = self.store.update(|snapshot| {
            let changed = snapshot.bind(id, tablespace_oid.to_u32())?;
            Ok(((), changed))
        })?;
        self.notify_if_changed(changed, false);
        Ok(changed)
    }

    pub(crate) fn request_reload(force_default_chain: bool) {
        super::super::state::request_reload(force_default_chain);
    }

    fn notify_if_changed(&self, changed: bool, force_default_chain: bool) {
        if changed {
            Self::request_reload(force_default_chain);
        }
    }
}
