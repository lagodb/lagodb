//! Machine-managed storage-volume config as the registry's desired source.

use super::reconciler::{StoreConfigSource, VolumeStoreSpec};
use super::volume_config::{StorageVolumeConfigStore, StorageVolumeError};

pub(super) struct VolumeConfigSource {
    store: StorageVolumeConfigStore,
}

impl VolumeConfigSource {
    pub(super) const fn new(store: StorageVolumeConfigStore) -> Self {
        Self { store }
    }
}

impl StoreConfigSource for VolumeConfigSource {
    fn load(&mut self) -> Result<Vec<VolumeStoreSpec>, StorageVolumeError> {
        let snapshot = self.store.read()?;
        Ok(snapshot
            .volumes
            .into_values()
            .map(|volume| VolumeStoreSpec {
                volume_id: volume.id.get(),
                reload_on_force: volume.credential.uses_default_chain(),
                location: volume.location,
                credential: volume.credential,
            })
            .collect())
    }
}
