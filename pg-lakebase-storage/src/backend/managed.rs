//! Runtime-owned managed volume slots used only during connection attach.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::{
    BackendDataIdentity, BackendPool, ConfiguredObjectBackend, ObjectBackend,
    StoreConfig,
};
use crate::error::{StorageError, StorageResult};

pub struct ManagedStoreSlot {
    identity: BackendDataIdentity,
    backend: RwLock<Arc<dyn ObjectBackend>>,
}

impl ManagedStoreSlot {
    pub fn new(
        identity: BackendDataIdentity,
        backend: Arc<dyn ObjectBackend>,
    ) -> Self {
        Self {
            identity,
            backend: RwLock::new(backend),
        }
    }

    pub fn identity(&self) -> &BackendDataIdentity {
        &self.identity
    }

    pub fn backend(&self) -> Arc<dyn ObjectBackend> {
        self.backend
            .read()
            .expect("managed store slot rwlock poisoned; backend state is no longer trustworthy")
            .clone()
    }

    fn replace(&self, backend: Arc<dyn ObjectBackend>) {
        *self
            .backend
            .write()
            .expect("managed store slot rwlock poisoned; backend state is no longer trustworthy") = backend;
    }
}

#[derive(Clone, Default)]
pub struct ManagedStoreRegistry {
    slots: Arc<RwLock<HashMap<u64, Arc<ManagedStoreSlot>>>>,
    pool: Arc<BackendPool>,
}

impl ManagedStoreRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn backend_pool(&self) -> &Arc<BackendPool> {
        &self.pool
    }

    pub fn resolve(&self, volume_id: u64) -> StorageResult<Arc<ManagedStoreSlot>> {
        self.slots
            .read()
            .expect("managed store registry rwlock poisoned; volume state is no longer trustworthy")
            .get(&volume_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found(format!("managed volume {volume_id}")))
    }

    pub fn replace_config(
        &self,
        volume_id: u64,
        config: StoreConfig,
    ) -> StorageResult<()> {
        let identity = BackendDataIdentity::from_config(&config);
        let backend = self.pool.intern(Arc::new(config))?;
        self.publish(volume_id, identity, backend)
    }

    /// Rebuilds the configured backend even when the explicit config is unchanged.
    ///
    /// This is required for provider default credential chains whose effective
    /// credential changes outside the persisted volume configuration.
    pub fn refresh_config(
        &self,
        volume_id: u64,
        config: StoreConfig,
    ) -> StorageResult<()> {
        let identity = BackendDataIdentity::from_config(&config);
        let backend = self.pool.materialize_fresh(Arc::new(config))?;
        self.publish(volume_id, identity, backend)
    }

    fn publish(
        &self,
        volume_id: u64,
        identity: BackendDataIdentity,
        backend: Arc<ConfiguredObjectBackend>,
    ) -> StorageResult<()> {
        let backend: Arc<dyn ObjectBackend> = backend;
        let mut slots = self
            .slots
            .write()
            .expect("managed store registry rwlock poisoned; volume state is no longer trustworthy");
        if let Some(slot) = slots.get(&volume_id) {
            if slot.identity() != &identity {
                return Err(StorageError::conflict(format!(
                    "managed volume {volume_id} physical identity changed"
                )));
            }
            slot.replace(backend);
            return Ok(());
        }
        slots.insert(
            volume_id,
            Arc::new(ManagedStoreSlot::new(identity, backend)),
        );
        Ok(())
    }

    pub fn register_backend(
        &self,
        volume_id: u64,
        identity: BackendDataIdentity,
        backend: Arc<dyn ObjectBackend>,
    ) -> StorageResult<()> {
        let mut slots = self
            .slots
            .write()
            .expect("managed store registry rwlock poisoned; volume state is no longer trustworthy");
        if let Some(slot) = slots.get(&volume_id) {
            if slot.identity() != &identity {
                return Err(StorageError::conflict(format!(
                    "managed volume {volume_id} physical identity changed"
                )));
            }
            slot.replace(backend);
        } else {
            slots.insert(
                volume_id,
                Arc::new(ManagedStoreSlot::new(identity, backend)),
            );
        }
        Ok(())
    }

    pub fn remove(&self, volume_id: u64) -> bool {
        self.slots
            .write()
            .expect("managed store registry rwlock poisoned; volume state is no longer trustworthy")
            .remove(&volume_id)
            .is_some()
    }
}

#[cfg(test)]
impl ManagedStoreRegistry {
    pub(crate) fn with_shared_backend<B: ObjectBackend + 'static>(
        self,
        name: &str,
        backend: Arc<B>,
    ) -> StorageResult<Self> {
        self.register_backend(1, BackendDataIdentity::memory(name), backend)?;
        Ok(self)
    }
}
