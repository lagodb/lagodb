//! Weak interning pool for configured object-storage backends.

use std::sync::{Arc, Mutex, Weak};

use super::{ConfiguredObjectBackend, StoreConfig};
use crate::error::StorageResult;

/// Shares live configured backends without extending their lifetime.
#[derive(Default)]
pub struct BackendPool {
    entries: Mutex<Vec<Weak<ConfiguredObjectBackend>>>,
}

impl BackendPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn intern(
        &self,
        config: Arc<StoreConfig>,
    ) -> StorageResult<Arc<ConfiguredObjectBackend>> {
        config.validate()?;
        let mut entries = self
            .entries
            .lock()
            .expect("backend pool mutex poisoned; configured backend state is no longer trustworthy");
        let mut matching = None;
        entries.retain(|entry| {
            let Some(backend) = entry.upgrade() else {
                return false;
            };
            if matching.is_none() && backend.config() == config.as_ref() {
                matching = Some(backend);
            }
            true
        });
        if let Some(backend) = matching {
            return Ok(backend);
        }
        let backend = Arc::new(ConfiguredObjectBackend::new(config));
        entries.push(Arc::downgrade(&backend));
        Ok(backend)
    }

    /// Materializes a new backend even when an equal live configuration is pooled.
    ///
    /// Managed default-chain credential refresh uses this path because the effective
    /// credential can change without changing the serialized [`StoreConfig`].
    pub fn materialize_fresh(
        &self,
        config: Arc<StoreConfig>,
    ) -> StorageResult<Arc<ConfiguredObjectBackend>> {
        config.validate()?;
        let mut entries = self
            .entries
            .lock()
            .expect("backend pool mutex poisoned; configured backend state is no longer trustworthy");
        entries.retain(|entry| {
            entry
                .upgrade()
                .is_some_and(|backend| backend.config() != config.as_ref())
        });
        let backend = Arc::new(ConfiguredObjectBackend::new(config));
        entries.push(Arc::downgrade(&backend));
        Ok(backend)
    }
}
