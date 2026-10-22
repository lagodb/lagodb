use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use lagodb_storage::StoreConfig;

use crate::catalog::{
    ForeignConfigInvalidation, register_foreign_config_invalidation_observer,
};
use crate::storage::service::BackendStorageService;

use super::identity::StorageIdentity;

thread_local! {
    static STORAGE_CACHE: RefCell<StorageCache> = RefCell::new(StorageCache::new());
}

/// One backend-local storage entry.
pub(crate) struct StorageCacheEntry {
    pub(crate) server_hashvalue: u32,
    pub(crate) identity: StorageIdentity,
    pub(crate) config: Arc<StoreConfig>,
    pub(crate) service: BackendStorageService,
}

pub(crate) struct StorageCache {
    entries: HashMap<StorageIdentity, Rc<StorageCacheEntry>>,
    observer_registered: bool,
}

impl StorageCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            observer_registered: false,
        }
    }

    pub(crate) fn with_current<T>(operation: impl FnOnce(&mut Self) -> T) -> T {
        STORAGE_CACHE.with(|cache| operation(&mut cache.borrow_mut()))
    }

    pub(crate) fn find_matching(
        &mut self,
        identity: &StorageIdentity,
    ) -> Option<Rc<StorageCacheEntry>> {
        let entry = self.entries.get(identity)?;
        Some(Rc::clone(entry))
    }

    pub(crate) fn insert(
        &mut self,
        entry: StorageCacheEntry,
    ) -> Rc<StorageCacheEntry> {
        let entry = Rc::new(entry);
        self.entries.insert(entry.identity, Rc::clone(&entry));
        entry
    }

    fn initialize_invalidation_observer(&mut self) {
        if self.observer_registered {
            return;
        }
        register_foreign_config_invalidation_observer(invalidate_storage_cache);
        self.observer_registered = true;
    }

    fn invalidate(&mut self, invalidation: ForeignConfigInvalidation) {
        // A new per-user mapping changes PUBLIC fallback and a previously
        // unmapped identity. Evict all parsed identities on mapping changes;
        // active handles retain their existing entry through Rc.
        let ForeignConfigInvalidation::Server { hashvalue } = invalidation else {
            self.entries.clear();
            return;
        };
        self.entries.retain(|_, entry| {
            let matches = hashvalue == 0 || entry.server_hashvalue == hashvalue;
            !matches
        });
    }
}

fn invalidate_storage_cache(invalidation: ForeignConfigInvalidation) {
    // This callback is deliberately limited to evicting backend-local state.
    // Active scan/modify handles retain their entry through `Rc`, so eviction
    // cannot change the storage context beneath an in-flight operation.
    // It must not read catalogs, perform protocol I/O, or mutate the storage
    // worker while PostgreSQL is dispatching cache invalidations.
    STORAGE_CACHE.with(|cache| {
        cache.borrow_mut().invalidate(invalidation);
    });
}

pub(crate) fn initialize_invalidation_observer() {
    StorageCache::with_current(|cache| cache.initialize_invalidation_observer());
}
