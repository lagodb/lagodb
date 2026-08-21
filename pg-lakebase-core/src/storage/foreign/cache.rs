use std::cell::RefCell;
use std::collections::HashMap;
use std::os::raw::c_int;
use std::rc::Rc;
use std::sync::Arc;

use pg_lakebase_storage::StoreConfig;
use pgrx::pg_sys;

use crate::storage::service::BackendStorageService;
use crate::wrapper::CacheRegisterSyscacheCallback;

use super::identity::StorageIdentity;

thread_local! {
    static STORAGE_CACHE: RefCell<StorageCache> = RefCell::new(StorageCache::new());
}

/// One backend-local storage entry.
pub(crate) struct StorageCacheEntry {
    pub(crate) umid: pg_sys::Oid,
    pub(crate) server_hashvalue: u32,
    pub(crate) mapping_hashvalue: u32,
    pub(crate) identity: StorageIdentity,
    pub(crate) config: Arc<StoreConfig>,
    pub(crate) service: BackendStorageService,
}

pub(crate) struct StorageCache {
    entries: HashMap<pg_sys::Oid, Rc<StorageCacheEntry>>,
    callbacks_registered: bool,
}

impl StorageCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            callbacks_registered: false,
        }
    }

    pub(crate) fn with_current<T>(operation: impl FnOnce(&mut Self) -> T) -> T {
        STORAGE_CACHE.with(|cache| operation(&mut cache.borrow_mut()))
    }

    pub(crate) fn find_matching(
        &mut self,
        umid: pg_sys::Oid,
        identity: &StorageIdentity,
    ) -> Option<Rc<StorageCacheEntry>> {
        let entry = self.entries.get(&umid)?;
        if entry.identity != *identity {
            return None;
        }
        Some(Rc::clone(entry))
    }

    pub(crate) fn insert(
        &mut self,
        entry: StorageCacheEntry,
    ) -> Rc<StorageCacheEntry> {
        let entry = Rc::new(entry);
        self.entries.insert(entry.umid, Rc::clone(&entry));
        entry
    }

    fn initialize_callbacks(&mut self) {
        if self.callbacks_registered {
            return;
        }
        // SAFETY: callbacks have PostgreSQL's syscache ABI and remain loaded
        // for the backend lifetime. The callback touches backend-local state only.
        unsafe {
            CacheRegisterSyscacheCallback(
                pg_sys::SysCacheIdentifier::FOREIGNSERVEROID as i32,
                Some(invalidate_storage_cache_callback),
                0.into(),
            );
            CacheRegisterSyscacheCallback(
                pg_sys::SysCacheIdentifier::USERMAPPINGOID as i32,
                Some(invalidate_storage_cache_callback),
                0.into(),
            );
        }
        self.callbacks_registered = true;
    }

    fn invalidate(&mut self, cache_id: i32, hashvalue: u32) {
        let server_cache_id = pg_sys::SysCacheIdentifier::FOREIGNSERVEROID as i32;
        let mapping_cache_id = pg_sys::SysCacheIdentifier::USERMAPPINGOID as i32;
        self.entries.retain(|_, entry| {
            let matches = hashvalue == 0
                || (cache_id == server_cache_id
                    && entry.server_hashvalue == hashvalue)
                || (cache_id == mapping_cache_id
                    && entry.mapping_hashvalue == hashvalue);
            !matches
        });
    }
}

unsafe extern "C" fn invalidate_storage_cache_callback(
    _arg: pg_sys::Datum,
    cache_id: c_int,
    hashvalue: u32,
) {
    // This callback is deliberately limited to evicting backend-local state.
    // Active scan/modify handles retain their entry through `Rc`, so eviction
    // cannot change the storage context beneath an in-flight operation.
    // It must not read catalogs, perform protocol I/O, or mutate the storage
    // worker while PostgreSQL is dispatching cache invalidations.
    STORAGE_CACHE.with(|cache| {
        cache.borrow_mut().invalidate(cache_id, hashvalue);
    });
}

pub(crate) fn initialize_callbacks() {
    StorageCache::with_current(|cache| cache.initialize_callbacks());
}
