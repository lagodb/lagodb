//! Store-registry routing tests for [`StorageService`].

use std::sync::Arc;

use crate::backend::{MemoryObjectBackend, StoreRegistry};
use crate::cache::{CacheManager, InMemoryCacheIndex};
use crate::object::ObjectLocation;
use crate::service::StorageService;
use crate::session::handle_table::HandleTable;

use super::fixtures::{BUCKET, close, open_named_file, read, test_cache_dir};

#[tokio::test]
async fn register_store_routes_open_to_named_backend() {
    let default_backend = Arc::new(MemoryObjectBackend::new());
    let named_backend = Arc::new(MemoryObjectBackend::new());
    let key = ObjectLocation::new("named", BUCKET, "file").unwrap();
    named_backend.insert(key.clone(), b"named-data".to_vec());
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend("default", default_backend)
        .unwrap();
    registry
        .register_shared_backend("named", named_backend)
        .unwrap();
    let cache = Arc::new(
        CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new())
            .with_limits(32, 4),
    );
    cache.spawn_large_fill_reaper();
    let service = StorageService::with_registry(registry, cache);
    let handles = HandleTable::new();

    let open = open_named_file(&service, &handles, "named", BUCKET, "file").await;
    let read = read(&service, &handles, open.handle, 0, 10).await;

    assert_eq!(read.data, b"named-data");
    assert!(read.eof);

    close(&service, &handles, open.handle).await;
}
