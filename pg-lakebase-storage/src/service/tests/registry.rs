//! Store-registry routing tests for [`StorageService`].

use std::sync::Arc;

use crate::backend::{
    MemoryObjectBackend, S3StoreConfig, StoreConfig, StoreRegistry,
};
use crate::cache::{CacheManager, InMemoryCacheIndex};
use crate::config::{StorageRuntime, StorageRuntimeConfig, StorageServiceConfig};
use crate::error::StorageErrorKind;
use crate::object::{ObjectLocation, StoreId};
use crate::service::StorageService;
use crate::service::command::{
    RegisterStoreCommand, StorageCommand, UnregisterStoreCommand,
};
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
        CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
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

#[tokio::test]
async fn externally_managed_registry_rejects_register_and_unregister() {
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend("preexisting", Arc::new(MemoryObjectBackend::new()))
        .unwrap();

    let cache = Arc::new(
        CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(32, 4),
    );
    cache.spawn_large_fill_reaper();

    let config = StorageServiceConfig::default().with_externally_managed_registry();
    let service =
        StorageService::with_registry_config(registry.clone(), cache, config);
    let handles = HandleTable::new();

    let register_result = service
        .execute(
            &handles,
            StorageCommand::RegisterStore(RegisterStoreCommand {
                store_id: "wire_register".to_string(),
                config: StoreConfig::S3(S3StoreConfig::default()),
            }),
        )
        .await;
    let register_err = register_result.err().expect("RegisterStore should fail");
    assert_eq!(register_err.kind(), StorageErrorKind::Unsupported);
    assert!(
        !registry.contains(&StoreId::new("wire_register").unwrap()),
        "RegisterStore must not have mutated the registry"
    );

    let unregister_result = service
        .execute(
            &handles,
            StorageCommand::UnregisterStore(UnregisterStoreCommand {
                store_id: "preexisting".to_string(),
            }),
        )
        .await;
    let unregister_err = unregister_result
        .err()
        .expect("UnregisterStore should fail");
    assert_eq!(unregister_err.kind(), StorageErrorKind::Unsupported);
    assert!(
        registry.contains(&StoreId::new("preexisting").unwrap()),
        "UnregisterStore must not have mutated the registry"
    );
}
