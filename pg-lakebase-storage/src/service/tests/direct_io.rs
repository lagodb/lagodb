//! Tests for the `CompleteFile` direct-IO open path and its invalidation contract.

use std::sync::Arc;
use std::time::Duration;

use crate::backend::{MemoryObjectBackend, StoreRegistry};
use crate::cache::{CacheIndex, CacheManager, InMemoryCacheIndex};
use crate::cache::{CacheState, CachedObjectMeta};
use crate::config::{CacheRuntimeConfig, StorageRuntime, StorageRuntimeConfig};
use crate::error::StorageError;
use crate::handle::OpenFlags;
use crate::object::ObjectInfo;
use crate::service::StorageService;
use crate::service::command::{OpenCommand, StorageCommand};
use crate::service::reply::{CommandOutput, ResponseAttachment};
use crate::session::handle_table::HandleTable;

use super::fixtures::{
    BUCKET, DEFAULT_STORE, LARGE_KEY, close, default_location, invalidate_cmd,
    memory_cache, open_file, residency_hint, seed_complete_cache,
    seed_complete_cache_with_meta, test_cache_dir,
};

#[tokio::test]
async fn complete_file_open_uses_direct_io() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefgh".to_vec());
    let cache = memory_cache();
    seed_complete_cache(cache.as_ref(), &key, b"abcdefgh").await;
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache,
    );
    let handles = HandleTable::new();

    let reply = service
        .execute(
            &handles,
            StorageCommand::Open(OpenCommand {
                store_id: DEFAULT_STORE.to_string(),
                bucket: BUCKET.to_string(),
                key: LARGE_KEY.to_string(),
                flags: OpenFlags::READ_ONLY,
            }),
        )
        .await
        .unwrap();

    let CommandOutput::Open(output) = reply.output else {
        panic!("unexpected open output");
    };
    assert!(output.direct_io);
    assert!(matches!(
        reply.attachment,
        Some(ResponseAttachment::File(_))
    ));

    close(&service, &handles, output.handle).await;
}

#[tokio::test]
async fn complete_file_open_hit_touches_access_time_for_direct_io() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefgh".to_vec());
    let runtime_cfg = StorageRuntimeConfig {
        cache: CacheRuntimeConfig {
            touch_granularity: Duration::ZERO,
            ..CacheRuntimeConfig::default()
        },
    };
    let runtime = StorageRuntime::new(runtime_cfg).unwrap();
    let cache = Arc::new(
        CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new(), runtime)
            .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let mut meta = CachedObjectMeta::complete(
        key.clone(),
        ObjectInfo {
            size: 8,
            etag: None,
        },
    );
    meta.last_access_ns = 1;
    seed_complete_cache_with_meta(cache.as_ref(), &key, b"abcdefgh", meta).await;
    let service = StorageService::with_registry_config(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache.clone(),
        crate::config::StorageServiceConfig::default(),
    );
    let handles = HandleTable::new();

    let reply = service
        .execute(
            &handles,
            StorageCommand::Open(OpenCommand {
                store_id: DEFAULT_STORE.to_string(),
                bucket: BUCKET.to_string(),
                key: LARGE_KEY.to_string(),
                flags: OpenFlags::READ_ONLY,
            }),
        )
        .await
        .unwrap();

    let CommandOutput::Open(output) = reply.output else {
        panic!("unexpected open output");
    };
    assert!(output.direct_io);
    assert!(
        cache
            .index()
            .get_meta(&key)
            .await
            .unwrap()
            .unwrap()
            .last_access_ns
            > 1
    );

    close(&service, &handles, output.handle).await;
}

#[tokio::test]
async fn invalidate_complete_file_cache_is_busy_while_direct_handle_is_open() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefgh".to_vec());
    let cache = memory_cache();
    seed_complete_cache(cache.as_ref(), &key, b"abcdefgh").await;
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache.clone(),
    );
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let error = match service.execute(&handles, invalidate_cmd(LARGE_KEY)).await {
        Ok(_) => panic!("expected busy invalidate error"),
        Err(error) => error,
    };

    assert!(matches!(error, StorageError::Busy { .. }));
    assert!(
        tokio::fs::try_exists(cache.complete_path(&key).unwrap())
            .await
            .unwrap()
    );

    close(&service, &handles, open.handle).await;
    service
        .execute(&handles, invalidate_cmd(LARGE_KEY))
        .await
        .unwrap();
    assert!(
        !tokio::fs::try_exists(cache.complete_path(&key).unwrap())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn cache_hit_uses_cached_body_until_explicit_invalidate() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefgh".to_vec());
    let cache = memory_cache();
    seed_complete_cache(cache.as_ref(), &key, b"abcdefgh").await;
    backend.insert(key.clone(), b"abc".to_vec());
    let service = StorageService::with_registry(
        StoreRegistry::new()
            .with_shared_backend(DEFAULT_STORE, Arc::new(backend))
            .unwrap(),
        cache.clone(),
    );
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;

    assert!(open.direct_io);
    let state = handles.get(open.handle).unwrap();
    assert_eq!(state.size, 8);
    assert_eq!(
        residency_hint(&handles, open.handle),
        Some(crate::cache::ResidencyStateHint::CompleteFile)
    );
    assert_eq!(
        cache
            .index()
            .get_meta(&key)
            .await
            .unwrap()
            .unwrap()
            .cache_state(),
        CacheState::CompleteFile
    );

    close(&service, &handles, open.handle).await;
}
