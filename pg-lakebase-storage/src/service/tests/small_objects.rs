//! Tests for the SmallKV open/read/invalidate path.

use crate::backend::{MemoryObjectBackend, StoreRegistry};
use crate::cache::CacheIndex;
use crate::cache::CachedObjectMeta;
use crate::error::StorageError;
use crate::handle::OpenFlags;
use crate::object::{ObjectInfo, ObjectLocation};
use crate::service::command::{OpenCommand, ReadCommand, StorageCommand};
use crate::service::reply::CommandOutput;
use crate::service::StorageService;
use crate::session::handle_table::HandleTable;

use super::fixtures::{
    close, default_location, invalidate_cmd, memory_cache, open_file, read, residency_hint, BUCKET, DEFAULT_STORE,
    SMALL_KEY,
};

#[tokio::test]
async fn open_small_object_populates_small_kv() {
    let key = default_location(SMALL_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abc".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, std::sync::Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, SMALL_KEY).await;

    assert!(!open.direct_io);
    assert_eq!(residency_hint(&handles, open.handle), Some(crate::cache::ResidencyStateHint::SmallKv));
    assert_eq!(cache.index().get_small(&key).await.unwrap(), Some(b"abc".to_vec()));

    close(&service, &handles, open.handle).await;
}

#[tokio::test]
async fn small_cache_hit_open_does_not_head_backend() {
    let key = default_location(SMALL_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"abc".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, std::sync::Arc::new(backend.clone())).unwrap(), cache);
    let handles = HandleTable::new();

    let first = open_file(&service, &handles, BUCKET, SMALL_KEY).await;
    assert_eq!(backend.head_call_count(), 1);
    close(&service, &handles, first.handle).await;

    let second = open_file(&service, &handles, BUCKET, SMALL_KEY).await;

    assert!(!second.direct_io);
    assert_eq!(residency_hint(&handles, second.handle), Some(crate::cache::ResidencyStateHint::SmallKv));
    assert_eq!(backend.head_call_count(), 1);

    close(&service, &handles, second.handle).await;
}

/// Defense-in-depth at the KV layer: `admit_small_if_absent` on the index must deduplicate
/// concurrent writes and return the winning meta/payload to the race loser.
///
/// In production the per-object establishment single-flight (see [`crate::cache::establish`])
/// guarantees at most one leader per cache lifecycle reaches this path, so the race loser
/// branch is unreachable on the normal OPEN flow. The branch is retained as belt-and-suspenders
/// against future changes that weaken the single-flight, and this test exercises it by going
/// straight to the index.
#[tokio::test]
async fn small_admit_if_absent_returns_existing_meta_on_race_loser() {
    let key = default_location(SMALL_KEY);
    let cache = memory_cache();
    let old_info = ObjectInfo {
        size: 3,
        etag: Some("old".to_string()),
    };
    let new_info = ObjectInfo {
        size: 3,
        etag: Some("new".to_string()),
    };

    let first = cache
        .index()
        .admit_small_if_absent(CachedObjectMeta::small(key.clone(), old_info, 3), b"old".to_vec(), 0)
        .await
        .unwrap();
    let second = cache
        .index()
        .admit_small_if_absent(CachedObjectMeta::small(key.clone(), new_info, 3), b"new".to_vec(), 0)
        .await
        .unwrap();

    let crate::cache::AdmitSmallOutcome::Admitted { meta: m1, .. } = first else {
        panic!("expected first admit to win the insert-if-absent race");
    };
    let crate::cache::AdmitSmallOutcome::AlreadyPresent { meta: m2, .. } = second else {
        panic!("expected second admit to observe AlreadyPresent");
    };
    assert_eq!(m1, m2, "race loser must see the winner's meta verbatim");
    assert_eq!(cache.index().get_small(&key).await.unwrap(), Some(b"old".to_vec()));
}

#[tokio::test]
async fn invalidate_small_object_cache_is_busy_while_handle_is_open() {
    let key = default_location(SMALL_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abc".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, std::sync::Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, SMALL_KEY).await;
    let error = match service.execute(&handles, invalidate_cmd(SMALL_KEY)).await {
        Ok(_) => panic!("expected busy invalidate error"),
        Err(error) => error,
    };

    assert!(matches!(error, StorageError::Busy { .. }));
    assert_eq!(cache.index().get_small(&key).await.unwrap(), Some(b"abc".to_vec()));

    close(&service, &handles, open.handle).await;
    let reply = service.execute(&handles, invalidate_cmd(SMALL_KEY)).await.unwrap();
    let CommandOutput::InvalidateObjectCache(output) = reply.output else {
        panic!("unexpected invalidate output");
    };
    assert!(output.removed);
    assert!(cache.index().get_meta(&key).await.unwrap().is_none());
    assert_eq!(cache.index().get_small(&key).await.unwrap(), None);
}

/// Under the three cache-design invariants, the residency observed by an open handle is frozen
/// for its lifetime. A durable delete-and-reinsert race against a live handle is a cache
/// invariant violation on the writer's side, not a case the reader must reconcile: the in-
/// memory `Residency` keeps serving the bytes it saw at OPEN. This test simply documents that
/// behavior.
#[tokio::test]
async fn small_read_keeps_residency_bytes_after_durable_rewrite() {
    let key = default_location(SMALL_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abc".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, std::sync::Arc::new(backend.clone())).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, SMALL_KEY).await;
    assert_eq!(handles.get(open.handle).unwrap().size, 3);

    cache.index().delete_meta_and_small(&key).await.unwrap();
    cache
        .index()
        .admit_small_if_absent(
            CachedObjectMeta::small(key.clone(), ObjectInfo { size: 4, etag: None }, 4),
            b"abcd".to_vec(),
            0,
        )
        .await
        .unwrap();

    let reply = read(&service, &handles, open.handle, 0, 3).await;
    assert_eq!(reply.data, b"abc");

    close(&service, &handles, open.handle).await;
}

/// Explicit invalidation is allowed once the handle is closed; it can run from another async
/// task as long as ordering is coordinated (here: close before unblocking the spawned
/// invalidate).
#[tokio::test]
async fn invalidate_object_cache_from_task_after_close_removes_small_kv() {
    use tokio::sync::oneshot;

    let key = default_location(SMALL_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abc".to_vec());
    let cache = memory_cache();
    let service = std::sync::Arc::new(StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, std::sync::Arc::new(backend)).unwrap(), cache.clone()));
    let handles = std::sync::Arc::new(HandleTable::new());

    let open = open_file(&service, &handles, BUCKET, SMALL_KEY).await;
    let (tx, rx) = oneshot::channel::<()>();
    let service_task = service.clone();
    let handles_task = handles.clone();
    let invalidator = tokio::spawn(async move {
        rx.await.expect("close signal");
        let reply = service_task.execute(&handles_task, invalidate_cmd(SMALL_KEY)).await.unwrap();
        let CommandOutput::InvalidateObjectCache(output) = reply.output else {
            panic!("unexpected invalidate output");
        };
        assert!(output.removed);
    });

    close(&service, &handles, open.handle).await;
    tx.send(()).expect("invalidator should wait on close");

    invalidator.await.expect("invalidator join");
    assert!(cache.index().get_meta(&key).await.unwrap().is_none());
    assert_eq!(cache.index().get_small(&key).await.unwrap(), None);
}

#[tokio::test]
async fn explicit_invalidate_allows_next_open_to_refresh_small_object() {
    let key = default_location(SMALL_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abc".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, std::sync::Arc::new(backend.clone())).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, SMALL_KEY).await;
    close(&service, &handles, open.handle).await;
    backend.insert(key.clone(), b"new".to_vec());

    service.execute(&handles, invalidate_cmd(SMALL_KEY)).await.unwrap();
    let refreshed = open_file(&service, &handles, BUCKET, SMALL_KEY).await;
    let refreshed_read = read(&service, &handles, refreshed.handle, 0, 3).await;

    assert_eq!(refreshed_read.data, b"new");
    assert_eq!(cache.index().get_small(&key).await.unwrap(), Some(b"new".to_vec()));

    close(&service, &handles, refreshed.handle).await;
}

#[tokio::test]
async fn open_rejects_unrepresentable_cache_paths_before_small_kv_cache() {
    let object_name = "x".repeat(300);
    let key = ObjectLocation::new(DEFAULT_STORE, BUCKET, object_name.clone()).unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"abc".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, std::sync::Arc::new(backend)).unwrap(), cache);
    let handles = HandleTable::new();

    let error = service
        .execute(
            &handles,
            StorageCommand::Open(OpenCommand {
                store_id: DEFAULT_STORE.to_string(),
                bucket: BUCKET.to_string(),
                key: object_name,
                flags: OpenFlags::READ_ONLY,
            }),
        )
        .await
        .err()
        .unwrap();

    assert!(matches!(error, StorageError::InvalidPath { .. }));
    assert!(error.wire_message().contains("maximum component length"));
}

/// Regression: a handle opened directly via HandleTable::open without binding a Residency must
/// reject READ instead of hitting undefined behavior. Production `handle_open` always attaches a
/// residency; this guards the test-only direct open entry point.
#[tokio::test]
async fn read_rejects_handle_without_bound_residency() {
    let key = default_location(SMALL_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abc".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, std::sync::Arc::new(backend)).unwrap(), cache);
    let handles = HandleTable::new();
    let store = service.registry().resolve(key.store_id()).unwrap();
    let state = handles
        .open(key.clone(), store, ObjectInfo { size: 3, etag: None }, OpenFlags::READ_ONLY)
        .unwrap();

    let error = match service
        .execute(
            &handles,
            StorageCommand::Read(ReadCommand {
                handle: state.handle,
                offset: 0,
                len: 3,
            }),
        )
        .await
    {
        Ok(_) => panic!("expected cache error"),
        Err(error) => error,
    };
    assert!(matches!(error, StorageError::Cache { .. }));
    close(&service, &handles, state.handle).await;
}
