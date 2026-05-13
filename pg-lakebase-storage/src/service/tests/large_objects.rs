//! Tests for the large-fill lease lifecycle (partial files, aborts, promotion).

use std::sync::Arc;
use std::time::Duration;

use crate::backend::{MemoryObjectBackend, StoreRegistry};
use crate::cache::CacheState;
use crate::cache::{CacheCleanupPolicy, CacheIndex};
use crate::error::{StorageError, StorageErrorKind};
use crate::handle::OpenFlags;
use crate::object::ObjectInfo;
use crate::service::command::{CloseCommand, OpenCommand, ReadCommand, StorageCommand};
use crate::service::StorageService;
use crate::session::handle_table::HandleTable;

use super::fixtures::{
    close, default_location, invalidate_cmd, memory_cache, open_file, read, wait_until, wait_until_async,
    write_cache_file, BlockingRangeBackend, CountingCompleteIndex, BUCKET, DEFAULT_STORE, LARGE_KEY,
};

#[tokio::test]
async fn large_partial_read_does_not_persist_metadata() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let read = read(&service, &handles, open.handle, 3, 5).await;

    assert_eq!(read.data, b"defgh");
    assert!(!read.eof);
    assert!(cache.index().get_meta(&key).await.unwrap().is_none());
    let partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert!(tokio::fs::try_exists(partial).await.unwrap());

    close(&service, &handles, open.handle).await;
}

#[tokio::test]
async fn invalidate_large_fill_is_busy_while_handle_is_open() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend)).unwrap(), cache);
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let error = match service.execute(&handles, invalidate_cmd(LARGE_KEY)).await {
        Ok(_) => panic!("expected busy invalidate error"),
        Err(error) => error,
    };

    assert!(matches!(error, StorageError::Busy { .. }));
    close(&service, &handles, open.handle).await;
}

/// Regression guard for the session/reaper identity contract: if `invalidate_object_cache` races
/// ahead of a stale `ReapRequest` and admits a brand-new session for the same key, the stale
/// request must **not** delete the new partial or vacate the new registry entry.
///
/// Sequence modeled here:
/// 1. OPEN + first chunk read materialize a partial for session S1.
/// 2. The handle closes. S1's last `Arc` drops and enqueues a `ReapRequest` carrying S1's nonce.
///    Because the reaper and the foreground OPEN below both serialize on the same per-object
///    lock, we control whether the reaper runs first or after.
/// 3. A fresh OPEN admits session S2 (different nonce, same partial path). S2 reads and writes
///    the partial again.
/// 4. The assertion: while S2 is alive, the partial stays on disk and the registry still
///    resolves to S2. Either the reaper already processed the stale request (entry_matches ==
///    false, short-circuit) or it will process it after S2 drops (nonce does not match, still a
///    short-circuit). Neither outcome should touch S2's state.
#[tokio::test]
async fn stale_reap_request_does_not_clobber_newer_session() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let first = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    // Partially fill so a partial file exists and a reap would have something to delete.
    assert_eq!(read(&service, &handles, first.handle, 0, 4).await.data, b"abcd");
    let partial_path = cache.live_large_fill_partial_path(&key).unwrap();
    assert!(tokio::fs::try_exists(&partial_path).await.unwrap());

    close(&service, &handles, first.handle).await;

    // Before (or concurrently with) the reaper processing S1's request, admit S2 for the same
    // key. Whether the reaper runs before or after this OPEN, the stale nonce must prevent
    // cross-generation interference.
    let second = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    assert_eq!(read(&service, &handles, second.handle, 0, 4).await.data, b"abcd");

    // Let any pending reaper work drain on the object lock.
    let new_partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert_eq!(partial_path, new_partial, "partial path is deterministic per key");
    assert!(tokio::fs::try_exists(&new_partial).await.unwrap(), "S2 partial must survive stale reap");
    assert!(cache.has_live_large_fill(&key), "registry must still resolve to S2");

    close(&service, &handles, second.handle).await;
}

#[tokio::test]
async fn last_large_fill_close_deletes_incomplete_partial() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let read = read(&service, &handles, open.handle, 0, 4).await;
    assert_eq!(read.data, b"abcd");
    let partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert!(tokio::fs::try_exists(&partial).await.unwrap());

    close(&service, &handles, open.handle).await;
    // Large-fill cleanup runs on the cache manager's reaper task; close itself only releases the
    // per-handle Arc. Wait for the reaper to finalize — it will clear the registry entry and
    // unlink the partial.
    wait_until("reaper clears live large fill registry entry", || !cache.has_live_large_fill(&key)).await;
    wait_until_async("reaper unlinks partial payload", || async { !tokio::fs::try_exists(&partial).await.unwrap() })
        .await;

    let report = cache.cleanup(CacheCleanupPolicy::new(1024)).await.unwrap();

    assert_eq!(report.orphan_partial_files_deleted, 0);
}

#[tokio::test]
async fn aborted_large_fill_rejects_new_open_until_last_close_finalizes() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend.clone())).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let old_open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    assert_eq!(read(&service, &handles, old_open.handle, 0, 4).await.data, b"abcd");
    let old_partial = cache.live_large_fill_partial_path(&key).unwrap();
    let heads_after_old_open = backend.head_call_count();
    assert!(heads_after_old_open >= 1);

    cache.abort_live_large_fill_for_test(&key).await.unwrap();
    assert!(cache.has_live_large_fill(&key));

    let error = match service
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
    {
        Ok(_) => panic!("aborted large fill should reject a new open"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StorageErrorKind::CacheFillAborted);
    // An aborted lifecycle must short-circuit inside the cache: no HEAD, no backend I/O.
    assert_eq!(
        backend.head_call_count(),
        heads_after_old_open,
        "rejecting an aborted lifecycle must not issue a backend HEAD",
    );

    close(&service, &handles, old_open.handle).await;
    // Partial unlink and registry vacancy are driven by the reaper task.
    wait_until_async("reaper unlinks aborted partial", || async {
        !tokio::fs::try_exists(&old_partial).await.unwrap()
    })
    .await;
    wait_until("reaper clears aborted session entry", || !cache.has_live_large_fill(&key)).await;

    let new_open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let new_partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert_eq!(old_partial, new_partial);
    assert_eq!(read(&service, &handles, new_open.handle, 0, 4).await.data, b"abcd");
    assert!(tokio::fs::try_exists(&new_partial).await.unwrap());
    close(&service, &handles, new_open.handle).await;
}

#[tokio::test]
async fn stale_partial_from_failed_unlink_is_truncated_on_next_fill() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();

    // Plant a stale partial twice the object size with a recognizable byte pattern. If the new
    // session failed to truncate on first write, chunk 0 would leave trailing stale bytes that
    // surface in later reads and break commit equality.
    let partial = cache.partial_path(&key).unwrap();
    write_cache_file(partial.clone(), &[0xCCu8; 20]).await;
    assert!(!cache.has_live_large_fill(&key));

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let new_partial = cache.live_large_fill_partial_path(&key).unwrap();
    assert_eq!(partial, new_partial);

    // First read triggers the first chunk write: the partial must be opened with O_TRUNC so no
    // stale 0xCC bytes remain beyond the written range.
    let first_read = read(&service, &handles, open.handle, 0, 4).await;
    assert_eq!(first_read.data, b"abcd");
    let after_first_write = tokio::fs::read(&partial).await.unwrap();
    assert_eq!(after_first_write.len(), 4, "stale bytes past the first chunk must be truncated");
    assert_eq!(after_first_write, b"abcd");

    // Finish the fill to confirm promotion still yields the backend's bytes, not a mix with the
    // stale payload.
    let tail = read(&service, &handles, open.handle, 4, 6).await;
    assert_eq!(tail.data, b"efghij");
    assert!(tail.eof);
    let complete = tokio::fs::read(cache.complete_path(&key).unwrap()).await.unwrap();
    assert_eq!(complete, b"abcdefghij");

    close(&service, &handles, open.handle).await;
}

#[tokio::test]
async fn abort_large_fill_wakes_waiting_chunk_followers() {
    let key = default_location(LARGE_KEY);
    let inner_backend = MemoryObjectBackend::new();
    inner_backend.insert(key.clone(), b"abcdefgh".to_vec());
    let backend = Arc::new(BlockingRangeBackend::new(inner_backend));
    let cache = memory_cache();
    let service = Arc::new(StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, backend.clone()).unwrap(), cache.clone()));
    let handles = Arc::new(HandleTable::new());

    let leader_open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let follower_open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let leader_handle = leader_open.handle;
    let follower_handle = follower_open.handle;

    let leader_service = service.clone();
    let leader_handles = handles.clone();
    let leader_read = tokio::spawn(async move {
        leader_service
            .execute(
                &leader_handles,
                StorageCommand::Read(ReadCommand {
                    handle: leader_handle,
                    offset: 0,
                    len: 4,
                }),
            )
            .await
    });
    backend.wait_until_first_range_get_starts().await;

    let follower_service = service.clone();
    let follower_handles = handles.clone();
    let follower_read = tokio::spawn(async move {
        follower_service
            .execute(
                &follower_handles,
                StorageCommand::Read(ReadCommand {
                    handle: follower_handle,
                    offset: 0,
                    len: 4,
                }),
            )
            .await
    });
    tokio::task::yield_now().await;

    cache.abort_live_large_fill_for_test(&key).await.unwrap();

    let follower = tokio::time::timeout(Duration::from_secs(1), follower_read)
        .await
        .expect("follower read should wake after abort")
        .expect("follower read task should not panic");
    assert!(matches!(follower, Err(StorageError::CacheFillAborted { .. })));

    backend.release_first_range_get();
    let leader = tokio::time::timeout(Duration::from_secs(1), leader_read)
        .await
        .expect("leader read should finish after backend unblocks")
        .expect("leader read task should not panic");
    assert!(matches!(leader, Err(StorageError::CacheFillAborted { .. })));

    service
        .execute(&handles, StorageCommand::Close(CloseCommand { handle: leader_handle }))
        .await
        .unwrap();
    service
        .execute(
            &handles,
            StorageCommand::Close(CloseCommand {
                handle: follower_handle,
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn large_open_joins_live_fill_without_second_head() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend.clone())).unwrap(), cache);
    let handles = HandleTable::new();

    let first = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let second = open_file(&service, &handles, BUCKET, LARGE_KEY).await;

    let first_state = handles.get(first.handle).unwrap();
    let second_state = handles.get(second.handle).unwrap();
    assert_eq!(backend.head_call_count(), 1);
    let first_session = first_state
        .residency
        .as_ref()
        .and_then(|r| r.large_fill_session())
        .expect("large fill");
    let second_session = second_state
        .residency
        .as_ref()
        .and_then(|r| r.large_fill_session())
        .expect("large fill");
    assert!(Arc::ptr_eq(&first_session, &second_session));

    close(&service, &handles, first.handle).await;
    close(&service, &handles, second.handle).await;
}

#[tokio::test]
async fn read_rejects_large_handle_without_bound_fill_session() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();
    let store = service.registry().resolve(key.store_id()).unwrap();
    let state = handles
        .open(key.clone(), store, ObjectInfo { size: 10, etag: None }, OpenFlags::READ_ONLY)
        .unwrap();

    let error = match service
        .execute(
            &handles,
            StorageCommand::Read(ReadCommand {
                handle: state.handle,
                offset: 0,
                len: 4,
            }),
        )
        .await
    {
        Ok(_) => panic!("expected cache error for large handle without fill session"),
        Err(error) => error,
    };

    assert!(matches!(error, StorageError::Cache { .. }));
    assert!(!cache.has_live_large_fill(&key));

    close(&service, &handles, state.handle).await;
}

#[tokio::test]
async fn live_large_fill_is_removed_after_last_open_close() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let first = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let second = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    assert!(cache.has_live_large_fill(&key));

    close(&service, &handles, first.handle).await;
    assert!(cache.has_live_large_fill(&key));

    close(&service, &handles, second.handle).await;
    assert!(!cache.has_live_large_fill(&key));
}

#[tokio::test]
async fn large_full_read_commits_complete_metadata() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let partial = cache.live_large_fill_partial_path(&key).unwrap();
    let read = read(&service, &handles, open.handle, 0, 10).await;

    assert_eq!(read.data, b"abcdefghij");
    assert!(read.eof);
    let meta = cache.index().get_meta(&key).await.unwrap().unwrap();
    assert_eq!(meta.cache_state(), CacheState::CompleteFile);
    assert!(tokio::fs::try_exists(cache.complete_path(&key).unwrap()).await.unwrap());
    assert!(!tokio::fs::try_exists(partial).await.unwrap());

    close(&service, &handles, open.handle).await;
}

#[tokio::test]
async fn large_full_read_replaces_unclaimed_complete_payload() {
    let key = default_location(LARGE_KEY);
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = memory_cache();
    let complete_path = cache.complete_path(&key).unwrap();
    write_cache_file(complete_path.clone(), b"stale").await;
    let service = StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, Arc::new(backend)).unwrap(), cache.clone());
    let handles = HandleTable::new();

    let open = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let read = read(&service, &handles, open.handle, 0, 10).await;

    assert_eq!(read.data, b"abcdefghij");
    assert!(read.eof);
    assert_eq!(tokio::fs::read(complete_path).await.unwrap(), b"abcdefghij");
    assert_eq!(cache.index().get_meta(&key).await.unwrap().unwrap().cache_state(), CacheState::CompleteFile);

    close(&service, &handles, open.handle).await;
}

#[tokio::test]
async fn concurrent_large_reads_commit_complete_metadata_once() {
    use crate::cache::{CacheManager, InMemoryCacheIndex};

    let key = default_location(LARGE_KEY);
    let inner_backend = MemoryObjectBackend::new();
    inner_backend.insert(key.clone(), b"abcdefgh".to_vec());
    let backend = Arc::new(BlockingRangeBackend::new(inner_backend));
    let index = CountingCompleteIndex::new(InMemoryCacheIndex::new());
    let cache = Arc::new(CacheManager::new(super::fixtures::test_cache_dir(), index).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let service = Arc::new(StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, backend.clone()).unwrap(), cache.clone()));
    let handles = Arc::new(HandleTable::new());

    let first = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let second = open_file(&service, &handles, BUCKET, LARGE_KEY).await;
    let partial = cache.live_large_fill_partial_path(&key).unwrap();

    let first_service = service.clone();
    let first_handles = handles.clone();
    let first_read = tokio::spawn(async move { read(&first_service, &first_handles, first.handle, 0, 8).await });
    backend.wait_until_first_range_get_starts().await;

    let second_service = service.clone();
    let second_handles = handles.clone();
    let second_read = tokio::spawn(async move { read(&second_service, &second_handles, second.handle, 0, 8).await });
    tokio::task::yield_now().await;
    backend.release_first_range_get();

    let first = first_read.await.unwrap();
    let second = second_read.await.unwrap();

    assert_eq!(first.data, b"abcdefgh");
    assert_eq!(second.data, b"abcdefgh");
    assert!(first.eof);
    assert!(second.eof);
    assert_eq!(cache.index().complete_puts(), 1);
    assert_eq!(backend.range_gets(), 2);
    assert_eq!(cache.index().get_meta(&key).await.unwrap().unwrap().cache_state(), CacheState::CompleteFile);
    assert!(tokio::fs::try_exists(cache.complete_path(&key).unwrap()).await.unwrap());
    assert!(!tokio::fs::try_exists(partial).await.unwrap());
}
