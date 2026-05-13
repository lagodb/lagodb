use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use crate::object::{ObjectInfo, ObjectLocation};

static TEST_CACHE_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn maps_read_to_chunks() {
    let cache = CacheManager::new(PathBuf::from("/tmp/cache"), InMemoryCacheIndex::new()).with_limits(4, 4);
    assert_eq!(cache.chunks_for_read(0, 1, 10), vec![0]);
    assert_eq!(cache.chunks_for_read(3, 2, 10), vec![0, 1]);
    assert_eq!(cache.chunks_for_read(8, 8, 10), vec![2]);
    assert_eq!(cache.chunks_for_read(u64::MAX - 2, 4, u64::MAX), vec![u64::MAX / 4]);
}

#[test]
fn manager_normalizes_zero_chunk_size() {
    let cache = CacheManager::new(PathBuf::from("/tmp/cache"), InMemoryCacheIndex::new()).with_limits(4, 0);

    assert_eq!(cache.chunk_size(), 1);
    assert_eq!(cache.chunks_for_read(0, 3, 3), vec![0, 1, 2]);
}

#[tokio::test]
async fn admit_small_uses_small_meta_and_payload_transaction() {
    let key = ObjectLocation::new("default", "bucket", "tiny").unwrap();
    let cache = CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new()).with_limits(8, 2);
    let info = ObjectInfo {
        size: 3,
        etag: Some("v1".to_string()),
    };

    let leader = match cache.lookup_for_open(&key).await.unwrap() {
        crate::cache::OpenOutcome::Establish(leader) => leader,
        _ => panic!("expected establish"),
    };
    let residency = cache.admit_small(&leader, b"abc".to_vec(), info).await.unwrap();
    leader.succeed();

    let crate::cache::ResidencyBody::Small { meta, payload } = &residency.body else {
        panic!("expected small residency");
    };
    assert_eq!(meta.cache_state(), CacheState::SmallKv);
    assert_eq!(meta.cached_bytes(), 3);
    assert_eq!(payload.as_ref(), b"abc");
    assert_eq!(cache.index().get_meta(&key).await.unwrap().unwrap().cache_state(), CacheState::SmallKv);
    assert_eq!(cache.index().get_small(&key).await.unwrap(), Some(b"abc".to_vec()));
}

#[tokio::test]
async fn recovery_deletes_partial_payload_without_metadata() {
    let key = ObjectLocation::new("default", "bucket", "large-missing").unwrap();
    let cache = CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new()).with_limits(4, 4);
    cache.prepare_dirs().await.unwrap();
    let partial_path = cache.partial_path(&key).unwrap();
    write_cache_file(partial_path.clone(), b"abcdefgh").await;

    let report = cache.recover().await.unwrap();

    assert_eq!(report.orphan_partial_files, 1);
    assert_eq!(report.logical_usage_after.resident_bytes, 0);
    assert!(cache.index().get_meta(&key).await.unwrap().is_none());
    assert!(!tokio::fs::try_exists(partial_path).await.unwrap());
}

#[tokio::test]
async fn recovery_does_not_repair_missing_complete_payload() {
    let key = ObjectLocation::new("default", "bucket", "large").unwrap();
    let cache = CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new()).with_limits(4, 4);
    let meta = CachedObjectMeta::complete(key.clone(), ObjectInfo { size: 8, etag: None });
    cache.index().put_new_complete(meta).await.unwrap();

    let report = cache.recover().await.unwrap();

    assert_eq!(report.objects_seen, 1);
    assert_eq!(report.logical_usage_after.resident_bytes, 8);
    assert_eq!(cache.index().get_meta(&key).await.unwrap().unwrap().cache_state(), CacheState::CompleteFile);
}

#[tokio::test]
async fn cleanup_deletes_unclaimed_complete_and_partial_candidates() {
    let live_key = ObjectLocation::new("default", "bucket", "live").unwrap();
    let complete_orphan = ObjectLocation::new("default", "bucket", "complete-orphan").unwrap();
    let partial_orphan = ObjectLocation::new("default", "bucket", "partial-orphan").unwrap();
    let cache = CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new()).with_limits(4, 4);
    cache.prepare_dirs().await.unwrap();
    let live_meta = CachedObjectMeta::complete(live_key.clone(), ObjectInfo { size: 4, etag: None });
    cache.index().put_new_complete(live_meta).await.unwrap();
    write_cache_file(cache.complete_path(&live_key).unwrap(), b"live").await;
    let complete_path = cache.complete_path(&complete_orphan).unwrap();
    let partial_path = cache.partial_path(&partial_orphan).unwrap();
    write_cache_file(complete_path.clone(), b"orphan").await;
    write_cache_file(partial_path.clone(), b"orphan").await;
    cache.orphan_candidates.record_file_candidate(complete_path.clone());
    cache.orphan_candidates.record_file_candidate(partial_path.clone());

    let report = cache.cleanup(CacheCleanupPolicy::new(u64::MAX)).await.unwrap();

    assert_eq!(report.orphan_complete_files_deleted, 1);
    assert_eq!(report.orphan_partial_files_deleted, 1);
    assert!(tokio::fs::try_exists(cache.complete_path(&live_key).unwrap()).await.unwrap());
    assert!(!tokio::fs::try_exists(complete_path).await.unwrap());
    assert!(!tokio::fs::try_exists(partial_path).await.unwrap());
}

fn test_cache_dir() -> PathBuf {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let id = TEST_CACHE_ID.fetch_add(1, Ordering::Relaxed);
    PathBuf::from("/tmp").join(format!("pg-lakebase-storage-cache-test-{}-{stamp}-{id}", std::process::id()))
}

async fn write_cache_file(path: PathBuf, data: &[u8]) {
    create_parent_dir(&path).await.unwrap();
    tokio::fs::write(path, data).await.unwrap();
}

#[tokio::test]
async fn best_effort_invalidate_reports_not_present_when_no_cache_entry() {
    let key = ObjectLocation::new("default", "bucket", "ghost").unwrap();
    let cache = CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new()).with_limits(4, 4);

    let outcome = cache.invalidate_object_cache_best_effort(&key).await;

    assert_eq!(outcome, BestEffortInvalidateOutcome::NotPresent);
}

#[tokio::test]
async fn best_effort_invalidate_removes_cached_small_entry() {
    let key = ObjectLocation::new("default", "bucket", "tiny-best-effort").unwrap();
    let cache = CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new()).with_limits(8, 2);
    let info = ObjectInfo {
        size: 3,
        etag: Some("v1".to_string()),
    };

    let leader = match cache.lookup_for_open(&key).await.unwrap() {
        crate::cache::OpenOutcome::Establish(leader) => leader,
        _ => panic!("expected establish"),
    };
    let residency = cache.admit_small(&leader, b"abc".to_vec(), info).await.unwrap();
    leader.succeed();
    drop(residency); // release the activity lease so invalidate can proceed.

    let outcome = cache.invalidate_object_cache_best_effort(&key).await;

    assert!(matches!(outcome, BestEffortInvalidateOutcome::Removed { bytes } if bytes == 3));
    assert!(cache.index().get_meta(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn best_effort_invalidate_skips_when_object_is_active() {
    let key = ObjectLocation::new("default", "bucket", "busy").unwrap();
    let cache = CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new()).with_limits(8, 2);
    let info = ObjectInfo {
        size: 3,
        etag: None,
    };

    let leader = match cache.lookup_for_open(&key).await.unwrap() {
        crate::cache::OpenOutcome::Establish(leader) => leader,
        _ => panic!("expected establish"),
    };
    // Holding the residency keeps an `OpenLease` activity guard alive, so `is_active(key)` is
    // true and best-effort invalidation must skip rather than tear the entry out from under the
    // (simulated) live reader.
    let _residency = cache.admit_small(&leader, b"abc".to_vec(), info).await.unwrap();
    leader.succeed();

    let outcome = cache.invalidate_object_cache_best_effort(&key).await;

    assert_eq!(outcome, BestEffortInvalidateOutcome::Skipped);
    assert!(
        cache.index().get_meta(&key).await.unwrap().is_some(),
        "active key must keep its cache row; janitor will clean it once activity drains"
    );
}
