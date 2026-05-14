use super::RedbCacheIndex;
use super::test_support::{counting_redb_index, unique_redb_path};
use super::tracking::{RuntimeCacheTracking, TrackingDelta};
use crate::cache::index::{
    AdmitSmallOutcome, CacheIndex, LogicalCacheUsage, OpenHit,
};
use crate::cache::meta::{CacheState, CachedObjectMeta};
use crate::object::{ObjectInfo, ObjectLocation};

#[test]
fn runtime_tracking_applies_out_of_order_deltas() {
    let tracking = RuntimeCacheTracking::default();
    tracking.replace_total(0);

    tracking.apply_delta(TrackingDelta {
        old_bytes: 8,
        new_bytes: 2,
    });
    tracking.apply_delta(TrackingDelta {
        old_bytes: 0,
        new_bytes: 8,
    });

    assert_eq!(tracking.logical_usage().resident_bytes, 2);
}

#[tokio::test]
async fn admit_small_if_absent_installs_meta_and_payload() {
    let index = RedbCacheIndex::open(unique_redb_path("admit-small")).unwrap();
    let key = ObjectLocation::new("default", "bucket", "tiny").unwrap();
    let info = ObjectInfo {
        size: 3,
        etag: Some("v1".to_string()),
    };
    let mut meta = CachedObjectMeta::small(key.clone(), info, 3);
    meta.generation = 7;

    let outcome = index
        .admit_small_if_absent(meta, b"abc".to_vec(), 10)
        .await
        .unwrap();
    let AdmitSmallOutcome::Admitted { meta, payload } = outcome else {
        panic!("expected Admitted outcome on first admit");
    };

    assert_eq!(meta.cache_state(), CacheState::SmallKv);
    assert_eq!(meta.etag(), Some("v1"));
    assert_eq!(meta.generation, 7);
    assert_eq!(payload.as_ref(), b"abc");

    let racer = CachedObjectMeta::small(
        key.clone(),
        ObjectInfo {
            size: 3,
            etag: Some("v2".into()),
        },
        3,
    );
    let second = index
        .admit_small_if_absent(racer, b"xyz".to_vec(), 20)
        .await
        .unwrap();
    let AdmitSmallOutcome::AlreadyPresent {
        meta: existing,
        payload: existing_payload,
    } = second
    else {
        panic!("expected AlreadyPresent outcome on second admit");
    };
    assert_eq!(existing.etag(), Some("v1"));
    assert_eq!(existing_payload.as_ref(), b"abc");
}

#[tokio::test]
async fn tracks_complete_usage_and_oldest_cached_metadata() {
    let index = RedbCacheIndex::open(unique_redb_path("usage")).unwrap();
    let old_key = ObjectLocation::new("default", "bucket", "old").unwrap();
    let new_key = ObjectLocation::new("default", "bucket", "new").unwrap();

    let mut old_meta = CachedObjectMeta::small(
        old_key.clone(),
        ObjectInfo {
            size: 4,
            etag: None,
        },
        4,
    );
    old_meta.last_access_ns = 1;
    index
        .admit_small_if_absent(old_meta, b"old!".to_vec(), 1)
        .await
        .unwrap();

    let mut new_meta = CachedObjectMeta::complete(
        new_key.clone(),
        ObjectInfo {
            size: 8,
            etag: None,
        },
    );
    new_meta.last_access_ns = 2;
    index.put_new_complete(new_meta).await.unwrap();

    assert_eq!(
        index.logical_cache_usage().await.unwrap().resident_bytes,
        12
    );
    let oldest = index.oldest_cached_metas_page(None, 1).await.unwrap().metas;
    assert_eq!(oldest[0].key(), &old_key);
}

#[tokio::test]
async fn delete_meta_and_small_removes_payload_tracking() {
    let index = RedbCacheIndex::open(unique_redb_path("delete")).unwrap();
    let key = ObjectLocation::new("default", "bucket", "tiny").unwrap();
    let meta = CachedObjectMeta::small(
        key.clone(),
        ObjectInfo {
            size: 4,
            etag: None,
        },
        4,
    );
    index
        .admit_small_if_absent(meta, b"tiny".to_vec(), 1)
        .await
        .unwrap();

    let deleted = index.delete_meta_and_small(&key).await.unwrap().unwrap();

    assert_eq!(deleted.key(), &key);
    assert!(index.get_meta(&key).await.unwrap().is_none());
    assert!(index.get_small(&key).await.unwrap().is_none());
    assert_eq!(index.logical_cache_usage().await.unwrap().resident_bytes, 0);
    assert!(
        index
            .oldest_cached_metas_page(None, 1)
            .await
            .unwrap()
            .metas
            .is_empty()
    );
}

#[tokio::test]
async fn persisted_lru_survives_reopen_and_usage_is_installed_by_startup() {
    let path = unique_redb_path("lru-persist");
    let key = ObjectLocation::new("default", "bucket", "persisted").unwrap();

    {
        let index = RedbCacheIndex::open(&path).unwrap();
        let mut meta = CachedObjectMeta::complete(
            key.clone(),
            ObjectInfo {
                size: 8,
                etag: None,
            },
        );
        meta.last_access_ns = 1;
        index.put_new_complete(meta).await.unwrap();
        assert_eq!(index.logical_cache_usage().await.unwrap().resident_bytes, 8);
    }

    let index = RedbCacheIndex::open(&path).unwrap();

    assert_eq!(index.logical_cache_usage().await.unwrap().resident_bytes, 0);
    let oldest = index.oldest_cached_metas_page(None, 1).await.unwrap().metas;
    assert_eq!(oldest[0].key(), &key);

    index
        .replace_runtime_cache_usage(LogicalCacheUsage::resident(8))
        .await
        .unwrap();
    assert_eq!(index.logical_cache_usage().await.unwrap().resident_bytes, 8);
}

#[tokio::test]
async fn scans_and_removes_unclaimed_small_payloads() {
    let index = RedbCacheIndex::open(unique_redb_path("unclaimed")).unwrap();
    let key = ObjectLocation::new("default", "bucket", "orphan-small").unwrap();
    let meta = CachedObjectMeta::small(
        key.clone(),
        ObjectInfo {
            size: 3,
            etag: None,
        },
        3,
    );
    index
        .admit_small_if_absent(meta, b"abc".to_vec(), 1)
        .await
        .unwrap();
    index.delete_meta(&key).await.unwrap();

    let page = index.scan_small_entries_page(None, 10).await.unwrap();
    assert_eq!(page.entries[0].key, key);

    index.remove_unclaimed_small_payload(&key).await.unwrap();
    assert!(index.get_small(&key).await.unwrap().is_none());
}

/// Contract: `open_hit` for a `SmallKv` row inside the touch-granularity window uses exactly one
/// read transaction, one `meta_get`, one `small_get`, and zero writes.
#[tokio::test]
async fn open_hit_small_window_in_does_not_touch() {
    let (index, counts) = counting_redb_index(unique_redb_path("small-warm"));
    let key = ObjectLocation::new("default", "bucket", "tiny-warm").unwrap();
    let meta = CachedObjectMeta::small(
        key.clone(),
        ObjectInfo {
            size: 4,
            etag: None,
        },
        4,
    );
    index
        .admit_small_if_absent(meta, b"tiny".to_vec(), 1_000)
        .await
        .unwrap();
    counts.reset();

    let hit = index
        .open_hit(&key, 5_000, 60_000_000_000)
        .await
        .unwrap()
        .unwrap();
    let OpenHit { meta, payload } = hit;

    assert_eq!(meta.last_access_ns, 1_000);
    assert_eq!(payload.as_ref().unwrap().as_ref(), b"tiny");
    let snapshot = counts.snapshot();
    assert_eq!(snapshot.read_txns, 1);
    assert_eq!(snapshot.write_txns, 0);
    assert_eq!(snapshot.meta_get, 1);
    assert_eq!(snapshot.small_get, 1);
    assert_eq!(snapshot.meta_put, 0);
    assert_eq!(snapshot.lru_put, 0);
    assert_eq!(snapshot.lru_remove, 0);
}

/// Contract: `open_hit` crossing the touch window touches without re-reading meta — one read txn
/// upgrades into one write txn whose LRU update is derived from the observed meta.
#[tokio::test]
async fn open_hit_small_cross_window_touches_without_second_meta_get() {
    let (index, counts) = counting_redb_index(unique_redb_path("small-touch"));
    let key = ObjectLocation::new("default", "bucket", "tiny-touch").unwrap();
    let mut meta = CachedObjectMeta::small(
        key.clone(),
        ObjectInfo {
            size: 4,
            etag: None,
        },
        4,
    );
    meta.last_access_ns = 1;
    index
        .admit_small_if_absent(meta, b"tiny".to_vec(), 1)
        .await
        .unwrap();
    counts.reset();

    let hit = index.open_hit(&key, 10, 0).await.unwrap().unwrap();
    let OpenHit { meta, payload } = hit;

    assert_eq!(meta.last_access_ns, 10);
    assert_eq!(payload.as_ref().unwrap().as_ref(), b"tiny");
    let snapshot = counts.snapshot();
    assert_eq!(snapshot.read_txns, 1);
    assert_eq!(snapshot.write_txns, 1);
    assert_eq!(snapshot.meta_get, 1);
    assert_eq!(snapshot.small_get, 1);
    assert_eq!(snapshot.meta_put, 1);
    assert_eq!(snapshot.lru_remove, 1);
    assert_eq!(snapshot.lru_put, 1);
}

/// Contract: Complete-file hit inside the window is a single read transaction with one
/// `meta_get` and no small-object read.
#[tokio::test]
async fn open_hit_complete_window_in_uses_one_read_txn_and_one_meta_get() {
    let (index, counts) = counting_redb_index(unique_redb_path("complete-warm"));
    let key = ObjectLocation::new("default", "bucket", "complete-warm").unwrap();
    let mut meta = CachedObjectMeta::complete(
        key.clone(),
        ObjectInfo {
            size: 8,
            etag: None,
        },
    );
    meta.last_access_ns = 1_000;
    index.put_new_complete(meta).await.unwrap();
    counts.reset();

    let hit = index
        .open_hit(&key, 5_000, 60_000_000_000)
        .await
        .unwrap()
        .unwrap();
    let OpenHit { meta, payload } = hit;

    assert!(payload.is_none());
    assert_eq!(meta.last_access_ns, 1_000);
    let snapshot = counts.snapshot();
    assert_eq!(snapshot.read_txns, 1);
    assert_eq!(snapshot.write_txns, 0);
    assert_eq!(snapshot.meta_get, 1);
    assert_eq!(snapshot.small_get, 0);
    assert_eq!(snapshot.meta_put, 0);
}

#[tokio::test]
async fn open_hit_complete_cross_window_touches_without_second_meta_get() {
    let (index, counts) = counting_redb_index(unique_redb_path("complete-touch"));
    let key = ObjectLocation::new("default", "bucket", "complete-touch").unwrap();
    let mut meta = CachedObjectMeta::complete(
        key.clone(),
        ObjectInfo {
            size: 8,
            etag: None,
        },
    );
    meta.last_access_ns = 1;
    index.put_new_complete(meta).await.unwrap();
    counts.reset();

    let hit = index.open_hit(&key, 10, 0).await.unwrap().unwrap();
    let OpenHit { meta, payload } = hit;
    assert!(payload.is_none());
    assert_eq!(meta.last_access_ns, 10);
    let snapshot = counts.snapshot();
    assert_eq!(snapshot.read_txns, 1);
    assert_eq!(snapshot.write_txns, 1);
    assert_eq!(snapshot.meta_get, 1);
    assert_eq!(snapshot.small_get, 0);
    assert_eq!(snapshot.meta_put, 1);
    assert_eq!(snapshot.lru_remove, 1);
    assert_eq!(snapshot.lru_put, 1);
}

/// Contract: Cold small admit uses exactly one write transaction. That txn does the
/// insert-if-absent (`meta_get`), then writes small-payload, meta, and LRU once each.
#[tokio::test]
async fn admit_small_if_absent_uses_exactly_one_write_txn_on_insert() {
    let (index, counts) = counting_redb_index(unique_redb_path("admit-cold"));
    let key = ObjectLocation::new("default", "bucket", "admit-cold").unwrap();
    let meta = CachedObjectMeta::small(
        key,
        ObjectInfo {
            size: 3,
            etag: None,
        },
        3,
    );
    counts.reset();

    let outcome = index
        .admit_small_if_absent(meta, b"abc".to_vec(), 100)
        .await
        .unwrap();
    assert!(matches!(outcome, AdmitSmallOutcome::Admitted { .. }));

    let snapshot = counts.snapshot();
    assert_eq!(snapshot.read_txns, 0);
    assert_eq!(snapshot.write_txns, 1);
    assert_eq!(snapshot.meta_get, 1);
    assert_eq!(snapshot.meta_put, 1);
    assert_eq!(snapshot.small_put, 1);
    assert_eq!(snapshot.lru_put, 1);
    assert_eq!(snapshot.lru_remove, 0);
}

#[tokio::test]
async fn admit_small_if_absent_already_present_issues_one_write_txn_no_writes() {
    let (index, counts) = counting_redb_index(unique_redb_path("admit-race"));
    let key = ObjectLocation::new("default", "bucket", "admit-race").unwrap();
    let meta = CachedObjectMeta::small(
        key.clone(),
        ObjectInfo {
            size: 3,
            etag: None,
        },
        3,
    );
    index
        .admit_small_if_absent(meta, b"abc".to_vec(), 100)
        .await
        .unwrap();
    counts.reset();

    let racer = CachedObjectMeta::small(
        key,
        ObjectInfo {
            size: 3,
            etag: Some("ignored".into()),
        },
        3,
    );
    let outcome = index
        .admit_small_if_absent(racer, b"xyz".to_vec(), 200)
        .await
        .unwrap();
    assert!(matches!(outcome, AdmitSmallOutcome::AlreadyPresent { .. }));

    let snapshot = counts.snapshot();
    assert_eq!(snapshot.read_txns, 0);
    assert_eq!(snapshot.write_txns, 1);
    assert_eq!(snapshot.meta_get, 1);
    assert_eq!(snapshot.small_get, 1);
    assert_eq!(snapshot.meta_put, 0);
    assert_eq!(snapshot.small_put, 0);
    assert_eq!(snapshot.lru_put, 0);
}
