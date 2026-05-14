//! Staging / commit / delete E2E tests.

use pg_lakebase_storage::SeekFrom;

use crate::harness::{CacheIndexKind, E2eHarness, STORE_ID, TEST_BUCKET};

#[tokio::test]
async fn stage_commit_then_read_back() {
    stage_commit_then_read_back_on(CacheIndexKind::InMemory).await;
}

#[tokio::test]
async fn redb_stage_commit_then_read_back() {
    stage_commit_then_read_back_on(CacheIndexKind::Redb).await;
}

async fn stage_commit_then_read_back_on(kind: CacheIndexKind) {
    let h = E2eHarness::start_with_index(kind).await;
    let payload = b"staged data written by the integration test client";

    tokio::task::spawn_blocking(move || {
        let client = h.connect();

        let mut staging = client
            .stage(STORE_ID, TEST_BUCKET, "staged/object.bin")
            .unwrap();
        staging.write(payload).unwrap();
        staging.sync().unwrap();
        drop(staging);

        let info = client
            .commit(STORE_ID, TEST_BUCKET, "staged/object.bin")
            .unwrap();
        assert_eq!(info.size, payload.len() as u64);

        client
            .invalidate_object_cache(STORE_ID, TEST_BUCKET, "staged/object.bin")
            .unwrap();

        let mut file = client
            .open(STORE_ID, TEST_BUCKET, "staged/object.bin")
            .unwrap();
        assert_eq!(file.size(), payload.len() as u64);
        assert_eq!(file.read(payload.len() as u32).unwrap(), payload.as_ref());
        file.close().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stage_abort_does_not_upload() {
    let h = E2eHarness::start().await;

    tokio::task::spawn_blocking(move || {
        let client = h.connect();

        let mut staging = client
            .stage(STORE_ID, TEST_BUCKET, "aborted/file.txt")
            .unwrap();
        staging
            .write(b"these bytes should never reach MinIO")
            .unwrap();
        drop(staging);

        client
            .abort(STORE_ID, TEST_BUCKET, "aborted/file.txt")
            .unwrap();

        let result = client.open(STORE_ID, TEST_BUCKET, "aborted/file.txt");
        assert!(result.is_err(), "expected open to fail after abort");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn commit_overwrites_and_readback_sees_new_data() {
    commit_overwrites_and_readback_sees_new_data_on(CacheIndexKind::InMemory).await;
}

#[tokio::test]
async fn redb_commit_overwrites_and_readback_sees_new_data() {
    commit_overwrites_and_readback_sees_new_data_on(CacheIndexKind::Redb).await;
}

async fn commit_overwrites_and_readback_sees_new_data_on(kind: CacheIndexKind) {
    let h = E2eHarness::start_with_index(kind).await;
    let original = b"original content";
    h.seed_object("overwrite.txt", original).await;

    let replacement = b"replacement content written via staging";

    tokio::task::spawn_blocking(move || {
        let client = h.connect();

        let mut f = client.open(STORE_ID, TEST_BUCKET, "overwrite.txt").unwrap();
        assert_eq!(f.read(original.len() as u32).unwrap(), original.as_ref());
        f.close().unwrap();

        let mut staging = client
            .stage(STORE_ID, TEST_BUCKET, "overwrite.txt")
            .unwrap();
        staging.write(replacement).unwrap();
        drop(staging);

        let info = client
            .commit(STORE_ID, TEST_BUCKET, "overwrite.txt")
            .unwrap();
        assert_eq!(info.size, replacement.len() as u64);

        client
            .invalidate_object_cache(STORE_ID, TEST_BUCKET, "overwrite.txt")
            .unwrap();

        let mut f = client.open(STORE_ID, TEST_BUCKET, "overwrite.txt").unwrap();
        assert_eq!(f.size(), replacement.len() as u64);
        f.seek(SeekFrom::Start(0));
        assert_eq!(
            f.read(replacement.len() as u32).unwrap(),
            replacement.as_ref()
        );
        f.close().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn delete_removes_object() {
    delete_removes_object_on(CacheIndexKind::InMemory).await;
}

#[tokio::test]
async fn redb_delete_removes_object() {
    delete_removes_object_on(CacheIndexKind::Redb).await;
}

async fn delete_removes_object_on(kind: CacheIndexKind) {
    let h = E2eHarness::start_with_index(kind).await;
    h.seed_object("deleteme.txt", b"delete me").await;

    tokio::task::spawn_blocking(move || {
        let client = h.connect();

        let mut f = client.open(STORE_ID, TEST_BUCKET, "deleteme.txt").unwrap();
        f.close().unwrap();

        client
            .delete(STORE_ID, TEST_BUCKET, "deleteme.txt")
            .unwrap();

        let result = client.open(STORE_ID, TEST_BUCKET, "deleteme.txt");
        assert!(result.is_err(), "expected open to fail after delete");
    })
    .await
    .unwrap();
}
