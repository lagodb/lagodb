//! Staging / upload / delete E2E tests.

use pg_lakebase_storage::{SeekFrom, StagingFile, StagingPathResolver};

use crate::harness::{CacheIndexKind, E2eHarness, TEST_BUCKET};

#[tokio::test]
async fn stage_upload_then_read_back() {
    stage_upload_then_read_back_on(CacheIndexKind::InMemory).await;
}

#[tokio::test]
async fn redb_stage_upload_then_read_back() {
    stage_upload_then_read_back_on(CacheIndexKind::Redb).await;
}

async fn stage_upload_then_read_back_on(kind: CacheIndexKind) {
    let h = E2eHarness::start_with_index(kind).await;
    let payload = b"staged data written by the integration test client";

    let cache_dir = h.cache_dir().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(cache_dir);
        let client = h.connect();

        let mut staging = StagingFile::create(
            &resolver,
            client.backend_identity(),
            TEST_BUCKET,
            "staged/object.bin",
        )
        .unwrap();
        staging.write(payload).unwrap();
        staging.sync().unwrap();
        drop(staging);

        let info = client.upload(TEST_BUCKET, "staged/object.bin").unwrap();
        assert_eq!(info.size, payload.len() as u64);

        client
            .invalidate_object_cache(TEST_BUCKET, "staged/object.bin")
            .unwrap();

        let mut file = client.open(TEST_BUCKET, "staged/object.bin").unwrap();
        assert_eq!(file.size(), payload.len() as u64);
        assert_eq!(file.read(payload.len() as u32).unwrap(), payload.as_ref());
        file.close().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn caller_unlinks_staging_file_to_discard_without_upload() {
    let h = E2eHarness::start().await;

    let cache_dir = h.cache_dir().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(cache_dir);
        let client = h.connect();

        let mut staging = StagingFile::create(
            &resolver,
            client.backend_identity(),
            TEST_BUCKET,
            "discarded/file.txt",
        )
        .unwrap();
        staging
            .write(b"these bytes should never reach MinIO")
            .unwrap();
        let path = staging.path().to_path_buf();
        drop(staging);

        std::fs::remove_file(&path).unwrap();

        let result = client.open(TEST_BUCKET, "discarded/file.txt");
        assert!(result.is_err(), "expected open to fail without upload");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn upload_overwrites_and_readback_sees_new_data() {
    upload_overwrites_and_readback_sees_new_data_on(CacheIndexKind::InMemory).await;
}

#[tokio::test]
async fn redb_upload_overwrites_and_readback_sees_new_data() {
    upload_overwrites_and_readback_sees_new_data_on(CacheIndexKind::Redb).await;
}

async fn upload_overwrites_and_readback_sees_new_data_on(kind: CacheIndexKind) {
    let h = E2eHarness::start_with_index(kind).await;
    let original = b"original content";
    h.seed_object("overwrite.txt", original).await;

    let replacement = b"replacement content written via staging";

    let cache_dir = h.cache_dir().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(cache_dir);
        let client = h.connect();

        let mut f = client.open(TEST_BUCKET, "overwrite.txt").unwrap();
        assert_eq!(f.read(original.len() as u32).unwrap(), original.as_ref());
        f.close().unwrap();

        let mut staging = StagingFile::create(
            &resolver,
            client.backend_identity(),
            TEST_BUCKET,
            "overwrite.txt",
        )
        .unwrap();
        staging.write(replacement).unwrap();
        drop(staging);

        let info = client.upload(TEST_BUCKET, "overwrite.txt").unwrap();
        assert_eq!(info.size, replacement.len() as u64);

        client
            .invalidate_object_cache(TEST_BUCKET, "overwrite.txt")
            .unwrap();

        let mut f = client.open(TEST_BUCKET, "overwrite.txt").unwrap();
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

        let mut f = client.open(TEST_BUCKET, "deleteme.txt").unwrap();
        f.close().unwrap();

        client.delete(TEST_BUCKET, "deleteme.txt").unwrap();

        let result = client.open(TEST_BUCKET, "deleteme.txt");
        assert!(result.is_err(), "expected open to fail after delete");
    })
    .await
    .unwrap();
}
