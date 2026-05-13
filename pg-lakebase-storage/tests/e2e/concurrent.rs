//! Concurrent-access E2E tests.

use std::sync::Arc;

use pg_lakebase_storage::StorageClient;

use crate::harness::{CacheIndexKind, E2eHarness, STORE_ID, TEST_BUCKET};

#[tokio::test]
async fn concurrent_reads_on_same_object() {
    concurrent_reads_on_same_object_on(CacheIndexKind::InMemory).await;
}

#[tokio::test]
async fn redb_concurrent_reads_on_same_object() {
    concurrent_reads_on_same_object_on(CacheIndexKind::Redb).await;
}

async fn concurrent_reads_on_same_object_on(kind: CacheIndexKind) {
    let h = E2eHarness::start_with_index(kind).await;
    let payload: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    h.seed_object("shared.bin", &payload).await;

    let socket = h.socket_path().to_path_buf();
    let expected = Arc::new(payload);

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let socket = socket.clone();
            let expected = expected.clone();
            tokio::task::spawn_blocking(move || {
                let client = StorageClient::connect(&socket).unwrap_or_else(|e| panic!("client {i} connect: {e}"));
                let mut f = client
                    .open(STORE_ID, TEST_BUCKET, "shared.bin")
                    .unwrap_or_else(|e| panic!("client {i} open: {e}"));
                assert_eq!(f.read(expected.len() as u32).unwrap(), &expected[..]);
                f.close().unwrap();
            })
        })
        .collect();

    for (i, h) in handles.into_iter().enumerate() {
        h.await.unwrap_or_else(|e| panic!("client {i} panicked: {e}"));
    }
}

#[tokio::test]
async fn concurrent_reads_on_different_objects() {
    concurrent_reads_on_different_objects_on(CacheIndexKind::InMemory).await;
}

#[tokio::test]
async fn redb_concurrent_reads_on_different_objects() {
    concurrent_reads_on_different_objects_on(CacheIndexKind::Redb).await;
}

async fn concurrent_reads_on_different_objects_on(kind: CacheIndexKind) {
    let h = E2eHarness::start_with_index(kind).await;
    let objects: Vec<(String, Vec<u8>)> = (0..4)
        .map(|i| (format!("multi/obj-{i}.txt"), format!("content of object {i}").into_bytes()))
        .collect();
    for (key, data) in &objects {
        h.seed_object(key, data).await;
    }

    let socket = h.socket_path().to_path_buf();

    let handles: Vec<_> = objects
        .into_iter()
        .map(|(key, expected)| {
            let socket = socket.clone();
            tokio::task::spawn_blocking(move || {
                let client = StorageClient::connect(&socket).unwrap();
                let mut f = client.open(STORE_ID, TEST_BUCKET, &key).unwrap();
                assert_eq!(f.read(expected.len() as u32).unwrap(), expected);
                f.close().unwrap();
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_stage_commit() {
    concurrent_stage_commit_on(CacheIndexKind::InMemory).await;
}

#[tokio::test]
async fn redb_concurrent_stage_commit() {
    concurrent_stage_commit_on(CacheIndexKind::Redb).await;
}

async fn concurrent_stage_commit_on(kind: CacheIndexKind) {
    let h = E2eHarness::start_with_index(kind).await;
    let socket = h.socket_path().to_path_buf();

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let socket = socket.clone();
            tokio::task::spawn_blocking(move || {
                let client = StorageClient::connect(&socket).unwrap();

                let key = format!("concurrent-stage/file-{i}.txt");
                let payload = format!("payload for concurrent file {i}");

                let mut staging = client.stage(STORE_ID, TEST_BUCKET, &key).unwrap();
                staging.write(payload.as_bytes()).unwrap();
                staging.sync().unwrap();
                drop(staging);

                let info = client.commit(STORE_ID, TEST_BUCKET, &key).unwrap();
                assert_eq!(info.size, payload.len() as u64);

                client.invalidate_object_cache(STORE_ID, TEST_BUCKET, &key).unwrap();

                let mut f = client.open(STORE_ID, TEST_BUCKET, &key).unwrap();
                assert_eq!(f.read(payload.len() as u32).unwrap(), payload.as_bytes());
                f.close().unwrap();
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }
}
