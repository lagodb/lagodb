//! Read-path E2E tests: client → Unix socket → server → cache → MinIO.

use pg_lakebase_storage::{SeekFrom, StorageClient};

use crate::harness::{CacheIndexKind, E2eHarness, STORE_ID, TEST_BUCKET};

#[tokio::test]
async fn open_and_read_object() {
    let h = E2eHarness::start().await;
    let payload = b"hello from minio integration test!";
    h.seed_object("dir/file.txt", payload).await;

    tokio::task::spawn_blocking(move || {
        let client = h.connect();
        let mut file = client.open(STORE_ID, TEST_BUCKET, "dir/file.txt").unwrap();
        assert_eq!(file.size(), payload.len() as u64);

        let data = file.read(5).unwrap();
        assert_eq!(&data, b"hello");

        let rest = file.read(payload.len() as u32).unwrap();
        assert_eq!(&rest, &payload[5..]);
        file.close().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn seek_and_read() {
    let h = E2eHarness::start().await;
    let payload = b"AAAA-BBBB-CCCC-DDDD";
    h.seed_object("seek.bin", payload).await;

    tokio::task::spawn_blocking(move || {
        let client = h.connect();
        let mut f = client.open(STORE_ID, TEST_BUCKET, "seek.bin").unwrap();

        f.seek(SeekFrom::Start(5));
        assert_eq!(f.read(4).unwrap(), b"BBBB");

        f.seek(SeekFrom::End(-4));
        assert_eq!(f.read(4).unwrap(), b"DDDD");

        // cursor is now at 19; CCCC starts at offset 10
        f.seek(SeekFrom::Current(-9));
        assert_eq!(f.read(4).unwrap(), b"CCCC");

        f.close().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn read_into_buffer() {
    let h = E2eHarness::start().await;
    let payload = b"buffer-read-integration-test";
    h.seed_object("buf.txt", payload).await;

    tokio::task::spawn_blocking(move || {
        let client = h.connect();
        let mut file = client.open(STORE_ID, TEST_BUCKET, "buf.txt").unwrap();

        let mut buf = [0u8; 64];
        let n = file.read_into(&mut buf).unwrap();
        assert_eq!(n, payload.len());
        assert_eq!(&buf[..n], payload.as_ref());
        file.close().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn second_open_uses_direct_io_after_cache_fill() {
    let h = E2eHarness::start().await;
    let payload = b"direct-io-test";
    h.seed_object("dio.txt", payload).await;

    tokio::task::spawn_blocking(move || {
        let client = h.connect();

        let mut f1 = client.open(STORE_ID, TEST_BUCKET, "dio.txt").unwrap();
        assert_eq!(f1.read(payload.len() as u32).unwrap(), payload.as_ref());
        f1.close().unwrap();

        let mut f2 = client.open(STORE_ID, TEST_BUCKET, "dio.txt").unwrap();
        assert!(f2.is_direct_io(), "expected direct-IO on cached re-open");
        assert_eq!(f2.read(payload.len() as u32).unwrap(), payload.as_ref());
        f2.close().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn list_objects() {
    let h = E2eHarness::start().await;
    h.seed_object("list/a.txt", b"a").await;
    h.seed_object("list/b.txt", b"bb").await;
    h.seed_object("list/sub/c.txt", b"ccc").await;

    tokio::task::spawn_blocking(move || {
        let client = h.connect();
        let entries: Vec<_> = client
            .list(STORE_ID, TEST_BUCKET, Some("list/"))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
        assert!(
            keys.contains(&"list/a.txt"),
            "missing list/a.txt in {keys:?}"
        );
        assert!(
            keys.contains(&"list/b.txt"),
            "missing list/b.txt in {keys:?}"
        );
        assert!(
            keys.contains(&"list/sub/c.txt"),
            "missing list/sub/c.txt in {keys:?}"
        );
    })
    .await
    .unwrap();
}

/// Payload ≤ `small_object_limit` (4 bytes) exercises the SmallKV cache path
/// instead of the large-object chunk/direct-IO path.
#[tokio::test]
async fn small_object_uses_kv_path() {
    let h = E2eHarness::start().await;
    let payload = b"smol";
    h.seed_object("small.txt", payload).await;

    tokio::task::spawn_blocking(move || {
        let client = h.connect();
        let mut f = client.open(STORE_ID, TEST_BUCKET, "small.txt").unwrap();
        assert_eq!(f.size(), 4);
        assert_eq!(f.read(4).unwrap(), payload.as_ref());
        f.close().unwrap();

        // Second open — SmallKV objects may not promote to direct-IO.
        let mut f2 = client.open(STORE_ID, TEST_BUCKET, "small.txt").unwrap();
        assert_eq!(f2.read(4).unwrap(), payload.as_ref());
        f2.close().unwrap();
    })
    .await
    .unwrap();
}

/// Uses [`RedbCacheIndex`] (the production default) instead of the in-memory
/// index, so we cover the persistent cache / redb wiring.
#[tokio::test]
async fn redb_index_read_path() {
    let h = E2eHarness::start_with_redb().await;
    let payload = b"redb backed read";
    h.seed_object("redb.txt", payload).await;

    tokio::task::spawn_blocking(move || {
        let client = h.connect();
        let mut f = client.open(STORE_ID, TEST_BUCKET, "redb.txt").unwrap();
        assert_eq!(f.read(payload.len() as u32).unwrap(), payload.as_ref());
        f.close().unwrap();

        let mut f2 = client.open(STORE_ID, TEST_BUCKET, "redb.txt").unwrap();
        assert!(
            f2.is_direct_io(),
            "expected direct-IO on redb cached re-open"
        );
        assert_eq!(f2.read(payload.len() as u32).unwrap(), payload.as_ref());
        f2.close().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn redb_restart_recovers_cached_complete_file() {
    let mut h = E2eHarness::start_with_index(CacheIndexKind::Redb).await;
    let key = "restart-cache.txt";
    let payload = b"redb cache should survive server restart".to_vec();
    h.seed_object(key, &payload).await;

    let socket = h.socket_path().to_path_buf();
    let expected = payload.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&socket).unwrap();

        let mut cold = client.open(STORE_ID, TEST_BUCKET, key).unwrap();
        assert_eq!(
            cold.read(expected.len() as u32).unwrap(),
            expected.as_slice()
        );
        cold.close().unwrap();

        let mut cached = client.open(STORE_ID, TEST_BUCKET, key).unwrap();
        assert!(cached.is_direct_io(), "expected direct-IO before restart");
        assert_eq!(
            cached.read(expected.len() as u32).unwrap(),
            expected.as_slice()
        );
        cached.close().unwrap();
    })
    .await
    .unwrap();

    h.restart_server().await;

    let socket = h.socket_path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&socket).unwrap();
        let mut file = client.open(STORE_ID, TEST_BUCKET, key).unwrap();
        assert!(
            file.is_direct_io(),
            "expected direct-IO from persisted redb cache after restart"
        );
        assert_eq!(file.read(payload.len() as u32).unwrap(), payload.as_slice());
        file.close().unwrap();
    })
    .await
    .unwrap();
}
