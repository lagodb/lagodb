use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::backend::{
    MemoryObjectBackend, ObjectBackend, S3CompatibleStoreConfig, SecretString, StoreConfig, StoreRegistry,
};
use crate::cache::{CacheCleanupPolicy, CacheManager, InMemoryCacheIndex};
use crate::error::StorageErrorKind;
use crate::object::ObjectLocation;
use crate::server::StorageServer;
use crate::service::StorageService;
use crate::staging::StagingArea;

use super::*;

const TEST_STORE_ID: &str = "test-store";

#[tokio::test]
async fn client_reads_through_unix_socket_server() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "hello.txt").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"hello from storage".to_vec());

    let root = test_root("cache");
    let socket = test_root("storage.sock");
    let cache = Arc::new(CacheManager::new(root.clone(), InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, Arc::new(backend))
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let mut file = client.open(TEST_STORE_ID, "bucket", "hello.txt").unwrap();
        file.seek(SeekFrom::Start(6));
        let mut data = [0_u8; 4];
        let n = file.read_into(&mut data).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&data, b"from");
        file.close().unwrap();
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn client_head_and_exists_do_not_admit_cache() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "head.txt").unwrap();
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(key, b"hello metadata".to_vec());

    let root = test_root("head-cache");
    let socket = test_root("head.sock");
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache.clone()));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let info = client.head(TEST_STORE_ID, "bucket", "head.txt").unwrap();
        assert_eq!(info.size, b"hello metadata".len() as u64);
        assert!(client.exists(TEST_STORE_ID, "bucket", "head.txt").unwrap());
        assert!(!client.exists(TEST_STORE_ID, "bucket", "missing.txt").unwrap());
    })
    .await
    .unwrap();

    assert_eq!(backend.head_call_count(), 3);
    assert_eq!(cache.logical_cache_usage().await.unwrap().resident_bytes, 0);

    server_task.abort();
}

#[tokio::test]
async fn client_head_returns_from_cache_after_open() {
    let data = b"cached content";
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "cached.txt").unwrap();
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(key, data.to_vec());

    let root = test_root("head-cached");
    let socket = test_root("head-cached.sock");
    // small_object_limit must be >= data length so the object is admitted as SmallKv
    // (large-fill objects don't persist metadata to the index until fill completes).
    let cache = Arc::new(
        CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(data.len() as u64, 4096),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache.clone()));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();

        // Open admits the object into the cache (triggers one backend head during establishment).
        let mut file = client.open(TEST_STORE_ID, "bucket", "cached.txt").unwrap();
        file.close().unwrap();
        let heads_after_open = backend.head_call_count();

        // Subsequent head calls should be served from the cache index — no new backend heads.
        let info = client.head(TEST_STORE_ID, "bucket", "cached.txt").unwrap();
        assert_eq!(info.size, data.len() as u64);

        let info2 = client.head(TEST_STORE_ID, "bucket", "cached.txt").unwrap();
        assert_eq!(info2.size, data.len() as u64);

        assert_eq!(
            backend.head_call_count(),
            heads_after_open,
            "head should be served from cache, not backend"
        );
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn client_stage_write_commit_roundtrip_uploads_to_backend() {
    let root = test_root("staging-cache");
    let socket = test_root("staging.sock");
    let staging = Arc::new(StagingArea::new(root.clone()));
    staging.prepare_dirs().await.unwrap();
    staging.wipe().await.unwrap();

    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_staging(
        registry,
        cache,
        staging,
        crate::config::StorageServiceConfig::default(),
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    let commit_info = tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let mut staging = client.stage(TEST_STORE_ID, "bucket", "uploaded.txt").unwrap();
        staging.write(b"hello ").unwrap();
        staging.write(b"commit").unwrap();
        staging.sync().unwrap();
        drop(staging);
        client.commit(TEST_STORE_ID, "bucket", "uploaded.txt").unwrap()
    })
    .await
    .unwrap();

    assert_eq!(commit_info.size, b"hello commit".len() as u64);
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "uploaded.txt").unwrap();
    let data = backend.get_range(&key, 0..commit_info.size).await.unwrap();
    assert_eq!(&data[..], b"hello commit");

    server_task.abort();
}

#[tokio::test]
async fn client_stage_abort_removes_staging_file_without_upload() {
    let root = test_root("staging-abort-cache");
    let socket = test_root("staging-abort.sock");
    let staging = Arc::new(StagingArea::new(root.clone()));
    staging.prepare_dirs().await.unwrap();
    staging.wipe().await.unwrap();

    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_staging(
        registry,
        cache,
        staging.clone(),
        crate::config::StorageServiceConfig::default(),
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    let staging_path = tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let mut staging_file = client.stage(TEST_STORE_ID, "bucket", "aborted.txt").unwrap();
        staging_file.write(b"doomed").unwrap();
        let path = staging_file.path().to_path_buf();
        drop(staging_file);
        client.abort(TEST_STORE_ID, "bucket", "aborted.txt").unwrap();
        // Second abort is a no-op.
        client.abort(TEST_STORE_ID, "bucket", "aborted.txt").unwrap();
        path
    })
    .await
    .unwrap();

    assert!(!tokio::fs::try_exists(&staging_path).await.unwrap(), "abort must remove the staging file");
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "aborted.txt").unwrap();
    assert!(
        backend.get_range(&key, 0..1).await.is_err(),
        "aborted key must not have been uploaded to the backend"
    );

    server_task.abort();
}

#[tokio::test]
async fn client_commit_can_be_issued_from_a_different_connection() {
    let root = test_root("staging-xconn-cache");
    let socket = test_root("staging-xconn.sock");
    let staging = Arc::new(StagingArea::new(root.clone()));
    staging.prepare_dirs().await.unwrap();
    staging.wipe().await.unwrap();

    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_staging(
        registry,
        cache,
        staging,
        crate::config::StorageServiceConfig::default(),
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_writer = socket.clone();
    tokio::task::spawn_blocking(move || {
        let writer_client = StorageClient::connect(&socket_for_writer).unwrap();
        let mut staging_file = writer_client.stage(TEST_STORE_ID, "bucket", "cross-conn.txt").unwrap();
        staging_file.write(b"cross-connection commit").unwrap();
        drop(staging_file);
        drop(writer_client);
    })
    .await
    .unwrap();

    let socket_for_committer = socket.clone();
    let commit_info = tokio::task::spawn_blocking(move || {
        let committer = StorageClient::connect(&socket_for_committer).unwrap();
        committer.commit(TEST_STORE_ID, "bucket", "cross-conn.txt").unwrap()
    })
    .await
    .unwrap();

    assert_eq!(commit_info.size, b"cross-connection commit".len() as u64);
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "cross-conn.txt").unwrap();
    let readback = backend.get_range(&key, 0..commit_info.size).await.unwrap();
    assert_eq!(&readback[..], b"cross-connection commit");

    server_task.abort();
}

#[tokio::test]
async fn stage_twice_without_finalize_returns_busy() {
    let root = test_root("staging-busy-cache");
    let socket = test_root("staging-busy.sock");
    let staging = Arc::new(StagingArea::new(root.clone()));
    staging.prepare_dirs().await.unwrap();
    staging.wipe().await.unwrap();
    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend)
        .unwrap();
    let service = Arc::new(StorageService::with_staging(
        registry,
        cache,
        staging,
        crate::config::StorageServiceConfig::default(),
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let _first = client.stage(TEST_STORE_ID, "bucket", "duplicate.txt").unwrap();
        let error = client.stage(TEST_STORE_ID, "bucket", "duplicate.txt").unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Busy);
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn dropping_storage_file_closes_server_handle_best_effort() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "drop-close.txt").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghijklmnop".to_vec());

    let root = test_root("drop-close-cache");
    let socket = test_root("drop-close.sock");
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, Arc::new(backend))
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache.clone()));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    let client = tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();

        let mut file = client.open(TEST_STORE_ID, "bucket", "drop-close.txt").unwrap();
        let data = file.read(16).unwrap();
        assert_eq!(data, b"abcdefghijklmnop");
        file.close().unwrap();

        let file = client.open(TEST_STORE_ID, "bucket", "drop-close.txt").unwrap();
        assert!(file.is_direct_io());
        drop(file);

        client
    })
    .await
    .unwrap();

    let mut policy = CacheCleanupPolicy::new(16);
    policy.cleanup_start_ratio = 0.0;
    policy.cleanup_target_ratio = 0.0;
    let report = cache.cleanup(policy).await.unwrap();

    assert_eq!(report.evicted_objects, 1);
    assert_eq!(cache.logical_cache_usage().await.unwrap().resident_bytes, 0);
    assert!(!tokio::fs::try_exists(cache.complete_path(&key).unwrap()).await.unwrap());

    drop(client);
    server_task.abort();
}

#[tokio::test]
async fn client_registers_unregisters_and_purges_store_over_wire() {
    let root = test_root("client-store-cache");
    let socket = test_root("client-store.sock");
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let service = Arc::new(StorageService::with_registry(StoreRegistry::new(), cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let config = StoreConfig::S3Compatible(S3CompatibleStoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            region: Some("us-east-1".to_string()),
            access_key_id: Some(SecretString::new("access")),
            secret_access_key: Some(SecretString::new("secret")),
            token: None,
            allow_http: true,
            virtual_hosted_style_request: false,
            skip_signature: false,
        });

        assert!(!client.register_store("store-a", config.clone()).unwrap());
        assert!(client.register_store("store-a", config).unwrap());
        client.purge_store_cache("store-a").unwrap();
        assert!(!client.invalidate_object_cache("store-a", "bucket", "file").unwrap());
        assert!(client.unregister_store("store-a").unwrap());
        assert!(!client.unregister_store("store-a").unwrap());

        let error = client
            .register_store(
                "bad-store",
                StoreConfig::S3Compatible(S3CompatibleStoreConfig {
                    endpoint: String::new(),
                    region: None,
                    access_key_id: None,
                    secret_access_key: None,
                    token: None,
                    allow_http: false,
                    virtual_hosted_style_request: false,
                    skip_signature: false,
                }),
            )
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Configuration);
    })
    .await
    .unwrap();

    server_task.abort();
}

fn test_root(name: &str) -> PathBuf {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    PathBuf::from("/tmp").join(format!("pss-{name}-{stamp}"))
}

#[tokio::test]
async fn client_delete_removes_object_from_backend_and_is_idempotent() {
    let root = test_root("delete-cache");
    let socket = test_root("delete.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "doomed.txt").unwrap();
    backend.insert(key.clone(), b"bye".to_vec());
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_client = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&socket_for_client).unwrap();
        client.delete(TEST_STORE_ID, "bucket", "doomed.txt").unwrap();
        // Second delete is idempotent regardless of backend's missing-key behavior.
        client.delete(TEST_STORE_ID, "bucket", "doomed.txt").unwrap();
    })
    .await
    .unwrap();

    assert!(
        backend.get_range(&key, 0..1).await.is_err(),
        "deleted key must not survive in the backend"
    );

    server_task.abort();
}

#[tokio::test]
async fn client_delete_prefix_removes_all_matching_objects_and_rejects_empty_prefix() {
    let root = test_root("delete-prefix-cache");
    let socket = test_root("delete-prefix.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    for key in ["scope/a", "scope/b", "scope/nested/c", "other/d"] {
        backend.insert(ObjectLocation::new(TEST_STORE_ID, "bucket", key).unwrap(), b"x".to_vec());
    }
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_client = socket.clone();
    let deleted = tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&socket_for_client).unwrap();

        let empty_prefix_error = client.delete_prefix(TEST_STORE_ID, "bucket", "").unwrap_err();
        assert_eq!(empty_prefix_error.kind(), StorageErrorKind::InvalidPath);

        client.delete_prefix(TEST_STORE_ID, "bucket", "scope/").unwrap()
    })
    .await
    .unwrap();

    assert_eq!(deleted, 3);
    let surviving = ObjectLocation::new(TEST_STORE_ID, "bucket", "other/d").unwrap();
    assert!(
        backend.get_range(&surviving, 0..1).await.is_ok(),
        "delete_prefix must not touch keys outside the prefix"
    );
    for gone in ["scope/a", "scope/b", "scope/nested/c"] {
        let key = ObjectLocation::new(TEST_STORE_ID, "bucket", gone).unwrap();
        assert!(
            backend.get_range(&key, 0..1).await.is_err(),
            "key {gone} should have been deleted by delete_prefix",
        );
    }

    server_task.abort();
}

#[tokio::test]
async fn client_list_iterates_pages_and_returns_every_object() {
    let root = test_root("list-cache");
    let socket = test_root("list.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    let mut expected: Vec<String> = Vec::with_capacity(50);
    for i in 0..50 {
        let key = format!("scope/{i:03}");
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", key.clone()).unwrap(),
            b"x".to_vec(),
        );
        expected.push(key);
    }
    expected.sort();
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_client = socket.clone();
    let listed = tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&socket_for_client).unwrap();
        let mut keys: Vec<String> = client
            .list(TEST_STORE_ID, "bucket", Some("scope/"))
            .map(|item| item.unwrap().key)
            .collect();
        keys.sort();
        keys
    })
    .await
    .unwrap();

    assert_eq!(listed, expected);

    server_task.abort();
}

#[tokio::test]
async fn client_list_page_drives_pagination_explicitly() {
    let root = test_root("list-page-cache");
    let socket = test_root("list-page.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    for i in 0..5 {
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", format!("k/{i}")).unwrap(),
            b"x".to_vec(),
        );
    }
    let cache = Arc::new(CacheManager::new(root, InMemoryCacheIndex::new()).with_limits(4, 4));
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, backend)
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let socket_for_client = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&socket_for_client).unwrap();

        let page1 = client
            .list_page(TEST_STORE_ID, "bucket", Some("k/"), None, 2)
            .unwrap();
        assert_eq!(page1.entries.len(), 2);
        let cursor1 = page1.next_cursor.expect("more pages must remain");

        let page2 = client
            .list_page(TEST_STORE_ID, "bucket", Some("k/"), Some(cursor1), 2)
            .unwrap();
        assert_eq!(page2.entries.len(), 2);
        let cursor2 = page2.next_cursor.expect("one entry should remain");

        let page3 = client
            .list_page(TEST_STORE_ID, "bucket", Some("k/"), Some(cursor2), 2)
            .unwrap();
        assert_eq!(page3.entries.len(), 1);
        assert!(page3.next_cursor.is_none(), "final page must not return a cursor");
    })
    .await
    .unwrap();

    server_task.abort();
}
