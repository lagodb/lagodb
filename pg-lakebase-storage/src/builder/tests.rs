use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectStorePath;

use crate::cache::InMemoryCacheIndex;
use crate::client::SeekFrom;
use crate::client::StorageClient;
use crate::config::StorageServerConfig;
use crate::error::{StorageError, StorageErrorKind};
use crate::object::ObjectLocation;

use super::*;

#[tokio::test]
async fn builder_starts_redb_backed_object_store_server() {
    let store = Arc::new(InMemory::new());
    store
        .put(
            &ObjectStorePath::from("dir/file.txt"),
            b"hello builder".as_ref().into(),
        )
        .await
        .unwrap();

    let root = test_root("cache");
    let socket = test_root("socket.sock");
    let server = StorageServerBuilder::new(&socket, &root)
        .with_service_config(StorageServiceConfig::default().with_cache_limits(4, 4))
        .bind()
        .await
        .unwrap();
    server
        .store_registry()
        .register_object_store_bucket("default", store, "bucket")
        .unwrap();

    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let mut file = client.open("default", "bucket", "dir/file.txt").unwrap();
        let data = file.read(5).unwrap();
        assert_eq!(data, b"hello");
        file.seek(SeekFrom::Start(6));
        let data = file.read(7).unwrap();
        assert_eq!(data, b"builder");
        file.close().unwrap();

        let mut file = client.open("default", "bucket", "dir/file.txt").unwrap();
        assert!(file.is_direct_io());
        file.seek(SeekFrom::Start(6));
        let data = file.read(7).unwrap();
        assert_eq!(data, b"builder");
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn builder_allows_dynamic_store_registration_after_bind() {
    let store = Arc::new(InMemory::new());
    store
        .put(
            &ObjectStorePath::from("dir/file.txt"),
            b"dynamic store".as_ref().into(),
        )
        .await
        .unwrap();

    let root = test_root("cache-dynamic-store");
    let socket = test_root("socket-dynamic-store.sock");
    let server = StorageServerBuilder::new(&socket, &root)
        .with_service_config(StorageServiceConfig::default().with_cache_limits(4, 4))
        .bind()
        .await
        .unwrap();
    server
        .store_registry()
        .register_object_store_bucket("dynamic", store, "bucket")
        .unwrap();

    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let mut file = client.open("dynamic", "bucket", "dir/file.txt").unwrap();
        let data = file.read(7).unwrap();
        assert_eq!(data, b"dynamic");
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn builder_applies_open_handle_limit_per_connection() {
    let store = Arc::new(InMemory::new());
    store
        .put(
            &ObjectStorePath::from("dir/file.txt"),
            b"handle limit".as_ref().into(),
        )
        .await
        .unwrap();

    let root = test_root("cache-handle-limit");
    let socket = test_root("socket-handle-limit.sock");
    let server = StorageServerBuilder::new(&socket, &root)
        .with_service_config(StorageServiceConfig::default().with_cache_limits(4, 4))
        .with_server_config(
            StorageServerConfig::default().with_max_open_handles_per_connection(1),
        )
        .bind()
        .await
        .unwrap();
    server
        .store_registry()
        .register_object_store_bucket("default", store, "bucket")
        .unwrap();

    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let first = client.open("default", "bucket", "dir/file.txt").unwrap();
        let error = match client.open("default", "bucket", "dir/file.txt") {
            Ok(_) => {
                panic!("expected second open to exceed the connection handle limit")
            }
            Err(error) => error,
        };
        assert_eq!(error.kind(), StorageErrorKind::ResourceExhausted);

        drop(first);
        let _second = client.open("default", "bucket", "dir/file.txt").unwrap();
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn second_bind_fails_when_unix_socket_already_listening() {
    use std::io;

    let root_a = test_root("cache-dup-a");
    let root_b = test_root("cache-dup-b");
    let socket = test_root("socket-dup.sock");
    let server = StorageServerBuilder::new(&socket, &root_a)
        .with_service_config(StorageServiceConfig::default().with_cache_limits(4, 4))
        .bind()
        .await
        .unwrap();

    let dup = StorageServerBuilder::new(&socket, &root_b)
        .with_service_config(StorageServiceConfig::default().with_cache_limits(4, 4))
        .bind()
        .await;

    match dup {
        Err(StorageError::Io { source, .. }) => {
            assert_eq!(source.kind(), io::ErrorKind::AddrInUse)
        }
        Err(other) => panic!("expected Io AddrInUse, got {other:?}"),
        Ok(_) => panic!("expected second bind to fail"),
    }

    drop(server);
}

#[tokio::test]
async fn builder_can_bind_with_custom_cache_index() {
    let store = Arc::new(InMemory::new());
    store
        .put(
            &ObjectStorePath::from("dir/file.txt"),
            b"custom index".as_ref().into(),
        )
        .await
        .unwrap();

    let root = test_root("cache-custom-index");
    let socket = test_root("socket-custom-index.sock");
    let server = StorageServerBuilder::new(&socket, &root)
        .with_service_config(StorageServiceConfig::default().with_cache_limits(4, 4))
        .bind_with_index(InMemoryCacheIndex::new())
        .await
        .unwrap();
    server
        .store_registry()
        .register_object_store_bucket("default", store, "bucket")
        .unwrap();

    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect(&client_socket).unwrap();
        let mut file = client.open("default", "bucket", "dir/file.txt").unwrap();
        let data = file.read(6).unwrap();
        assert_eq!(data, b"custom");
        file.seek(SeekFrom::Start(7));
        let data = file.read(5).unwrap();
        assert_eq!(data, b"index");
        file.close().unwrap();
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn builder_wipes_staging_tree_on_startup_so_crashed_client_bytes_do_not_persist()
 {
    use crate::staging::StagingPathResolver;

    let root = test_root("staging-boot-cache");
    let socket = test_root("staging-boot.sock");
    tokio::fs::create_dir_all(&root).await.unwrap();
    let staging_resolver = StagingPathResolver::new(root.clone());
    let stale_key = ObjectLocation::new("default", "bucket", "crashed.txt").unwrap();
    let stale_path = staging_resolver.path_for(&stale_key).unwrap();
    if let Some(parent) = stale_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(&stale_path, b"crashed bytes")
        .await
        .unwrap();

    let server = StorageServerBuilder::new(&socket, &root)
        .with_service_config(StorageServiceConfig::default().with_cache_limits(4, 4))
        .bind()
        .await
        .unwrap();

    assert!(!tokio::fs::try_exists(&stale_path).await.unwrap());

    drop(server);
}

fn test_root(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    PathBuf::from("/tmp").join(format!("lfsb-{}-{stamp}-{name}", std::process::id()))
}
