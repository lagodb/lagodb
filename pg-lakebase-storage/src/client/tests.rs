use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::backend::{
    BackendDataIdentity, ManagedStoreRegistry, MemoryObjectBackend, ObjectBackend,
    S3CompatibleStoreConfig, SecretString, StoreConfig,
};
use crate::cache::{
    CacheCleanupPolicy, CacheIndex, CacheManager, CachedObjectMeta,
    InMemoryCacheIndex,
};
use crate::config::{StorageRuntime, StorageRuntimeConfig, StorageServerConfig};
use crate::error::StorageErrorKind;
use crate::object::{ObjectInfo, ObjectLocation};
use crate::protocol::{
    WireRequestPayload, WireResponse, WireResponsePayload, decode_request,
    encode_response,
};
use crate::server::StorageServer;
use crate::service::StorageService;
use crate::staging::{StagingPathResolver, StagingUploader};
use crate::transport::{read_frame_blocking, write_frame_blocking};

use super::*;

const TEST_STORE_ID: &str = "test-store";
const TEST_VOLUME_ID: u64 = 1;

fn test_identity() -> BackendDataIdentity {
    BackendDataIdentity::memory(TEST_STORE_ID)
}

trait TestManagedRegistryExt {
    fn register_shared_backend<B: ObjectBackend + 'static>(
        &self,
        _name: &str,
        backend: Arc<B>,
    ) -> StorageResult<()>;
}

impl TestManagedRegistryExt for ManagedStoreRegistry {
    fn register_shared_backend<B: ObjectBackend + 'static>(
        &self,
        _name: &str,
        backend: Arc<B>,
    ) -> StorageResult<()> {
        self.register_backend(TEST_VOLUME_ID, test_identity(), backend)
    }
}

type StoreRegistry = ManagedStoreRegistry;

#[derive(Clone)]
struct CountingFdPolicy {
    active: Arc<AtomicUsize>,
    limit: Option<usize>,
}

impl ExternalFdPolicy for CountingFdPolicy {
    fn acquire(&self) -> StorageResult<Box<dyn ExternalFdLease>> {
        if let Some(limit) = self.limit {
            self.active
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                    (active < limit).then_some(active + 1)
                })
                .map_err(|_| {
                    StorageError::resource_exhausted(
                        "test external file descriptor budget exhausted",
                    )
                })?;
        } else {
            self.active.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Box::new(CountingFdLease {
            active: Arc::clone(&self.active),
        }))
    }
}

struct CountingFdLease {
    active: Arc<AtomicUsize>,
}

impl ExternalFdLease for CountingFdLease {}

impl Drop for CountingFdLease {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn client_reads_through_unix_socket_server() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "hello.txt").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"hello from storage".to_vec());

    let root = test_root("cache");
    let socket = test_root("storage.sock");
    let cache = Arc::new(
        CacheManager::new(
            root.clone(),
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
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
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let mut file = client.open("bucket", "hello.txt").unwrap();
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
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
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
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let info = client.head("bucket", "head.txt").unwrap();
        assert_eq!(info.size, b"hello metadata".len() as u64);
        assert!(client.exists("bucket", "head.txt").unwrap());
        assert!(!client.exists("bucket", "missing.txt").unwrap());
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
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(data.len() as u64, 4096),
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
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();

        // Open admits the object into the cache (triggers one backend head during establishment).
        let mut file = client.open("bucket", "cached.txt").unwrap();
        file.close().unwrap();
        let heads_after_open = backend.head_call_count();

        // Subsequent head calls should be served from the cache index — no new backend heads.
        let info = client.head("bucket", "cached.txt").unwrap();
        assert_eq!(info.size, data.len() as u64);

        let info2 = client.head("bucket", "cached.txt").unwrap();
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
async fn client_stage_write_upload_roundtrip_uploads_to_backend() {
    let root = test_root("staging-cache");
    let socket = test_root("staging.sock");
    let staging_uploader = Arc::new(StagingUploader::new(root.clone()));
    let staging_root = root.clone();

    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let config = crate::config::StorageServiceConfig::default();
    let service = Arc::new(StorageService::with_staging_uploader(
        registry,
        cache,
        staging_uploader,
        config,
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    let upload_info = tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(staging_root);
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let mut staging_file = StagingFile::create(
            &resolver,
            client.backend_identity(),
            "bucket",
            "uploaded.txt",
        )
        .unwrap();
        staging_file.write(b"hello ").unwrap();
        staging_file.write(b"upload").unwrap();
        staging_file.sync().unwrap();
        drop(staging_file);
        client.upload("bucket", "uploaded.txt").unwrap()
    })
    .await
    .unwrap();

    assert_eq!(upload_info.size, b"hello upload".len() as u64);
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "uploaded.txt").unwrap();
    let data = backend
        .get_range(key.path(), 0..upload_info.size)
        .await
        .unwrap();
    assert_eq!(&data[..], b"hello upload");

    server_task.abort();
}

#[tokio::test]
async fn caller_unlinks_staging_file_to_discard_without_upload() {
    // The database (caller) owns the staging directory: a transaction abort, or any other
    // reason to discard a staged file before upload, is implemented by unlinking the path
    // it created via `StagingFile::create`. The server has no abort verb.
    let root = test_root("staging-discard-cache");
    let socket = test_root("staging-discard.sock");
    let staging_uploader = Arc::new(StagingUploader::new(root.clone()));
    let staging_root = root.clone();

    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let config = crate::config::StorageServiceConfig::default();
    let service = Arc::new(StorageService::with_staging_uploader(
        registry,
        cache,
        staging_uploader,
        config,
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    let staging_path = tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(staging_root);
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let mut staging_file = StagingFile::create(
            &resolver,
            client.backend_identity(),
            "bucket",
            "discarded.txt",
        )
        .unwrap();
        staging_file.write(b"doomed").unwrap();
        let path = staging_file.path().to_path_buf();
        drop(staging_file);
        // Caller-side cleanup: ordinary filesystem unlink. Idempotent because the database is
        // the only writer of this path.
        std::fs::remove_file(&path).unwrap();
        path
    })
    .await
    .unwrap();

    assert!(
        !tokio::fs::try_exists(&staging_path).await.unwrap(),
        "caller-issued unlink must remove the staging file",
    );
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "discarded.txt").unwrap();
    assert!(
        backend.get_range(key.path(), 0..1).await.is_err(),
        "discarded key must not have been uploaded to the backend"
    );

    server_task.abort();
}

#[tokio::test]
async fn client_upload_can_be_issued_from_a_different_connection() {
    let root = test_root("staging-xconn-cache");
    let socket = test_root("staging-xconn.sock");
    let staging_uploader = Arc::new(StagingUploader::new(root.clone()));
    let staging_root = root.clone();

    let backend = Arc::new(MemoryObjectBackend::new());
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new();
    registry
        .register_shared_backend(TEST_STORE_ID, backend.clone())
        .unwrap();
    let config = crate::config::StorageServiceConfig::default();
    let service = Arc::new(StorageService::with_staging_uploader(
        registry,
        cache,
        staging_uploader,
        config,
    ));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(staging_root);
        let mut staging_file = StagingFile::create(
            &resolver,
            &test_identity(),
            "bucket",
            "cross-conn.txt",
        )
        .unwrap();
        staging_file.write(b"cross-connection upload").unwrap();
        drop(staging_file);
    })
    .await
    .unwrap();

    let socket_for_uploader = socket.clone();
    let upload_info = tokio::task::spawn_blocking(move || {
        let uploader =
            StorageClient::connect_managed(&socket_for_uploader, TEST_VOLUME_ID)
                .unwrap();
        uploader.upload("bucket", "cross-conn.txt").unwrap()
    })
    .await
    .unwrap();

    assert_eq!(upload_info.size, b"cross-connection upload".len() as u64);
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "cross-conn.txt").unwrap();
    let readback = backend
        .get_range(key.path(), 0..upload_info.size)
        .await
        .unwrap();
    assert_eq!(&readback[..], b"cross-connection upload");

    server_task.abort();
}

#[tokio::test]
async fn stage_twice_without_finalize_returns_busy() {
    let root = test_root("staging-busy-cache");
    tokio::task::spawn_blocking(move || {
        let resolver = StagingPathResolver::new(root);
        let _first = StagingFile::create(
            &resolver,
            &test_identity(),
            "bucket",
            "duplicate.txt",
        )
        .unwrap();
        let error = StagingFile::create(
            &resolver,
            &test_identity(),
            "bucket",
            "duplicate.txt",
        )
        .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Busy);
    })
    .await
    .unwrap();
}

#[test]
fn staging_file_holds_external_fd_lease_until_drop() {
    let root = test_root("staging-fd-accounting");
    let resolver = StagingPathResolver::new(root);
    let active_fds = Arc::new(AtomicUsize::new(0));
    let policy = CountingFdPolicy {
        active: Arc::clone(&active_fds),
        limit: None,
    };

    let staging = StagingFile::create_with_fd_policy(
        &resolver,
        &test_identity(),
        "bucket",
        "accounted.txt",
        &policy,
    )
    .unwrap();
    assert_eq!(active_fds.load(Ordering::SeqCst), 1);

    drop(staging);
    assert_eq!(active_fds.load(Ordering::SeqCst), 0);

    let error = StagingFile::create_with_fd_policy(
        &resolver,
        &test_identity(),
        "bucket",
        "accounted.txt",
        &policy,
    )
    .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Busy);
    assert_eq!(active_fds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_storage_file_closes_server_handle_best_effort() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "drop-close.txt").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghijklmnop".to_vec());

    let root = test_root("drop-close-cache");
    let socket = test_root("drop-close.sock");
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, Arc::new(backend))
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache.clone()));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let active_fds = Arc::new(AtomicUsize::new(0));
    let active_fds_for_client = Arc::clone(&active_fds);
    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect_managed_with_fd_policy(
            &client_socket,
            TEST_VOLUME_ID,
            Box::new(CountingFdPolicy {
                active: Arc::clone(&active_fds_for_client),
                limit: None,
            }),
        )
        .unwrap();
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 1);

        let mut file = client.open("bucket", "drop-close.txt").unwrap();
        let data = file.read(16).unwrap();
        assert_eq!(data, b"abcdefghijklmnop");
        file.close().unwrap();
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 1);

        let mut file = client.open("bucket", "drop-close.txt").unwrap();
        assert!(file.is_direct_io());
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 2);
        file.close().unwrap();
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 1);
        let error = file.read_at(0, 1).unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::ClosedHandle);
        drop(file);
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 1);

        drop(client);
        assert_eq!(active_fds_for_client.load(Ordering::SeqCst), 0);
    })
    .await
    .unwrap();
    assert_eq!(active_fds.load(Ordering::SeqCst), 0);

    let mut policy = CacheCleanupPolicy::new(16);
    policy.cleanup_start_ratio = 0.0;
    policy.cleanup_target_ratio = 0.0;
    let report = cache.cleanup_with_capacity(policy).await.unwrap();

    assert_eq!(report.evicted_objects, 1);
    assert_eq!(cache.logical_cache_usage().await.unwrap().resident_bytes, 0);
    assert!(
        !tokio::fs::try_exists(cache.complete_path(&key).unwrap())
            .await
            .unwrap()
    );

    let limited_fds = Arc::new(AtomicUsize::new(0));
    let limited_fds_for_client = Arc::clone(&limited_fds);
    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client = StorageClient::connect_managed_with_fd_policy(
            &client_socket,
            TEST_VOLUME_ID,
            Box::new(CountingFdPolicy {
                active: Arc::clone(&limited_fds_for_client),
                limit: Some(1),
            }),
        )
        .unwrap();
        let mut warming_file = client.open("bucket", "drop-close.txt").unwrap();
        assert!(!warming_file.is_direct_io());
        assert_eq!(warming_file.read(16).unwrap(), b"abcdefghijklmnop");
        warming_file.close().unwrap();

        let error = match client.open("bucket", "drop-close.txt") {
            Ok(_) => panic!("direct open unexpectedly exceeded the FD budget"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StorageErrorKind::ResourceExhausted);
        assert!(!client.is_usable());
        assert_eq!(limited_fds_for_client.load(Ordering::SeqCst), 0);
    })
    .await
    .unwrap();
    assert_eq!(limited_fds.load(Ordering::SeqCst), 0);

    server_task.abort();
}

#[tokio::test]
async fn panic_unwind_poisons_connection_and_allows_reconnect() {
    let key =
        ObjectLocation::new(TEST_STORE_ID, "bucket", "unwind-close.txt").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key, b"small object".to_vec());

    let root = test_root("unwind-close-cache");
    let socket = test_root("unwind-close.sock");
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(64, 64),
    );
    cache.spawn_large_fill_reaper();
    let registry = StoreRegistry::new()
        .with_shared_backend(TEST_STORE_ID, Arc::new(backend))
        .unwrap();
    let service = Arc::new(StorageService::with_registry(registry, cache));
    let server = StorageServer::bind_with_config(
        &socket,
        service,
        StorageServerConfig::default().with_max_open_handles_per_connection(1),
    )
    .await
    .unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _file = client.open("bucket", "unwind-close.txt").unwrap();
            panic!("injected error-report unwind");
        }));
        assert!(unwind.is_err());
        assert!(!client.is_usable());

        let replacement =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let mut reopened = replacement.open("bucket", "unwind-close.txt").unwrap();
        assert_eq!(reopened.read(12).unwrap(), b"small object");
        reopened.close().unwrap();
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn cached_bytes_are_shared_across_credentials_for_one_physical_identity() {
    let root = test_root("client-store-cache");
    let socket = test_root("client-store.sock");
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
    cache.spawn_large_fill_reaper();
    let config_a = StoreConfig::S3Compatible(S3CompatibleStoreConfig {
        endpoint: "http://127.0.0.1:9".to_string(),
        region: Some("us-east-1".to_string()),
        access_key_id: Some(SecretString::new("access-a")),
        secret_access_key: Some(SecretString::new("secret-a")),
        token: None,
        allow_http: true,
        virtual_hosted_style_request: false,
        skip_signature: false,
    });
    let config_b = StoreConfig::S3Compatible(S3CompatibleStoreConfig {
        access_key_id: Some(SecretString::new("access-b")),
        secret_access_key: Some(SecretString::new("secret-b")),
        ..match config_a.clone() {
            StoreConfig::S3Compatible(config) => config,
            _ => unreachable!(),
        }
    });
    let identity = BackendDataIdentity::from_config(&config_a);
    assert_eq!(identity, BackendDataIdentity::from_config(&config_b));
    let location = ObjectLocation::new(identity, "bucket", "file").unwrap();
    let cached_bytes = b"shared-cache-body".to_vec();
    cache
        .index()
        .admit_small_if_absent(
            CachedObjectMeta::small(
                location,
                ObjectInfo {
                    size: cached_bytes.len() as u64,
                    etag: Some("cached".to_string()),
                },
                cached_bytes.len() as u64,
            ),
            cached_bytes.clone(),
            1,
        )
        .await
        .unwrap();
    let service =
        Arc::new(StorageService::with_registry(StoreRegistry::new(), cache));
    let server = StorageServer::bind(&socket, service).await.unwrap();
    let server_task = tokio::spawn(async move {
        let _ = server.serve_forever().await;
    });

    let client_socket = socket.clone();
    tokio::task::spawn_blocking(move || {
        let first =
            StorageClient::connect_configured(&client_socket, Arc::new(config_a))
                .unwrap();
        let second =
            StorageClient::connect_configured(&client_socket, Arc::new(config_b))
                .unwrap();
        let mut first_file = first.open("bucket", "file").unwrap();
        let mut second_file = second.open("bucket", "file").unwrap();
        assert_eq!(first_file.read(64).unwrap(), cached_bytes);
        assert_eq!(second_file.read(64).unwrap(), cached_bytes);
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn read_at_returns_bytes_without_advancing_cursor() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "read-at.txt").unwrap();
    let data = b"abcdefghijklmnop";
    let backend = MemoryObjectBackend::new();
    backend.insert(key, data.to_vec());

    let root = test_root("read-at-cache");
    let socket = test_root("read-at.sock");
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
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
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let file = client.open("bucket", "read-at.txt").unwrap();
        assert_eq!(file.position(), 0);

        // read_at at offset 4 returns 4 bytes
        let chunk = file.read_at(4, 4).unwrap();
        assert_eq!(&chunk, b"efgh");

        // cursor did NOT advance
        assert_eq!(file.position(), 0);

        // read_at at offset 0 returns the beginning
        let chunk2 = file.read_at(0, 3).unwrap();
        assert_eq!(&chunk2, b"abc");
        assert_eq!(file.position(), 0);

        // read_at past EOF returns empty
        let empty = file.read_at(100, 10).unwrap();
        assert!(empty.is_empty());
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn read_at_into_fills_buffer_without_advancing_cursor() {
    let key =
        ObjectLocation::new(TEST_STORE_ID, "bucket", "read-at-into.txt").unwrap();
    let data = b"0123456789";
    let backend = MemoryObjectBackend::new();
    backend.insert(key, data.to_vec());

    let root = test_root("read-at-into-cache");
    let socket = test_root("read-at-into.sock");
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
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
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let file = client.open("bucket", "read-at-into.txt").unwrap();
        let mut buf = [0u8; 5];
        let n = file.read_at_into(3, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"34567");
        assert_eq!(file.position(), 0);

        // empty buffer returns 0
        let n = file.read_at_into(0, &mut []).unwrap();
        assert_eq!(n, 0);
    })
    .await
    .unwrap();

    server_task.abort();
}

#[tokio::test]
async fn read_at_and_cursor_read_are_independent() {
    let key =
        ObjectLocation::new(TEST_STORE_ID, "bucket", "read-at-indep.txt").unwrap();
    let data = b"ABCDEFGHIJ";
    let backend = MemoryObjectBackend::new();
    backend.insert(key, data.to_vec());

    let root = test_root("read-at-indep-cache");
    let socket = test_root("read-at-indep.sock");
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
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
        let client =
            StorageClient::connect_managed(&client_socket, TEST_VOLUME_ID).unwrap();
        let mut file = client.open("bucket", "read-at-indep.txt").unwrap();

        // Advance cursor to offset 5 via seek + cursor-based read
        file.seek(SeekFrom::Start(5));
        let cursor_data = file.read(3).unwrap();
        assert_eq!(&cursor_data, b"FGH");
        assert_eq!(file.position(), 8);

        // read_at at a different offset does not disturb the cursor
        let at_data = file.read_at(0, 3).unwrap();
        assert_eq!(&at_data, b"ABC");
        assert_eq!(file.position(), 8, "cursor must not move after read_at");

        // Continue cursor-based read from where it left off
        let more = file.read(2).unwrap();
        assert_eq!(&more, b"IJ");
        assert_eq!(file.position(), 10);
    })
    .await
    .unwrap();

    server_task.abort();
}

fn test_root(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    PathBuf::from("/tmp").join(format!("pss-{name}-{stamp}"))
}

fn complete_mock_attach(stream: &mut std::os::unix::net::UnixStream) {
    let frame = read_frame_blocking(stream).unwrap().unwrap();
    let request = decode_request(&frame).unwrap();
    assert!(matches!(
        request.payload,
        WireRequestPayload::AttachManaged { .. }
    ));
    let response = encode_response(&WireResponse {
        request_id: request.request_id,
        payload: WireResponsePayload::Attach {
            backend_identity: test_identity().cache_key().to_owned(),
        },
    })
    .unwrap();
    write_frame_blocking(stream, &response).unwrap();
}

#[test]
fn client_poisons_connection_after_response_id_mismatch() {
    let socket = test_root("poison-mismatched-response.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        complete_mock_attach(&mut stream);
        let _request = read_frame_blocking(&mut stream).unwrap().unwrap();
        let response = encode_response(&WireResponse {
            request_id: 99,
            payload: WireResponsePayload::Head {
                size: 1,
                etag: None,
            },
        })
        .unwrap();
        write_frame_blocking(&mut stream, &response).unwrap();
    });

    let active_fds = Arc::new(AtomicUsize::new(0));
    let client = StorageClient::connect_managed_with_fd_policy(
        &socket,
        TEST_VOLUME_ID,
        Box::new(CountingFdPolicy {
            active: Arc::clone(&active_fds),
            limit: None,
        }),
    )
    .unwrap();
    assert_eq!(active_fds.load(Ordering::SeqCst), 1);
    let error = client.head("bucket", "object").unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Protocol);
    assert!(!client.is_usable());
    assert_eq!(active_fds.load(Ordering::SeqCst), 0);

    let second = client.head("bucket", "object").unwrap_err();
    assert_eq!(second.kind(), StorageErrorKind::Protocol);
    server.join().unwrap();
}

#[test]
fn client_keeps_connection_after_remote_operation_error() {
    let socket = test_root("remote-error-keeps-connection.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        complete_mock_attach(&mut stream);
        let _first = read_frame_blocking(&mut stream).unwrap().unwrap();
        let not_found = encode_response(&WireResponse::error(
            2,
            StorageError::not_found("missing"),
        ))
        .unwrap();
        write_frame_blocking(&mut stream, &not_found).unwrap();

        let _second = read_frame_blocking(&mut stream).unwrap().unwrap();
        let found = encode_response(&WireResponse {
            request_id: 3,
            payload: WireResponsePayload::Head {
                size: 7,
                etag: None,
            },
        })
        .unwrap();
        write_frame_blocking(&mut stream, &found).unwrap();
    });

    let client = StorageClient::connect_managed(&socket, TEST_VOLUME_ID).unwrap();
    let error = client.head("bucket", "missing").unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::NotFound);
    assert!(client.is_usable());
    assert_eq!(client.head("bucket", "present").unwrap().size, 7);
    server.join().unwrap();
}

#[tokio::test]
async fn client_delete_removes_object_from_backend_and_is_idempotent() {
    let root = test_root("delete-cache");
    let socket = test_root("delete.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "doomed.txt").unwrap();
    backend.insert(key.clone(), b"bye".to_vec());
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
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
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();
        client.delete("bucket", "doomed.txt").unwrap();
        // Second delete is idempotent regardless of backend's missing-key behavior.
        client.delete("bucket", "doomed.txt").unwrap();
    })
    .await
    .unwrap();

    assert!(
        backend.get_range(key.path(), 0..1).await.is_err(),
        "deleted key must not survive in the backend"
    );

    server_task.abort();
}

#[tokio::test]
async fn client_delete_prefix_removes_all_matching_objects_and_rejects_empty_prefix()
{
    let root = test_root("delete-prefix-cache");
    let socket = test_root("delete-prefix.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    for key in ["scope/a", "scope/b", "scope/nested/c", "other/d"] {
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", key).unwrap(),
            b"x".to_vec(),
        );
    }
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
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
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();

        let empty_prefix_error = client.delete_prefix("bucket", "").unwrap_err();
        assert_eq!(empty_prefix_error.kind(), StorageErrorKind::InvalidPath);

        client.delete_prefix("bucket", "scope/").unwrap()
    })
    .await
    .unwrap();

    assert_eq!(deleted, 3);
    let surviving = ObjectLocation::new(TEST_STORE_ID, "bucket", "other/d").unwrap();
    assert!(
        backend.get_range(surviving.path(), 0..1).await.is_ok(),
        "delete_prefix must not touch keys outside the prefix"
    );
    for gone in ["scope/a", "scope/b", "scope/nested/c"] {
        let key = ObjectLocation::new(TEST_STORE_ID, "bucket", gone).unwrap();
        assert!(
            backend.get_range(key.path(), 0..1).await.is_err(),
            "key {gone} should have been deleted by delete_prefix",
        );
    }

    server_task.abort();
}

#[tokio::test]
async fn client_bulk_delete_and_explicit_cursor_close_are_end_to_end() {
    let root = test_root("bulk-delete-cache");
    let socket = test_root("bulk-delete.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    for key in ["scope/a", "scope/b", "scope/c"] {
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", key).unwrap(),
            b"x".to_vec(),
        );
    }
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
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
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();
        let first_page = client.list_page("bucket", Some("scope/"), None, 1).unwrap();
        let cursor = first_page.next_cursor.expect("more objects must remain");
        client.close_list_cursor(cursor.clone()).unwrap();
        let closed = client
            .list_page("bucket", Some("scope/"), Some(cursor), 1)
            .unwrap_err();
        assert_eq!(closed.kind(), StorageErrorKind::ExpiredCursor);

        let deleted = client
            .delete_objects(
                "bucket",
                vec!["scope/a".to_owned(), "scope/b".to_owned()],
            )
            .unwrap();
        assert_eq!(deleted, 2);
    })
    .await
    .unwrap();

    for gone in ["scope/a", "scope/b"] {
        let key = ObjectLocation::new(TEST_STORE_ID, "bucket", gone).unwrap();
        assert!(backend.get_range(key.path(), 0..1).await.is_err());
    }
    let survivor = ObjectLocation::new(TEST_STORE_ID, "bucket", "scope/c").unwrap();
    assert!(backend.get_range(survivor.path(), 0..1).await.is_ok());

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
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
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
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();
        let mut keys: Vec<String> = client
            .list("bucket", Some("scope/"))
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
async fn client_list_session_owns_connection_and_drives_pages() {
    let root = test_root("list-page-cache");
    let socket = test_root("list-page.sock");
    let backend = Arc::new(MemoryObjectBackend::new());
    for i in 0..5 {
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", format!("k/{i}")).unwrap(),
            b"x".to_vec(),
        );
    }
    let cache = Arc::new(
        CacheManager::new(
            root,
            InMemoryCacheIndex::new(),
            StorageRuntime::new(StorageRuntimeConfig::default()).unwrap(),
        )
        .with_limits(4, 4),
    );
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
        let client =
            StorageClient::connect_managed(&socket_for_client, TEST_VOLUME_ID)
                .unwrap();
        let mut listing = client.list_session("bucket", Some("k/"), 2);
        drop(client);

        let page1 = listing.next_page().unwrap().expect("first page must exist");
        assert_eq!(page1.len(), 2);

        let page2 = listing
            .next_page()
            .unwrap()
            .expect("second page must exist");
        assert_eq!(page2.len(), 2);

        let page3 = listing.next_page().unwrap().expect("final page must exist");
        assert_eq!(page3.len(), 1);
        assert!(listing.next_page().unwrap().is_none());
    })
    .await
    .unwrap();

    server_task.abort();
}
