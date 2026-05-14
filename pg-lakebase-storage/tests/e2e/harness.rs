//! Shared E2E test fixtures: [`MinioFixture`], [`ServerFixture`], and [`E2eHarness`].
//!
//! Each fixture owns its resources and cleans up via RAII / `Drop`:
//!
//! - [`MinioFixture`]: MinIO Docker container lifetime.
//! - [`ServerFixture`]: server task abort + [`tempfile::TempDir`] cleanup (cache, socket, db).
//! - [`E2eHarness`]: composes both; drop order is server-first, then MinIO.
//!
//! # Cache index coverage
//!
//! [`CacheIndexKind`] is an explicit harness dimension. Fast broad-path tests use
//! [`CacheIndexKind::InMemory`], while production-wiring tests use [`CacheIndexKind::Redb`] for
//! read, staging/commit, delete, concurrent access, and restart recovery.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use aws_sdk_s3::Client as AwsS3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use pg_lakebase_storage::{
    InMemoryCacheIndex, S3CompatibleStoreConfig, SecretString, StorageClient,
    StorageServerBuilder, StorageServiceConfig, StoreConfig,
};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub const MINIO_USER: &str = "minioadmin";
pub const MINIO_PASSWORD: &str = "minioadmin";
pub const TEST_BUCKET: &str = "test-bucket";
pub const STORE_ID: &str = "minio";

/// Cache index implementation used by an E2E server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheIndexKind {
    /// Fast, process-local test index.
    InMemory,
    /// Production default persistent index backed by redb.
    Redb,
}

// ---------------------------------------------------------------------------
// MinioFixture
// ---------------------------------------------------------------------------

/// Running MinIO container, bucket lifecycle, and data seeding.
pub struct MinioFixture {
    endpoint: String,
    _container: ContainerAsync<GenericImage>,
}

impl MinioFixture {
    /// Starts a MinIO container.
    ///
    /// Uses [`GenericImage`] instead of the testcontainers-modules MinIO helper
    /// because the upstream module waits on stderr while this MinIO release
    /// emits the ready marker on stdout.
    pub async fn start() -> Self {
        let container =
            GenericImage::new("minio/minio", "RELEASE.2024-01-16T16-07-38Z")
                .with_exposed_port(ContainerPort::Tcp(9000))
                .with_wait_for(WaitFor::message_on_stdout("S3-API:"))
                .with_env_var("MINIO_ROOT_USER", MINIO_USER)
                .with_env_var("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
                .with_cmd(["server".to_string(), "/data".to_string()])
                .start()
                .await
                .expect("failed to start MinIO container — is Docker running?");

        let host = container.get_host().await.expect("MinIO host");
        let port = container
            .get_host_port_ipv4(9000)
            .await
            .expect("MinIO port");
        let endpoint = format!("http://{}:{}", host, port);

        Self {
            endpoint,
            _container: container,
        }
    }

    #[allow(dead_code)]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Creates a bucket inside the running container.
    pub async fn create_bucket(&self, bucket: &str) {
        self.s3_client()
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .unwrap_or_else(|error| {
                panic!("failed to create bucket {bucket} via S3 API: {error}")
            });
    }

    /// Uploads a test object directly to MinIO (bypasses the storage server).
    pub async fn seed_object(&self, key: &str, data: &[u8]) {
        let store = self.object_store();
        let payload = bytes::Bytes::copy_from_slice(data);
        store
            .put(&ObjectStorePath::from(key), payload.into())
            .await
            .unwrap_or_else(|e| panic!("seed_object({key}): {e}"));
    }

    /// Builds the [`StoreConfig`] that points at this MinIO instance.
    pub fn store_config(&self) -> StoreConfig {
        StoreConfig::S3Compatible(S3CompatibleStoreConfig {
            endpoint: self.endpoint.clone(),
            region: Some("us-east-1".to_string()),
            access_key_id: Some(SecretString::new(MINIO_USER)),
            secret_access_key: Some(SecretString::new(MINIO_PASSWORD)),
            token: None,
            allow_http: true,
            virtual_hosted_style_request: false,
            skip_signature: false,
        })
    }

    fn object_store(&self) -> Box<dyn ObjectStore> {
        Box::new(
            AmazonS3Builder::new()
                .with_bucket_name(TEST_BUCKET)
                .with_endpoint(&self.endpoint)
                .with_region("us-east-1")
                .with_access_key_id(MINIO_USER)
                .with_secret_access_key(MINIO_PASSWORD)
                .with_allow_http(true)
                .build()
                .expect("failed to build object_store S3 client"),
        )
    }

    fn s3_client(&self) -> AwsS3Client {
        let config = aws_sdk_s3::config::Builder::default()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                MINIO_USER,
                MINIO_PASSWORD,
                None,
                None,
                "minio-e2e",
            ))
            .endpoint_url(self.endpoint.clone())
            .force_path_style(true)
            .build();
        AwsS3Client::from_conf(config)
    }
}

// ---------------------------------------------------------------------------
// ServerFixture
// ---------------------------------------------------------------------------

struct ServerWorkspace {
    _dir: tempfile::TempDir,
    cache_dir: PathBuf,
    socket_path: PathBuf,
}

impl ServerWorkspace {
    fn new() -> Arc<Self> {
        let dir = tempfile::Builder::new()
            .prefix("lfsb-e2e-")
            .tempdir()
            .expect("failed to create temp directory");
        let cache_dir = dir.path().join("cache");
        let socket_path = dir.path().join("server.sock");
        Arc::new(Self {
            _dir: dir,
            cache_dir,
            socket_path,
        })
    }
}

/// Running storage server with RAII-managed temp workspace (cache dir + socket).
pub struct ServerFixture {
    kind: CacheIndexKind,
    workspace: Arc<ServerWorkspace>,
    server_task: Option<tokio::task::JoinHandle<()>>,
}

impl ServerFixture {
    /// Starts a server backed by the selected cache index.
    pub async fn start_with_index(kind: CacheIndexKind) -> Self {
        let workspace = ServerWorkspace::new();
        let task = Self::spawn_server(kind, &workspace).await;
        Self {
            kind,
            workspace,
            server_task: Some(task),
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.workspace.socket_path
    }

    pub fn connect(&self) -> StorageClient {
        StorageClient::connect(&self.workspace.socket_path)
            .expect("failed to connect client")
    }

    /// Restarts the server in the same workspace, preserving cache files and redb state.
    pub async fn restart(&mut self) {
        self.shutdown().await;
        self.server_task = Some(Self::spawn_server(self.kind, &self.workspace).await);
    }

    async fn shutdown(&mut self) {
        if let Some(task) = self.server_task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    async fn spawn_server(
        kind: CacheIndexKind,
        workspace: &ServerWorkspace,
    ) -> tokio::task::JoinHandle<()> {
        let socket_path = workspace.socket_path.clone();
        let cache_dir = workspace.cache_dir.clone();
        let builder = StorageServerBuilder::new(&socket_path, &cache_dir)
            .with_service_config(
                StorageServiceConfig::default().with_cache_limits(4, 4),
            );

        match kind {
            CacheIndexKind::InMemory => {
                let server = builder
                    .bind_with_index(InMemoryCacheIndex::new())
                    .await
                    .expect("failed to bind server (in-memory index)");
                tokio::spawn(async move {
                    let _ = server.serve_forever().await;
                })
            }
            CacheIndexKind::Redb => {
                let server = builder
                    .bind()
                    .await
                    .expect("failed to bind server (redb index)");
                tokio::spawn(async move {
                    let _ = server.serve_forever().await;
                })
            }
        }
    }
}

impl Drop for ServerFixture {
    fn drop(&mut self) {
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// E2eHarness
// ---------------------------------------------------------------------------

/// Full E2E harness: MinIO container + storage server + store registered via
/// the client wire protocol (`RegisterStore` RPC).
///
/// Drop order: server first (abort task, clean temp dir), then MinIO container.
pub struct E2eHarness {
    server: ServerFixture,
    minio: MinioFixture,
}

impl E2eHarness {
    /// Starts MinIO + an in-memory-index server and registers the store via the
    /// client wire path.
    pub async fn start() -> Self {
        Self::start_with_index(CacheIndexKind::InMemory).await
    }

    /// Same as [`Self::start`] but uses the production [`RedbCacheIndex`].
    pub async fn start_with_redb() -> Self {
        Self::start_with_index(CacheIndexKind::Redb).await
    }

    /// Starts MinIO + a server backed by the selected cache index.
    pub async fn start_with_index(kind: CacheIndexKind) -> Self {
        let minio = MinioFixture::start().await;
        minio.create_bucket(TEST_BUCKET).await;
        let server = ServerFixture::start_with_index(kind).await;
        Self::register_store_via_wire(&server, &minio).await;
        Self { server, minio }
    }

    /// Restarts the storage server in-place and re-registers the MinIO store through the wire path.
    pub async fn restart_server(&mut self) {
        self.server.restart().await;
        Self::register_store_via_wire(&self.server, &self.minio).await;
    }

    /// Registers the MinIO store through the `RegisterStore` wire protocol —
    /// the same path production clients use.
    async fn register_store_via_wire(server: &ServerFixture, minio: &MinioFixture) {
        let socket = server.socket_path().to_path_buf();
        let config = minio.store_config();
        tokio::task::spawn_blocking(move || {
            let client = StorageClient::connect(&socket).unwrap();
            client.register_store(STORE_ID, config).unwrap();
        })
        .await
        .expect("register_store via wire panicked");
    }

    /// Creates a blocking [`StorageClient`] connected to the server.
    pub fn connect(&self) -> StorageClient {
        self.server.connect()
    }

    /// Socket path (for concurrent tests that open many connections).
    pub fn socket_path(&self) -> &Path {
        self.server.socket_path()
    }

    /// Uploads an object directly to MinIO (bypasses the storage server).
    pub async fn seed_object(&self, key: &str, data: &[u8]) {
        self.minio.seed_object(key, data).await;
    }
}
