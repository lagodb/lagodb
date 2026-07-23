#![cfg_attr(feature = "strict", deny(warnings))]

//! Object cache, Unix streaming protocol, and a minimal storage service façade (`open`/`read`/…).

pub mod backend;
pub mod builder;
pub mod cache;
pub mod client;
pub mod config;
pub mod connection;
pub mod error;
pub mod handle;
pub mod object;
pub mod protocol;
pub mod request;
pub mod server;
pub mod service;
pub mod session;
pub mod staging;
pub mod transport;

pub use backend::{
    AzureStoreConfig, ConfiguredObjectBackend, GcsStoreConfig, MemoryObjectBackend,
    ObjectBackend, ObjectStoreBackend, RegisteredStore, S3CompatibleStoreConfig,
    S3StoreConfig, SecretString, StorageProbeResult, StoreConfig, StoreRegistry,
};
pub use builder::StorageServerBuilder;
pub use cache::{
    CacheCleanupPolicy, CacheCleanupReport, CacheDeleteReason, CacheIndex,
    CacheInvalidateReport, CacheManager, CachePathResolver, CachePurgeReport,
    CacheRecoveryReport, CacheState, CacheStore, CacheStoreKind, CacheUsageSnapshot,
    CachedObjectMeta, InMemoryCacheIndex, LogicalCacheUsage, PhysicalCacheEntry,
    PhysicalCacheId, PhysicalCacheStat, PhysicalCacheUsage, RedbCacheIndex,
};
pub use client::{
    DEFAULT_CLIENT_CLEANUP_TIMEOUT, ExternalFdLease, ExternalFdPolicy, ListIter,
    ListPage, SeekFrom, SocketInterest, SocketWait, SocketWaitContext, StagingFile,
    StorageClient, StorageClientBuilder, StorageFile, UploadInfo,
};
pub use config::{
    CacheCleanupConfig, CacheRuntimeConfig, DEFAULT_CACHE_CLEANUP_BATCH_BYTES,
    DEFAULT_CACHE_CLEANUP_BATCH_ITEMS, DEFAULT_CACHE_CLEANUP_INTERVAL,
    DEFAULT_CACHE_CLEANUP_START_PERCENT, DEFAULT_CACHE_CLEANUP_TARGET_PERCENT,
    DEFAULT_CACHE_TOUCH_GRANULARITY, DEFAULT_CONNECTION_DRAIN_TIMEOUT,
    DEFAULT_CONNECTION_DRAIN_TIMEOUT_MS, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_IN_FLIGHT_REQUESTS, DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION,
    DEFAULT_MAX_PENDING_RESPONSE_BYTES, DEFAULT_MAX_PENDING_RESPONSES,
    DEFAULT_MAX_READ_SIZE, DEFAULT_RESPONSE_WRITE_TIMEOUT, RuntimeApplyReport,
    StorageRuntime, StorageRuntimeConfig, StorageServerConfig, StorageServiceConfig,
};
pub use error::{StorageError, StorageErrorKind, StorageResult};
pub use handle::{FileHandle, OpenFileState, OpenFlags};
pub use object::{
    DEFAULT_CHUNK_SIZE, DEFAULT_SMALL_OBJECT_LIMIT, ListEntry, ObjectInfo,
    ObjectLocation, StoreId,
};
pub use protocol::ListCursor;
pub use request::{
    NoopRequestObserver, NoopRequestPolicy, OperationMeta, RequestContext,
    RequestHooks, RequestObserver, RequestOperation, RequestOutcome, RequestPolicy,
    RequestStatus, TracingRequestObserver,
};
pub use server::StorageServer;
pub use service::{LIST_CURSOR_IDLE_TTL_MS, StorageService};
pub use session::StorageContext;
pub use staging::StagingPathResolver;
pub use tokio_util::sync::CancellationToken;
