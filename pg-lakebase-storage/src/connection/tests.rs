use std::future;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::{sleep, timeout};

use crate::backend::{MemoryObjectBackend, ObjectBackend, StoreRegistry};
use crate::cache::{CacheManager, InMemoryCacheIndex};
use crate::config::{StorageRuntime, StorageRuntimeConfig, StorageServerConfig};
use crate::error::StorageResult;
use crate::handle::OpenFlags;
use crate::object::{ObjectInfo, ObjectLocation, ObjectPath};
use crate::protocol::{
    WireRequest, WireRequestPayload, WireResponsePayload, decode_response,
    encode_request,
};
use crate::service::StorageService;
use crate::service::reply::ReadBody;
use crate::transport::{read_frame, write_frame};

use super::dispatch::{StorageHandlerPayload, StorageHandlerResponse};
use super::pipeline::process_connection_with_shutdown;
use super::request_tasks::RequestTasks;
use super::response_budget::{QueuedResponse, ResponseByteLimiter};
use super::shutdown::ConnectionShutdown;
use super::writer::ResponseWriter;

static TEST_WIRE_ID: AtomicU64 = AtomicU64::new(0);
const TEST_STORE_ID: &str = "test-store";

#[tokio::test]
async fn disconnect_aborts_in_flight_request_tasks() {
    let (server_stream, mut client_stream) = UnixStream::pair().unwrap();
    let (started_send, started_recv) = oneshot::channel();
    let (dropped_send, dropped_recv) = oneshot::channel();
    let backend = HangingBackend {
        started: Arc::new(Mutex::new(Some(started_send))),
        dropped: Arc::new(Mutex::new(Some(dropped_send))),
    };
    let cache = Arc::new(
        CacheManager::new(
            test_cache_dir(),
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
    let context = service.test_context();
    let server_task = tokio::spawn(process_connection_with_shutdown(
        server_stream,
        context,
        test_server_config(1),
        test_shutdown(Duration::from_millis(50)),
    ));

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 1,
            payload: WireRequestPayload::Open {
                bucket: "bucket".to_string(),
                key: "file".to_string(),
                flags: OpenFlags::READ_ONLY,
            },
        },
    )
    .await;
    let handle = match read_response(&mut client_stream).await {
        WireResponsePayload::Open { handle, .. } => handle,
        other => panic!("unexpected open response: {other:?}"),
    };

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 2,
            payload: WireRequestPayload::Read {
                handle,
                offset: 0,
                len: 4,
            },
        },
    )
    .await;

    timeout(Duration::from_secs(1), started_recv)
        .await
        .unwrap()
        .unwrap();
    drop(client_stream);
    timeout(Duration::from_secs(1), dropped_recv)
        .await
        .unwrap()
        .unwrap();
    let result = timeout(Duration::from_secs(1), server_task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn write_half_shutdown_still_receives_in_flight_response() {
    let (server_stream, mut client_stream) = UnixStream::pair().unwrap();
    let (started_send, started_recv) = oneshot::channel();
    let (release_send, release_recv) = oneshot::channel();
    let backend = DelayedHeadBackend {
        started: Mutex::new(Some(started_send)),
        release: Mutex::new(Some(release_recv)),
    };
    let cache = Arc::new(
        CacheManager::new(
            test_cache_dir(),
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
    let context = service.test_context();
    let server_task = tokio::spawn(process_connection_with_shutdown(
        server_stream,
        context,
        test_server_config(1),
        test_shutdown(Duration::from_secs(1)),
    ));

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 1,
            payload: WireRequestPayload::Open {
                bucket: "bucket".to_string(),
                key: "file".to_string(),
                flags: OpenFlags::READ_ONLY,
            },
        },
    )
    .await;
    timeout(Duration::from_secs(1), started_recv)
        .await
        .unwrap()
        .unwrap();
    client_stream.shutdown().await.unwrap();
    release_send.send(()).unwrap();

    match timeout(Duration::from_secs(1), read_response(&mut client_stream))
        .await
        .unwrap()
    {
        WireResponsePayload::Open {
            size, direct_io, ..
        } => {
            assert_eq!(size, 10);
            assert!(!direct_io);
        }
        other => panic!("unexpected open response: {other:?}"),
    }

    let result = timeout(Duration::from_secs(1), server_task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn close_waits_for_prior_read_on_same_handle() {
    let (server_stream, mut client_stream) = UnixStream::pair().unwrap();
    let (started_send, started_recv) = oneshot::channel();
    let (release_send, release_recv) = oneshot::channel();
    let backend = DelayedRangeBackend {
        started: Mutex::new(Some(started_send)),
        release: Mutex::new(Some(release_recv)),
    };
    let cache = Arc::new(
        CacheManager::new(
            test_cache_dir(),
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
    let context = service.test_context();
    let server_task = tokio::spawn(process_connection_with_shutdown(
        server_stream,
        context,
        test_server_config(2),
        test_shutdown(Duration::from_secs(1)),
    ));

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 1,
            payload: WireRequestPayload::Open {
                bucket: "bucket".to_string(),
                key: "file".to_string(),
                flags: OpenFlags::READ_ONLY,
            },
        },
    )
    .await;
    let handle = match read_response(&mut client_stream).await {
        WireResponsePayload::Open { handle, .. } => handle,
        other => panic!("unexpected open response: {other:?}"),
    };

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 2,
            payload: WireRequestPayload::Read {
                handle,
                offset: 0,
                len: 4,
            },
        },
    )
    .await;
    timeout(Duration::from_secs(1), started_recv)
        .await
        .unwrap()
        .unwrap();

    send_request(
        &mut client_stream,
        WireRequest {
            request_id: 3,
            payload: WireRequestPayload::Close { handle },
        },
    )
    .await;
    assert!(
        timeout(Duration::from_millis(30), read_frame(&mut client_stream))
            .await
            .is_err()
    );

    release_send.send(()).unwrap();
    match timeout(Duration::from_secs(1), read_response(&mut client_stream))
        .await
        .unwrap()
    {
        WireResponsePayload::Read { data, eof } => {
            assert_eq!(data, b"abcd");
            assert!(!eof);
        }
        other => panic!("unexpected first response: {other:?}"),
    }
    assert!(matches!(
        timeout(Duration::from_secs(1), read_response(&mut client_stream))
            .await
            .unwrap(),
        WireResponsePayload::Close
    ));

    drop(client_stream);
    let result = timeout(Duration::from_secs(1), server_task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn read_admission_precedes_response_budget_wait() {
    let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "file").unwrap();
    let backend = MemoryObjectBackend::new();
    backend.insert(key.clone(), b"abcdefghij".to_vec());
    let cache = Arc::new(
        CacheManager::new(
            test_cache_dir(),
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
    let context = service.test_context();
    let backend = context.attached().unwrap().backend();
    let state = context
        .handles
        .open(
            key,
            backend,
            ObjectInfo {
                size: 10,
                etag: None,
            },
            OpenFlags::READ_ONLY,
        )
        .unwrap();
    let mut tasks = RequestTasks::new();
    let request_limiter = Arc::new(Semaphore::new(2));
    let response_byte_limiter = ResponseByteLimiter::new(0);
    let (response_tx, mut response_rx) = mpsc::channel::<QueuedResponse>(2);

    tasks
        .spawn_request(
            WireRequest {
                request_id: 1,
                payload: WireRequestPayload::Read {
                    handle: state.handle,
                    offset: 0,
                    len: 4,
                },
            },
            context.clone(),
            request_limiter.clone(),
            response_byte_limiter.clone(),
            4,
            response_tx.clone(),
        )
        .await
        .unwrap();
    tasks
        .spawn_request(
            WireRequest {
                request_id: 2,
                payload: WireRequestPayload::Close {
                    handle: state.handle,
                },
            },
            context,
            request_limiter,
            response_byte_limiter,
            4,
            response_tx,
        )
        .await
        .unwrap();

    assert!(
        timeout(Duration::from_millis(50), response_rx.recv())
            .await
            .is_err()
    );
    tasks.abort_all().await;
}

#[tokio::test]
async fn inbound_close_uses_one_total_drain_budget() {
    let (response_tx, _response_rx) = mpsc::channel::<QueuedResponse>(1);
    let mut request_tasks = RequestTasks::new();
    request_tasks.spawn_background(async {
        sleep(Duration::from_millis(60)).await;
    });
    let writer_task =
        tokio::spawn(async { future::pending::<StorageResult<()>>().await });
    let mut writer = ResponseWriter::from_parts_for_test(response_tx, writer_task);

    let result = timeout(
        Duration::from_millis(120),
        test_shutdown(Duration::from_millis(80)).drain_on_inbound_closed(
            &mut request_tasks,
            &mut writer,
            "test-client",
        ),
    )
    .await
    .expect("shutdown should not get a second drain timeout");

    assert!(result.is_ok());
}

#[tokio::test]
async fn response_byte_limiter_waits_until_permit_is_released() {
    let limiter = ResponseByteLimiter::new(4);
    let first = limiter.acquire(4).await.unwrap();

    assert!(
        timeout(Duration::from_millis(20), limiter.acquire(1))
            .await
            .is_err()
    );

    drop(first);
    timeout(Duration::from_secs(1), limiter.acquire(4))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queued_read_response_holds_byte_budget_until_dropped() {
    let limiter = ResponseByteLimiter::new(4);
    let response_bytes = limiter.acquire(4).await.unwrap();
    let queued = QueuedResponse::new(
        StorageHandlerResponse {
            request_id: 1,
            payload: StorageHandlerPayload::Read {
                body: ReadBody::Bytes(bytes::Bytes::from_static(&[1, 2, 3, 4])),
                eof: false,
            },
            direct_file: None,
        },
        response_bytes,
    );

    assert!(
        timeout(Duration::from_millis(20), limiter.acquire(1))
            .await
            .is_err()
    );

    drop(queued);
    timeout(Duration::from_secs(1), limiter.acquire(4))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queued_non_read_response_releases_reserved_read_budget() {
    let limiter = ResponseByteLimiter::new(4);
    let response_bytes = limiter.acquire(4).await.unwrap();
    let _queued = QueuedResponse::new(
        StorageHandlerResponse {
            request_id: 1,
            payload: StorageHandlerPayload::Wire(WireResponsePayload::Close),
            direct_file: None,
        },
        response_bytes,
    );

    timeout(Duration::from_secs(1), limiter.acquire(4))
        .await
        .unwrap()
        .unwrap();
}

async fn send_request(stream: &mut UnixStream, request: WireRequest) {
    let frame = encode_request(&request).unwrap();
    write_frame(stream, &frame).await.unwrap();
}

async fn read_response(stream: &mut UnixStream) -> WireResponsePayload {
    let frame = read_frame(stream).await.unwrap().unwrap();
    decode_response(&frame).unwrap().into_result().unwrap()
}

struct HangingBackend {
    started: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    dropped: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[async_trait]
impl ObjectBackend for HangingBackend {
    async fn head(&self, _key: &ObjectPath) -> StorageResult<ObjectInfo> {
        Ok(ObjectInfo {
            size: 10,
            etag: None,
        })
    }

    async fn get_range(
        &self,
        _key: &ObjectPath,
        _range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        if let Some(started) =
            self.started.lock().expect("started mutex poisoned").take()
        {
            let _ = started.send(());
        }
        let _guard = DropSignal {
            dropped: self.dropped.clone(),
        };
        future::pending::<StorageResult<bytes::Bytes>>().await
    }

    async fn put_from_file(
        &self,
        _key: &ObjectPath,
        _path: &std::path::Path,
        _len: u64,
    ) -> StorageResult<ObjectInfo> {
        unreachable!("HangingBackend does not participate in staging uploads")
    }

    fn list(
        &self,
        _bucket: &str,
        _prefix: Option<&str>,
    ) -> futures::stream::BoxStream<'static, StorageResult<crate::object::ListEntry>>
    {
        unreachable!("HangingBackend does not participate in list")
    }

    async fn delete(&self, _key: &ObjectPath) -> StorageResult<()> {
        unreachable!("HangingBackend does not participate in delete")
    }

    fn delete_stream(
        &self,
        _bucket: &str,
        _keys: futures::stream::BoxStream<'static, StorageResult<String>>,
    ) -> futures::stream::BoxStream<'static, StorageResult<String>> {
        unreachable!("HangingBackend does not participate in delete_stream")
    }
}

struct DropSignal {
    dropped: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(dropped) =
            self.dropped.lock().expect("dropped mutex poisoned").take()
        {
            let _ = dropped.send(());
        }
    }
}

struct DelayedHeadBackend {
    started: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

struct DelayedRangeBackend {
    started: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait]
impl ObjectBackend for DelayedHeadBackend {
    async fn head(&self, _key: &ObjectPath) -> StorageResult<ObjectInfo> {
        if let Some(started) =
            self.started.lock().expect("started mutex poisoned").take()
        {
            let _ = started.send(());
        }
        let release = self.release.lock().expect("release mutex poisoned").take();
        if let Some(release) = release {
            let _ = release.await;
        }
        Ok(ObjectInfo {
            size: 10,
            etag: None,
        })
    }

    async fn get_range(
        &self,
        _key: &ObjectPath,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        Ok(bytes::Bytes::from(vec![
            0;
            (range.end - range.start) as usize
        ]))
    }

    async fn put_from_file(
        &self,
        _key: &ObjectPath,
        _path: &std::path::Path,
        _len: u64,
    ) -> StorageResult<ObjectInfo> {
        unreachable!("DelayedHeadBackend does not participate in staging uploads")
    }

    fn list(
        &self,
        _bucket: &str,
        _prefix: Option<&str>,
    ) -> futures::stream::BoxStream<'static, StorageResult<crate::object::ListEntry>>
    {
        unreachable!("DelayedHeadBackend does not participate in list")
    }

    async fn delete(&self, _key: &ObjectPath) -> StorageResult<()> {
        unreachable!("DelayedHeadBackend does not participate in delete")
    }

    fn delete_stream(
        &self,
        _bucket: &str,
        _keys: futures::stream::BoxStream<'static, StorageResult<String>>,
    ) -> futures::stream::BoxStream<'static, StorageResult<String>> {
        unreachable!("DelayedHeadBackend does not participate in delete_stream")
    }
}

#[async_trait]
impl ObjectBackend for DelayedRangeBackend {
    async fn head(&self, _key: &ObjectPath) -> StorageResult<ObjectInfo> {
        Ok(ObjectInfo {
            size: 10,
            etag: None,
        })
    }

    async fn get_range(
        &self,
        _key: &ObjectPath,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        if let Some(started) =
            self.started.lock().expect("started mutex poisoned").take()
        {
            let _ = started.send(());
        }
        let release = self.release.lock().expect("release mutex poisoned").take();
        if let Some(release) = release {
            let _ = release.await;
        }
        Ok(bytes::Bytes::copy_from_slice(
            &b"abcdefghij"[range.start as usize..range.end as usize],
        ))
    }

    async fn put_from_file(
        &self,
        _key: &ObjectPath,
        _path: &std::path::Path,
        _len: u64,
    ) -> StorageResult<ObjectInfo> {
        unreachable!("DelayedRangeBackend does not participate in staging uploads")
    }

    fn list(
        &self,
        _bucket: &str,
        _prefix: Option<&str>,
    ) -> futures::stream::BoxStream<'static, StorageResult<crate::object::ListEntry>>
    {
        unreachable!("DelayedRangeBackend does not participate in list")
    }

    async fn delete(&self, _key: &ObjectPath) -> StorageResult<()> {
        unreachable!("DelayedRangeBackend does not participate in delete")
    }

    fn delete_stream(
        &self,
        _bucket: &str,
        _keys: futures::stream::BoxStream<'static, StorageResult<String>>,
    ) -> futures::stream::BoxStream<'static, StorageResult<String>> {
        unreachable!("DelayedRangeBackend does not participate in delete_stream")
    }
}

fn test_shutdown(drain_timeout: Duration) -> ConnectionShutdown {
    ConnectionShutdown { drain_timeout }
}

fn test_server_config(max_in_flight_requests: usize) -> StorageServerConfig {
    StorageServerConfig::default()
        .with_max_in_flight_requests(max_in_flight_requests)
        .without_response_write_timeout()
}

fn test_cache_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = TEST_WIRE_ID.fetch_add(1, Ordering::Relaxed);
    PathBuf::from("/tmp").join(format!(
        "pg-lakebase-storage-wire-test-{}-{stamp}-{id}",
        std::process::id()
    ))
}
