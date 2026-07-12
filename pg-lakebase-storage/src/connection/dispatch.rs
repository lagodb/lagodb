//! Dispatches a decoded [`WireRequest`] through the service layer into a queued response.
//!
//! [`RequestDispatcher`] owns the admission step (e.g. read-handle guards must be taken in wire
//! order, before any async await, so a later CLOSE cannot race ahead and invalidate the handle).
//! Wire↔command and output→wire conversions are plain `From` impls on the respective types so the
//! direction of data flow is clear without reading a dispatcher helper.

use std::time::Instant;

use crate::cache::CacheIndex;
use crate::error::StorageError;
use crate::protocol::{
    WireRequest, WireRequestPayload, WireResponse, WireResponsePayload,
};
use crate::request::{RequestContext, RequestOutcome};
use crate::service::command::{
    CloseCommand, CloseListCommand, DeleteCommand, DeleteObjectsCommand,
    DeletePrefixCommand, HeadCommand, InvalidateObjectCacheCommand, ListCommand,
    OpenCommand, PurgeStoreCacheCommand, ReadCommand, RegisterStoreCommand,
    StorageCommand, UnregisterStoreCommand, UploadCommand,
};
use crate::service::reply::{
    CommandOutput, ReadBody, ResponseAttachment, ServiceReply,
};
use crate::session::StorageContext;
use crate::session::handle_table::ReadHandleGuard;

// ---- response envelope that the writer task consumes -------------------------------------------

pub(crate) struct StorageHandlerResponse {
    pub(crate) request_id: u64,
    pub(crate) payload: StorageHandlerPayload,
    pub(crate) direct_file: Option<std::fs::File>,
}

#[derive(Debug)]
pub(crate) enum StorageHandlerPayload {
    /// Fully-structured response that is encoded through the wire codec.
    Wire(WireResponsePayload),
    /// Streaming READ body emitted directly by the writer (bytes or a file range) without passing
    /// through the codec.
    Read { body: ReadBody, eof: bool },
}

impl StorageHandlerResponse {
    pub(crate) fn request_id(&self) -> u64 {
        self.request_id
    }

    pub(crate) fn read_body_len(&self) -> Option<usize> {
        match &self.payload {
            StorageHandlerPayload::Read { body, .. } => Some(body.len()),
            StorageHandlerPayload::Wire(_) => None,
        }
    }
}

// ---- dispatcher --------------------------------------------------------------------------------

/// Dispatches a single request.
///
/// The dispatcher is built in two steps so admission can be run synchronously on the connection's
/// read loop, separately from the async `dispatch` future that is usually spawned onto a task. See
/// [`RequestDispatcher::admit`] for the ordering contract.
///
/// The dispatcher itself is `'static`: the admission state is owned, so the value can be moved
/// across `tokio::spawn` alongside the owned [`StorageContext`].
pub(crate) struct RequestDispatcher {
    admission: Admission,
}

enum Admission {
    /// Non-READ request or READ that has not been pre-admitted.
    None,
    /// Pre-admitted READ carrying a guard that keeps the handle alive through dispatch.
    ReadHandle(ReadHandleGuard),
    /// Admission probe failed; the error is surfaced on dispatch as the wire response.
    Rejected(Option<StorageError>),
}

impl RequestDispatcher {
    /// Runs admission synchronously and returns a dispatcher carrying the resulting state.
    ///
    /// # Ordering contract
    ///
    /// For correctness of the READ / CLOSE ordering invariant, callers must preserve two rules:
    ///
    /// 1. `admit` must run on the connection's inbound loop in wire order, before the task that
    ///    will run [`Self::dispatch`] is spawned. A READ registered here keeps the target handle
    ///    alive so a later CLOSE (decoded from the same inbound stream) cannot remove the handle
    ///    before the READ finishes.
    /// 2. Between `admit` and `dispatch`, nothing awaits on the *same* handle's CLOSE or relies on
    ///    the handle being absent. Awaits on unrelated work (e.g. response-budget permits) are
    ///    fine because the handle guard is already owned by the returned dispatcher.
    ///
    /// The two methods are deliberately not merged into a single `async fn`: in that shape
    /// admission would only run when the future is first polled, defeating rule 1 for spawned
    /// dispatch futures.
    pub(crate) fn admit<I: CacheIndex>(
        request: &WireRequest,
        context: &StorageContext<I>,
    ) -> Self {
        let admission = match &request.payload {
            WireRequestPayload::Read { handle, .. } => {
                match context.handles.begin_read(*handle) {
                    Ok(guard) => Admission::ReadHandle(guard),
                    Err(error) => Admission::Rejected(Some(error)),
                }
            }
            _ => Admission::None,
        };
        Self { admission }
    }

    /// Dispatches the request, running observer/policy hooks and invoking the service.
    ///
    /// Must be preceded by [`Self::admit`] on the inbound read loop; see that method for the full
    /// ordering contract.
    pub(crate) async fn dispatch<I: CacheIndex + 'static>(
        mut self,
        request: WireRequest,
        context: &StorageContext<I>,
    ) -> StorageHandlerResponse {
        let request_id = request.request_id;
        let request_context = RequestContext::new(
            request_id,
            context.client_addr.clone(),
            request.payload.clone(),
        );
        let started = Instant::now();
        context
            .request_hooks
            .observer()
            .on_request_start(&request_context);

        let result = match context
            .request_hooks
            .policy()
            .before_dispatch(&request_context)
        {
            Err(error) => Err(error),
            Ok(()) => {
                self.run_command(StorageCommand::from(request.payload), context)
                    .await
            }
        };

        match result {
            Ok(reply) => {
                context.request_hooks.observer().on_request_finish(
                    &request_context,
                    &RequestOutcome::success(started.elapsed()),
                );
                let ServiceReply { output, attachment } = reply;
                StorageHandlerResponse {
                    request_id,
                    payload: StorageHandlerPayload::from(output),
                    direct_file: attachment.map(std::fs::File::from),
                }
            }
            Err(error) => {
                let kind = error.kind();
                context.request_hooks.observer().on_request_finish(
                    &request_context,
                    &RequestOutcome::error(kind, started.elapsed()),
                );
                StorageHandlerResponse {
                    request_id,
                    payload: StorageHandlerPayload::Wire(
                        WireResponse::error(request_id, error).payload,
                    ),
                    direct_file: None,
                }
            }
        }
    }

    async fn run_command<I: CacheIndex + 'static>(
        &mut self,
        command: StorageCommand,
        context: &StorageContext<I>,
    ) -> crate::error::StorageResult<ServiceReply> {
        match command {
            StorageCommand::Read(command) => match &mut self.admission {
                Admission::ReadHandle(read_handle) => {
                    context
                        .service
                        .handle_admitted_read(read_handle, command)
                        .await
                }
                Admission::Rejected(error) => {
                    Err(error.take().unwrap_or_else(|| {
                        StorageError::cache(
                            "request admission error was already consumed",
                        )
                    }))
                }
                // `admit` always maps a `Read` request to `ReadHandle` or `Rejected`, never `None`.
                // Hitting this arm means the admit-before-dispatch ordering contract was violated
                // (see `RequestDispatcher::admit` docs) — fail loudly rather than silently route
                // through `execute` and skip the handle guard.
                Admission::None => {
                    unreachable!("Read command dispatched without prior admit")
                }
            },
            command => {
                context
                    .service
                    .execute(context.handles.as_ref(), command)
                    .await
            }
        }
    }
}

// ---- direction conversions: wire → command, output → wire, attachment → file ------------------

impl From<WireRequestPayload> for StorageCommand {
    fn from(payload: WireRequestPayload) -> Self {
        match payload {
            WireRequestPayload::Open {
                store_id,
                bucket,
                key,
                flags,
            } => Self::Open(OpenCommand {
                store_id,
                bucket,
                key,
                flags,
            }),
            WireRequestPayload::Head {
                store_id,
                bucket,
                key,
            } => Self::Head(HeadCommand {
                store_id,
                bucket,
                key,
            }),
            WireRequestPayload::Read {
                handle,
                offset,
                len,
            } => Self::Read(ReadCommand {
                handle,
                offset,
                len,
            }),
            WireRequestPayload::Close { handle } => {
                Self::Close(CloseCommand { handle })
            }
            WireRequestPayload::Upload {
                store_id,
                bucket,
                key,
            } => Self::Upload(UploadCommand {
                store_id,
                bucket,
                key,
            }),
            WireRequestPayload::RegisterStore { store_id, config } => {
                Self::RegisterStore(RegisterStoreCommand { store_id, config })
            }
            WireRequestPayload::UnregisterStore { store_id } => {
                Self::UnregisterStore(UnregisterStoreCommand { store_id })
            }
            WireRequestPayload::PurgeStoreCache { store_id } => {
                Self::PurgeStoreCache(PurgeStoreCacheCommand { store_id })
            }
            WireRequestPayload::InvalidateObjectCache {
                store_id,
                bucket,
                key,
            } => Self::InvalidateObjectCache(InvalidateObjectCacheCommand {
                store_id,
                bucket,
                key,
            }),
            WireRequestPayload::Delete {
                store_id,
                bucket,
                key,
            } => Self::Delete(DeleteCommand {
                store_id,
                bucket,
                key,
            }),
            WireRequestPayload::DeletePrefix {
                store_id,
                bucket,
                prefix,
            } => Self::DeletePrefix(DeletePrefixCommand {
                store_id,
                bucket,
                prefix,
            }),
            WireRequestPayload::DeleteObjects {
                store_id,
                bucket,
                keys,
            } => Self::DeleteObjects(DeleteObjectsCommand {
                store_id,
                bucket,
                keys,
            }),
            WireRequestPayload::List {
                store_id,
                bucket,
                prefix,
                page_size,
                cursor,
            } => Self::List(ListCommand {
                store_id,
                bucket,
                prefix,
                page_size,
                cursor,
            }),
            WireRequestPayload::CloseList { cursor } => {
                Self::CloseList(CloseListCommand { cursor })
            }
        }
    }
}

impl From<CommandOutput> for StorageHandlerPayload {
    fn from(output: CommandOutput) -> Self {
        match output {
            CommandOutput::Open(output) => Self::Wire(WireResponsePayload::Open {
                handle: output.handle,
                size: output.size,
                direct_io: output.direct_io,
            }),
            CommandOutput::Head(output) => Self::Wire(WireResponsePayload::Head {
                size: output.size,
                etag: output.etag,
            }),
            CommandOutput::Read(output) => Self::Read {
                body: output.body,
                eof: output.eof,
            },
            CommandOutput::Close => Self::Wire(WireResponsePayload::Close),
            CommandOutput::Upload(output) => {
                Self::Wire(WireResponsePayload::Upload {
                    size: output.size,
                    etag: output.etag,
                })
            }
            CommandOutput::RegisterStore(output) => {
                Self::Wire(WireResponsePayload::RegisterStore {
                    replaced: output.replaced,
                })
            }
            CommandOutput::UnregisterStore(output) => {
                Self::Wire(WireResponsePayload::UnregisterStore {
                    removed: output.removed,
                })
            }
            CommandOutput::PurgeStoreCache => {
                Self::Wire(WireResponsePayload::PurgeStoreCache)
            }
            CommandOutput::InvalidateObjectCache(output) => {
                Self::Wire(WireResponsePayload::InvalidateObjectCache {
                    removed: output.removed,
                })
            }
            CommandOutput::Delete => Self::Wire(WireResponsePayload::Delete),
            CommandOutput::DeletePrefix(output) => {
                Self::Wire(WireResponsePayload::DeletePrefix {
                    deleted: output.deleted,
                })
            }
            CommandOutput::DeleteObjects(output) => {
                Self::Wire(WireResponsePayload::DeleteObjects {
                    deleted: output.deleted,
                })
            }
            CommandOutput::List(output) => Self::Wire(WireResponsePayload::List {
                entries: output.entries,
                next_cursor: output.next_cursor,
            }),
            CommandOutput::CloseList => Self::Wire(WireResponsePayload::CloseList),
        }
    }
}

impl From<ResponseAttachment> for std::fs::File {
    fn from(attachment: ResponseAttachment) -> Self {
        match attachment {
            ResponseAttachment::File(file) => file,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::backend::{MemoryObjectBackend, StoreRegistry};
    use crate::cache::{CacheManager, InMemoryCacheIndex};
    use crate::config::{StorageRuntime, StorageRuntimeConfig};
    use crate::error::{StorageError, StorageErrorKind, StorageResult};
    use crate::handle::{FileHandle, OpenFlags};
    use crate::object::ObjectLocation;
    use crate::protocol::{WireRequest, WireRequestPayload, WireResponsePayload};
    use crate::request::{
        RequestContext, RequestHooks, RequestObserver, RequestOutcome, RequestPolicy,
        RequestStatus,
    };
    use crate::service::StorageService;
    use crate::session::StorageContext;

    const TEST_STORE_ID: &str = "test-store";

    #[tokio::test]
    async fn observer_wraps_successful_request() {
        let observer = Arc::new(RecordingObserver::default());
        let context = test_context(
            RequestHooks::default().with_shared_observer(observer.clone()),
        );

        let request = WireRequest {
            request_id: 7,
            payload: WireRequestPayload::Open {
                store_id: TEST_STORE_ID.to_string(),
                bucket: "bucket".to_string(),
                key: "file".to_string(),
                flags: OpenFlags::READ_ONLY,
            },
        };
        let response = RequestDispatcher::admit(&request, &context)
            .dispatch(request, &context)
            .await;

        assert!(matches!(
            response.payload,
            StorageHandlerPayload::Wire(WireResponsePayload::Open { .. })
        ));
        let events = observer.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, RecordedEventKind::Start);
        assert_eq!(events[0].request_id, 7);
        assert_eq!(events[0].client_addr, "test-client");
        assert_eq!(events[0].operation, "open");
        assert_eq!(events[1].kind, RecordedEventKind::Finish);
        assert_eq!(events[1].status, Some(RequestStatus::Success));
    }

    #[tokio::test]
    async fn policy_error_returns_wire_error_and_notifies_observer() {
        let observer = Arc::new(RecordingObserver::default());
        let hooks = RequestHooks::default()
            .with_shared_observer(observer.clone())
            .with_policy(DenyReads);
        let context = test_context(hooks);

        let request = WireRequest {
            request_id: 8,
            payload: WireRequestPayload::Read {
                handle: FileHandle(1),
                offset: 0,
                len: 4,
            },
        };
        let response = RequestDispatcher::admit(&request, &context)
            .dispatch(request, &context)
            .await;

        match response.payload {
            StorageHandlerPayload::Wire(WireResponsePayload::Error {
                kind,
                message,
            }) => {
                assert_eq!(kind, StorageErrorKind::Unsupported);
                assert_eq!(message, "read denied by request policy");
            }
            other => panic!("unexpected response: {other:?}"),
        }
        assert!(response.direct_file.is_none());

        let events = observer.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, RecordedEventKind::Start);
        assert_eq!(
            events[1].status,
            Some(RequestStatus::Error {
                kind: StorageErrorKind::Unsupported
            })
        );
    }

    fn test_context(hooks: RequestHooks) -> StorageContext<InMemoryCacheIndex> {
        let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "file").unwrap();
        let backend = MemoryObjectBackend::new();
        backend.insert(key, b"abc".to_vec());
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
        StorageContext::new_with_hooks("test-client", service, hooks)
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedEvent {
        kind: RecordedEventKind,
        request_id: u64,
        client_addr: String,
        operation: String,
        status: Option<RequestStatus>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RecordedEventKind {
        Start,
        Finish,
    }

    #[derive(Default)]
    struct RecordingObserver {
        events: Mutex<Vec<RecordedEvent>>,
    }

    impl RecordingObserver {
        fn events(&self) -> Vec<RecordedEvent> {
            self.events
                .lock()
                .expect("recording observer mutex poisoned")
                .clone()
        }
    }

    impl RequestObserver for RecordingObserver {
        fn on_request_start(&self, context: &RequestContext) {
            self.events
                .lock()
                .expect("recording observer mutex poisoned")
                .push(RecordedEvent {
                    kind: RecordedEventKind::Start,
                    request_id: context.request_id(),
                    client_addr: context.client_addr().to_string(),
                    operation: context.operation_name().to_string(),
                    status: None,
                });
        }

        fn on_request_finish(
            &self,
            context: &RequestContext,
            outcome: &RequestOutcome,
        ) {
            self.events
                .lock()
                .expect("recording observer mutex poisoned")
                .push(RecordedEvent {
                    kind: RecordedEventKind::Finish,
                    request_id: context.request_id(),
                    client_addr: context.client_addr().to_string(),
                    operation: context.operation_name().to_string(),
                    status: Some(outcome.status()),
                });
        }
    }

    struct DenyReads;

    impl RequestPolicy for DenyReads {
        fn before_dispatch(&self, context: &RequestContext) -> StorageResult<()> {
            if matches!(context.payload(), WireRequestPayload::Read { .. }) {
                Err(StorageError::unsupported("read denied by request policy"))
            } else {
                Ok(())
            }
        }
    }

    fn test_cache_dir() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        PathBuf::from("/tmp")
            .join(format!("pg-lakebase-storage-request-hooks-{stamp}"))
    }
}
