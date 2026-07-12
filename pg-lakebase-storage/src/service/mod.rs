//! [`StorageService`] request dispatch plus cache / backend wiring.
//!
//! Layout:
//! * [`command`]      — typed input verbs decoded from [`crate::protocol`].
//! * [`reply`]        — all output types (including [`reply::ReadBody`] and [`reply::ServiceReply`]).
//! * [`open`]         — OPEN handler (`impl StorageService` block).
//! * [`range_reader`] — READ handler and its request-scoped [`range_reader::RangeReader`] state machine (`impl
//!   StorageService` block).
//!
//! Other command variants (close, staging verbs, register/unregister/purge, invalidate) are
//! one- to three-liners and are implemented inline on [`StorageService`] below rather than in
//! per-variant stub files.

use std::sync::Arc;

use tracing::{info, warn};

use crate::backend::StoreRegistry;
use crate::cache::{CacheIndex, CacheManager};
use crate::config::StorageServiceConfig;
use crate::error::{StorageError, StorageResult};
use crate::handle::FileHandle;
use crate::object::{ObjectLocation, StoreId};
use crate::protocol::WireListEntry;
use crate::service::command::{
    CloseCommand, CloseListCommand, DeleteCommand, DeleteObjectsCommand,
    DeletePrefixCommand, HeadCommand, InvalidateObjectCacheCommand, ListCommand,
    PurgeStoreCacheCommand, RegisterStoreCommand, StorageCommand,
    UnregisterStoreCommand, UploadCommand,
};
use crate::service::list_session::{
    DEFAULT_PAGE_SIZE, ListSessionError, ListSessionTable, MAX_PAGE_SIZE,
};
use crate::service::reply::{
    CommandOutput, DeleteObjectsOutput, DeletePrefixOutput, HeadOutput,
    InvalidateObjectCacheOutput, ListOutput, RegisterStoreOutput, ServiceReply,
    UnregisterStoreOutput, UploadOutput,
};
use crate::session::handle_table::HandleTable;
use crate::staging::StagingUploader;

pub(crate) mod command;
mod list_session;
mod open;
mod range_reader;
pub(crate) mod reply;

/// Idle TTL for retained paginated list cursors, in milliseconds.
pub const LIST_CURSOR_IDLE_TTL_MS: i32 = 5 * 60 * 1000;

/// Holds the backend registry, the [`CacheManager`], the staging uploader, and per-service limits.
///
/// Each inbound wire operation maps to one internally dispatched [`StorageCommand`]. Execution
/// is [`execute`](Self::execute) for most verbs, with one specialization —
/// [`handle_admitted_read`](Self::handle_admitted_read) — for READs that were pre-admitted on
/// the connection's inbound path.
pub struct StorageService<I: CacheIndex> {
    registry: StoreRegistry,
    pub(super) cache: Arc<CacheManager<I>>,
    staging_uploader: Arc<StagingUploader>,
    list_sessions: Arc<ListSessionTable>,
    config: StorageServiceConfig,
}

impl<I: CacheIndex + 'static> StorageService<I> {
    pub fn with_registry(
        registry: StoreRegistry,
        cache: Arc<CacheManager<I>>,
    ) -> Self {
        Self::with_registry_config(registry, cache, StorageServiceConfig::default())
    }

    /// Constructs a service with a default staging uploader rooted under the cache
    /// directory. Tests that exercise the service directly (without going through
    /// [`crate::builder::StorageServerBuilder`]) use this entry point; production code routes
    /// through `with_staging_uploader` below so the staging root matches the builder's on-disk
    /// layout.
    pub fn with_registry_config(
        registry: StoreRegistry,
        cache: Arc<CacheManager<I>>,
        config: StorageServiceConfig,
    ) -> Self {
        let staging_root = cache.paths.root().to_path_buf();
        Self::with_staging_uploader(
            registry,
            cache,
            Arc::new(StagingUploader::new(staging_root)),
            config,
        )
    }

    pub(crate) fn with_staging_uploader(
        registry: StoreRegistry,
        cache: Arc<CacheManager<I>>,
        staging_uploader: Arc<StagingUploader>,
        config: StorageServiceConfig,
    ) -> Self {
        Self {
            registry,
            cache,
            staging_uploader,
            list_sessions: Arc::new(ListSessionTable::new()),
            config: config.normalized(),
        }
    }

    pub fn registry(&self) -> &StoreRegistry {
        &self.registry
    }

    pub(crate) fn max_read_size(&self) -> u32 {
        self.config.max_read_size
    }

    /// Dispatches `command` on the given per-connection [`HandleTable`].
    pub(crate) async fn execute(
        &self,
        handles: &HandleTable,
        command: StorageCommand,
    ) -> StorageResult<ServiceReply> {
        match command {
            StorageCommand::Open(command) => self.handle_open(handles, command).await,
            StorageCommand::Head(command) => self.handle_head(command).await,
            StorageCommand::Read(command) => self.handle_read(handles, command).await,
            StorageCommand::Close(command) => {
                self.handle_close(handles, command).await
            }
            StorageCommand::Upload(command) => self.handle_upload(command).await,
            StorageCommand::RegisterStore(command) => {
                self.handle_register_store(command)
            }
            StorageCommand::UnregisterStore(command) => {
                self.handle_unregister_store(command)
            }
            StorageCommand::PurgeStoreCache(command) => {
                self.handle_purge_store_cache(command).await
            }
            StorageCommand::InvalidateObjectCache(command) => {
                self.handle_invalidate_object_cache(command).await
            }
            StorageCommand::Delete(command) => self.handle_delete(command).await,
            StorageCommand::DeletePrefix(command) => {
                self.handle_delete_prefix(command).await
            }
            StorageCommand::DeleteObjects(command) => {
                self.handle_delete_objects(command).await
            }
            StorageCommand::List(command) => self.handle_list(command).await,
            StorageCommand::CloseList(command) => self.handle_close_list(command),
        }
    }

    // -- head ------------------------------------------------------------------------------------

    async fn handle_head(&self, command: HeadCommand) -> StorageResult<ServiceReply> {
        let key = ObjectLocation::new(command.store_id, command.bucket, command.key)?;

        if let Some(meta) = self.cache.index.get_meta(&key).await? {
            return Ok(ServiceReply::new(CommandOutput::Head(HeadOutput {
                size: meta.size(),
                etag: meta.etag().map(str::to_string),
            })));
        }

        let store = self.registry.resolve(key.store_id())?;
        let info = store.head(&key).await?;
        Ok(ServiceReply::new(CommandOutput::Head(HeadOutput {
            size: info.size,
            etag: info.etag,
        })))
    }

    // -- close -----------------------------------------------------------------------------------

    async fn handle_close(
        &self,
        handles: &HandleTable,
        command: CloseCommand,
    ) -> StorageResult<ServiceReply> {
        self.close_handle(handles, command.handle).await?;
        Ok(ServiceReply::new(CommandOutput::Close))
    }

    /// Removes `handle` from the table and releases every resource it carried. Cache activity
    /// leases and the large-fill session reference (when present) drop through their own RAII
    /// chains — if the handle was the last large-fill participant, the session's `Drop` enqueues
    /// partial cleanup with the reaper task.
    pub(crate) async fn close_handle(
        &self,
        handles: &HandleTable,
        handle: FileHandle,
    ) -> StorageResult<()> {
        let closed = handles.close(handle).await?;
        info!(handle = handle.0, "handle closed");
        drop(closed);
        Ok(())
    }

    pub(crate) async fn close_all_handles(
        &self,
        handles: &HandleTable,
    ) -> StorageResult<()> {
        for closed in handles.close_all().await {
            drop(closed?);
        }
        Ok(())
    }

    // -- staging ---------------------------------------------------------------------------------

    async fn handle_upload(
        &self,
        command: UploadCommand,
    ) -> StorageResult<ServiceReply> {
        let key = ObjectLocation::new(command.store_id, command.bucket, command.key)?;
        let store = self.registry.resolve(key.store_id())?;
        let info = self.staging_uploader.upload(&key, &store).await?;
        Ok(ServiceReply::new(CommandOutput::Upload(UploadOutput {
            size: info.size,
            etag: info.etag,
        })))
    }

    // -- store registry --------------------------------------------------------------------------

    fn handle_register_store(
        &self,
        command: RegisterStoreCommand,
    ) -> StorageResult<ServiceReply> {
        if self.config.registry_managed_externally {
            warn!(
                store_id = command.store_id.as_str(),
                "rejecting RegisterStore: store registry is owned by an external reconciler"
            );
            return Err(StorageError::unsupported(
                "RegisterStore is disabled: the store registry is managed by an external reconciler",
            ));
        }
        let store_id_str = command.store_id.clone();
        let previous = self
            .registry
            .register_config(command.store_id, command.config)?;
        let replaced = previous.is_some();
        info!(
            store_id = store_id_str.as_str(),
            replaced, "store registered"
        );
        Ok(ServiceReply::new(CommandOutput::RegisterStore(
            RegisterStoreOutput { replaced },
        )))
    }

    fn handle_unregister_store(
        &self,
        command: UnregisterStoreCommand,
    ) -> StorageResult<ServiceReply> {
        if self.config.registry_managed_externally {
            warn!(
                store_id = command.store_id.as_str(),
                "rejecting UnregisterStore: store registry is owned by an external reconciler"
            );
            return Err(StorageError::unsupported(
                "UnregisterStore is disabled: the store registry is managed by an external reconciler",
            ));
        }
        let store_id = StoreId::new(command.store_id)?;
        let removed = self.registry.unregister(&store_id).is_some();
        info!(store_id = %store_id, removed, "store unregistered");
        Ok(ServiceReply::new(CommandOutput::UnregisterStore(
            UnregisterStoreOutput { removed },
        )))
    }

    async fn handle_purge_store_cache(
        &self,
        command: PurgeStoreCacheCommand,
    ) -> StorageResult<ServiceReply> {
        let store_id = StoreId::new(command.store_id)?;
        let _report = self.cache.purge_store_cache(&store_id).await?;
        info!(store_id = %store_id, "store cache purged");
        Ok(ServiceReply::new(CommandOutput::PurgeStoreCache))
    }

    // -- object-cache invalidation ---------------------------------------------------------------

    async fn handle_invalidate_object_cache(
        &self,
        command: InvalidateObjectCacheCommand,
    ) -> StorageResult<ServiceReply> {
        let key = ObjectLocation::new(command.store_id, command.bucket, command.key)?;
        self.cache.validate_file_cache_paths(&key)?;
        let report = self.cache.invalidate_object_cache(&key).await?;
        info!(
            key = %key,
            removed = report.removed,
            bytes_removed = report.bytes_removed,
            "object cache invalidated",
        );
        Ok(ServiceReply::new(CommandOutput::InvalidateObjectCache(
            InvalidateObjectCacheOutput {
                removed: report.removed,
            },
        )))
    }

    // -- delete ----------------------------------------------------------------------------------

    async fn handle_delete(
        &self,
        command: DeleteCommand,
    ) -> StorageResult<ServiceReply> {
        let key = ObjectLocation::new(command.store_id, command.bucket, command.key)?;
        let store = self.registry.resolve(key.store_id())?;
        store.delete(&key).await?;
        let outcome = self.cache.invalidate_object_cache_best_effort(&key).await;
        info!(key = %key, ?outcome, "delete completed");
        Ok(ServiceReply::new(CommandOutput::Delete))
    }

    async fn handle_delete_prefix(
        &self,
        command: DeletePrefixCommand,
    ) -> StorageResult<ServiceReply> {
        if command.prefix.is_empty() {
            return Err(StorageError::invalid_path(
                "delete_prefix requires a non-empty prefix; pass an explicit prefix to scope the deletion",
            ));
        }
        let DeletePrefixCommand {
            store_id,
            bucket,
            prefix,
        } = command;
        let store_id = StoreId::new(store_id)?;
        let store = self.registry.resolve(&store_id)?;

        // list → strip ListEntry to keys → feed into delete_stream → for each acknowledged
        // delete, best-effort drop the local cache row. Two streams wired through
        // `Arc<RegisteredStore>` so any per-key error from the bulk-delete machinery surfaces
        // immediately and stops the pipeline.
        use futures::StreamExt;
        let key_stream = store
            .list(&bucket, Some(&prefix))
            .map(|item| item.map(|entry| entry.key))
            .boxed();
        let mut deleted_stream = store.delete_stream(&bucket, key_stream);

        let mut deleted: u64 = 0;
        while let Some(result) = deleted_stream.next().await {
            let key_str = result?;
            // `bucket` is borrowed (not cloned) per iteration; `ObjectLocation::new` takes
            // `impl Into<String>` so it must own the strings, and we cannot avoid that
            // allocation, but at least we limit it to the `(store_id_str, bucket, key_str)`
            // ones the function inherently needs.
            self.try_invalidate_local_cache_for_key(
                store_id.as_str(),
                &bucket,
                key_str,
            )
            .await;
            deleted = deleted.saturating_add(1);
        }
        info!(
            store_id = %store_id,
            bucket = bucket.as_str(),
            prefix = prefix.as_str(),
            deleted,
            "delete_prefix completed",
        );
        Ok(ServiceReply::new(CommandOutput::DeletePrefix(
            DeletePrefixOutput { deleted },
        )))
    }

    async fn handle_delete_objects(
        &self,
        command: DeleteObjectsCommand,
    ) -> StorageResult<ServiceReply> {
        const MAX_KEYS: usize = crate::protocol::MAX_BULK_DELETE_OBJECT_KEYS;
        if command.keys.len() > MAX_KEYS {
            return Err(StorageError::resource_exhausted(format!(
                "delete_objects accepts at most {MAX_KEYS} keys"
            )));
        }
        if command.keys.is_empty() {
            return Ok(ServiceReply::new(CommandOutput::DeleteObjects(
                DeleteObjectsOutput { deleted: 0 },
            )));
        }

        let DeleteObjectsCommand {
            store_id,
            bucket,
            keys,
        } = command;
        let store_id = StoreId::new(store_id)?;
        let store = self.registry.resolve(&store_id)?;
        use futures::StreamExt;
        let key_stream =
            futures::stream::iter(keys.into_iter().map(Ok::<_, StorageError>))
                .boxed();
        let mut deleted_stream = store.delete_stream(&bucket, key_stream);
        let mut deleted = 0_u32;
        while let Some(result) = deleted_stream.next().await {
            let key = result?;
            self.try_invalidate_local_cache_for_key(store_id.as_str(), &bucket, key)
                .await;
            deleted = deleted.saturating_add(1);
        }
        Ok(ServiceReply::new(CommandOutput::DeleteObjects(
            DeleteObjectsOutput { deleted },
        )))
    }

    /// Best-effort local-cache cleanup for a key whose backend object has just been deleted.
    ///
    /// Shared between [`Self::handle_delete_prefix`] and any future bulk-delete callers. The
    /// `(store_id, bucket, key)` tuple is consumed because `ObjectLocation::new` requires
    /// owned strings — we accept that cost rather than re-validating the path on every entry
    /// or carrying a half-built `ObjectLocation` through the bulk-delete pipeline.
    ///
    /// Errors (invalid path components, busy cache entry, transient cache I/O failures) never
    /// propagate: backend deletion has already succeeded, the cache is derived data, and the
    /// janitor will clean any leftover entries on its next pass.
    async fn try_invalidate_local_cache_for_key(
        &self,
        store_id: &str,
        bucket: &str,
        key: String,
    ) {
        match ObjectLocation::new(store_id, bucket, key) {
            Ok(location) => {
                let _ = self
                    .cache
                    .invalidate_object_cache_best_effort(&location)
                    .await;
            }
            Err(error) => {
                warn!(
                    store_id = %store_id,
                    bucket = %bucket,
                    %error,
                    "skipping cache invalidation for deleted key whose path is not representable as ObjectLocation",
                );
            }
        }
    }

    // -- list ------------------------------------------------------------------------------------

    async fn handle_list(&self, command: ListCommand) -> StorageResult<ServiceReply> {
        let store_id = StoreId::new(command.store_id.clone())?;
        let store = self.registry.resolve(&store_id)?;

        // Resolve the cursor: `None` means "open a fresh stream from the backend"; `Some`
        // means "continue an existing stream named by this cursor". Either way we end up with
        // a cursor we can drain.
        let cursor = match command.cursor {
            Some(cursor) => cursor,
            None => {
                let stream = store.list(&command.bucket, command.prefix.as_deref());
                self.list_sessions.insert(stream)
            }
        };

        let page_size = clamp_page_size(command.page_size);
        let drain = self.list_sessions.drain(&cursor, page_size as usize).await;
        let drain = match drain {
            Ok(drain) => drain,
            Err(ListSessionError::UnknownCursor) => {
                return Err(StorageError::expired_cursor(
                    "unknown or expired list cursor",
                ));
            }
            // Stream-error path: `drain` has already removed the session so the cursor is
            // dead. The wire response shape has no partial-progress slot, so we surface the
            // error to the client (the matching `ListIter::next` will mark itself `Failed`).
            Err(ListSessionError::StreamError(error)) => return Err(error),
        };

        // Convert backend `ListEntry` to the wire shape. There are no per-entry errors at this
        // point: `drain` either returned all-`Ok` entries or already split into the
        // `StreamError` branch above.
        let entries = drain
            .entries
            .into_iter()
            .map(|entry| WireListEntry {
                key: entry.key,
                size: entry.size,
                etag: entry.etag,
            })
            .collect();
        let next_cursor = (!drain.exhausted).then_some(cursor);

        Ok(ServiceReply::new(CommandOutput::List(ListOutput {
            entries,
            next_cursor,
        })))
    }

    fn handle_close_list(
        &self,
        command: CloseListCommand,
    ) -> StorageResult<ServiceReply> {
        self.list_sessions.forget(&command.cursor);
        Ok(ServiceReply::new(CommandOutput::CloseList))
    }
}

fn clamp_page_size(page_size: u32) -> u32 {
    match page_size {
        0 => DEFAULT_PAGE_SIZE,
        n => n.min(MAX_PAGE_SIZE),
    }
}

#[cfg(test)]
mod tests;
