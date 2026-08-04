//! [`StorageService`] request dispatch plus cache / backend wiring.
//!
//! Layout:
//! * [`command`]      — typed input verbs decoded from [`crate::protocol`].
//! * [`reply`]        — all output types (including [`reply::ReadBody`] and [`reply::ServiceReply`]).
//! * [`open`]         — OPEN handler (`impl StorageService` block).
//! * [`range_reader`] — READ handler and its request-scoped state machine.
//! * [`object_ops`]   — metadata, publication, invalidation, and deletion.
//! * [`list_ops`]     — connection-local paginated listing.

use std::sync::Arc;

use tracing::info;

use crate::backend::{BackendPool, ManagedStoreRegistry};
use crate::cache::{CacheIndex, CacheManager};
use crate::config::StorageServiceConfig;
use crate::error::{StorageError, StorageResult};
use crate::handle::FileHandle;
use crate::service::command::{CloseCommand, StorageCommand};
use crate::service::reply::{CommandOutput, ServiceReply};
use crate::session::handle_table::HandleTable;
use crate::session::{AttachedStorageContext, StorageContext};
use crate::staging::StagingUploader;

pub(crate) mod command;
mod list_ops;
pub(crate) mod list_session;
mod object_ops;
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
    managed_stores: ManagedStoreRegistry,
    backend_pool: Arc<BackendPool>,
    pub(super) cache: Arc<CacheManager<I>>,
    staging_uploader: Arc<StagingUploader>,
    config: StorageServiceConfig,
}

impl<I: CacheIndex> StorageService<I> {
    #[cfg(test)]
    pub(crate) fn test_context(&self) -> StorageContext<I> {
        self.test_context_with_handle_limit(
            crate::config::DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_context_with_handle_limit(
        &self,
        max_open_handles: usize,
    ) -> StorageContext<I> {
        let service = Arc::new(Self {
            managed_stores: self.managed_stores.clone(),
            backend_pool: Arc::clone(&self.backend_pool),
            cache: Arc::clone(&self.cache),
            staging_uploader: Arc::clone(&self.staging_uploader),
            config: self.config.clone(),
        });
        let slot = service
            .managed_stores
            .resolve(1)
            .expect("test managed volume must be registered");
        let context = StorageContext::new_with_hooks_and_handle_limit(
            "test",
            service,
            crate::request::RequestHooks::default(),
            max_open_handles,
        );
        context
            .attach(AttachedStorageContext::Managed(slot))
            .expect("test context is initially unattached");
        context
    }

    pub(crate) fn resolve_attach(
        &self,
        payload: crate::protocol::WireRequestPayload,
    ) -> StorageResult<AttachedStorageContext> {
        match payload {
            crate::protocol::WireRequestPayload::AttachManaged { volume_id } => self
                .managed_stores
                .resolve(volume_id)
                .map(AttachedStorageContext::Managed),
            crate::protocol::WireRequestPayload::AttachConfigured { config } => {
                let identity =
                    crate::backend::BackendDataIdentity::from_config(&config);
                let backend = self.backend_pool.intern(config)?;
                let backend: Arc<dyn crate::backend::ObjectBackend> = backend;
                Ok(AttachedStorageContext::Configured { identity, backend })
            }
            _ => Err(StorageError::protocol(
                "the first storage request must attach exactly one context",
            )),
        }
    }

    pub fn with_registry(
        managed_stores: ManagedStoreRegistry,
        cache: Arc<CacheManager<I>>,
    ) -> Self {
        Self::with_registry_config(
            managed_stores,
            cache,
            StorageServiceConfig::default(),
        )
    }

    /// Constructs a service with a default staging uploader rooted under the cache
    /// directory. Tests that exercise the service directly (without going through
    /// [`crate::builder::StorageServerBuilder`]) use this entry point; production code routes
    /// through `with_staging_uploader` below so the staging root matches the builder's on-disk
    /// layout.
    pub fn with_registry_config(
        managed_stores: ManagedStoreRegistry,
        cache: Arc<CacheManager<I>>,
        config: StorageServiceConfig,
    ) -> Self {
        let staging_root = cache.paths.root().to_path_buf();
        Self::with_staging_uploader(
            managed_stores,
            cache,
            Arc::new(StagingUploader::new(staging_root)),
            config,
        )
    }

    pub(crate) fn with_staging_uploader(
        managed_stores: ManagedStoreRegistry,
        cache: Arc<CacheManager<I>>,
        staging_uploader: Arc<StagingUploader>,
        config: StorageServiceConfig,
    ) -> Self {
        let backend_pool = Arc::clone(managed_stores.backend_pool());
        Self {
            managed_stores,
            backend_pool,
            cache,
            staging_uploader,
            config: config.normalized(),
        }
    }

    pub fn managed_stores(&self) -> &ManagedStoreRegistry {
        &self.managed_stores
    }

    pub(crate) fn max_read_size(&self) -> u32 {
        self.config.max_read_size
    }

    /// Dispatches `command` on the given per-connection [`HandleTable`].
    pub(crate) async fn execute(
        &self,
        context: &StorageContext<I>,
        command: StorageCommand,
    ) -> StorageResult<ServiceReply> {
        match command {
            StorageCommand::AttachManaged(command) => {
                let attached = context.attach(self.resolve_attach(
                    crate::protocol::WireRequestPayload::AttachManaged {
                        volume_id: command.volume_id,
                    },
                )?)?;
                Ok(ServiceReply::new(CommandOutput::Attach {
                    backend_identity: attached.identity().cache_key().to_owned(),
                }))
            }
            StorageCommand::AttachConfigured(command) => {
                let attached = context.attach(self.resolve_attach(
                    crate::protocol::WireRequestPayload::AttachConfigured {
                        config: command.config,
                    },
                )?)?;
                Ok(ServiceReply::new(CommandOutput::Attach {
                    backend_identity: attached.identity().cache_key().to_owned(),
                }))
            }
            StorageCommand::Open(command) => self.handle_open(context, command).await,
            StorageCommand::Head(command) => self.handle_head(context, command).await,
            StorageCommand::Read(command) => {
                self.handle_read(&context.handles, command).await
            }
            StorageCommand::Close(command) => {
                self.handle_close(&context.handles, command).await
            }
            StorageCommand::Upload(command) => {
                self.handle_upload(context, command).await
            }
            StorageCommand::ProbeStore(command) => {
                self.handle_probe_store(context, command).await
            }
            StorageCommand::InvalidateObjectCache(command) => {
                self.handle_invalidate_object_cache(context, command).await
            }
            StorageCommand::Delete(command) => {
                self.handle_delete(context, command).await
            }
            StorageCommand::DeletePrefix(command) => {
                self.handle_delete_prefix(context, command).await
            }
            StorageCommand::DeleteObjects(command) => {
                self.handle_delete_objects(context, command).await
            }
            StorageCommand::List(command) => self.handle_list(context, command).await,
            StorageCommand::CloseList(command) => {
                self.handle_close_list(context, command)
            }
        }
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
}

#[cfg(test)]
mod tests;
