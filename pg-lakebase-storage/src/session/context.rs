use std::fmt;
use std::sync::{Arc, OnceLock};

use super::handle_table::HandleTable;
use crate::backend::{BackendDataIdentity, ManagedStoreSlot, ObjectBackend};
use crate::cache::CacheIndex;
use crate::config::DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION;
use crate::request::RequestHooks;
use crate::service::StorageService;
use crate::service::list_session::ListSessionTable;

/// Physical storage context fixed for the lifetime of one socket.
pub(crate) enum AttachedStorageContext {
    Managed(Arc<ManagedStoreSlot>),
    Configured {
        identity: BackendDataIdentity,
        backend: Arc<dyn ObjectBackend>,
    },
}

impl AttachedStorageContext {
    pub(crate) fn identity(&self) -> &BackendDataIdentity {
        match self {
            Self::Managed(slot) => slot.identity(),
            Self::Configured { identity, .. } => identity,
        }
    }

    pub(crate) fn backend(&self) -> Arc<dyn ObjectBackend> {
        match self {
            Self::Managed(slot) => slot.backend(),
            Self::Configured { backend, .. } => Arc::clone(backend),
        }
    }
}

/// Per-client connection state: isolates open-file handles from other connections sharing the same [`StorageService`].
pub struct StorageContext<I: CacheIndex> {
    pub client_addr: Arc<str>,
    pub service: Arc<StorageService<I>>,
    pub handles: Arc<HandleTable>,
    pub(crate) list_sessions: Arc<ListSessionTable>,
    attached: Arc<OnceLock<AttachedStorageContext>>,
    pub request_hooks: RequestHooks,
}

impl<I: CacheIndex> Clone for StorageContext<I> {
    fn clone(&self) -> Self {
        Self {
            client_addr: self.client_addr.clone(),
            service: self.service.clone(),
            handles: self.handles.clone(),
            list_sessions: self.list_sessions.clone(),
            attached: self.attached.clone(),
            request_hooks: self.request_hooks.clone(),
        }
    }
}

impl<I: CacheIndex> StorageContext<I> {
    pub fn new(
        client_addr: impl Into<Arc<str>>,
        service: Arc<StorageService<I>>,
    ) -> Self {
        Self::new_with_hooks(client_addr, service, RequestHooks::default())
    }

    pub fn new_with_hooks(
        client_addr: impl Into<Arc<str>>,
        service: Arc<StorageService<I>>,
        request_hooks: RequestHooks,
    ) -> Self {
        Self::new_with_hooks_and_handle_limit(
            client_addr,
            service,
            request_hooks,
            DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION,
        )
    }

    pub(crate) fn new_with_hooks_and_handle_limit(
        client_addr: impl Into<Arc<str>>,
        service: Arc<StorageService<I>>,
        request_hooks: RequestHooks,
        max_open_handles_per_connection: usize,
    ) -> Self {
        Self {
            client_addr: client_addr.into(),
            service,
            handles: Arc::new(HandleTable::with_max_open_handles(
                max_open_handles_per_connection,
            )),
            list_sessions: Arc::new(ListSessionTable::new()),
            attached: Arc::new(OnceLock::new()),
            request_hooks,
        }
    }

    pub(crate) fn new_attached_with_hooks_and_handle_limit(
        client_addr: impl Into<Arc<str>>,
        service: Arc<StorageService<I>>,
        request_hooks: RequestHooks,
        max_open_handles_per_connection: usize,
        attached: AttachedStorageContext,
    ) -> Self {
        let context = Self::new_with_hooks_and_handle_limit(
            client_addr,
            service,
            request_hooks,
            max_open_handles_per_connection,
        );
        context
            .attach(attached)
            .expect("new storage context is unattached");
        context
    }

    pub(crate) fn attach(
        &self,
        attached: AttachedStorageContext,
    ) -> crate::error::StorageResult<&AttachedStorageContext> {
        self.attached.set(attached).map_err(|_| {
            crate::error::StorageError::conflict(
                "storage connection is already attached",
            )
        })?;
        Ok(self
            .attached
            .get()
            .expect("attached context was just initialized"))
    }

    pub(crate) fn attached(
        &self,
    ) -> crate::error::StorageResult<&AttachedStorageContext> {
        self.attached.get().ok_or_else(|| {
            crate::error::StorageError::protocol(
                "storage connection must attach a context before data operations",
            )
        })
    }
}

impl<I: CacheIndex> fmt::Debug for StorageContext<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageContext")
            .field("client_addr", &self.client_addr)
            .finish()
    }
}
