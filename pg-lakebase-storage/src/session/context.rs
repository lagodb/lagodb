use std::fmt;
use std::sync::Arc;

use super::handle_table::HandleTable;
use crate::cache::CacheIndex;
use crate::config::DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION;
use crate::request::RequestHooks;
use crate::service::StorageService;

/// Per-client connection state: isolates open-file handles from other connections sharing the same [`StorageService`].
pub struct StorageContext<I: CacheIndex> {
    pub client_addr: Arc<str>,
    pub service: Arc<StorageService<I>>,
    pub handles: Arc<HandleTable>,
    pub request_hooks: RequestHooks,
}

impl<I: CacheIndex> Clone for StorageContext<I> {
    fn clone(&self) -> Self {
        Self {
            client_addr: self.client_addr.clone(),
            service: self.service.clone(),
            handles: self.handles.clone(),
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
            request_hooks,
        }
    }
}

impl<I: CacheIndex> fmt::Debug for StorageContext<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageContext")
            .field("client_addr", &self.client_addr)
            .finish()
    }
}
