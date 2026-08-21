//! PostgreSQL-backend-local attached storage contexts.
//!
//! The PostgreSQL backend is single-threaded, so live contexts use `Rc`/`Weak`
//! ownership. Context metadata remains interned while reusable sockets form a
//! bounded backend-local idle cache. Fresh configured contexts use monotonic,
//! credential-free generation keys. Open files retain their client generation
//! directly and therefore cannot be evicted underneath READ.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use pg_lakebase_storage::{
    ExternalFdLease, ExternalFdPolicy, StorageClient, StorageError, StorageResult,
    StoreConfig,
};
use pgrx::pg_sys;

use super::socket_wait::PostgresSocketWait;

thread_local! {
    static ATTACHED_CONTEXTS: RefCell<BackendConnectionManager> =
        RefCell::new(BackendConnectionManager::new());
}

pub(crate) fn attached_context(
    socket_path: &Path,
    key: BackendContextKey,
    attach: BackendAttach,
    max_idle_connections: usize,
) -> StorageResult<Rc<BackendAttachedContext>> {
    ATTACHED_CONTEXTS.with(|manager| {
        let mut manager = manager.try_borrow_mut().map_err(|_| {
            StorageError::protocol(
                "backend storage context manager is already in use",
            )
        })?;
        Ok(manager.resolve(socket_path, key, attach, max_idle_connections))
    })
}

pub(crate) fn configured_context(
    socket_path: &Path,
    config: Arc<StoreConfig>,
    max_idle_connections: usize,
) -> StorageResult<Rc<BackendAttachedContext>> {
    ATTACHED_CONTEXTS.with(|manager| {
        let mut manager = manager.try_borrow_mut().map_err(|_| {
            StorageError::protocol(
                "backend storage context manager is already in use",
            )
        })?;
        Ok(manager.resolve_configured(socket_path, config, max_idle_connections))
    })
}

pub(crate) fn acquire_attached_client(
    context: &Rc<BackendAttachedContext>,
) -> StorageResult<StorageClient> {
    ATTACHED_CONTEXTS.with(|manager| {
        let mut manager = manager.try_borrow_mut().map_err(|_| {
            StorageError::protocol(
                "backend storage context manager is already in use",
            )
        })?;
        manager.acquire_client(context)
    })
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum BackendContextKey {
    Managed(u64),
    Foreign(u64),
    Configured(u64),
}

#[derive(Clone)]
pub(crate) enum BackendAttach {
    Managed(u64),
    Configured(Arc<StoreConfig>),
}

impl PartialEq for BackendAttach {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Managed(left), Self::Managed(right)) => left == right,
            (Self::Configured(left), Self::Configured(right)) => {
                Arc::ptr_eq(left, right)
            }
            _ => false,
        }
    }
}

impl Eq for BackendAttach {}

/// One lazily connected storage context owned by its backend-local callers.
pub(crate) struct BackendAttachedContext {
    socket_path: PathBuf,
    key: BackendContextKey,
    attach: BackendAttach,
    current: RefCell<Option<StorageClient>>,
    last_used: Cell<u64>,
}

impl BackendAttachedContext {
    fn new(
        socket_path: PathBuf,
        key: BackendContextKey,
        attach: BackendAttach,
    ) -> Self {
        Self {
            socket_path,
            key,
            attach,
            current: RefCell::new(None),
            last_used: Cell::new(0),
        }
    }

    pub(crate) const fn key(&self) -> BackendContextKey {
        self.key
    }

    fn reusable_client(&self) -> StorageResult<Option<StorageClient>> {
        let stale = {
            let mut current = self.current.try_borrow_mut().map_err(|_| {
                StorageError::protocol(
                    "backend attached storage context is already in use",
                )
            })?;
            if let Some(client) = current.as_ref()
                && client.is_usable()
            {
                return Ok(Some(client.clone()));
            }
            current.take()
        };

        if let Some(stale) = stale {
            let _ = stale.invalidate();
        }

        Ok(None)
    }

    fn connect_client(&self) -> StorageResult<StorageClient> {
        let builder = StorageClient::builder(&self.socket_path)
            .fd_policy(Box::new(PostgresExternalFdPolicy))
            .socket_waiter(Box::new(PostgresSocketWait::new()));
        match &self.attach {
            BackendAttach::Managed(volume_id) => builder.managed_volume(*volume_id),
            BackendAttach::Configured(config) => {
                builder.configured(Arc::clone(config))
            }
        }
        .connect()
    }

    fn cache_client(&self, client: &StorageClient) -> StorageResult<()> {
        *self.current.try_borrow_mut().map_err(|_| {
            StorageError::protocol(
                "backend attached storage context is already in use",
            )
        })? = Some(client.clone());
        Ok(())
    }

    fn has_reclaimable_client(&self) -> bool {
        self.current.try_borrow().is_ok_and(|current| {
            current
                .as_ref()
                .is_some_and(StorageClient::is_unshared_and_usable)
        })
    }

    fn take_reclaimable_client(&self) -> Option<StorageClient> {
        let mut current = self.current.try_borrow_mut().ok()?;
        if current
            .as_ref()
            .is_some_and(StorageClient::is_unshared_and_usable)
        {
            current.take()
        } else {
            None
        }
    }

    fn matches(&self, socket_path: &Path, attach: &BackendAttach) -> bool {
        self.socket_path == socket_path && &self.attach == attach
    }
}

struct BackendConnectionManager {
    contexts: HashMap<BackendContextKey, CachedContext>,
    next_configured_generation: u64,
    use_clock: u64,
    max_idle_connections: usize,
}

enum CachedContext {
    /// Managed volume IDs are stable and bounded by runtime configuration, so
    /// their healthy sockets remain backend-cached across SQL statements.
    Managed(Rc<BackendAttachedContext>),
    /// Owners of foreign and one-response configured contexts control their
    /// lifetime. The manager must not retain credentials after the owner drops.
    Configured(Weak<BackendAttachedContext>),
}

impl CachedContext {
    fn context(&self) -> Option<Rc<BackendAttachedContext>> {
        match self {
            Self::Managed(context) => Some(Rc::clone(context)),
            Self::Configured(context) => context.upgrade(),
        }
    }

    fn is_live(&self) -> bool {
        match self {
            Self::Managed(_) => true,
            Self::Configured(context) => context.strong_count() > 0,
        }
    }
}

impl BackendConnectionManager {
    fn new() -> Self {
        Self {
            contexts: HashMap::new(),
            next_configured_generation: 0,
            use_clock: 0,
            max_idle_connections: 1,
        }
    }

    fn resolve(
        &mut self,
        socket_path: &Path,
        key: BackendContextKey,
        attach: BackendAttach,
        max_idle_connections: usize,
    ) -> Rc<BackendAttachedContext> {
        self.max_idle_connections = max_idle_connections;
        self.contexts.retain(|_, context| context.is_live());

        if let Some(current) =
            self.contexts.get(&key).and_then(CachedContext::context)
            && current.matches(socket_path, &attach)
        {
            return current;
        }

        let context = Rc::new(BackendAttachedContext::new(
            socket_path.to_path_buf(),
            key,
            attach,
        ));
        let cached = match key {
            BackendContextKey::Managed(_) => {
                CachedContext::Managed(Rc::clone(&context))
            }
            BackendContextKey::Foreign(_) | BackendContextKey::Configured(_) => {
                CachedContext::Configured(Rc::downgrade(&context))
            }
        };
        self.contexts.insert(key, cached);
        context
    }

    fn resolve_configured(
        &mut self,
        socket_path: &Path,
        config: Arc<StoreConfig>,
        max_idle_connections: usize,
    ) -> Rc<BackendAttachedContext> {
        let generation = self.next_configured_generation;
        self.next_configured_generation += 1;
        self.resolve(
            socket_path,
            BackendContextKey::Configured(generation),
            BackendAttach::Configured(config),
            max_idle_connections,
        )
    }

    fn acquire_client(
        &mut self,
        context: &Rc<BackendAttachedContext>,
    ) -> StorageResult<StorageClient> {
        self.use_clock = self.use_clock.saturating_add(1);
        context.last_used.set(self.use_clock);

        self.trim_idle_connections(context);
        if let Some(client) = context.reusable_client()? {
            return Ok(client);
        }

        let client = context.connect_client()?;
        context.cache_client(&client)?;
        Ok(client)
    }

    fn trim_idle_connections(&mut self, requested: &Rc<BackendAttachedContext>) {
        while self.reclaimable_count_excluding(requested) >= self.max_idle_connections
        {
            if !self.reclaim_oldest_idle(requested) {
                break;
            }
        }
    }

    fn reclaimable_count_excluding(
        &self,
        excluded: &Rc<BackendAttachedContext>,
    ) -> usize {
        self.contexts
            .values()
            .filter_map(CachedContext::context)
            .filter(|context| {
                !Rc::ptr_eq(context, excluded) && context.has_reclaimable_client()
            })
            .count()
    }

    fn reclaim_oldest_idle(&mut self, excluded: &Rc<BackendAttachedContext>) -> bool {
        let candidate = self
            .contexts
            .values()
            .filter_map(CachedContext::context)
            .filter(|context| {
                !Rc::ptr_eq(context, excluded) && context.has_reclaimable_client()
            })
            .min_by_key(|context| context.last_used.get());

        let Some(client) =
            candidate.and_then(|context| context.take_reclaimable_client())
        else {
            return false;
        };

        // `take_reclaimable_client` only returns the cache's sole usable
        // `StorageClient`. Dropping it releases the last ClientConnection,
        // closes its UnixStream, and releases PostgreSQL external-FD
        // accounting through the socket lease.
        drop(client);
        true
    }
}

pub(super) struct PostgresExternalFdPolicy;

impl ExternalFdPolicy for PostgresExternalFdPolicy {
    fn acquire(&self) -> StorageResult<Box<dyn ExternalFdLease>> {
        // SAFETY: BackendStorageService is used only by the PostgreSQL backend
        // main thread. AcquireExternalFD updates backend-local fd.c accounting.
        if unsafe { pg_sys::AcquireExternalFD() } {
            Ok(Box::new(PostgresExternalFdLease))
        } else {
            Err(StorageError::resource_exhausted(
                "PostgreSQL external file descriptor budget exhausted",
            ))
        }
    }
}

struct PostgresExternalFdLease;

impl ExternalFdLease for PostgresExternalFdLease {}

impl Drop for PostgresExternalFdLease {
    fn drop(&mut self) {
        // SAFETY: every lease is created only after AcquireExternalFD
        // succeeds. BackendStorageService and the surrounding PostgreSQL
        // extension remain confined to the owning backend thread.
        unsafe {
            pg_sys::ReleaseExternalFD();
        }
    }
}
