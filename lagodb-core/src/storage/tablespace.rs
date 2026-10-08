//! Typed access to the runtime's authoritative tablespace bindings.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_int};
use std::rc::Rc;
use std::slice;
use std::sync::Once;

use pgrx::prelude::PgSqlErrorCode;
use pgrx::{PgMemoryContexts, pg_sys};

use super::volume::{StorageVolumeId, StorageVolumeRoute};
use crate::diag::{PgReportError, SqlStateError};
use crate::runtime_api::{
    RuntimeApiError, RuntimeClient, TablespaceBindingEntry, TablespaceBindingsOutput,
};
use crate::wrapper::CacheRegisterSyscacheCallback;

#[derive(Debug, thiserror::Error)]
pub enum TablespaceStorageError {
    #[error(transparent)]
    Runtime(#[from] RuntimeApiError),
    #[error(transparent)]
    Lookup(PgReportError),
    #[error("invalid tablespace storage response: {0}")]
    InvalidResponse(&'static str),
}

impl SqlStateError for TablespaceStorageError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Runtime(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::Lookup(error) => error.sql_error_code(),
            Self::InvalidResponse(_) => PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TablespaceStorageBinding {
    volume_id: StorageVolumeId,
    route: StorageVolumeRoute,
}

impl TablespaceStorageBinding {
    pub const fn volume_id(&self) -> StorageVolumeId {
        self.volume_id
    }

    pub fn object_namespace(&self) -> &str {
        self.route.object_namespace()
    }

    pub fn effective_base_uri(&self) -> &str {
        self.route.effective_base_uri()
    }

    pub fn rooted_object_key(&self, suffix: &str) -> String {
        let root = self.route.effective_root_key();
        let suffix = suffix.trim_start_matches('/');
        let mut key = String::with_capacity(root.len() + 1 + suffix.len());
        key.push_str(root);
        key.push('/');
        key.push_str(suffix);
        key
    }

    /// Copy one entry while the synchronous runtime output is still live.
    ///
    /// # Safety
    /// `entry` must contain the strings supplied by the exact-build callback.
    unsafe fn from_entry(
        entry: &TablespaceBindingEntry,
    ) -> Result<Self, TablespaceStorageError> {
        let volume_id = StorageVolumeId::new(entry.volume_id).map_err(|_| {
            TablespaceStorageError::InvalidResponse("invalid volume id")
        })?;
        // SAFETY: the caller keeps the namespace string live through this copy.
        let namespace = unsafe { Self::read_string(entry.object_namespace)? };
        // SAFETY: the base URI belongs to the same live callback output.
        let base_uri = unsafe { Self::read_string(entry.effective_base_uri)? };
        let route = StorageVolumeRoute::new(namespace, base_uri)
            .map_err(|_| TablespaceStorageError::InvalidResponse("invalid route"))?;
        Ok(Self { volume_id, route })
    }

    // SAFETY: non-null ABI strings must remain valid through this synchronous call.
    unsafe fn read_string<'a>(
        value: *const c_char,
    ) -> Result<&'a str, TablespaceStorageError> {
        if value.is_null() {
            return Err(TablespaceStorageError::InvalidResponse("missing string"));
        }
        unsafe { CStr::from_ptr(value) }
            .to_str()
            .map_err(|_| TablespaceStorageError::InvalidResponse("invalid UTF-8"))
    }
}

/// A complete query projection, not the worker's volume registry. None means
/// unloaded; a loaded map's absent OID means local, including for an empty map.
struct TablespaceBindingCache {
    bindings: Option<HashMap<pg_sys::Oid, Rc<TablespaceStorageBinding>>>,
}

impl TablespaceBindingCache {
    const fn new() -> Self {
        Self { bindings: None }
    }

    fn find(&self, oid: pg_sys::Oid) -> Option<Option<Rc<TablespaceStorageBinding>>> {
        self.bindings
            .as_ref()
            .map(|bindings| bindings.get(&oid).cloned())
    }

    fn load() -> Result<
        HashMap<pg_sys::Oid, Rc<TablespaceStorageBinding>>,
        TablespaceStorageError,
    > {
        let client = RuntimeClient::connect()?;
        let mut context = PgMemoryContexts::new("LagoDB tablespace binding snapshot");
        // SAFETY: this owned context lives through the callback and decoding.
        // Only Rust-owned bindings or an owned error leave it; Drop releases
        // the entire ABI array and strings on success and failure alike.
        unsafe {
            context.switch_to(|_| {
                let mut output = TablespaceBindingsOutput::default();
                let status = client.load_tablespace_bindings(&mut output);
                output
                    .error
                    .into_result(status, "load_tablespace_bindings")
                    .map_err(TablespaceStorageError::Lookup)?;
                let entries = if output.count == 0 {
                    &[]
                } else {
                    // The exact-build producer owns this complete array and
                    // validates OID uniqueness and lifecycle before returning it.
                    slice::from_raw_parts(output.entries, output.count)
                };
                let mut bindings = HashMap::with_capacity(entries.len());
                for entry in entries {
                    let binding = TablespaceStorageBinding::from_entry(entry)?;
                    bindings.insert(entry.tablespace_oid, Rc::new(binding));
                }
                Ok(bindings)
            })
        }
    }

    fn invalidate(&mut self) {
        // Clearing only the map would incorrectly mark every tablespace local.
        // Existing users retain their bindings through Rc.
        self.bindings = None;
    }
}

thread_local! {
    static CACHE: RefCell<TablespaceBindingCache> =
        const { RefCell::new(TablespaceBindingCache::new()) };
}

static INIT: Once = Once::new();

pub struct TablespaceStorage;

impl TablespaceStorage {
    pub fn is_object_backed(
        oid: pg_sys::Oid,
    ) -> Result<bool, TablespaceStorageError> {
        Ok(Self::resolve(oid)?.is_some())
    }

    /// Load all Bound routes once per invalidation cycle. An absent OID in the
    /// complete snapshot is local and needs no further lookup. Config publication
    /// and native tablespace DDL invalidate it through PG's syscache channel.
    pub fn resolve(
        oid: pg_sys::Oid,
    ) -> Result<Option<Rc<TablespaceStorageBinding>>, TablespaceStorageError> {
        if oid == pg_sys::DEFAULTTABLESPACE_OID || oid == pg_sys::GLOBALTABLESPACE_OID
        {
            return Ok(None);
        }
        // SAFETY: PG retains this static callback for the backend lifetime.
        INIT.call_once(|| unsafe {
            CacheRegisterSyscacheCallback(
                pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
                Some(Self::invalidate_cache),
                0.into(),
            );
        });
        if let Some(binding) = CACHE.with_borrow(|cache| cache.find(oid)) {
            return Ok(binding);
        }
        // Do not hold the cache borrow across runtime/PG calls: they can process
        // invalidation messages and invoke our callback.
        let bindings = TablespaceBindingCache::load()?;
        // Publish only after the complete snapshot has been copied successfully.
        Ok(CACHE.with_borrow_mut(|cache| {
            let binding = bindings.get(&oid).cloned();
            cache.bindings = Some(bindings);
            binding
        }))
    }

    /// Invalidate all DSO-local tablespace caches in this backend after a
    /// nontransactional file publication, including negative lookup results.
    pub fn invalidate_local() {
        let mut message = Self::invalidation_message();
        // SAFETY: a complete shared-catalog invalidation on the PG main thread.
        unsafe { pg_sys::LocalExecuteInvalidationMessage(&mut message) };
    }

    /// Broadcast an immediate, nontransactional invalidation to all backends.
    /// Unlike CacheInvalidateCatalog, this does not wait for transaction commit:
    /// the published config file survives SQL rollback.
    pub fn invalidate_all() {
        let message = Self::invalidation_message();
        // SAFETY: PG copies this call-scoped message into its shared queue.
        unsafe { pg_sys::SendSharedInvalidMessages(&message, 1) };
        Self::invalidate_local();
    }

    fn invalidation_message() -> pg_sys::SharedInvalidationMessage {
        let mut message = pg_sys::SharedInvalidationMessage::default();
        message.cat = pg_sys::SharedInvalCatalogMsg {
            id: pg_sys::SHAREDINVALCATALOG_ID as i8,
            dbId: pg_sys::InvalidOid,
            catId: pg_sys::TableSpaceRelationId,
        };
        message
    }

    unsafe extern "C" fn invalidate_cache(
        _arg: pg_sys::Datum,
        _cacheid: c_int,
        _hashvalue: u32,
    ) {
        CACHE.with_borrow_mut(TablespaceBindingCache::invalidate);
    }
}
