use super::storage::{
    TablespaceStorage, TablespaceStorageError, store_id_from_tablespace_name,
};
use crate::pg_wrapper::{CacheRegisterSyscacheCallback, PgWrapper, PgWrapperError};
use pg_lakebase_storage::{StoreConfig, StoreId};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Once;
use thiserror::Error;

// ============================================================================
//  Error Type
// ============================================================================

/// Errors that can occur when looking up tablespace options.
#[derive(Error, Debug)]
pub enum TablespaceCacheError {
    #[error("failed to lookup tablespace: {0}")]
    LookupFailed(#[from] PgWrapperError),

    #[error("tablespace storage config: {0}")]
    Storage(#[from] TablespaceStorageError),
}

// ============================================================================
//  CachedTablespaceOpts
// ============================================================================

/// Cached tablespace options for a distributed storage tablespace.
///
/// Instances only exist for tablespaces that have Lakebase storage options
/// (protocol, bucket/container, credentials, etc.). Native PostgreSQL
/// tablespaces (pg_default, pg_global) are represented as `None` in the cache.
#[derive(Debug, Clone)]
pub struct CachedTablespaceOpts {
    tablespace_name: String,
    store_id: StoreId,
    storage: TablespaceStorage,
}

impl CachedTablespaceOpts {
    /// Returns the PostgreSQL tablespace name.
    pub fn tablespace_name(&self) -> &str {
        &self.tablespace_name
    }

    /// Returns the object-storage store id for this tablespace.
    ///
    /// Lakebase maps one distributed tablespace to one storage-service store.
    /// The store id is intentionally the human-readable tablespace name.
    pub fn store_id(&self) -> &str {
        self.store_id.as_str()
    }

    /// Returns the storage protocol from the tablespace option.
    pub fn protocol(&self) -> &'static str {
        self.storage.protocol_name()
    }

    /// Returns the URL scheme used in Iceberg metadata paths.
    pub fn url_scheme(&self) -> &'static str {
        self.storage.url_scheme()
    }

    /// Returns the object namespace: S3/GCS bucket or Azure container.
    pub fn object_namespace(&self) -> &str {
        self.storage.object_namespace()
    }

    /// Returns the name of the option that supplies the object namespace.
    pub fn namespace_option_name(&self) -> &'static str {
        self.storage.namespace_option_name()
    }

    /// Returns the base URL for this tablespace storage configuration.
    pub fn base_url(&self) -> String {
        self.storage.base_url()
    }

    /// Returns the storage-service configuration represented by this tablespace.
    pub fn store_config(&self) -> StoreConfig {
        self.storage.store_config()
    }
}

// ============================================================================
//  Cache Infrastructure
// ============================================================================
thread_local! {
    static TABLESPACE_CACHE: RefCell<HashMap<pg_sys::Oid, Option<Rc<CachedTablespaceOpts>>>> =
        RefCell::new(HashMap::new());
}

static INIT: Once = Once::new();

unsafe extern "C" fn invalidate_tablespace_cache_callback(
    _arg: pg_sys::Datum,
    _cacheid: std::os::raw::c_int,
    _hashvalue: u32,
) {
    // TODO: Maintain distributed store registry in pg-lakebase-storage.
    // When a tablespace is modified/deleted, we should unregister or re-register
    // its store config in the global storage service to keep them in sync.
    TABLESPACE_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

fn initialize_tablespace_cache() {
    INIT.call_once(|| unsafe {
        CacheRegisterSyscacheCallback(
            pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
            Some(invalidate_tablespace_cache_callback),
            0.into(),
        );
    });
}

// ============================================================================
//  Public API
// ============================================================================

/// Check if a tablespace uses distributed storage.
///
/// Returns `false` for native PostgreSQL tablespaces and `true` for any
/// tablespace that has Lakebase storage options configured.
pub fn is_distributed_tablespace(
    spcid: pg_sys::Oid,
) -> Result<bool, TablespaceCacheError> {
    Ok(get_tablespace(spcid)?.is_some())
}

/// Get tablespace options for a given OID.
///
/// Returns `None` for native PostgreSQL tablespaces (no Lakebase storage
/// options). Returns `Some(Rc<…>)` for distributed tablespaces.
///
/// Uses `Rc` instead of `Arc` because PostgreSQL backends are single-threaded.
pub fn get_tablespace(
    spcid: pg_sys::Oid,
) -> Result<Option<Rc<CachedTablespaceOpts>>, TablespaceCacheError> {
    initialize_tablespace_cache();

    TABLESPACE_CACHE.with(|cache| {
        if let Some(entry) = cache.borrow().get(&spcid) {
            return Ok(entry.clone());
        }

        let opts: Option<Rc<CachedTablespaceOpts>> =
            lookup_tablespace_options(spcid)?.map(Rc::new);
        cache.borrow_mut().insert(spcid, opts.clone());
        Ok(opts)
    })
}

// ============================================================================
//  Internal Parsing
// ============================================================================

fn lookup_tablespace_options(
    spcid: pg_sys::Oid,
) -> Result<Option<CachedTablespaceOpts>, TablespaceCacheError> {
    let cache_id = pg_sys::SysCacheIdentifier::TABLESPACEOID as i32;

    let tp = PgWrapper::search_sys_cache1(
        cache_id,
        pg_sys::Datum::from(u32::from(spcid) as usize),
    )?;

    let Some(tp) = tp else {
        return Ok(None);
    };

    let result = (|| {
        let tablespace_name = lookup_tablespace_name(cache_id, tp)?;

        let mut is_null = false;
        let datum = PgWrapper::sys_cache_get_attr(
            cache_id,
            tp,
            pg_sys::Anum_pg_tablespace_spcoptions as i16,
            &mut is_null,
        )?;

        if is_null {
            Ok(None)
        } else {
            // SAFETY: datum is valid (obtained from SysCacheGetAttr on a live tuple).
            // Vec::from_datum copies the data out of Postgres memory before we
            // release the syscache tuple below.
            let options_vec = unsafe { Vec::<String>::from_datum(datum, false) }
                .unwrap_or_default();
            if options_vec.is_empty() {
                Ok(None)
            } else {
                parse_options_to_cached(tablespace_name, options_vec)
            }
        }
    })();

    let _ = PgWrapper::release_sys_cache(tp);

    result
}

fn lookup_tablespace_name(
    cache_id: i32,
    tuple: pg_sys::HeapTuple,
) -> Result<String, TablespaceCacheError> {
    let mut is_null = false;
    let datum = PgWrapper::sys_cache_get_attr(
        cache_id,
        tuple,
        pg_sys::Anum_pg_tablespace_spcname as i16,
        &mut is_null,
    )?;

    // pg_tablespace.spcname is defined as `name NOT NULL` in the PostgreSQL
    // catalog schema. A null value here would indicate catalog corruption,
    // which is unrecoverable — panic is the correct response.
    let datum = (!is_null)
        .then_some(datum)
        .expect("pg_tablespace.spcname is a NOT NULL catalog column");

    // SAFETY: datum is non-null and points at a valid NameData within the
    // syscache tuple. We copy to an owned String before releasing the tuple.
    let name = unsafe {
        let name_ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
        std::ffi::CStr::from_ptr((*name_ptr).data.as_ptr())
            .to_string_lossy()
            .into_owned()
    };
    Ok(name)
}

fn parse_options_to_cached(
    tablespace_name: String,
    options_vec: Vec<String>,
) -> Result<Option<CachedTablespaceOpts>, TablespaceCacheError> {
    let Some(storage) =
        TablespaceStorage::from_catalog_options(&tablespace_name, options_vec)?
    else {
        return Ok(None);
    };
    let store_id = store_id_from_tablespace_name(&tablespace_name)?;

    Ok(Some(CachedTablespaceOpts {
        tablespace_name,
        store_id,
        storage,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(
        tablespace_name: &str,
        options: &[&str],
    ) -> Result<Option<CachedTablespaceOpts>, TablespaceCacheError> {
        parse_options_to_cached(
            tablespace_name.to_string(),
            options.iter().map(|option| option.to_string()).collect(),
        )
    }

    #[test]
    fn parsed_tablespace_name_is_store_id() {
        let opts = parse(
            "lake_spc",
            &["protocol=s3", "bucket=my-lake", "region=us-east-1"],
        )
        .unwrap()
        .unwrap();

        assert_eq!(opts.tablespace_name(), "lake_spc");
        assert_eq!(opts.store_id(), "lake_spc");
        assert_eq!(opts.protocol(), "s3");
        assert_eq!(opts.url_scheme(), "s3");
        assert_eq!(opts.object_namespace(), "my-lake");
        assert_eq!(opts.base_url(), "s3://my-lake");
    }

    #[test]
    fn distributed_tablespace_requires_namespace() {
        let error =
            parse("lake_spc", &["protocol=s3", "region=us-east-1"]).unwrap_err();

        assert!(matches!(
            error,
            TablespaceCacheError::Storage(
                TablespaceStorageError::MissingRequiredOption {
                    tablespace,
                    protocol: "s3",
                    option: "bucket",
                }
            ) if tablespace == "lake_spc"
        ));
    }

    #[test]
    fn unsupported_protocol_returns_error() {
        let error =
            parse("lake_spc", &["protocol=ftp", "bucket=my-lake"]).unwrap_err();

        assert!(matches!(
            error,
            TablespaceCacheError::Storage(
                TablespaceStorageError::UnsupportedProtocol {
                    tablespace,
                    protocol,
                }
            ) if tablespace == "lake_spc" && protocol == "ftp"
        ));
    }

    #[test]
    fn distributed_tablespace_name_must_be_valid_store_id() {
        let error =
            parse("bad/store", &["protocol=s3", "bucket=my-lake"]).unwrap_err();

        assert!(matches!(
            error,
            TablespaceCacheError::Storage(
                TablespaceStorageError::InvalidStoreId { tablespace, .. }
            ) if tablespace == "bad/store"
        ));
    }

    #[test]
    fn native_tablespace_options_are_not_distributed_storage_options() {
        let opts = parse("local_spc", &["seq_page_cost=1.1"]).unwrap();

        assert!(opts.is_none());
    }
}
