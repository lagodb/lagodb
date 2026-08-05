use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Once;

use pgrx::FromDatum;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use super::options::{TablespaceBinding, TablespaceError, parse_catalog_binding};
use crate::catalog::{SysCacheTuple, search_syscache1};
use crate::diag::{PgError, SqlStateError};
use crate::runtime_api::{RuntimeClient, StorageVolumeRouteLookupError};
use crate::storage::volume::{StorageVolumeId, StorageVolumeRoute};
use crate::wrapper::CacheRegisterSyscacheCallback;

#[derive(Debug, Error)]
pub enum TablespaceCacheError {
    #[error("failed to look up tablespace: {0}")]
    LookupFailed(#[from] PgError),

    #[error("invalid Lakebase tablespace binding: {0}")]
    InvalidBinding(#[from] TablespaceError),

    #[error("failed to resolve Lakebase tablespace routing: {0}")]
    Route(#[from] StorageVolumeRouteLookupError),
}

impl SqlStateError for TablespaceCacheError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::LookupFailed(error) => error.sql_error_code(),
            Self::InvalidBinding(error) => error.sql_error_code(),
            Self::Route(
                StorageVolumeRouteLookupError::NotFound(_)
                | StorageVolumeRouteLookupError::InvalidUtf8(_)
                | StorageVolumeRouteLookupError::InvalidRoute { .. },
            ) => PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
            Self::Route(StorageVolumeRouteLookupError::Resolution { .. }) => {
                PgSqlErrorCode::ERRCODE_CONFIG_FILE_ERROR
            }
            Self::Route(
                StorageVolumeRouteLookupError::Runtime(_)
                | StorageVolumeRouteLookupError::UnknownStatus { .. },
            ) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachedTablespaceOpts {
    binding: TablespaceBinding,
    route: StorageVolumeRoute,
}

impl CachedTablespaceOpts {
    pub const fn volume_id(&self) -> StorageVolumeId {
        self.binding.volume_id()
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

    pub fn from_catalog_options(
        options: &[String],
    ) -> Result<Option<Self>, TablespaceCacheError> {
        let Some(binding) = parse_catalog_binding(options)? else {
            return Ok(None);
        };
        let volume_id = binding.volume_id();
        let route = RuntimeClient::connect()
            .map_err(StorageVolumeRouteLookupError::from)?
            .storage_volume_route(volume_id)?;
        Ok(Some(Self { binding, route }))
    }
}

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
    TABLESPACE_CACHE.with(|cache| cache.borrow_mut().clear());
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

pub fn is_distributed_tablespace(
    spcid: pg_sys::Oid,
) -> Result<bool, TablespaceCacheError> {
    let tuple = search_syscache1(
        pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
        pg_sys::Datum::from(u32::from(spcid) as usize),
    )?;
    let Some(tuple) = tuple else {
        return Ok(false);
    };
    Ok(parse_catalog_binding(&read_options(&tuple)?)?.is_some())
}

pub fn get_tablespace(
    spcid: pg_sys::Oid,
) -> Result<Option<Rc<CachedTablespaceOpts>>, TablespaceCacheError> {
    initialize_tablespace_cache();
    TABLESPACE_CACHE.with(|cache| {
        if let Some(entry) = cache.borrow().get(&spcid) {
            return Ok(entry.clone());
        }
        let options = lookup_tablespace_options(spcid)?.map(Rc::new);
        cache.borrow_mut().insert(spcid, options.clone());
        Ok(options)
    })
}

fn lookup_tablespace_options(
    spcid: pg_sys::Oid,
) -> Result<Option<CachedTablespaceOpts>, TablespaceCacheError> {
    let tuple = search_syscache1(
        pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
        pg_sys::Datum::from(u32::from(spcid) as usize),
    )?;
    let Some(tuple) = tuple else {
        return Ok(None);
    };
    let options = read_options(&tuple)?;
    CachedTablespaceOpts::from_catalog_options(&options)
}

fn read_options(tuple: &SysCacheTuple) -> Result<Vec<String>, PgError> {
    let Some(datum) = tuple.get_attr(pg_sys::Anum_pg_tablespace_spcoptions as i16)?
    else {
        return Ok(Vec::new());
    };
    // SAFETY: FromDatum copies the array while the syscache tuple is pinned.
    Ok(unsafe { Vec::<String>::from_datum(datum, false) }.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_internal_volume_binding() {
        let options = vec![
            "seq_page_cost=1.1".to_owned(),
            "lakebase_volume_id=42".to_owned(),
        ];
        let binding = parse_catalog_binding(&options).unwrap().unwrap();
        assert_eq!(binding.volume_id().get(), 42);
    }

    #[test]
    fn native_options_are_not_distributed() {
        let options = vec!["seq_page_cost=1.1".to_owned()];
        assert!(parse_catalog_binding(&options).unwrap().is_none());
    }
}
