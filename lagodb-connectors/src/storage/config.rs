//! Connector adapter for the shared core storage-profile schema.

use pg_lakebase_core::storage::profile::StorageProfileConfig;
use pgrx::pg_sys;

use crate::error::ConnectorError;

pub(crate) type ConnectorStoreConfig = StorageProfileConfig;

pub(crate) fn validate_storage_options(
    options: &[Option<String>],
    catalog: Option<pg_sys::Oid>,
) -> Result<(), ConnectorError> {
    StorageProfileConfig::validate_options(options, catalog)
        .map_err(ConnectorError::from)
}
