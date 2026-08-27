//! Explicit SQL cache invalidation for externally replaced exact object keys.

use lagodb_core::diag::PgReportError;
use pgrx::prelude::*;

use crate::error::ConnectorError;

use super::{ObjectUri, ResolvedStorageLocation};

fn invalidate(
    object_uri: &str,
    server: Option<&str>,
) -> Result<bool, ConnectorError> {
    let object = ObjectUri::parse(object_uri)?;
    let location = ResolvedStorageLocation::resolve(object, server)?;
    let object = location.acquire_object_access_from_pg_gucs()?;
    object.invalidate_cache().map_err(ConnectorError::from)
}

/// Invalidates the current cache residency for one exact object URI.
///
/// This operation is intentionally explicit: replacing an object outside
/// the storage service does not cause generation or tag validation. It
/// reports `Busy` while an existing reader or fill owns the residency, so the
/// caller can retry after that operation ends. The result is true when cached
/// state was removed and false when the object was not resident.
#[pg_extern(sql = r#"
    CREATE FUNCTION lagodb.invalidate_object_cache(
        object_uri text,
        server text DEFAULT NULL
    ) RETURNS bool
    LANGUAGE c
    AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
"#)]
fn invalidate_object_cache(
    object_uri: &str,
    server: default!(Option<String>, "NULL"),
) -> bool {
    invalidate(object_uri, server.as_deref())
        .unwrap_or_else(|error| PgReportError::from_domain_error(error).report())
}
