//! Explicit SQL cache invalidation for externally replaced exact object keys.

use pg_lakebase_core::diag::PgReportError;
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

#[pg_schema]
mod lagodb {
    use super::*;

    /// Invalidates the current cache residency for one exact object URI.
    ///
    /// This operation is intentionally explicit: replacing an object outside
    /// the storage service does not cause generation or tag validation. It
    /// reports `Busy` while an existing reader or fill owns the residency, so
    /// the caller can retry after that operation ends. The result is true when
    /// cached state was removed and false when the object was not resident.
    #[pg_extern]
    fn invalidate_object_cache(
        object_uri: &str,
        server: default!(Option<String>, "NULL"),
    ) -> bool {
        super::invalidate(object_uri, server.as_deref())
            .unwrap_or_else(|error| PgReportError::from_domain_error(error).report())
    }
}
