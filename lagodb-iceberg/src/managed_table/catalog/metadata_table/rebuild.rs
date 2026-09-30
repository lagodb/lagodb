//! Publish a new local table generation while its relation is exclusively locked.

use iceberg_lite::table::Table;
use lagodb_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, CatalogUpdateResult,
};
use pgrx::pg_sys;

use super::{CatalogOp, CatalogResultExt, IcebergMetadata, TupleReplacement, column};
use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::catalog::error::MetadataCatalogError;
use crate::managed_table::gucs;

impl IcebergMetadata {
    pub(crate) fn publish_rebuilt_table(
        relid: pg_sys::Oid,
        table: &Table,
    ) -> IcebergResult<()> {
        let location = table
            .metadata_location()
            .ok_or(IcebergError::MetadataLocationNull)?;
        let catalog =
            CatalogRelation::open(Self::table_oid()?, pg_sys::RowExclusiveLock as _)
                .map_catalog_err(CatalogOp::Update)?;
        let pkey_oid = Self::pkey_oid()?;
        let descriptor = catalog.as_handle().tuple_desc();
        let max_retries = gucs::max_commit_retries();
        // The relation lock excludes other table mutations, but a worker that
        // skips that lock can still defer maintenance on this catalog row.
        // Rescan after a tuple-version conflict; SelfVisible also retains our
        // earlier catalog changes within the same command.
        for _ in 0..=max_retries {
            let mut scan = catalog
                .begin_scan(
                    pkey_oid,
                    true,
                    CatalogSnapshot::SelfVisible,
                    [CatalogScanKey::oid_eq(column::RELID as _, relid)],
                )
                .map_catalog_err(CatalogOp::Update)?;
            let tuple = scan.get_next().map_catalog_err(CatalogOp::Update)?.ok_or(
                IcebergError::MetadataCatalog(MetadataCatalogError::NotFound(relid)),
            )?;
            // SAFETY: the descriptor and scanned tuple belong to this open catalog.
            let mut replacement = unsafe { TupleReplacement::new(descriptor) };
            replacement.set(column::METADATA_LOCATION, Some(location));
            replacement.set::<&str>(column::PREVIOUS_METADATA_LOCATION, None);
            replacement.set(
                column::DEFAULT_SPEC_ID,
                Some(table.metadata().default_partition_spec_id()),
            );
            replacement.set::<pg_sys::TimestampTz>(column::MAINTENANCE_DUE_AT, None);
            // SAFETY: replacement and tuple use the same live descriptor.
            let updated = unsafe { replacement.apply(descriptor, tuple.as_raw()) };
            match catalog
                .catalog_update_optimistic(tuple, &updated)
                .map_catalog_err(CatalogOp::Update)?
            {
                CatalogUpdateResult::Success => return Ok(()),
                CatalogUpdateResult::Conflict => continue,
            }
        }
        Err(IcebergError::MetadataCommitConflict { relid, max_retries })
    }
}
