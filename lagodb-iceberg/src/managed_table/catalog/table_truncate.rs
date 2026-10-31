//! Local TRUNCATE creates an empty Iceberg table in a PG storage generation.

use iceberg_lite::catalog::TableCreation;
use lagodb_core::catalog::CatalogRelation;
use lagodb_core::handles::{RelFileLocator, RelationHandle};

use super::bridge::{BootstrapWriter, IcebergTableId};
use super::local_storage::LocalTableRoot;
use super::metadata_table::IcebergMetadata;
use super::metadata_tracker::TxMetadata;
use crate::error::IcebergResult;
use crate::managed_table::storage::StorageContext;
use crate::storage::transaction_resources::register_table_dir_created;

pub(crate) struct ManagedTableTruncate;

impl ManagedTableTruncate {
    pub(crate) fn execute(
        rel: &RelationHandle<'_>,
        new_locator: Option<RelFileLocator>,
    ) -> IcebergResult<()> {
        let storage = StorageContext::for_write(rel)?;
        if storage.object_tablespace().is_some() {
            return TxMetadata::current()
                .stage_truncate(rel.oid(), storage.file_io());
        }
        let local_storage = storage.local_storage().expect("local storage selected");

        let tracker = TxMetadata::current();
        tracker.prepare_local_rebuild(rel.oid())?;
        let metadata =
            tracker.begin_table_definition_change(rel.oid(), storage.file_io())?;
        let previous = LocalTableRoot::for_relation(rel);
        let new_root =
            new_locator.map(|locator| LocalTableRoot::for_locator(rel, locator));
        let replacement = new_root.as_ref().unwrap_or(&previous);
        let same_directory = previous.as_str() == replacement.as_str();
        if same_directory {
            // PostgreSQL selects this path only when the table or locator
            // belongs to this subtransaction. Rollback discards the generation,
            // so no old file set needs to be retained.
            local_storage.truncate_directory(previous.as_str())?;
        } else {
            storage.ensure_location_is_empty(replacement.as_str())?;
            register_table_dir_created(
                replacement.as_str().to_owned(),
                storage.file_io().clone(),
            );
        }

        let creation = TableCreation::builder()
            .name(String::new())
            .location(replacement.as_str().to_owned())
            .schema((**metadata.current_schema()).clone())
            .partition_spec(
                (**metadata.default_partition_spec()).clone().into_unbound(),
            )
            .sort_order((**metadata.default_sort_order()).clone())
            .properties(metadata.properties().clone())
            .format_version(metadata.format_version())
            .build();
        let table = BootstrapWriter::new(storage.file_io().clone())
            .write_initial_metadata(
                IcebergTableId::for_relation(rel.oid()),
                creation,
            )?;
        IcebergMetadata::publish_rebuilt_table(rel.oid(), &table)?;
        tracker.record_local_rebuild(rel.oid(), storage.file_io());
        // Bootstrap resets Iceberg schema and field IDs even when the PG
        // locator and table options stay unchanged. Complete every local
        // rebuild by invalidating dependent plans after catalog and tracker
        // state agree, before the command counter exposes the new generation.
        rel.invalidate_relcache()?;
        CatalogRelation::make_changes_visible()?;

        if !same_directory {
            previous.retire(storage.into_file_io())?;
        }
        Ok(())
    }
}
