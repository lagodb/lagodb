//! CREATE-time orchestration for a managed Iceberg table.

use iceberg_lite::catalog::TableCreation;
use iceberg_lite::spec::SortOrder;
use lagodb_core::handles::RelationHandle;
use lagodb_core::object_cleanup::ObjectCleanupQueue;
use lagodb_core::options::TableOptions;

use super::bridge::{BootstrapWriter, IcebergTableId};
use super::metadata_table::IcebergMetadata;
use super::metadata_tracker::TxMetadata;
use super::table_definition::ManagedTableDefinition;
use super::table_location::ManagedTableLocation;
use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::options::ResolvedIcebergOptions;
use crate::managed_table::storage::StorageContext;
use crate::storage::transaction_resources::register_table_dir_created;

/// Owns the complete CREATE operation after PostgreSQL has created the
/// relation: option persistence, storage identity, metadata bootstrap,
/// and the Iceberg catalog row.
pub(crate) struct ManagedTableCreation<'a> {
    rel: &'a RelationHandle<'a>,
    storage: StorageContext,
    location: ManagedTableLocation,
}

impl<'a> ManagedTableCreation<'a> {
    /// Create the managed-table state associated with an existing PostgreSQL
    /// relation. Definition lowering is completed before storage-side effects.
    pub(crate) fn execute(
        rel: &'a RelationHandle<'a>,
        table_options: Option<TableOptions>,
    ) -> IcebergResult<()> {
        let mut table_options = table_options.unwrap_or_default();
        let resolved_options =
            ResolvedIcebergOptions::from_table_options(Some(&table_options))?;
        let definition = ManagedTableDefinition::build(rel)?;
        let operation = Self::prepare(rel)?;
        operation.location.persist_option(&mut table_options)?;
        table_options.persist_to_catalog(rel.oid())?;
        let local_file_io = operation
            .storage
            .local_storage()
            .map(|_| operation.storage.file_io().clone());
        let metadata_location = operation.bootstrap(resolved_options, definition)?;

        IcebergMetadata::new(rel.oid())
            .with_metadata_location(metadata_location)
            .with_default_spec_id(0)
            .insert()?;
        if let Some(file_io) = local_file_io {
            TxMetadata::current().record_local_rebuild(rel.oid(), &file_io);
        }
        Ok(())
    }

    fn prepare(rel: &'a RelationHandle<'a>) -> IcebergResult<Self> {
        let storage = StorageContext::for_write(rel)?;
        let location = ManagedTableLocation::for_create(rel, &storage)?;
        if let Some(target) = location.remote_cleanup_target()
            && ObjectCleanupQueue::has_tree_target(target)?
        {
            return Err(IcebergError::ManagedTableLocationCleanupPending {
                location: location.as_str().to_owned(),
            });
        }
        storage.ensure_location_is_empty(location.as_str())?;
        Ok(Self {
            rel,
            storage,
            location,
        })
    }

    fn bootstrap(
        self,
        table_options: ResolvedIcebergOptions,
        definition: ManagedTableDefinition,
    ) -> IcebergResult<String> {
        let Self {
            rel,
            storage,
            location,
        } = self;
        let table_location = location.into_string();
        let (schema, partition_spec) = definition.into_parts();
        let creation = TableCreation::builder()
            .name(String::new())
            .location(table_location.clone())
            .schema(schema)
            .properties(table_options.properties())
            .partition_spec(partition_spec)
            .sort_order(SortOrder::unsorted_order()) // TODO: parse sort order
            .format_version(table_options.format_version())
            .build();

        // Register cleanup before the first metadata write. A later catalog
        // failure therefore removes the newly-created root on transaction abort.
        register_table_dir_created(table_location.clone(), storage.file_io().clone());
        let table = BootstrapWriter::new(storage.into_file_io())
            .write_initial_metadata(
                IcebergTableId::for_relation(rel.oid()),
                creation,
            )?;
        let metadata_location = table
            .metadata_location()
            .map(str::to_owned)
            .ok_or(IcebergError::MetadataLocationNull)?;
        Ok(metadata_location)
    }
}
