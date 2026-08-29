//! Relation-local writable Iceberg foreign modify session.

use iceberg_lite::expr::Predicate;
use iceberg_lite::spec::FormatVersion;
use iceberg_lite::table::Table;
use iceberg_lite::transaction::{RowDeltaValidation, RowLevelCommand};
use lagodb_core::fdw::{
    ForeignInsertBatch, ForeignModifyError, ForeignModifyOperation,
    ForeignModifyOutcome, ForeignModifyState, ForeignRowIdentity, ModifyPlanSlot,
    ModifySlot,
};
use lagodb_core::handles::ItemPointer;
use parquet::file::properties::WriterProperties;
use pgrx::pg_sys;

use super::super::relation::RemoteTableKey;
use super::super::scan::ForeignMutationScan;
use super::super::transaction::ForeignTransaction;
use crate::config::mutation_buffer_flush_bytes;
use crate::engine::schema::relation::RelationShape;
use crate::engine::write::{
    DataFileSink, IcebergRowIdentity, MutationSinks, PgTransactionIsolation,
    RowDeleteClaim, RowDeleteState,
};
use crate::error::IcebergError;

pub(crate) struct IcebergFdwModifyState {
    key: RemoteTableKey,
    command_id: pg_sys::CommandId,
    starting_snapshot_id: Option<i64>,
    validation: Option<(RowLevelCommand, iceberg_lite::transaction::IsolationLevel)>,
    sinks: MutationSinks,
}

impl IcebergFdwModifyState {
    pub(crate) fn new(
        key: &RemoteTableKey,
        operation: ForeignModifyOperation,
        table: &Table,
        shape: &RelationShape,
        mutation_scan: Option<&ForeignMutationScan>,
        command_id: pg_sys::CommandId,
    ) -> Result<Self, ForeignModifyError> {
        let writes_rows = matches!(
            operation,
            ForeignModifyOperation::Insert | ForeignModifyOperation::Update
        );
        let writes_deletes = matches!(
            operation,
            ForeignModifyOperation::Update | ForeignModifyOperation::Delete
        );
        let mutation_scan = if writes_deletes {
            Some(mutation_scan.ok_or(IcebergError::InvariantViolated(
                "foreign UPDATE/DELETE has no mutation target scan",
            ))?)
        } else {
            None
        };
        if writes_deletes && table.metadata().format_version() < FormatVersion::V2 {
            return Err(IcebergError::NotImplemented(
                "writable Iceberg foreign-table UPDATE/DELETE requires format v2 or later",
            )
            .into());
        }
        let writer_properties = WriterProperties::builder().build();
        let rows = writes_rows
            .then(|| {
                DataFileSink::new(
                    table.file_io(),
                    table.metadata().current_schema(),
                    shape,
                    table.metadata(),
                    &writer_properties,
                    mutation_buffer_flush_bytes(),
                )
            })
            .transpose()?;
        let deletes = if writes_deletes {
            let registry = ForeignTransaction::row_registry(key)?;
            let state_id = registry.begin_modify_state()?;
            let scan = mutation_scan
                .as_ref()
                .expect("delete-producing operation retains its scan");
            Some(RowDeleteState::new(
                table.metadata().format_version(),
                table.file_io(),
                table.metadata(),
                &writer_properties,
                registry,
                state_id,
                (table.metadata().format_version() == FormatVersion::V3)
                    .then(|| scan.tasks()),
            )?)
        } else {
            None
        };
        let sinks = MutationSinks::new(rows, deletes)?;
        let validation = match operation {
            ForeignModifyOperation::Insert => None,
            ForeignModifyOperation::Update => {
                let properties = table
                    .metadata()
                    .table_properties()
                    .map_err(IcebergError::from)?;
                Some((
                    RowLevelCommand::Update,
                    PgTransactionIsolation::current()?
                        .effective_iceberg(properties.write_update_isolation_level),
                ))
            }
            ForeignModifyOperation::Delete => {
                let properties = table
                    .metadata()
                    .table_properties()
                    .map_err(IcebergError::from)?;
                Some((
                    RowLevelCommand::Delete,
                    PgTransactionIsolation::current()?
                        .effective_iceberg(properties.write_delete_isolation_level),
                ))
            }
        };
        Ok(Self {
            key: key.clone(),
            command_id,
            starting_snapshot_id: mutation_scan
                .as_ref()
                .and_then(|scan| scan.starting_snapshot_id()),
            validation,
            sinks,
        })
    }

    fn identity(
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<IcebergRowIdentity, ForeignModifyError> {
        let ForeignRowIdentity::ItemPointer(pointer) = plan_slot.identity(0)? else {
            return Err(ForeignModifyError::unsupported(
                "Iceberg foreign modify requires an item-pointer row identity",
            ));
        };
        Ok(IcebergRowIdentity::decode(&ItemPointer {
            block_number: pointer.block_number(),
            offset: pointer.offset(),
        })?)
    }

    fn abort(&mut self) {
        self.sinks.abort();
    }
}

impl ForeignModifyState for IcebergFdwModifyState {
    fn batch_size(&self) -> Result<core::ffi::c_int, ForeignModifyError> {
        Ok(1_000)
    }

    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        self.sinks.insert(slot.tuple_row())?;
        Ok(ForeignModifyOutcome::Applied)
    }

    fn insert_batch(
        &mut self,
        batch: &mut ForeignInsertBatch<'_>,
    ) -> Result<(), ForeignModifyError> {
        batch.process_each(|_, slot| self.insert(slot))
    }

    fn update(
        &mut self,
        slot: &mut ModifySlot<'_>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        let identity = Self::identity(plan_slot)?;
        match self
            .sinks
            .update(identity, slot.tuple_row(), self.command_id)?
        {
            RowDeleteClaim::FirstTouch => Ok(ForeignModifyOutcome::Applied),
            RowDeleteClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(ForeignModifyOutcome::SelfModified {
                modifying_command_id,
            }),
        }
    }

    fn delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        let identity = Self::identity(plan_slot)?;
        match self.sinks.delete(identity, self.command_id)? {
            RowDeleteClaim::FirstTouch => Ok(ForeignModifyOutcome::Applied),
            RowDeleteClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(ForeignModifyOutcome::SelfModified {
                modifying_command_id,
            }),
        }
    }

    fn finish(&mut self) -> Result<(), ForeignModifyError> {
        let referenced_files = self.sinks.referenced_data_files()?;
        let data_files = self.sinks.finish_data_files()?;
        let delete_files = self.sinks.finish_position_deletes()?;
        let has_delta = !data_files.is_empty() || !delete_files.is_empty();
        if !data_files.is_empty() {
            ForeignTransaction::stage_data_files(&self.key, data_files)?;
        }
        for output in delete_files {
            for identity in output.removed_delete_files {
                ForeignTransaction::stage_remove_delete_file(&self.key, identity)?;
            }
            ForeignTransaction::stage_position_delete_file(
                &self.key,
                output.delete_file,
                output.referenced_data_files,
            )?;
        }
        if let Some((command, isolation)) = self.validation.take()
            && has_delta
        {
            let validation =
                RowDeltaValidation::new(command, Predicate::AlwaysTrue, isolation)
                    .with_starting_snapshot_id(self.starting_snapshot_id)
                    .with_referenced_data_files(referenced_files);
            ForeignTransaction::stage_validation(&self.key, validation)?;
        }
        Ok(())
    }
}

impl Drop for IcebergFdwModifyState {
    fn drop(&mut self) {
        self.abort();
    }
}
