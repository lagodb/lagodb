//! Runtime state for one relation-local mutation session.

use std::collections::BTreeSet;

use iceberg_lite::io::FileIO;
use iceberg_lite::spec::{DataFile, FormatVersion};
use iceberg_lite::transaction::{RowDeltaValidation, RowLevelCommand};
use lagodb_core::handles::RelationHandle;
use lagodb_core::prelude::*;
use parquet::file::properties::WriterProperties;
use pgrx::pg_sys;

use super::plan::{ModifyCommand, TargetDependency, ValidationPlan};
use super::row_identity::{IcebergModifyQueryState, IcebergModifyScanContext};
use crate::config::mutation_buffer_flush_bytes;
use crate::engine::schema::relation::RelationShape;
use crate::engine::write::PgTransactionIsolation;
use crate::engine::write::{
    DataFileSink, IcebergRowIdentity, MutationSinks, RowDeleteClaim, RowDeleteOutput,
    RowDeleteState,
};
use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::catalog::metadata_tracker::TxMetadata;
use crate::managed_table::options::IcebergTableOptions;
use crate::managed_table::storage::StorageContext;

/// Final output of one relation-local PostgreSQL ModifyTable session.
struct StatementOutcome {
    command: ModifyCommand,
    target_dependency: TargetDependency,
    new_data_files: Vec<DataFile>,
    row_delete_files: Vec<RowDeleteOutput>,
    referenced_data_files: BTreeSet<String>,
}

impl StatementOutcome {
    fn has_delta(&self) -> bool {
        !self.new_data_files.is_empty() || !self.row_delete_files.is_empty()
    }

    fn row_delta_validation(
        &self,
    ) -> IcebergResult<Option<(RowLevelCommand, Option<i64>)>> {
        if !self.has_delta() {
            return Ok(None);
        }
        let Some(command) = self.command.validation_command() else {
            return Ok(None);
        };
        match self.target_dependency {
            TargetDependency::Independent => Ok(None),
            dependency => Ok(Some((command, dependency.required_snapshot()?))),
        }
    }
}

/// Iceberg mutation state for INSERT/UPDATE/DELETE/MERGE operations.
///
/// Constructed eagerly: storage context, schemas, and writer are all wired up
/// by the time this struct exists. `MutationSinks` encodes the action
/// capability selected by PostgreSQL, while `ValidationPlan` retains the
/// command/dependency decisions needed after writers finish.
pub struct IcebergModifyState {
    rel_oid: pg_sys::Oid,
    file_io: FileIO,
    validation: ValidationPlan,
    sinks: MutationSinks,
}

impl AmModifyState for IcebergModifyState {
    type QueryState = IcebergModifyQueryState;
    type ScanContext = IcebergModifyScanContext;

    fn begin_modify(
        rel: &RelationHandle,
        context: ModifyStateContext<Self::QueryState, Self::ScanContext>,
    ) -> AmResult<Self> {
        Self::open(rel, context)
    }

    fn insert_slot(
        &mut self,
        new: TupleSlotRow<'_>,
        _context: MutationWriteContext,
    ) -> AmResult<()> {
        self.sinks.insert(new)?;
        Ok(())
    }

    fn update_slot(
        &mut self,
        row_id: ItemPointer,
        _old: TupleSlotRow<'_>,
        new: TupleSlotRow<'_>,
        context: MutationUpdateContext<'_>,
    ) -> AmResult<MutationOutcome> {
        let identity = IcebergRowIdentity::decode(&row_id)?;
        Ok(match self.sinks.update(identity, new, context.cid)? {
            RowDeleteClaim::FirstTouch => MutationOutcome::Applied,
            RowDeleteClaim::PreviouslyModified {
                modifying_command_id,
            } => MutationOutcome::AlreadyModifiedInCurrentTransaction {
                modifying_command_id,
            },
        })
    }

    fn delete_slot(
        &mut self,
        row_id: ItemPointer,
        context: MutationDeleteContext<'_>,
    ) -> AmResult<MutationOutcome> {
        let identity = IcebergRowIdentity::decode(&row_id)?;
        Ok(match self.sinks.delete(identity, context.cid)? {
            RowDeleteClaim::FirstTouch => MutationOutcome::Applied,
            RowDeleteClaim::PreviouslyModified {
                modifying_command_id,
            } => MutationOutcome::AlreadyModifiedInCurrentTransaction {
                modifying_command_id,
            },
        })
    }

    fn abort_modify(&mut self) {
        // Best-effort in-memory cleanup; persistent artifacts are unwound by
        // ResourceOwner cleanup (see the orphan-file note in `end_modify`).
        self.sinks.abort();
    }

    fn end_modify(&mut self) -> AmResult<()> {
        // Orphan-file note: data files already uploaded before a later flush
        // failure are NOT leaked. Every produced file is registered via
        // `register_object_file_staged()` / `mark_object_file_uploaded()`, and
        // `StorageTransactionResource::on_abort` unlinks staging files or
        // issues remote deletes on abort.
        let outcome = self.finish_statement()?;
        self.stage_statement(outcome)?;
        Ok(())
    }
}

impl AmCopySession for IcebergModifyState {
    fn begin_copy(rel: &RelationHandle) -> AmResult<Self> {
        let query_state = ModifyQueryState::<IcebergModifyQueryState>::new()?;
        Self::open(
            rel,
            ModifyStateContext::<
                IcebergModifyQueryState,
                IcebergModifyScanContext,
            >::independent(
                query_state,
                pg_sys::CmdType::CMD_INSERT,
                ModifyActions::INSERT,
            ),
        )
    }

    fn end_copy(&mut self) -> AmResult<()> {
        let outcome = self.finish_statement()?;
        self.stage_statement(outcome)?;
        Ok(())
    }

    fn abort_copy(&mut self) {
        self.sinks.abort();
    }

    fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        _cid: pg_sys::CommandId,
        _options: i32,
    ) -> AmResult<()> {
        self.sinks.insert(row)?;
        Ok(())
    }

    fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        _cid: pg_sys::CommandId,
        _options: i32,
    ) -> AmResult<()> {
        let sink = self.sinks.data_sink_mut()?;
        for row in rows.iter() {
            // SAFETY: every slot in this multi-insert callback belongs to the
            // relation whose bound plan owns `sink`.
            unsafe { sink.append(row)? };
        }
        Ok(())
    }
}

impl IcebergModifyState {
    /// Construct a fully-initialized session, performing all storage IO and
    /// schema/writer setup inline. The relation handle is not retained.
    fn open(
        rel: &RelationHandle,
        context: ModifyStateContext<
            IcebergModifyQueryState,
            IcebergModifyScanContext,
        >,
    ) -> AmResult<Self> {
        let (query_state, cmd_type, actions, scan_context) = context.into_parts();
        let command = ModifyCommand::from_pg(cmd_type)?;
        let target_dependency = TargetDependency::from_context(scan_context.as_ref());
        let transaction_isolation = PgTransactionIsolation::current()?;
        let rel_oid = rel.oid();
        // `locator().spc_oid` is the resolved physical tablespace (default
        // tablespaces resolve here), unlike `reltablespace`.
        let ctx = StorageContext::for_tablespace_with_wal(
            rel.locator().spc_oid,
            rel.needs_wal(),
        )?;
        let file_io = ctx.into_file_io();

        // Registers the relation with the per-transaction tracker, rebases
        // pending changes, and returns the base metadata in one step.
        let loaded = TxMetadata::current().begin_table_modify(rel_oid, &file_io)?;
        let writes_position_deletes =
            actions.writes_position_deletes() && scan_context.is_some();
        if writes_position_deletes
            && loaded.metadata.format_version() < FormatVersion::V2
        {
            return Err(IcebergError::NotImplemented(
                "UPDATE/DELETE and MERGE actions that update or delete require Iceberg format v2 or later",
            )
            .into());
        }
        let iceberg_schema = loaded.metadata.current_schema().clone();
        let table_properties = loaded
            .metadata
            .table_properties()
            .map_err(IcebergError::from)?;
        let validation = ValidationPlan::new(
            command,
            target_dependency,
            &table_properties,
            transaction_isolation,
            scan_context.as_ref(),
        );
        let write_options = IcebergTableOptions::for_relation(rel)?;
        let writer_properties = WriterProperties::builder()
            .set_compression(write_options.parquet_compression())
            .build();

        let data_sink = if actions.writes_rows() {
            // The shared relation shape drives the read and write column
            // mappings, keeping dropped-column and type-position handling
            // consistent. DELETE-only sessions do not allocate it.
            let relation_shape = RelationShape::from_relation(rel)?;
            Some(DataFileSink::new(
                &file_io,
                &iceberg_schema,
                &relation_shape,
                &loaded.metadata,
                &writer_properties,
                mutation_buffer_flush_bytes(),
            )?)
        } else {
            None
        };

        let row_delete_state = if writes_position_deletes {
            let row_registry =
                query_state.update(|state| state.relation_registry(rel_oid))?;
            let modify_state_id = row_registry.begin_modify_state()?;
            let scan_tasks = if loaded.metadata.format_version() == FormatVersion::V3
            {
                scan_context
                    .as_ref()
                    .map(IcebergModifyScanContext::scan_tasks)
            } else {
                None
            };
            Some(RowDeleteState::new(
                loaded.metadata.format_version(),
                &file_io,
                &loaded.metadata,
                &writer_properties,
                row_registry,
                modify_state_id,
                scan_tasks,
            )?)
        } else {
            None
        };

        let sinks = MutationSinks::new(data_sink, row_delete_state)?;

        Ok(Self {
            rel_oid,
            file_io,
            validation,
            sinks,
        })
    }

    fn stage_data_files(&self, new_files: Vec<DataFile>) -> IcebergResult<()> {
        TxMetadata::current().stage_data_files(self.rel_oid, new_files, &self.file_io)
    }

    fn stage_row_delete_file(&self, output: RowDeleteOutput) -> IcebergResult<()> {
        for identity in output.removed_delete_files {
            TxMetadata::current().stage_remove_delete_file(
                self.rel_oid,
                identity,
                &self.file_io,
            )?;
        }
        TxMetadata::current().stage_position_delete_file(
            self.rel_oid,
            output.delete_file,
            output.referenced_data_files,
            &self.file_io,
        )
    }

    fn stage_validation(
        &mut self,
        command: RowLevelCommand,
        starting_snapshot_id: Option<i64>,
        referenced_data_files: BTreeSet<String>,
    ) -> IcebergResult<()> {
        let Some(isolation_level) = self.validation.isolation_level else {
            return Err(IcebergError::InvariantViolated(
                "row-level mutation validation has no effective isolation level",
            ));
        };
        let conflict_filter = self
            .validation
            .conflict_scope
            .take()
            .ok_or(IcebergError::InvariantViolated(
                "row-delta conflict scope was already consumed",
            ))?
            .into_predicate();
        let validation =
            RowDeltaValidation::new(command, conflict_filter, isolation_level)
                .with_starting_snapshot_id(starting_snapshot_id)
                .with_referenced_data_files(referenced_data_files);
        TxMetadata::current().stage_row_delta_validation(
            self.rel_oid,
            validation,
            &self.file_io,
        )
    }

    fn finish_statement(&mut self) -> IcebergResult<StatementOutcome> {
        let new_data_files = self.sinks.finish_data_files()?;
        let referenced_data_files = self.sinks.referenced_data_files()?;
        let row_delete_files = self.sinks.finish_position_deletes()?;
        Ok(StatementOutcome {
            command: self.validation.command,
            target_dependency: self.validation.target_dependency,
            new_data_files,
            row_delete_files,
            referenced_data_files,
        })
    }

    fn stage_statement(&mut self, outcome: StatementOutcome) -> IcebergResult<()> {
        let validation = outcome.row_delta_validation()?;

        if !outcome.new_data_files.is_empty() {
            self.stage_data_files(outcome.new_data_files)?;
        }
        for output in outcome.row_delete_files {
            self.stage_row_delete_file(output)?;
        }
        if let Some((command, starting_snapshot_id)) = validation {
            self.stage_validation(
                command,
                starting_snapshot_id,
                outcome.referenced_data_files,
            )?;
        }
        Ok(())
    }
}
