//! Iceberg DML operations.
//!
//! Implements INSERT/UPDATE/DELETE for Iceberg tables. INSERT writes Parquet
//! data files; DELETE writes position delete files; UPDATE writes both a
//! position delete for the old row and a data-file row for the new version.
//! All files are staged in `TxMetadata` and committed through iceberg-lite's
//! transaction API.
//!
//! [`DataFileSink`] owns the slot -> Parquet data-file pipeline; [`IcebergModify`]
//! is the AM session that wires tuple callbacks to the sink, crosses the
//! `IcebergError -> AmError` boundary, and stages finished files into the
//! per-transaction Iceberg metadata. All initialization happens in
//! [`IcebergModify::open`], so `begin_modify` is a no-op.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, UInt8Array, UInt8DictionaryArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema};
use iceberg_lite::arrow::schema_to_arrow_schema;
use iceberg_lite::expr::Predicate;
use iceberg_lite::io::FileIO;
use iceberg_lite::metadata_columns::{delete_file_path_field, delete_file_pos_field};
use iceberg_lite::spec::{
    DataFile, DataFileFormat, FormatVersion, Schema as IcebergSchema, TableMetadata,
};
use iceberg_lite::transaction::{DmlCommand, IsolationLevel, RowDeltaValidation};

use iceberg_lite::writer::base_writer::data_file_writer::{
    DataFileWriter, DataFileWriterBuilder,
};
use iceberg_lite::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriter, PositionDeleteFileWriterBuilder,
    PositionDeleteWriterConfig,
};
use iceberg_lite::writer::file_writer::ParquetWriterBuilder;
use iceberg_lite::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg_lite::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg_lite::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::options::AmCache;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;
use roaring::RoaringTreemap;

use crate::access::column_mapping::{RelationShape, WriteColumns};
use crate::access::conflict_filter::DmlConflictFilterResolver;
use crate::access::row_location::{RowLocation, lookup_current};
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::gucs;
use crate::options::IcebergTableOptionCache;
use crate::storage::StorageContext;

type ParquetDataFileWriter = DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

type ParquetPositionDeleteFileWriter = PositionDeleteFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

const POSITION_DELETE_BATCH_ROWS: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModifyCommand {
    Insert,
    Delete,
    Update,
    Merge,
}

impl ModifyCommand {
    fn from_pg(cmd_type: pg_sys::CmdType::Type) -> IcebergResult<Self> {
        match cmd_type {
            pg_sys::CmdType::CMD_INSERT => Ok(Self::Insert),
            pg_sys::CmdType::CMD_DELETE => Ok(Self::Delete),
            pg_sys::CmdType::CMD_UPDATE => Ok(Self::Update),
            pg_sys::CmdType::CMD_MERGE => Ok(Self::Merge),
            _ => Err(IcebergError::NotImplemented(
                "unsupported PostgreSQL DML command for Iceberg table",
            )),
        }
    }

    fn writes_data(self) -> bool {
        matches!(self, Self::Insert | Self::Update | Self::Merge)
    }

    fn writes_position_deletes(self) -> bool {
        matches!(self, Self::Delete | Self::Update | Self::Merge)
    }

    fn validation_command(self) -> Option<DmlCommand> {
        match self {
            Self::Insert => None,
            Self::Delete => Some(DmlCommand::Delete),
            Self::Update => Some(DmlCommand::Update),
            Self::Merge => Some(DmlCommand::Merge),
        }
    }

    fn isolation_level(
        self,
        table_properties: &iceberg_lite::spec::TableProperties,
    ) -> Option<IsolationLevel> {
        match self {
            Self::Insert => None,
            Self::Delete => Some(table_properties.write_delete_isolation_level),
            Self::Update => Some(table_properties.write_update_isolation_level),
            Self::Merge => Some(table_properties.write_merge_isolation_level),
        }
    }
}

/// Buffers PostgreSQL tuple slots into Arrow columns and turns them into
/// Iceberg [`DataFile`]s through a rolling Parquet writer.
///
/// A Rust-heap session field (never in a PG memory context), so per-tuple
/// context resets cannot clobber it. Exits via [`Self::finish`] (success) or
/// [`Self::abort`] (failure).
struct DataFileSink {
    /// Relation-bound columnar write buffer: owns the per-column Arrow encoders
    /// and the name-resolved source-slot mapping, so each output column pulls
    /// from the correct slot index. See [`WriteColumns`].
    ///
    /// A Rust-heap session field (never in a PG memory context), so per-tuple
    /// context resets cannot clobber it.
    columns: WriteColumns,
    /// Row-buffer memory threshold for this DML session.
    flush_threshold_bytes: usize,
    /// Active rolling Parquet writer. `None` only after [`Self::close_writer`]
    /// consumes it (during `finish` / `abort`).
    writer: Option<ParquetDataFileWriter>,
}

impl DataFileSink {
    /// Resolve the write-side column plan / buffer and build the rolling Parquet
    /// writer. Fails fast on unsupported columns or a column/field desync before
    /// any row is accepted.
    fn new(
        file_io: &FileIO,
        iceberg_schema: &Arc<IcebergSchema>,
        relation_shape: &RelationShape,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
        flush_threshold_bytes: usize,
    ) -> IcebergResult<Self> {
        let columns = WriteColumns::resolve(iceberg_schema, relation_shape)?;
        let writer = Self::build_writer(
            file_io,
            iceberg_schema,
            table_metadata,
            writer_properties,
        )?;
        Ok(Self {
            columns,
            flush_threshold_bytes,
            writer: Some(writer),
        })
    }

    /// Append one tuple-slot row into the buffer, then flush if the memory
    /// threshold is reached. The borrowed slot view is consumed within this call.
    fn append(&mut self, row: TupleSlotRow<'_>) -> IcebergResult<()> {
        self.columns.append_slot_row(row)?;
        self.flush_if_needed()
    }

    /// Flush remaining rows and close the writer, returning every produced
    /// data file. The writer is always closed even if the flush fails, so a
    /// failing flush cannot leak a file descriptor.
    fn finish(&mut self) -> IcebergResult<Vec<DataFile>> {
        let flush_res = self.flush_buffer();
        let close_res = self.close_writer();
        flush_res?;
        close_res
    }

    /// Best-effort cleanup of in-memory state for the failure path. Persistent
    /// artifacts are unwound by ResourceOwner cleanup; see [`IcebergModify::end_modify`].
    fn abort(&mut self) {
        self.columns.clear();
        self.writer.take();
    }

    fn flush_if_needed(&mut self) -> IcebergResult<()> {
        if self.columns.should_flush(self.flush_threshold_bytes) {
            self.flush_buffer()?;
        }
        Ok(())
    }

    /// Finish the buffered columns into a RecordBatch and write it to the writer.
    fn flush_buffer(&mut self) -> IcebergResult<()> {
        if self.columns.is_empty() {
            return Ok(());
        }

        // `finish_batch` resets the buffer, so it is cleared even if the write fails.
        let record_batch = self.columns.finish_batch()?;

        // `None` here means a tuple callback fired after finalization — a
        // framework bug worth surfacing.
        match self.writer.as_mut() {
            Some(writer) => writer.write(record_batch)?,
            None => {
                return Err(IcebergError::InvariantViolated(
                    "tuple callback after writer close",
                ));
            }
        }

        Ok(())
    }

    fn close_writer(&mut self) -> IcebergResult<Vec<DataFile>> {
        match self.writer.take() {
            Some(mut writer) => Ok(writer.close()?),
            None => Ok(Vec::new()),
        }
    }

    /// Build the rolling Parquet data file writer for this sink.
    fn build_writer(
        file_io: &FileIO,
        schema: &Arc<IcebergSchema>,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
    ) -> IcebergResult<ParquetDataFileWriter> {
        let location_generator = DefaultLocationGenerator::new(table_metadata)?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("insert-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        );

        let parquet_writer_builder =
            ParquetWriterBuilder::new(writer_properties.clone(), schema.clone());

        let rolling_writer_builder =
            RollingFileWriterBuilder::new_with_default_file_size(
                parquet_writer_builder,
                file_io.clone(),
                location_generator,
                file_name_generator,
            );

        let data_file_writer_builder =
            DataFileWriterBuilder::new(rolling_writer_builder);
        Ok(data_file_writer_builder.build(None)?)
    }
}

struct PositionDeleteOutput {
    delete_file: DataFile,
    referenced_data_file: String,
}

/// Writes Iceberg position delete files for rows accumulated by
/// [`PositionDeleteAccumulator`].
struct PositionDeleteSink {
    file_io: FileIO,
    schema: Arc<IcebergSchema>,
    batch_schema: arrow_schema::SchemaRef,
    location_generator: DefaultLocationGenerator,
    writer_properties: WriterProperties,
}

impl PositionDeleteSink {
    fn new(
        file_io: &FileIO,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
    ) -> IcebergResult<Self> {
        let schema = Arc::new(
            IcebergSchema::builder()
                .with_fields([
                    Arc::clone(delete_file_path_field()),
                    Arc::clone(delete_file_pos_field()),
                ])
                .build()?,
        );
        let arrow_schema = schema_to_arrow_schema(&schema)?;
        let mut fields = arrow_schema.fields().to_vec();
        let file_path_field =
            fields.first_mut().ok_or(IcebergError::InvariantViolated(
                "position-delete schema is missing the file path field",
            ))?;
        if file_path_field.data_type() != &DataType::Utf8 {
            return Err(IcebergError::InvariantViolated(
                "position-delete file path field has an unexpected Arrow type",
            ));
        }
        *file_path_field = Arc::new(file_path_field.as_ref().clone().with_data_type(
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
        ));
        let batch_schema = Arc::new(ArrowSchema::new_with_metadata(
            fields,
            arrow_schema.metadata().clone(),
        ));
        Ok(Self {
            file_io: file_io.clone(),
            schema,
            batch_schema,
            location_generator: DefaultLocationGenerator::new(table_metadata)?,
            writer_properties: writer_properties.clone(),
        })
    }

    fn write_files(
        &self,
        deletes: &PositionDeleteAccumulator,
    ) -> IcebergResult<Vec<PositionDeleteOutput>> {
        let mut outputs = Vec::new();
        for (referenced_data_file, positions) in deletes.files() {
            let mut writer = self.build_writer(referenced_data_file)?;
            let mut chunk = Vec::with_capacity(POSITION_DELETE_BATCH_ROWS);
            for position in positions.iter() {
                chunk.push(position);
                if chunk.len() == POSITION_DELETE_BATCH_ROWS {
                    writer.write(self.record_batch(referenced_data_file, &chunk)?)?;
                    chunk.clear();
                }
            }
            if !chunk.is_empty() {
                writer.write(self.record_batch(referenced_data_file, &chunk)?)?;
            }
            for delete_file in writer.close()? {
                outputs.push(PositionDeleteOutput {
                    delete_file,
                    referenced_data_file: referenced_data_file.as_ref().to_owned(),
                });
            }
        }
        Ok(outputs)
    }

    fn build_writer(
        &self,
        referenced_data_file: &str,
    ) -> IcebergResult<ParquetPositionDeleteFileWriter> {
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("delete-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        );
        let parquet_writer_builder = ParquetWriterBuilder::new(
            self.writer_properties.clone(),
            Arc::clone(&self.schema),
        );
        let rolling_writer_builder =
            RollingFileWriterBuilder::new_with_default_file_size(
                parquet_writer_builder,
                self.file_io.clone(),
                self.location_generator.clone(),
                file_name_generator,
            );
        let builder = PositionDeleteFileWriterBuilder::new(
            rolling_writer_builder,
            PositionDeleteWriterConfig::new(referenced_data_file),
        );
        Ok(builder.build(None)?)
    }

    fn record_batch(
        &self,
        referenced_data_file: &str,
        positions: &[u64],
    ) -> IcebergResult<RecordBatch> {
        let mut pos_values = Vec::with_capacity(positions.len());
        for position in positions {
            pos_values.push(i64::try_from(*position).map_err(|_| {
                IcebergError::MetadataTracker(format!(
                    "Iceberg row position {position} is too large for position delete file"
                ))
            })?);
        }
        let file_keys = UInt8Array::from_value(0, positions.len());
        let file_values: ArrayRef = Arc::new(StringArray::from_iter_values(
            std::iter::once(referenced_data_file),
        ));
        let file_array: ArrayRef =
            Arc::new(UInt8DictionaryArray::try_new(file_keys, file_values)?);
        let pos_array: ArrayRef = Arc::new(Int64Array::from(pos_values));
        Ok(RecordBatch::try_new(
            Arc::clone(&self.batch_schema),
            vec![file_array, pos_array],
        )?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TouchResult {
    Added,
    SelfModified,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum RecordedSnapshot {
    #[default]
    Unrecorded,
    Recorded(Option<i64>),
}

#[derive(Debug, Default)]
struct PositionDeleteAccumulator {
    by_file: BTreeMap<Rc<str>, RoaringTreemap>,
    starting_snapshot: RecordedSnapshot,
}

impl PositionDeleteAccumulator {
    fn add(&mut self, location: RowLocation) -> IcebergResult<TouchResult> {
        self.record_snapshot(location.starting_snapshot_id)?;
        let inserted = self
            .by_file
            .entry(location.data_file_path)
            .or_default()
            .insert(location.position);
        Ok(if inserted {
            TouchResult::Added
        } else {
            TouchResult::SelfModified
        })
    }

    fn contains(&self, location: &RowLocation) -> bool {
        self.by_file
            .get(&location.data_file_path)
            .is_some_and(|positions| positions.contains(location.position))
    }

    fn is_empty(&self) -> bool {
        self.by_file.is_empty()
    }

    fn files(&self) -> &BTreeMap<Rc<str>, RoaringTreemap> {
        &self.by_file
    }

    fn referenced_data_files(&self) -> BTreeSet<String> {
        self.by_file
            .keys()
            .map(|path| path.as_ref().to_owned())
            .collect()
    }

    fn starting_snapshot_id(&self) -> Option<i64> {
        match self.starting_snapshot {
            RecordedSnapshot::Unrecorded => None,
            RecordedSnapshot::Recorded(snapshot_id) => snapshot_id,
        }
    }

    fn clear(&mut self) {
        self.by_file.clear();
        self.starting_snapshot = RecordedSnapshot::Unrecorded;
    }

    fn record_snapshot(
        &mut self,
        starting_snapshot_id: Option<i64>,
    ) -> IcebergResult<()> {
        match self.starting_snapshot {
            RecordedSnapshot::Unrecorded => {
                self.starting_snapshot =
                    RecordedSnapshot::Recorded(starting_snapshot_id);
                Ok(())
            }
            RecordedSnapshot::Recorded(existing)
                if existing == starting_snapshot_id =>
            {
                Ok(())
            }
            RecordedSnapshot::Recorded(existing) => {
                Err(IcebergError::MetadataTracker(format!(
                    "one Iceberg DML statement touched rows from inconsistent scan snapshots: \
                     {existing:?} and {starting_snapshot_id:?}"
                )))
            }
        }
    }
}

/// Iceberg DML state for INSERT/UPDATE/DELETE operations.
///
/// Constructed eagerly: storage context, schemas, and writer are all wired up
/// by the time this struct exists.
pub struct IcebergModify {
    /// OID of the relation being modified.
    rel_oid: pg_sys::Oid,
    /// File IO for staging produced data files into transaction metadata.
    file_io: FileIO,
    command: ModifyCommand,
    isolation_level: Option<IsolationLevel>,
    conflict_filter: Option<Predicate>,
    /// The slot -> data-file production pipeline.
    data_sink: Option<DataFileSink>,
    position_delete_sink: Option<PositionDeleteSink>,
    position_deletes: PositionDeleteAccumulator,
}

impl AmDmlSession for IcebergModify {
    fn new(rel: &RelationHandle, cmd_type: pg_sys::CmdType::Type) -> AmResult<Self> {
        Ok(Self::open(rel, cmd_type)?)
    }

    fn begin_modify(&mut self) -> AmResult<()> {
        // Intentionally empty: all initialization happens in `new`.
        Ok(())
    }

    fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        self.data_sink_mut()?.append(row)?;
        Ok(())
    }

    fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        for row in rows.iter() {
            self.data_sink_mut()?.append(row)?;
        }
        Ok(())
    }

    fn tuple_delete(
        &mut self,
        tid: &ItemPointer,
        cid: pg_sys::CommandId,
        _snapshot: &SnapshotHandle,
        _crosscheck: Option<&SnapshotHandle>,
        _wait: bool,
        tmfd: &mut TM_FailureData,
        _changing_part: bool,
    ) -> AmResult<pg_sys::TM_Result::Type> {
        self.ensure_position_delete_capable()?;
        // `tuple_delete` only runs inside `ExecModifyTable` (frame active) with a
        // ctid this relation's scan synthesized, so a missing row location is an
        // invariant violation, not a concurrently deleted row.
        let location = lookup_current(self.rel_oid, tid)?.ok_or(
            IcebergError::InvariantViolated(
                "tuple_delete reached a ctid with no active Iceberg row location",
            ),
        )?;
        match self.position_deletes.add(location)? {
            TouchResult::Added => Ok(pg_sys::TM_Result::TM_Ok),
            TouchResult::SelfModified => {
                self.mark_self_modified(tmfd, tid, cid);
                Ok(pg_sys::TM_Result::TM_SelfModified)
            }
        }
    }

    fn tuple_update_slot(
        &mut self,
        otid: &ItemPointer,
        row: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
        _snapshot: &SnapshotHandle,
        _crosscheck: Option<&SnapshotHandle>,
        _wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut pg_sys::LockTupleMode::Type,
        update_indexes: &mut pg_sys::TU_UpdateIndexes::Type,
    ) -> AmResult<pg_sys::TM_Result::Type> {
        self.ensure_position_delete_capable()?;
        // As in `tuple_delete`: `tuple_update_slot` only runs inside
        // `ExecModifyTable` with a ctid this relation's scan synthesized, so a
        // missing row location is an invariant violation.
        let location = lookup_current(self.rel_oid, otid)?.ok_or(
            IcebergError::InvariantViolated(
                "tuple_update_slot reached a ctid with no active Iceberg row location",
            ),
        )?;
        match self.position_deletes.add(location)? {
            TouchResult::Added => {
                self.data_sink_mut()?.append(row)?;
                *lockmode = pg_sys::LockTupleMode::LockTupleExclusive;
                *update_indexes = pg_sys::TU_UpdateIndexes::TU_None;
                Ok(pg_sys::TM_Result::TM_Ok)
            }
            TouchResult::SelfModified => {
                self.mark_self_modified(tmfd, otid, cid);
                *update_indexes = pg_sys::TU_UpdateIndexes::TU_None;
                Ok(pg_sys::TM_Result::TM_SelfModified)
            }
        }
    }

    fn tuple_lock(
        &mut self,
        tid: &ItemPointer,
        _snapshot: &SnapshotHandle,
        _row: &mut Row,
        cid: pg_sys::CommandId,
        _mode: pg_sys::LockTupleMode::Type,
        _wait_policy: pg_sys::LockWaitPolicy::Type,
        _flags: u8,
        tmfd: &mut TM_FailureData,
    ) -> AmResult<pg_sys::TM_Result::Type> {
        let Some(location) = lookup_current(self.rel_oid, tid)? else {
            return Ok(pg_sys::TM_Result::TM_Deleted);
        };
        if self.position_deletes.contains(&location) {
            self.mark_self_modified(tmfd, tid, cid);
            return Ok(pg_sys::TM_Result::TM_SelfModified);
        }
        Ok(pg_sys::TM_Result::TM_Ok)
    }

    fn abort_modify(&mut self) {
        // Best-effort in-memory cleanup; persistent artifacts are unwound by
        // ResourceOwner cleanup (see the orphan-file note in `end_modify`).
        if let Some(sink) = self.data_sink.as_mut() {
            sink.abort();
        }
        self.position_deletes.clear();
    }

    fn end_modify(&mut self) -> AmResult<()> {
        // Orphan-file note: data files already uploaded before a later flush
        // failure are NOT leaked. Every produced file is registered via
        // `register_object_file_staged()` / `mark_object_file_uploaded()`, and
        // `StorageArtifactResource::on_abort` unlinks staging files or issues
        // remote deletes on abort. Do not re-introduce a separate cleanup list here.
        let new_files = self.finish_data_files()?;
        let referenced_data_files = self.position_deletes.referenced_data_files();
        let delete_files = self.finish_position_deletes()?;

        if !new_files.is_empty() {
            self.stage_data_files(new_files)?;
        }
        for output in delete_files {
            self.stage_position_delete_file(output)?;
        }
        if !referenced_data_files.is_empty() {
            self.stage_validation(referenced_data_files)?;
        }

        Ok(())
    }
}

impl IcebergModify {
    /// Construct a fully-initialized session, performing all storage IO and
    /// schema/writer setup inline.
    ///
    /// Everything is derived from `rel` here — the file locator and WAL flag
    /// for storage, the relation OID for the metadata tracker, and the live
    /// columns / tuple width / attribute types that bind the write-side
    /// source-slot mapping ([`WriteColumns`]). The handle is not retained.
    fn open(
        rel: &RelationHandle,
        cmd_type: pg_sys::CmdType::Type,
    ) -> IcebergResult<Self> {
        let command = ModifyCommand::from_pg(cmd_type)?;
        let rel_oid = rel.oid();
        // `locator().spc_oid` is the *resolved* physical tablespace (default
        // tablespaces resolve here), unlike `reltablespace`.
        let ctx = StorageContext::for_tablespace_with_wal(
            rel.locator().spc_oid,
            rel.needs_wal(),
        )?;
        let file_io = ctx.into_file_io();

        // Registers the relation with the per-transaction tracker, rebases
        // pending changes, and returns the base metadata in one step.
        let loaded = TxMetadata::current().begin_table_modify(rel_oid, &file_io)?;
        if command.writes_position_deletes()
            && loaded.metadata.format_version() < FormatVersion::V2
        {
            return Err(IcebergError::NotImplemented(
                "UPDATE/DELETE/MERGE require Iceberg format v2 or later",
            ));
        }
        let iceberg_schema = loaded.metadata.current_schema().clone();
        let table_properties = loaded.metadata.table_properties()?;
        let isolation_level = command.isolation_level(&table_properties);
        let conflict_filter = command
            .validation_command()
            .map(|_| DmlConflictFilterResolver::new(rel_oid).resolve());
        let write_options = AmCache::get::<IcebergTableOptionCache>(rel)?;
        let writer_properties = WriterProperties::builder()
            .set_compression(write_options.parquet_compression()?)
            .build();

        let data_sink = if command.writes_data() {
            // The shared relation shape drives the read and write column
            // mappings, keeping dropped-column and type-position handling
            // consistent. DELETE-only sessions do not allocate it.
            let relation_shape = RelationShape::from_relation(rel);
            Some(DataFileSink::new(
                &file_io,
                &iceberg_schema,
                &relation_shape,
                &loaded.metadata,
                &writer_properties,
                gucs::dml_buffer_flush_bytes(),
            )?)
        } else {
            None
        };
        let position_delete_sink = if command.writes_position_deletes() {
            Some(PositionDeleteSink::new(
                &file_io,
                &loaded.metadata,
                &writer_properties,
            )?)
        } else {
            None
        };

        Ok(Self {
            rel_oid,
            file_io,
            command,
            isolation_level,
            conflict_filter,
            data_sink,
            position_delete_sink,
            position_deletes: PositionDeleteAccumulator::default(),
        })
    }

    /// Stage produced data files into transaction-local Iceberg metadata.
    fn stage_data_files(&self, new_files: Vec<DataFile>) -> IcebergResult<()> {
        TxMetadata::current().stage_data_files(self.rel_oid, new_files, &self.file_io)
    }

    fn stage_position_delete_file(
        &self,
        output: PositionDeleteOutput,
    ) -> IcebergResult<()> {
        TxMetadata::current().stage_position_delete_file(
            self.rel_oid,
            output.delete_file,
            std::iter::once(output.referenced_data_file),
            &self.file_io,
        )
    }

    fn stage_validation(
        &mut self,
        referenced_data_files: BTreeSet<String>,
    ) -> IcebergResult<()> {
        let Some(command) = self.command.validation_command() else {
            return Ok(());
        };
        let Some(isolation_level) = self.isolation_level else {
            return Ok(());
        };
        let conflict_filter =
            self.conflict_filter
                .take()
                .ok_or(IcebergError::InvariantViolated(
                    "row-delta conflict filter was already consumed",
                ))?;
        let validation =
            RowDeltaValidation::new(command, conflict_filter, isolation_level)
                .with_starting_snapshot_id(
                    self.position_deletes.starting_snapshot_id(),
                )
                .with_referenced_data_files(referenced_data_files);
        TxMetadata::current().stage_row_delta_validation(
            self.rel_oid,
            validation,
            &self.file_io,
        )
    }

    fn finish_data_files(&mut self) -> IcebergResult<Vec<DataFile>> {
        match self.data_sink.as_mut() {
            Some(sink) => sink.finish(),
            None => Ok(Vec::new()),
        }
    }

    fn finish_position_deletes(
        &mut self,
    ) -> IcebergResult<Vec<PositionDeleteOutput>> {
        if self.position_deletes.is_empty() {
            return Ok(Vec::new());
        }
        self.position_delete_sink_ref()?
            .write_files(&self.position_deletes)
    }

    fn data_sink_mut(&mut self) -> IcebergResult<&mut DataFileSink> {
        self.data_sink
            .as_mut()
            .ok_or(IcebergError::InvariantViolated(
                "data-file callback reached a DML command without a data sink",
            ))
    }

    fn position_delete_sink_ref(&self) -> IcebergResult<&PositionDeleteSink> {
        self.position_delete_sink
            .as_ref()
            .ok_or(IcebergError::InvariantViolated(
                "position-delete callback reached a DML command without a delete sink",
            ))
    }

    fn ensure_position_delete_capable(&self) -> IcebergResult<()> {
        if self.position_delete_sink.is_none() {
            return Err(IcebergError::InvariantViolated(
                "position-delete callback reached a DML command without a delete sink",
            ));
        }
        Ok(())
    }

    fn mark_self_modified(
        &self,
        tmfd: &mut TM_FailureData,
        tid: &ItemPointer,
        cid: pg_sys::CommandId,
    ) {
        tmfd.ctid = *tid;
        tmfd.cmax = cid;
    }
}

#[cfg(test)]
mod position_delete_accumulator_tests {
    use super::*;

    #[test]
    fn accepts_repeated_identical_snapshot_states() {
        let mut without_snapshot = PositionDeleteAccumulator::default();
        without_snapshot.record_snapshot(None).unwrap();
        without_snapshot.record_snapshot(None).unwrap();
        assert_eq!(without_snapshot.starting_snapshot_id(), None);

        let mut with_snapshot = PositionDeleteAccumulator::default();
        with_snapshot.record_snapshot(Some(42)).unwrap();
        with_snapshot.record_snapshot(Some(42)).unwrap();
        assert_eq!(with_snapshot.starting_snapshot_id(), Some(42));
    }

    #[test]
    fn rejects_mixed_snapshot_presence() {
        let mut snapshot_then_none = PositionDeleteAccumulator::default();
        snapshot_then_none.record_snapshot(Some(42)).unwrap();
        assert!(snapshot_then_none.record_snapshot(None).is_err());

        let mut none_then_snapshot = PositionDeleteAccumulator::default();
        none_then_snapshot.record_snapshot(None).unwrap();
        assert!(none_then_snapshot.record_snapshot(Some(42)).is_err());
    }

    #[test]
    fn rejects_different_snapshot_ids() {
        let mut accumulator = PositionDeleteAccumulator::default();
        accumulator.record_snapshot(Some(42)).unwrap();
        assert!(accumulator.record_snapshot(Some(43)).is_err());
    }

    #[test]
    fn clear_resets_recorded_snapshot_state() {
        let mut accumulator = PositionDeleteAccumulator::default();
        accumulator.record_snapshot(Some(42)).unwrap();

        accumulator.clear();

        accumulator.record_snapshot(None).unwrap();
        assert_eq!(accumulator.starting_snapshot_id(), None);
    }

    #[test]
    fn positions_are_deduplicated_and_iterated_in_order() {
        let location = |position| RowLocation {
            data_file_path: Rc::from("data.parquet"),
            position,
            starting_snapshot_id: Some(42),
        };
        let mut accumulator = PositionDeleteAccumulator::default();

        assert_eq!(accumulator.add(location(9)).unwrap(), TouchResult::Added);
        assert_eq!(accumulator.add(location(1)).unwrap(), TouchResult::Added);
        assert_eq!(accumulator.add(location(5)).unwrap(), TouchResult::Added);
        assert_eq!(
            accumulator.add(location(5)).unwrap(),
            TouchResult::SelfModified
        );
        assert!(accumulator.contains(&location(5)));

        let positions: Vec<_> = accumulator
            .files()
            .get("data.parquet")
            .unwrap()
            .iter()
            .collect();
        assert_eq!(positions, vec![1, 5, 9]);
    }
}
