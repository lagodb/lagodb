//! Iceberg mutation operations.
//!
//! Implements INSERT/UPDATE/DELETE/MERGE for Iceberg tables. INSERT writes
//! Parquet data files; DELETE writes position delete files; UPDATE writes both
//! a position delete for the old row and a data-file row for the new version;
//! MERGE may combine those outcomes. All files are staged in `TxMetadata` and
//! committed through iceberg-lite's transaction API.
//!
//! [`DataFileSink`] owns the slot -> Parquet data-file pipeline; [`IcebergModifyState`]
//! is the AM session that wires tuple callbacks to the sink, crosses the
//! `IcebergError -> AmError` boundary, and stages finished files into the
//! per-transaction Iceberg metadata. All initialization happens in
//! [`IcebergModifyState::open`], so `begin_modify` is a no-op.

use std::collections::{BTreeSet, HashMap};
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
use iceberg_lite::transaction::{
    IsolationLevel, RowDeltaValidation, RowLevelCommand,
};

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
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use crate::access::column_mapping::{RelationShape, WriteColumns};
use crate::access::isolation::PgTransactionIsolation;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::catalog::row_mutations::{
    ICEBERG_FILE_ID_BITS, IcebergFileId, ModifyStateId, OwnedRowPositions,
    RelationRowRegistry, RowMutationClaim,
};
use crate::error::{IcebergError, IcebergResult};
use crate::gucs;
use crate::options::IcebergTableOptions;
use crate::storage::StorageContext;
use pg_lakebase_core::api::TRIGGER_ROW_BLOCK_BASE;

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

/// Iceberg metadata captured once by a Modify-purpose target scan and consumed
/// when the corresponding relation-local modify state is opened.
#[derive(Debug, Clone, PartialEq)]
pub struct IcebergModifyScanContext {
    starting_snapshot_id: Option<i64>,
    conflict_filter: Predicate,
}

impl IcebergModifyScanContext {
    pub(crate) fn new(
        starting_snapshot_id: Option<i64>,
        conflict_filter: Predicate,
    ) -> Self {
        Self {
            starting_snapshot_id,
            conflict_filter,
        }
    }
}

/// Compact Iceberg row identity decoded from the PostgreSQL `ctid` carrier.
/// File paths remain interned and are resolved only when delete files are
/// finalized, never on the per-row UPDATE/DELETE hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IcebergRowIdentity {
    file_id: IcebergFileId,
    row_position: u32,
}

impl IcebergRowIdentity {
    const fn new(file_id: IcebergFileId, row_position: u32) -> Self {
        Self {
            file_id,
            row_position,
        }
    }

    pub(crate) const fn file_id(self) -> IcebergFileId {
        self.file_id
    }

    pub(crate) const fn row_position(self) -> u32 {
        self.row_position
    }

    fn encode(file_id: IcebergFileId, position: u64) -> IcebergResult<ItemPointer> {
        if u64::from(file_id.raw()) > FILE_MASK || position > MAX_POSITION {
            return Err(IcebergError::RowIdentityLimitExceeded);
        }
        let payload = (u64::from(file_id.raw()) << POSITION_BITS) | position;
        let block_number = u32::try_from(payload / OFFSET_BASE).map_err(|_| {
            IcebergError::InvariantViolated("synthetic ctid block number overflow")
        })?;
        if block_number >= TRIGGER_ROW_BLOCK_BASE {
            return Err(IcebergError::InvariantViolated(
                "Iceberg row identity overlaps the trigger-row namespace",
            ));
        }
        let offset = u16::try_from((payload % OFFSET_BASE) + 1).map_err(|_| {
            IcebergError::InvariantViolated("synthetic ctid offset overflow")
        })?;
        Ok(ItemPointer {
            block_number,
            offset,
        })
    }

    fn decode(tid: &ItemPointer) -> IcebergResult<Self> {
        if tid.offset == 0 || tid.block_number >= TRIGGER_ROW_BLOCK_BASE {
            return Err(IcebergError::InvariantViolated(
                "ctid is not an Iceberg physical row identity",
            ));
        }
        let payload = u64::from(tid.block_number)
            .checked_mul(OFFSET_BASE)
            .and_then(|base| base.checked_add(u64::from(tid.offset - 1)))
            .ok_or_else(|| {
                IcebergError::InvariantViolated("synthetic ctid payload overflow")
            })?;
        if payload >= PAYLOAD_LIMIT {
            return Err(IcebergError::InvariantViolated(
                "ctid is not an Iceberg physical row identity",
            ));
        }
        let file_id = u32::try_from((payload >> POSITION_BITS) & FILE_MASK)
            .map(IcebergFileId::from_raw)
            .map_err(|_| {
                IcebergError::InvariantViolated("synthetic ctid file id overflow")
            })?;
        let row_position = u32::try_from(payload & POSITION_MASK).map_err(|_| {
            IcebergError::InvariantViolated("synthetic ctid row position overflow")
        })?;
        Ok(Self::new(file_id, row_position))
    }
}

// TODO(synthetic-ctid-capacity): this 17/30-bit split caps one relation at
// 131,072 registered files and each file at 2^30 rows. Target scans may
// register files before quals eliminate all their rows, so redesign the
// identity carrier/registry before workloads can approach either bound.
const POSITION_BITS: u32 = 30;
const MAX_POSITION: u64 = (1u64 << POSITION_BITS) - 1;
const FILE_MASK: u64 = (1u64 << ICEBERG_FILE_ID_BITS) - 1;
const POSITION_MASK: u64 = (1u64 << POSITION_BITS) - 1;
const PAYLOAD_LIMIT: u64 = 1u64 << (ICEBERG_FILE_ID_BITS + POSITION_BITS);
const OFFSET_BASE: u64 = u16::MAX as u64;

/// Borrowed data-file source registered once per contiguous scan run.
#[derive(Debug, Clone, Copy)]
pub struct IcebergFileSource<'a>(&'a str);

impl<'a> IcebergFileSource<'a> {
    pub(crate) const fn new(path: &'a str) -> Self {
        Self(path)
    }
}

/// Iceberg identity registry shared by all ModifyTable nodes in one PostgreSQL
/// executor query. It caches only handles to transaction-owned relation
/// registries; file paths and file-ID namespaces never live at query scope.
#[derive(Debug, Default)]
pub struct IcebergModifyQueryState {
    relations: HashMap<pg_sys::Oid, RelationRowRegistry>,
}

impl IcebergModifyQueryState {
    fn relation_registry(
        &mut self,
        relation_oid: pg_sys::Oid,
    ) -> AmResult<RelationRowRegistry> {
        if let Some(registry) = self.relations.get(&relation_oid) {
            return Ok(registry.clone());
        }
        let registry = TxMetadata::current().row_registry(relation_oid)?;
        self.relations.insert(relation_oid, registry.clone());
        Ok(registry)
    }
}

impl AmModifyQueryState for IcebergModifyQueryState {
    type ScanIdentitySource<'a> = IcebergFileSource<'a>;
    type RegisteredScanIdentity = IcebergFileId;
    type ScanIdentity<'a> = u64;

    fn new() -> AmResult<Self> {
        Ok(Self::default())
    }

    fn register_scan_identity_source(
        &mut self,
        relation_oid: pg_sys::Oid,
        source: &Self::ScanIdentitySource<'_>,
    ) -> AmResult<Self::RegisteredScanIdentity> {
        Ok(self
            .relation_registry(relation_oid)?
            .register_file(source.0)?)
    }

    fn encode_row_identity(
        source: Self::RegisteredScanIdentity,
        position: &Self::ScanIdentity<'_>,
    ) -> AmResult<ItemPointer> {
        Ok(IcebergRowIdentity::encode(source, *position)?)
    }
}

#[derive(Debug)]
enum ConflictValidationScope {
    StaticTarget(Predicate),
    WholeTable,
}

impl ConflictValidationScope {
    fn from_predicate(predicate: Predicate) -> Self {
        if predicate == Predicate::AlwaysTrue {
            Self::WholeTable
        } else {
            Self::StaticTarget(predicate)
        }
    }

    fn into_predicate(self) -> Predicate {
        match self {
            Self::StaticTarget(predicate) => predicate,
            Self::WholeTable => Predicate::AlwaysTrue,
        }
    }
}

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
                "unsupported PostgreSQL mutation command for Iceberg table",
            )),
        }
    }

    fn validation_command(self) -> Option<RowLevelCommand> {
        match self {
            Self::Insert => None,
            Self::Delete => Some(RowLevelCommand::Delete),
            Self::Update => Some(RowLevelCommand::Update),
            Self::Merge => Some(RowLevelCommand::Merge),
        }
    }

    fn table_isolation_level(
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

    fn effective_isolation_level(
        self,
        table_properties: &iceberg_lite::spec::TableProperties,
        transaction_isolation: PgTransactionIsolation,
    ) -> Option<IsolationLevel> {
        self.table_isolation_level(table_properties)
            .map(|table_isolation| {
                transaction_isolation.effective_iceberg(table_isolation)
            })
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
    /// Row-buffer memory threshold for this modify state.
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
    /// artifacts are unwound by ResourceOwner cleanup; see [`IcebergModifyState::end_modify`].
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
        row_registry: &RelationRowRegistry,
    ) -> IcebergResult<Vec<PositionDeleteOutput>> {
        let mut outputs = Vec::new();
        for (file_id, positions) in deletes.files() {
            let referenced_data_file = row_registry.file_path(file_id)?;
            let mut writer = self.build_writer(&referenced_data_file)?;
            let mut chunk = Vec::with_capacity(POSITION_DELETE_BATCH_ROWS);
            let positions = positions.borrow()?;
            for position in positions.iter() {
                chunk.push(u64::from(position));
                if chunk.len() == POSITION_DELETE_BATCH_ROWS {
                    writer
                        .write(self.record_batch(&referenced_data_file, &chunk)?)?;
                    chunk.clear();
                }
            }
            if !chunk.is_empty() {
                writer.write(self.record_batch(&referenced_data_file, &chunk)?)?;
            }
            for delete_file in writer.close()? {
                outputs.push(PositionDeleteOutput {
                    delete_file,
                    referenced_data_file: referenced_data_file.to_string(),
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

#[derive(Debug, Default)]
struct PositionDeleteAccumulator {
    /// One shared owner bitmap per file touched by this ModifyState. The
    /// registry performs the only per-row insertion; this list is updated only
    /// when the state first touches a file.
    files: Vec<(IcebergFileId, OwnedRowPositions)>,
}

impl PositionDeleteAccumulator {
    fn add_file_positions(
        &mut self,
        file_id: IcebergFileId,
        positions: OwnedRowPositions,
    ) {
        debug_assert!(
            self.files.iter().all(|(existing, _)| *existing != file_id),
            "one ModifyState must own exactly one bitmap per file"
        );
        self.files.push((file_id, positions));
    }

    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    fn files(&self) -> impl Iterator<Item = (IcebergFileId, &OwnedRowPositions)> {
        self.files
            .iter()
            .map(|(file_id, positions)| (*file_id, positions))
    }

    fn referenced_data_files(
        &self,
        row_registry: &RelationRowRegistry,
    ) -> IcebergResult<BTreeSet<String>> {
        self.files()
            .map(|(file_id, _)| {
                row_registry.file_path(file_id).map(|path| path.to_string())
            })
            .collect()
    }

    fn clear(&mut self) {
        self.files.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetDependency {
    Independent,
    ReadRequired(Option<i64>),
}

impl TargetDependency {
    fn from_context(scan_context: Option<&IcebergModifyScanContext>) -> Self {
        scan_context.map_or(Self::Independent, |context| {
            Self::ReadRequired(context.starting_snapshot_id)
        })
    }

    fn required_snapshot(self) -> IcebergResult<Option<i64>> {
        match self {
            Self::ReadRequired(snapshot_id) => Ok(snapshot_id),
            Self::Independent => Err(IcebergError::InvariantViolated(
                "independent mutation has no target snapshot",
            )),
        }
    }
}

/// Final output of one relation-local PostgreSQL ModifyTable session.
///
/// Target dependency is explicit: the owning relation execution decides
/// whether the finalized plan needs a target read, and its pinned scan context
/// supplies the snapshot used for validation.
struct StatementOutcome {
    command: ModifyCommand,
    target_dependency: TargetDependency,
    new_data_files: Vec<DataFile>,
    position_delete_files: Vec<PositionDeleteOutput>,
    referenced_data_files: BTreeSet<String>,
}

impl StatementOutcome {
    fn has_delta(&self) -> bool {
        !self.new_data_files.is_empty() || !self.position_delete_files.is_empty()
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
/// by the time this struct exists.
pub struct IcebergModifyState {
    /// OID of the relation being modified.
    rel_oid: pg_sys::Oid,
    /// File IO for staging produced data files into transaction metadata.
    file_io: FileIO,
    command: ModifyCommand,
    target_dependency: TargetDependency,
    isolation_level: Option<IsolationLevel>,
    conflict_scope: Option<ConflictValidationScope>,
    /// The slot -> data-file production pipeline.
    data_sink: Option<DataFileSink>,
    position_delete_sink: Option<PositionDeleteSink>,
    position_deletes: PositionDeleteAccumulator,
    row_registry: RelationRowRegistry,
    modify_state_id: ModifyStateId,
}

impl AmModifyState for IcebergModifyState {
    type QueryState = IcebergModifyQueryState;
    type ScanContext = IcebergModifyScanContext;

    fn new(
        rel: &RelationHandle,
        context: ModifyStateContext<Self::QueryState, Self::ScanContext>,
    ) -> AmResult<Self> {
        Self::open(rel, context)
    }

    fn begin_modify(&mut self) -> AmResult<()> {
        // Intentionally empty: all initialization happens in `new`.
        Ok(())
    }

    fn insert_slot(
        &mut self,
        new: TupleSlotRow<'_>,
        _context: MutationWriteContext,
    ) -> AmResult<()> {
        self.data_sink_mut()?.append(new)?;
        Ok(())
    }

    fn update_slot(
        &mut self,
        row_id: ItemPointer,
        _old: TupleSlotRow<'_>,
        new: TupleSlotRow<'_>,
        context: MutationUpdateContext<'_>,
    ) -> AmResult<MutationOutcome> {
        self.ensure_position_delete_capable()?;
        let identity = IcebergRowIdentity::decode(&row_id)?;
        let claim = self.row_registry.claim(
            self.modify_state_id,
            identity.file_id(),
            identity.row_position(),
            context.cid,
        )?;
        match claim {
            RowMutationClaim::FirstTouch { new_file_positions } => {
                if let Some(positions) = new_file_positions {
                    self.position_deletes
                        .add_file_positions(identity.file_id(), positions);
                }
                self.data_sink_mut()?.append(new)?;
                Ok(MutationOutcome::Applied)
            }
            RowMutationClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(MutationOutcome::AlreadyModifiedInCurrentTransaction {
                modifying_command_id,
            }),
        }
    }

    fn delete_slot(
        &mut self,
        row_id: ItemPointer,
        context: MutationDeleteContext<'_>,
    ) -> AmResult<MutationOutcome> {
        self.ensure_position_delete_capable()?;
        let identity = IcebergRowIdentity::decode(&row_id)?;
        let claim = self.row_registry.claim(
            self.modify_state_id,
            identity.file_id(),
            identity.row_position(),
            context.cid,
        )?;
        match claim {
            RowMutationClaim::FirstTouch { new_file_positions } => {
                if let Some(positions) = new_file_positions {
                    self.position_deletes
                        .add_file_positions(identity.file_id(), positions);
                }
                Ok(MutationOutcome::Applied)
            }
            RowMutationClaim::PreviouslyModified {
                modifying_command_id,
            } => Ok(MutationOutcome::AlreadyModifiedInCurrentTransaction {
                modifying_command_id,
            }),
        }
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
        let outcome = self.finish_statement()?;
        self.stage_statement(outcome)?;
        Ok(())
    }
}

impl AmCopySession for IcebergModifyState {
    fn new(rel: &RelationHandle) -> AmResult<Self> {
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

    fn begin_copy(&mut self) -> AmResult<()> {
        Ok(())
    }

    fn end_copy(&mut self) -> AmResult<()> {
        let outcome = self.finish_statement()?;
        self.stage_statement(outcome)?;
        Ok(())
    }

    fn abort_copy(&mut self) {
        if let Some(sink) = self.data_sink.as_mut() {
            sink.abort();
        }
        self.position_deletes.clear();
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
}

impl IcebergModifyState {
    /// Construct a fully-initialized session, performing all storage IO and
    /// schema/writer setup inline.
    ///
    /// Everything is derived from `rel` here — the file locator and WAL flag
    /// for storage, the relation OID for the metadata tracker, and the live
    /// columns / tuple width / attribute types that bind the write-side
    /// source-slot mapping ([`WriteColumns`]). The handle is not retained.
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
        let row_registry =
            query_state.update(|state| state.relation_registry(rel_oid))?;
        let modify_state_id = row_registry.begin_modify_state()?;
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
        // TODO(iceberg-v3-deletion-vectors): this check currently lets format
        // v3 reach the position-delete sink, but v3 forbids adding new
        // position-delete files. Add deletion-vector writing (or reject these
        // actions) before claiming v3 UPDATE/DELETE support.
        let iceberg_schema = loaded.metadata.current_schema().clone();
        let table_properties = loaded
            .metadata
            .table_properties()
            .map_err(IcebergError::from)?;
        let isolation_level = command
            .effective_isolation_level(&table_properties, transaction_isolation);
        let conflict_scope = if command.validation_command().is_some() {
            scan_context.map(|context| {
                ConflictValidationScope::from_predicate(context.conflict_filter)
            })
        } else {
            None
        };
        let write_options = IcebergTableOptions::for_relation(rel)?;
        let writer_properties = WriterProperties::builder()
            .set_compression(write_options.parquet_compression())
            .build();

        let data_sink = if actions.writes_rows() {
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
                gucs::mutation_buffer_flush_bytes(),
            )?)
        } else {
            None
        };
        let position_delete_sink = if writes_position_deletes {
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
            target_dependency,
            isolation_level,
            conflict_scope,
            data_sink,
            position_delete_sink,
            position_deletes: PositionDeleteAccumulator::default(),
            row_registry,
            modify_state_id,
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
        command: RowLevelCommand,
        starting_snapshot_id: Option<i64>,
        referenced_data_files: BTreeSet<String>,
    ) -> IcebergResult<()> {
        let Some(isolation_level) = self.isolation_level else {
            return Err(IcebergError::InvariantViolated(
                "row-level mutation validation has no effective isolation level",
            ));
        };
        let conflict_filter = self
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
        let new_data_files = self.finish_data_files()?;
        let referenced_data_files = self
            .position_deletes
            .referenced_data_files(&self.row_registry)?;
        let position_delete_files = self.finish_position_deletes()?;
        Ok(StatementOutcome {
            command: self.command,
            target_dependency: self.target_dependency,
            new_data_files,
            position_delete_files,
            referenced_data_files,
        })
    }

    fn stage_statement(&mut self, outcome: StatementOutcome) -> IcebergResult<()> {
        let validation = outcome.row_delta_validation()?;

        if !outcome.new_data_files.is_empty() {
            self.stage_data_files(outcome.new_data_files)?;
        }
        for output in outcome.position_delete_files {
            self.stage_position_delete_file(output)?;
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
            .write_files(&self.position_deletes, &self.row_registry)
    }

    fn data_sink_mut(&mut self) -> IcebergResult<&mut DataFileSink> {
        self.data_sink
            .as_mut()
            .ok_or(IcebergError::InvariantViolated(
                "data-file callback reached a mutation command without a data sink",
            ))
    }

    fn position_delete_sink_ref(&self) -> IcebergResult<&PositionDeleteSink> {
        self.position_delete_sink
            .as_ref()
            .ok_or(IcebergError::InvariantViolated(
                "position-delete callback reached a mutation command without a delete sink",
            ))
    }

    fn ensure_position_delete_capable(&self) -> IcebergResult<()> {
        if self.position_delete_sink.is_none() {
            return Err(IcebergError::InvariantViolated(
                "position-delete callback reached a mutation command without a delete sink",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod mutation_state_tests {
    use super::*;

    #[test]
    fn synthetic_ctid_round_trips_boundaries() {
        let cases = [
            (0, 0),
            (0, MAX_POSITION),
            (u32::try_from(FILE_MASK).unwrap(), 0),
            (u32::try_from(FILE_MASK).unwrap(), MAX_POSITION),
        ];
        for (file_id, position) in cases {
            let file_id = IcebergFileId::from_raw(file_id);
            let tid = IcebergRowIdentity::encode(file_id, position).unwrap();
            assert_ne!(tid.offset, 0);
            assert!(tid.block_number < TRIGGER_ROW_BLOCK_BASE);
            let decoded = IcebergRowIdentity::decode(&tid).unwrap();
            assert_eq!(decoded.file_id(), file_id);
            assert_eq!(u64::from(decoded.row_position()), position);
        }
    }

    #[test]
    fn synthetic_ctid_rejects_out_of_range_values() {
        assert!(
            IcebergRowIdentity::encode(
                IcebergFileId::from_raw(1 << ICEBERG_FILE_ID_BITS),
                0,
            )
            .is_err()
        );
        assert!(
            IcebergRowIdentity::encode(IcebergFileId::from_raw(0), MAX_POSITION + 1,)
                .is_err()
        );
        assert!(IcebergRowIdentity::decode(&ItemPointer::default()).is_err());
        assert!(
            IcebergRowIdentity::decode(&ItemPointer {
                block_number: TRIGGER_ROW_BLOCK_BASE,
                offset: 1,
            })
            .is_err()
        );
    }

    #[test]
    fn relation_registry_interns_each_path_once() {
        let registry = RelationRowRegistry::default();
        let first_file = registry.register_file("data/a.parquet").unwrap();
        let second_file = registry.register_file("data/a.parquet").unwrap();
        let other_file = registry.register_file("data/b.parquet").unwrap();
        assert_eq!(first_file, second_file);
        assert_ne!(first_file, other_file);
        assert_eq!(
            registry.file_path(other_file).unwrap().as_ref(),
            "data/b.parquet"
        );
    }

    #[test]
    fn commands_read_their_own_isolation_property() {
        let cases = [
            (
                iceberg_lite::spec::TableProperties::PROPERTY_WRITE_DELETE_ISOLATION_LEVEL,
                ModifyCommand::Delete,
            ),
            (
                iceberg_lite::spec::TableProperties::PROPERTY_WRITE_UPDATE_ISOLATION_LEVEL,
                ModifyCommand::Update,
            ),
            (
                iceberg_lite::spec::TableProperties::PROPERTY_WRITE_MERGE_ISOLATION_LEVEL,
                ModifyCommand::Merge,
            ),
        ];

        for (snapshot_property, snapshot_command) in cases {
            let properties = iceberg_lite::spec::TableProperties::try_from(
                &std::collections::HashMap::from([(
                    snapshot_property.to_owned(),
                    "snapshot".to_owned(),
                )]),
            )
            .unwrap();

            for command in [
                ModifyCommand::Delete,
                ModifyCommand::Update,
                ModifyCommand::Merge,
            ] {
                let expected = if command == snapshot_command {
                    IsolationLevel::Snapshot
                } else {
                    IsolationLevel::Serializable
                };
                assert_eq!(
                    command.table_isolation_level(&properties),
                    Some(expected)
                );
            }
        }
    }

    #[test]
    fn target_dependency_separates_independent_and_required_reads() {
        let empty_scan = IcebergModifyScanContext::new(None, Predicate::AlwaysTrue);
        let empty_table_read = TargetDependency::from_context(Some(&empty_scan));
        assert_eq!(empty_table_read.required_snapshot().unwrap(), None);

        let independent = TargetDependency::from_context(None);
        assert_eq!(independent, TargetDependency::Independent);
    }
}

#[cfg(feature = "pg_test")]
#[path = "mutation_pg_test.rs"]
mod pg_test;
