//! Iceberg DML operations.
//!
//! Implements INSERT for Iceberg tables (UPDATE/DELETE not yet implemented).
//! Uses the `iceberg-lite` writer to produce Parquet data files and commits
//! through the transaction API.
//!
//! [`DataFileSink`] owns the slot -> Parquet data-file pipeline; [`IcebergModify`]
//! is the AM session that wires tuple callbacks to the sink, crosses the
//! `IcebergError -> AmError` boundary, and stages finished files into the
//! per-transaction Iceberg metadata. All initialization happens in
//! [`IcebergModify::open`], so every field is non-`Option` and `begin_modify`
//! is a no-op.

use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::spec::{
    DataFile, DataFileFormat, Schema as IcebergSchema, TableMetadata,
};

use iceberg_lite::writer::base_writer::data_file_writer::{
    DataFileWriter, DataFileWriterBuilder,
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

use crate::access::column_mapping::{LiveColumn, WriteColumns};
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::gucs;
use crate::storage::StorageContext;

type ParquetDataFileWriter = DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

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
    ///
    /// `live_columns` are the relation's live (non-dropped) columns in attno
    /// order, `slot_width` is the relation's full `natts`, and `attr_types` is
    /// the relation's full-width `(oid, typmod)` list; together they let
    /// [`WriteColumns`] bind each Arrow output column to its source slot index
    /// and resolve each rule against the column's real PG type instead of
    /// assuming positional alignment or a type round-tripped from Iceberg.
    fn new(
        file_io: &FileIO,
        iceberg_schema: &Arc<IcebergSchema>,
        live_columns: &[LiveColumn],
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
        table_metadata: &TableMetadata,
        flush_threshold_bytes: usize,
    ) -> IcebergResult<Self> {
        let columns = WriteColumns::resolve(
            iceberg_schema,
            live_columns,
            slot_width,
            attr_types,
        )?;
        let writer = Self::build_writer(file_io, iceberg_schema, table_metadata)?;
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
    ) -> IcebergResult<ParquetDataFileWriter> {
        let location_generator =
            DefaultLocationGenerator::new(table_metadata.clone())?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("insert-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
        );

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

/// Iceberg DML state for INSERT/UPDATE/DELETE operations.
///
/// Constructed eagerly: storage context, schemas, and writer are all wired up
/// by the time this struct exists.
pub struct IcebergModify {
    /// OID of the relation being modified.
    rel_oid: pg_sys::Oid,
    /// File IO for staging produced data files into transaction metadata.
    file_io: FileIO,
    /// The slot -> data-file production pipeline.
    sink: DataFileSink,
}

impl AmDmlSession for IcebergModify {
    fn new(rel: &RelationHandle, _cmd_type: pg_sys::CmdType::Type) -> AmResult<Self> {
        // INSERT/UPDATE/DELETE all reach this AM through the same slot
        // callbacks, so the frame command type is not needed here.
        Ok(Self::open(rel)?)
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
        self.sink.append(row)?;
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
            self.sink.append(row)?;
        }
        Ok(())
    }

    fn abort_modify(&mut self) {
        // Best-effort in-memory cleanup; persistent artifacts are unwound by
        // ResourceOwner cleanup (see the orphan-file note in `end_modify`).
        self.sink.abort();
    }

    fn end_modify(&mut self) -> AmResult<()> {
        // Orphan-file note: data files already uploaded before a later flush
        // failure are NOT leaked. Every produced file is registered via
        // `register_object_file_staged()` / `mark_object_file_uploaded()`, and
        // `StorageArtifactResource::on_abort` unlinks staging files or issues
        // remote deletes on abort. Do not re-introduce a separate cleanup list here.
        let new_files = self.sink.finish()?;

        if !new_files.is_empty() {
            self.stage(new_files)?;
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
    fn open(rel: &RelationHandle) -> IcebergResult<Self> {
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
        let iceberg_schema = loaded.metadata.current_schema().clone();

        // Live columns (attno order) + full tuple width drive the name-based
        // source-slot mapping, so dropped-column gaps and schema evolution can
        // never silently misalign the write path.
        let live_columns: Vec<LiveColumn> = rel
            .live_columns()
            .into_iter()
            .map(|(attno, name)| LiveColumn::new(attno, name))
            .collect();
        let slot_width = rel.natts();
        let attr_types = rel.attr_types();

        let sink = DataFileSink::new(
            &file_io,
            &iceberg_schema,
            &live_columns,
            slot_width,
            &attr_types,
            &loaded.metadata,
            gucs::dml_buffer_flush_bytes(),
        )?;

        Ok(Self {
            rel_oid,
            file_io,
            sink,
        })
    }

    /// Stage produced data files into transaction-local Iceberg metadata.
    fn stage(&self, new_files: Vec<DataFile>) -> IcebergResult<()> {
        TxMetadata::current().stage_data_files(self.rel_oid, new_files, &self.file_io)
    }
}
