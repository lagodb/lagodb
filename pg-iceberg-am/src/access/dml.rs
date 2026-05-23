//! Iceberg DML (Data Manipulation Language) operations.
//!
//! This module implements INSERT operations for Iceberg tables. Update and Delete
//! operations are currently not yet implemented.
//! It uses the `iceberg-lite` writer module to write data files in Parquet format
//! and commits the changes through the transaction API.
//!
//! # Initialization model
//!
//! Storage IO, schema resolution, and writer construction all happen up-front in
//! [`AmDmlSession::new`]. The session is therefore *always* fully initialized
//! once it has been handed back to the framework, and every field is non-`Option`.
//! `begin_modify` is a no-op kept around to satisfy the trait contract.
//!
//! This is a deliberate choice. The framework calls `new()` and `begin_modify()`
//! back-to-back inside `create_session` (see
//! `pg-lakebase-core/src/access/dml/session.rs`), and there is no information
//! available at `begin_modify` time that is not already available at `new` time:
//! `DmlTarget` carries the `rel_oid`, the `RelFileLocator` (which contains
//! `spc_oid`), and `relation_needs_wal`, which is everything the constructor
//! needs. Splitting initialization across `new` / `begin_modify` would only
//! re-introduce a state machine encoded as `Option<T>` fields without buying any
//! deferred-IO benefit.

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
use pg_lakebase_core::batch::RowBatchBuffer;
use pg_lakebase_core::prelude::*;
use pg_lakebase_core::tuple::Row;
use pgrx::pg_sys;

use crate::access::conversion::RowRecordBatchBuilder;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::gucs;
use crate::storage::StorageContext;

type ParquetDataFileWriter = DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// Iceberg DML state for INSERT/UPDATE/DELETE operations.
///
/// Constructed eagerly: by the time this struct exists, the storage context,
/// schemas, and writer are all wired up. See module docs for the rationale.
pub struct IcebergModify {
    /// OID of the relation being modified.
    rel_oid: pg_sys::Oid,
    /// Schema-bound builder that turns buffered `Row`s into Arrow
    /// `RecordBatch`es. Owns the resolved Arrow schema and the per-column
    /// dispatch plan.
    ///
    /// TODO(rowless-dml): replace this fallback with an Iceberg-owned Arrow
    /// batch buffer that implements the slot/datum model from
    /// `pg_lakebase_core::batch::SlotColumnarBatchBuffer`. `IcebergModify`
    /// should override `tuple_insert_slot` and `multi_insert_slots`, append
    /// `PgDatumRef` values into schema-bound Arrow column appenders, finish a
    /// `RecordBatch`, and hand it to the existing writer. That removes the hot
    /// path `TupleTableSlot -> Row -> Cell -> RowBatchBuffer -> Arrow builder`
    /// conversion and especially avoids per-row array/list/string
    /// materialization. Keep the current `RowRecordBatchBuilder` only as a
    /// row-mode fallback or test utility once the slot path is in place.
    batch_builder: RowRecordBatchBuilder,
    /// File IO for writing data files.
    file_io: FileIO,
    /// Buffered rows waiting to be written.
    row_buffer: RowBatchBuffer,
    /// Row-buffer memory threshold captured for this DML session.
    flush_threshold_bytes: usize,
    /// Data files produced during this modify operation.
    data_files: Vec<DataFile>,
    /// Active rolling Parquet writer.
    ///
    /// `Option` here is *not* an initialization flag; it is consumed by
    /// [`Self::close_writer`] (which calls `IcebergWriter::close`, taking the
    /// writer by `&mut self` and invalidating it). After `close_writer`, the
    /// session is finished from the writer's perspective and only `data_files`
    /// remains relevant.
    writer: Option<ParquetDataFileWriter>,
}

impl AmDmlSession for IcebergModify {
    fn new(target: DmlTarget) -> AmResult<Self> {
        // Funnel the IO-bearing work through a single `IcebergResult`-returning
        // entry point so all `iceberg_lite::Error -> IcebergError` conversions
        // happen in one place; `new` itself only crosses the
        // `IcebergError -> AmResult` boundary.
        Ok(Self::open(target)?)
    }

    fn begin_modify(&mut self) -> AmResult<()> {
        // Intentionally empty: see module docs. All initialization happens in
        // `new` so that every field on `Self` is non-`Option` and the
        // PostgreSQL-facing tuple callbacks have nothing to validate.
        Ok(())
    }

    fn abort_modify(&mut self) {
        // Best-effort cleanup of in-memory buffers. The session itself is
        // dropped by the framework after this returns, so we don't need to
        // reset to a "reusable" state; just release whatever we can cheaply.
        //
        // Persistent artifacts (staged or already-uploaded data files) are
        // tracked through `StorageArtifactResource` and unwound by
        // ResourceOwner cleanup; see the orphan-file note in `end_modify`.
        self.row_buffer.clear();
        self.data_files.clear();
        self.writer.take();
    }

    fn end_modify(&mut self) -> AmResult<()> {
        // Flush remaining buffered rows.
        //
        // Orphan-file note: if a previous batch in this `end_modify` already
        // rolled over a data file and uploaded it to remote storage, and a
        // later `flush_buffer` / `close_writer` then fails, returning the
        // error here is NOT going to leak a remote object. Every data file
        // produced by the underlying writer goes through
        // `ObjectStorage::writer()` -> `register_object_file_staged()` and,
        // on successful upload, `finalize_write()` -> `mark_object_file_uploaded()`
        // (see `storage/object.rs` and `storage/transactional_artifacts.rs`).
        // When this method returns an error and the transaction aborts, the
        // `StorageArtifactResource::on_abort` callback walks every registered
        // artifact and either unlinks the staging file (Staged) or issues a
        // remote delete (Uploaded). Local writers are tracked the same way via
        // `register_local_file_created`. Please do not "fix" the apparent
        // orphan risk by re-introducing a separate cleanup list here.
        let flush_res = self.flush_buffer();

        // Ensure writer is always taken and closed to avoid leaking file descriptors,
        // even if flushing failed.
        let writer_res = self.close_writer();

        // Propagate errors after ensuring resource cleanup attempt
        flush_res?;
        let new_files = writer_res?;

        self.data_files.extend(new_files);

        // If we have data files, stage them for transaction-level commit.
        if !self.data_files.is_empty() {
            self.stage_data_files()?;
        }

        Ok(())
    }

    fn tuple_insert(
        &mut self,
        row: Row,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        let _ = (cid, options, bistate);
        self.buffer_row(row)?;
        Ok(())
    }

    fn multi_insert(
        &mut self,
        rows: Vec<Row>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        let _ = (cid, options, bistate);
        self.buffer_rows(rows)?;
        Ok(())
    }
}

impl IcebergModify {
    /// Construct a fully-initialized session, performing all storage IO and
    /// schema/writer setup inline. Errors flow as `IcebergError` so the caller
    /// in `AmDmlSession::new` only has one error-domain hop to make.
    fn open(target: DmlTarget) -> IcebergResult<Self> {
        let ctx = StorageContext::for_tablespace_with_wal(
            target.locator.spc_oid,
            target.relation_needs_wal,
        )?;
        let file_io = ctx.into_file_io();

        // Single write-side entry point: registers the relation with the
        // per-transaction tracker, rebases pending changes, and returns the
        // base metadata in one step. Bundling these means a writer cannot
        // accidentally read metadata without enrolling in tracking.
        let loaded =
            TxMetadata::current().begin_table_modify(target.rel_oid, &file_io)?;
        let iceberg_schema = loaded.metadata.current_schema().clone();
        let batch_builder = RowRecordBatchBuilder::new(&iceberg_schema)?;

        let writer = build_writer(&file_io, &iceberg_schema, &loaded.metadata)?;

        Ok(Self {
            rel_oid: target.rel_oid,
            batch_builder,
            file_io,
            row_buffer: RowBatchBuffer::new(),
            flush_threshold_bytes: gucs::dml_buffer_flush_bytes(),
            data_files: Vec::new(),
            writer: Some(writer),
        })
    }

    fn close_writer(&mut self) -> IcebergResult<Vec<DataFile>> {
        match self.writer.take() {
            Some(mut writer) => Ok(writer.close()?),
            None => Ok(Vec::new()),
        }
    }

    fn buffer_row(&mut self, row: Row) -> IcebergResult<()> {
        self.row_buffer.push_row(row);
        self.flush_buffer_if_needed()
    }

    fn buffer_rows(&mut self, rows: Vec<Row>) -> IcebergResult<()> {
        for row in rows {
            self.row_buffer.push_row(row);
            self.flush_buffer_if_needed()?;
        }
        Ok(())
    }

    fn flush_buffer_if_needed(&mut self) -> IcebergResult<()> {
        // Flush when the row buffer's estimated memory footprint crosses the
        // configured threshold. This is a backend-side memory-pressure guard,
        // not a Parquet file-size target: the rolling file writer downstream
        // owns data-file sizing independently of when we flush this buffer.
        if self.row_buffer.should_flush(self.flush_threshold_bytes) {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Flush the row buffer to a data file.
    ///
    /// 1. Converts buffered rows to an Arrow RecordBatch
    /// 2. Writes the RecordBatch to the active writer
    fn flush_buffer(&mut self) -> IcebergResult<()> {
        if self.row_buffer.is_empty() {
            return Ok(());
        }

        // Extract rows and reset size tracking immediately.
        // This ensures the buffer is cleared even if subsequent steps fail.
        let rows = self.row_buffer.take_rows();

        // Convert rows to Arrow RecordBatch using the schema-bound builder.
        let record_batch = self.batch_builder.build(&rows)?;

        // Writer is only `None` after `close_writer` has run, which only
        // happens during `end_modify` / `abort_modify`. The buffer is cleared
        // before either of those, so reaching this branch with `None` means
        // a tuple callback fired after the session was finalized — a framework
        // bug worth surfacing as `InvariantViolated`.
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

    /// Stage pending data files into transaction-local Iceberg metadata.
    ///
    /// This method:
    /// 1. Collects all data files generated during this modify operation
    /// 2. Delegates to the metadata tracker to process these files (handles
    ///    rebasing, generating new metadata file, and staging the change)
    fn stage_data_files(&mut self) -> IcebergResult<()> {
        // Capture the files locally
        let new_files = std::mem::take(&mut self.data_files);
        // Process files via tracker (handles rebase and metadata file generation)
        TxMetadata::current().rebase_for_statement(
            self.rel_oid,
            new_files,
            &self.file_io,
        )?;

        Ok(())
    }
}

/// Build the rolling Parquet data file writer for this DML session.
///
/// Free function (not a method) because it has no need for `&self` state and
/// composes several builders that we'd otherwise have to thread through ad-hoc
/// helpers; keeping it private to this module makes the call site in `new`
/// readable without polluting the type's API.
fn build_writer(
    file_io: &FileIO,
    schema: &Arc<IcebergSchema>,
    table_metadata: &TableMetadata,
) -> IcebergResult<ParquetDataFileWriter> {
    let location_generator = DefaultLocationGenerator::new(table_metadata.clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("insert-{}", uuid::Uuid::now_v7()),
        None,
        DataFileFormat::Parquet,
    );

    let parquet_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        file_io.clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    Ok(data_file_writer_builder.build(None)?)
}
