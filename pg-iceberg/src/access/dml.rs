//! Iceberg DML (Data Manipulation Language) operations.
//!
//! This module implements INSERT operations for Iceberg tables. Update and Delete
//! operations are currently not yet implemented.
//! It uses the `iceberg-lite` writer module to write data files in Parquet format
//! and commits the changes through the transaction API.

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
use pg_tam::data::Row;
use pg_tam::handles::RelationHandle;
use pg_tam::prelude::*;
use pgrx::pg_sys;

use crate::access::rows_to_record_batch;
use crate::catalog::{
    get_or_rebase_metadata_location, process_new_data_files,
    register_table_for_tracking,
};
use crate::error::{IcebergError, IcebergResult};
use crate::storage::create_storage_context;

/// Default batch size for buffering rows (64MB) before writing.
const DEFAULT_BATCH_SIZE_IN_MB: usize = 64;

type ParquetDataFileWriter = DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// Iceberg DML state for INSERT/UPDATE/DELETE operations.
///
/// This struct holds the state needed during a modify operation:
/// - Buffered rows waiting to be written
/// - Writer for producing data files
/// - Table schema and storage context
pub struct IcebergModify {
    /// The relation being modified
    rel: pg_sys::Relation,
    /// Buffered rows waiting to be written
    row_buffer: Vec<Row>,
    /// Current size of buffered rows in bytes
    current_buffer_size: usize,
    /// Data files produced during this modify operation
    data_files: Vec<DataFile>,
    /// Iceberg schema for the table
    iceberg_schema: Option<Arc<IcebergSchema>>,
    /// File IO for writing
    file_io: Option<FileIO>,
    /// Whether the modify operation has been initialized
    initialized: bool,
    /// Current active writer for creating data files
    writer: Option<ParquetDataFileWriter>,
}

impl AmDml<IcebergError> for IcebergModify {
    /// Create a new IcebergModify instance.
    ///
    /// At this point we only store the relation handle.
    /// Actual initialization happens in `begin_modify`.
    fn new(rel: pg_sys::Relation) -> IcebergResult<Self> {
        Ok(IcebergModify {
            rel,
            row_buffer: Vec::new(),
            current_buffer_size: 0,
            data_files: Vec::new(),
            iceberg_schema: None,
            file_io: None,
            initialized: false,
            writer: None,
        })
    }

    /// Begin a modify operation.
    ///
    /// This method:
    /// 1. Loads the Iceberg metadata from PostgreSQL catalog
    /// 2. Reads the table metadata from storage
    /// 3. Initializes the schema and prepares for writing
    fn begin_modify(&mut self) -> IcebergResult<()> {
        if self.initialized {
            return Ok(());
        }

        let rel_handle = unsafe { RelationHandle::from_raw(self.rel) };
        let rel_oid = unsafe { (*(*self.rel).rd_rel).oid };
        let nsp_oid = unsafe { (*(*self.rel).rd_rel).relnamespace };
        let rel_num = unsafe { (*self.rel).rd_locator.relNumber };

        // Register table for metadata tracking
        register_table_for_tracking(rel_oid, nsp_oid, rel_num)?;

        // Create storage context for reading/writing
        let spc_oid = rel_handle.tablespace_oid();
        let ctx = create_storage_context(spc_oid)?;

        // Get Iceberg metadata location
        // Since we just registered the table for tracking, we ensure we see the latest
        // committed data (Read Committed) by calling the rebase-aware helper.
        let metadata_location =
            get_or_rebase_metadata_location(rel_oid, &ctx.file_io)?;

        // Read table metadata from storage
        let table_metadata =
            TableMetadata::read_from(&ctx.file_io, &metadata_location)?;
        let schema = table_metadata.current_schema().clone();

        self.iceberg_schema = Some(schema);
        self.file_io = Some(ctx.file_io);
        self.initialize_writer(&table_metadata)?;
        self.initialized = true;

        Ok(())
    }

    /// End a modify operation.
    ///
    /// This method:
    /// 1. Flushes any remaining buffered rows
    /// 2. Closes the active writer (if any) and collects generated data files
    /// 3. Commits the data files to the Iceberg table
    /// 4. Stages the new metadata location in the transaction tracker
    fn end_modify(&mut self) -> IcebergResult<()> {
        if !self.initialized {
            return Ok(());
        }

        // Reset initialized state immediately to ensure idempotent cleanup
        // and prevent double-clearing if an error occurs and this is called again.
        self.initialized = false;

        // Flush remaining buffered rows
        let flush_res = self.flush_buffer();

        // Ensure writer is always taken and closed to avoid leaking file descriptors,
        // even if flushing failed.
        let writer_res = if let Some(mut writer) = self.writer.take() {
            writer.close()
        } else {
            Ok(Vec::new())
        };

        // Propagate errors after ensuring resource cleanup attempt
        flush_res?;
        let new_data_files = writer_res?;
        self.data_files.extend(new_data_files);

        // If we have data files, apply them to the Iceberg table
        if !self.data_files.is_empty() {
            self.apply_iceberg_changes()?;
        }

        Ok(())
    }

    /// Insert a single tuple.
    ///
    /// The row is buffered and will be written when the buffer is full
    /// or when `end_modify` is called.
    fn tuple_insert(
        &mut self,
        row: &Row,
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&mut BulkInsertStateHandle>,
    ) -> IcebergResult<()> {
        if !self.initialized {
            self.begin_modify()?;
        }

        // Clone and buffer the row
        self.current_buffer_size += row.size;
        self.row_buffer.push(row.clone());

        // Flush if buffer is full (converting MB to bytes)
        if self.current_buffer_size >= DEFAULT_BATCH_SIZE_IN_MB * 1024 * 1024 {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Insert multiple tuples at once.
    ///
    /// This is more efficient than calling `tuple_insert` repeatedly.
    fn multi_insert(
        &mut self,
        rows: &[Row],
        _cid: pg_sys::CommandId,
        _options: i32,
        _bistate: Option<&mut BulkInsertStateHandle>,
    ) -> IcebergResult<()> {
        if !self.initialized {
            self.begin_modify()?;
        }

        for row in rows {
            self.current_buffer_size += row.size;
            self.row_buffer.push(row.clone());

            // Flush if buffer is full (converting MB to bytes)
            if self.current_buffer_size >= DEFAULT_BATCH_SIZE_IN_MB * 1024 * 1024 {
                self.flush_buffer()?;
            }
        }

        Ok(())
    }

    /// Delete a tuple (not yet implemented for Iceberg).
    ///
    /// Iceberg supports deletes through delete files (position or equality deletes).
    /// This requires additional implementation.
    fn tuple_delete(
        &mut self,
        _tid: &ItemPointer,
        _cid: pg_sys::CommandId,
        _snapshot: &SnapshotHandle,
        _crosscheck: Option<&SnapshotHandle>,
        _wait: bool,
        _tmfd: &mut TM_FailureData,
        _changing_part: bool,
    ) -> IcebergResult<pg_sys::TM_Result::Type> {
        Err(IcebergError::NotImplemented("tuple_delete"))
    }

    /// Update a tuple (not yet implemented for Iceberg).
    ///
    /// Iceberg implements updates as delete + insert.
    /// This requires additional implementation.
    fn tuple_update(
        &mut self,
        _otid: &ItemPointer,
        _row: &Row,
        _cid: pg_sys::CommandId,
        _snapshot: &SnapshotHandle,
        _crosscheck: Option<&SnapshotHandle>,
        _wait: bool,
        _tmfd: &mut TM_FailureData,
        _lockmode: &mut pg_sys::LockTupleMode::Type,
        _update_indexes: &mut pg_sys::TU_UpdateIndexes::Type,
    ) -> IcebergResult<pg_sys::TM_Result::Type> {
        Err(IcebergError::NotImplemented("tuple_update"))
    }

    /// Lock a tuple (not applicable for Iceberg).
    ///
    /// Iceberg tables don't support row-level locking in the PostgreSQL sense.
    fn tuple_lock(
        &mut self,
        _tid: &ItemPointer,
        _snapshot: &SnapshotHandle,
        _row: &mut Row,
        _cid: pg_sys::CommandId,
        _mode: pg_sys::LockTupleMode::Type,
        _wait_policy: pg_sys::LockWaitPolicy::Type,
        _flags: u8,
        _tmfd: &mut TM_FailureData,
    ) -> IcebergResult<pg_sys::TM_Result::Type> {
        Err(IcebergError::NotImplemented("tuple_lock"))
    }
}

impl IcebergModify {
    /// Initialize the data file writer components.
    ///
    /// This method sets up the location generator, file name generator, and
    /// Parquet writer builder to prepare for writing data files.
    fn initialize_writer(
        &mut self,
        table_metadata: &TableMetadata,
    ) -> IcebergResult<()> {
        let file_io = self
            .file_io
            .as_ref()
            .ok_or(IcebergError::NotImplemented("file_io not initialized"))?;

        let schema = self
            .iceberg_schema
            .as_ref()
            .ok_or(IcebergError::NotImplemented("schema not initialized"))?;

        // Create writer components
        let location_generator =
            DefaultLocationGenerator::new(table_metadata.clone())?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("insert-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        );

        // Create Parquet writer builder
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
        );

        // Create rolling file writer builder
        let rolling_writer_builder =
            RollingFileWriterBuilder::new_with_default_file_size(
                parquet_writer_builder,
                file_io.clone(),
                location_generator,
                file_name_generator,
            );

        // Create and use data file writer
        let data_file_writer_builder =
            DataFileWriterBuilder::new(rolling_writer_builder);
        self.writer = Some(data_file_writer_builder.build(None)?);

        Ok(())
    }

    /// Flush the row buffer to a data file.
    ///
    /// This method:
    /// 1. Converts buffered rows to an Arrow RecordBatch
    /// 2. Writes the RecordBatch to the active writer
    fn flush_buffer(&mut self) -> IcebergResult<()> {
        if self.row_buffer.is_empty() {
            return Ok(());
        }

        let schema = self
            .iceberg_schema
            .as_ref()
            .ok_or(IcebergError::NotImplemented("schema not initialized"))?;

        // Extract rows and reset size tracking immediately.
        // This ensures the buffer is cleared even if subsequent steps fail.
        let rows = std::mem::take(&mut self.row_buffer);
        self.current_buffer_size = 0;

        // Convert rows to Arrow RecordBatch
        let record_batch = rows_to_record_batch(&rows, schema)?;

        // Write the batch
        if let Some(writer) = &mut self.writer {
            writer.write(record_batch)?;
        } else {
            return Err(IcebergError::NotImplemented("writer not initialized"));
        }

        Ok(())
    }

    /// Apply pending data files and update Iceberg table metadata.
    ///
    /// This method:
    /// 1. Collects all data files generated during this modify operation
    /// 2. Delegates to the metadata tracker to process these files (handles
    ///    rebasing, generating new metadata file, and staging the change)
    fn apply_iceberg_changes(&mut self) -> IcebergResult<()> {
        let file_io = self
            .file_io
            .as_ref()
            .ok_or(IcebergError::NotImplemented("file_io not initialized"))?;

        // Capture the files locally
        let new_files: Vec<_> = self.data_files.drain(..).collect();
        let rel_oid = unsafe { (*(*self.rel).rd_rel).oid };

        // Process files via tracker (handles rebase and metadata file generation)
        process_new_data_files(rel_oid, new_files, file_io)?;

        Ok(())
    }
}
