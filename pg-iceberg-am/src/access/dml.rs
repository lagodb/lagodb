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
use pg_lakebase_core::batch::RowBatchBuffer;
use pg_lakebase_core::prelude::*;
use pg_lakebase_core::tuple::Row;
use pgrx::pg_sys;

use crate::access::{
    iceberg_schema_to_arrow_schema, rows_to_record_batch_with_schema,
};
use crate::catalog::{
    get_or_rebase_metadata_location, process_new_data_files,
    register_table_for_tracking,
};
use crate::error::{IcebergError, IcebergResult};
use crate::storage::create_storage_context_with_wal;

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
    /// OID of the relation being modified.
    rel_oid: pg_sys::Oid,
    /// Namespace OID captured while the PostgreSQL Relation pointer is valid.
    nsp_oid: pg_sys::Oid,
    /// Relation file number captured for transaction-local metadata tracking.
    rel_number: pg_sys::RelFileNumber,
    /// Tablespace OID used to build the storage context.
    spc_oid: pg_sys::Oid,
    /// Whether the relation requires WAL for local storage writes.
    relation_needs_wal: bool,
    /// Buffered rows waiting to be written
    row_buffer: RowBatchBuffer,
    /// Data files produced during this modify operation
    data_files: Vec<DataFile>,
    /// Iceberg schema for the table
    iceberg_schema: Option<Arc<IcebergSchema>>,
    /// Arrow schema for the table
    arrow_schema: Option<Arc<arrow_schema::Schema>>,
    /// File IO for writing
    file_io: Option<FileIO>,
    /// Whether the modify operation has been initialized
    initialized: bool,
    /// Current active writer for creating data files
    writer: Option<ParquetDataFileWriter>,
}

impl AmDmlSession for IcebergModify {
    fn new(target: DmlTarget) -> AmResult<Self> {
        Ok(Self {
            rel_oid: target.rel_oid,
            nsp_oid: target.namespace_oid,
            rel_number: target.locator.rel_number,
            spc_oid: target.locator.spc_oid,
            relation_needs_wal: target.relation_needs_wal,
            row_buffer: RowBatchBuffer::new(),
            data_files: Vec::new(),
            iceberg_schema: None,
            arrow_schema: None,
            file_io: None,
            initialized: false,
            writer: None,
        })
    }

    fn begin_modify(&mut self) -> AmResult<()> {
        if self.initialized {
            return Ok(());
        }

        // Register table for metadata tracking
        register_table_for_tracking(self.rel_oid, self.nsp_oid, self.rel_number)?;

        if self.file_io.is_none() {
            let ctx = create_storage_context_with_wal(
                self.spc_oid,
                self.relation_needs_wal,
            )?;
            self.file_io = Some(ctx.file_io);
        }

        let (table_metadata, schema, arrow_schema) = self.load_table_metadata()?;

        self.iceberg_schema = Some(schema);
        self.arrow_schema = Some(arrow_schema);
        self.initialize_writer(&table_metadata)?;
        self.initialized = true;

        Ok(())
    }

    fn abort_modify(&mut self) {
        self.initialized = false;
        self.row_buffer.clear();
        self.data_files.clear();
        self.writer.take();
    }

    fn end_modify(&mut self) -> AmResult<()> {
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
        let writer_res = self.close_writer();

        // Propagate errors after ensuring resource cleanup attempt
        flush_res?;
        let new_files = writer_res?;

        self.data_files.extend(new_files);

        // If we have data files, apply them to the Iceberg table
        if !self.data_files.is_empty() {
            self.apply_iceberg_changes()?;
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
        if !self.initialized {
            self.begin_modify()?;
        }

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
        if !self.initialized {
            self.begin_modify()?;
        }

        for row in rows {
            self.buffer_row(row)?;
        }

        Ok(())
    }
}

impl IcebergModify {
    fn load_table_metadata(
        &self,
    ) -> IcebergResult<(TableMetadata, Arc<IcebergSchema>, Arc<arrow_schema::Schema>)>
    {
        let file_io = self
            .file_io
            .as_ref()
            .ok_or(IcebergError::NotImplemented("file_io not initialized"))?;

        // Get Iceberg metadata location
        // Since we just registered the table for tracking, we ensure we see the latest
        // committed data (Read Committed) by calling the rebase-aware helper.
        let metadata_location =
            get_or_rebase_metadata_location(self.rel_oid, file_io)?;

        // Read table metadata from storage
        let table_metadata = TableMetadata::read_from(file_io, &metadata_location)?;
        let schema = table_metadata.current_schema().clone();
        let arrow_schema = Arc::new(iceberg_schema_to_arrow_schema(&schema)?);

        Ok((table_metadata, schema, arrow_schema))
    }

    fn close_writer(&mut self) -> IcebergResult<Vec<DataFile>> {
        if let Some(mut writer) = self.writer.take() {
            Ok(writer.close()?)
        } else {
            Ok(Vec::new())
        }
    }

    fn buffer_row(&mut self, row: Row) -> IcebergResult<()> {
        self.row_buffer.push_row(row);

        // Flush if buffer is full (converting MB to bytes)
        if self
            .row_buffer
            .should_flush(DEFAULT_BATCH_SIZE_IN_MB * 1024 * 1024)
        {
            self.flush_buffer()?;
        }

        Ok(())
    }

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

        let arrow_schema = self
            .arrow_schema
            .as_ref()
            .ok_or(IcebergError::NotImplemented("arrow_schema not initialized"))?;

        // Extract rows and reset size tracking immediately.
        // This ensures the buffer is cleared even if subsequent steps fail.
        let rows = self.row_buffer.take_rows();

        // Convert rows to Arrow RecordBatch using cached schema
        let record_batch =
            rows_to_record_batch_with_schema(&rows, schema, arrow_schema.clone())?;

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
        // Process files via tracker (handles rebase and metadata file generation)
        process_new_data_files(self.rel_oid, new_files, file_io)?;

        Ok(())
    }
}
