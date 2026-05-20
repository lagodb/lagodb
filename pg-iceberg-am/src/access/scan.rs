//! Iceberg Table Scan Implementation.
//!
//! This module implements sequential scan operations for Iceberg tables.
//! It uses the `iceberg-lite` scan module to read data files and converts
//! Arrow RecordBatches back to PostgreSQL Rows.

use std::sync::Arc;

use arrow_array::RecordBatch;
use iceberg_lite::catalog::{NamespaceIdent, TableIdent};
use iceberg_lite::scan::ArrowRecordBatchIterator;
use iceberg_lite::spec::{Schema as IcebergSchema, TableMetadata};
use iceberg_lite::table::Table;
use pg_lakebase_core::catalog::get_namespace_name;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::prelude::*;
use pg_lakebase_core::tuple::Row;
use pgrx::pg_sys;

use crate::IcebergTableAm;
use crate::access::conversion::extract_row_from_batch;
use crate::catalog::get_or_rebase_metadata_location;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::create_storage_context;

/// Iceberg sequential scan state.
///
/// This struct holds the state needed during a scan operation:
/// - The Iceberg table and its schema
/// - The Arrow RecordBatch iterator
/// - Current batch and row position within the batch
pub struct IcebergScan {
    /// The relation OID for accessing metadata
    rel_oid: pg_sys::Oid,
    /// The tablespace OID for storage context
    spc_oid: pg_sys::Oid,
    /// The relation name
    rel_name: String,
    /// The namespace name
    nsp_name: String,
    /// Arrow RecordBatch iterator from iceberg scan
    batch_iterator: Option<ArrowRecordBatchIterator>,
    /// Current RecordBatch being read
    current_batch: Option<RecordBatch>,
    /// Current row index within the current batch
    current_row_idx: usize,
    /// Iceberg schema for type conversion
    iceberg_schema: Option<Arc<IcebergSchema>>,
    /// Whether the scan has been initialized
    initialized: bool,
}

impl AmScan for IcebergTableAm {}

impl AmScanSession for IcebergScan {
    /// Create a new IcebergScan instance.
    ///
    /// At this point we only store the relation information.
    /// Actual initialization happens in `scan_begin`.
    fn new(
        rel: &RelationHandle,
        _snapshot: &SnapshotHandle,
        _key: Option<&ScanKeyHandle>,
        _pscan: Option<&ParallelTableScanDescHandle>,
        _flags: u32,
    ) -> AmResult<Self> {
        let rel_oid = unsafe { (*(*rel.as_raw()).rd_rel).oid };
        let spc_oid = rel.tablespace_oid();
        let rel_name = rel.relation_name();
        let nsp_oid = rel.namespace_oid();
        let nsp_name = Self::namespace_name(nsp_oid)?;

        Ok(IcebergScan {
            rel_oid,
            spc_oid,
            rel_name,
            nsp_name,
            batch_iterator: None,
            current_batch: None,
            current_row_idx: 0,
            iceberg_schema: None,
            initialized: false,
        })
    }

    /// Begin a scan operation.
    ///
    /// This method:
    /// 1. Loads the Iceberg metadata from PostgreSQL catalog
    /// 2. Reads the table metadata from storage
    /// 3. Creates a TableScan and obtains an Arrow iterator
    fn scan_begin(&mut self) -> AmResult<()> {
        if self.initialized {
            return Ok(());
        }

        let (batch_iterator, schema) = self.open_table_scan()?;

        self.batch_iterator = Some(batch_iterator);
        self.iceberg_schema = Some(schema);
        self.current_batch = None;
        self.current_row_idx = 0;
        self.initialized = true;

        Ok(())
    }

    /// Get the next row from the scan.
    ///
    /// This method:
    /// 1. Fetches the next RecordBatch if needed
    /// 2. Extracts the current row from the batch
    /// 3. Converts Arrow values to PostgreSQL Cells
    fn scan_getnextslot(
        &mut self,
        _direction: ScanDirection,
        row: &mut Row,
    ) -> AmResult<bool> {
        if !self.initialized {
            return Ok(false);
        }

        loop {
            // Check if we have a current batch and rows to read
            if let Some(ref batch) = self.current_batch
                && self.current_row_idx < batch.num_rows()
            {
                // Extract the current row from the batch
                let schema = self
                    .iceberg_schema
                    .as_ref()
                    .ok_or(IcebergError::NotImplemented("schema not initialized"))?;

                extract_row_from_batch(batch, self.current_row_idx, schema, row)?;
                self.current_row_idx += 1;
                return Ok(true);
            }

            match self.next_batch()? {
                Some(batch) => {
                    self.current_batch = Some(batch);
                    self.current_row_idx = 0;
                    // Continue loop to read from new batch
                }
                None => return Ok(false),
            }
        }
    }

    /// Rescan the table from the beginning.
    ///
    /// This reinitializes the scan by clearing current state
    /// and re-calling scan_begin.
    fn scan_rescan(
        &mut self,
        _key: Option<&ScanKeyHandle>,
        _set_params: bool,
        _allow_strat: bool,
        _allow_sync: bool,
        _allow_pagemode: bool,
    ) -> AmResult<()> {
        // Reset state
        self.batch_iterator = None;
        self.current_batch = None;
        self.current_row_idx = 0;
        self.initialized = false;

        // Re-initialize the scan
        self.scan_begin()
    }

    /// End the scan operation.
    ///
    /// Cleans up resources used by the scan.
    fn scan_end(&mut self) -> AmResult<()> {
        self.batch_iterator = None;
        self.current_batch = None;
        self.current_row_idx = 0;
        self.iceberg_schema = None;
        self.initialized = false;
        Ok(())
    }
}

impl IcebergScan {
    fn namespace_name(nsp_oid: pg_sys::Oid) -> IcebergResult<String> {
        get_namespace_name(nsp_oid)?.ok_or(IcebergError::NamespaceNull)
    }

    fn open_table_scan(
        &self,
    ) -> IcebergResult<(ArrowRecordBatchIterator, Arc<IcebergSchema>)> {
        let ctx = create_storage_context(self.spc_oid)?;

        // Ensure scans see the latest committed metadata when this transaction
        // has already staged writes for the table.
        let metadata_location =
            get_or_rebase_metadata_location(self.rel_oid, &ctx.file_io)?;

        let table_metadata =
            TableMetadata::read_from(&ctx.file_io, &metadata_location)?;
        let schema = table_metadata.current_schema().clone();

        let table = Table::builder()
            .file_io(ctx.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(table_metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new(self.nsp_name.clone()),
                self.rel_name.clone(),
            ))
            .build()?;

        let table_scan = table.scan().select_all().build()?;
        Ok((table_scan.to_arrow()?, schema))
    }

    fn next_batch(&mut self) -> IcebergResult<Option<RecordBatch>> {
        let Some(iterator) = self.batch_iterator.as_mut() else {
            return Ok(None);
        };

        iterator.next().transpose().map_err(IcebergError::from)
    }
}
