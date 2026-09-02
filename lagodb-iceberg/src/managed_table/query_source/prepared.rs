//! Begin-owned immutable managed-Iceberg source metadata.

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use iceberg_lite::scan::{FileScanTask, TableScan};

use super::IcebergArrowStream;
use crate::engine::scan::PreparedQueryScanInput;

/// Immutable scan facts shared by a prepared handle and run-local Arrow
/// streams. Each stream clones only this `Arc`, not task/schema contents.
#[derive(Debug)]
pub(super) struct PreparedIcebergScan {
    pub(super) scan: TableScan,
    pub(super) tasks: Arc<[FileScanTask]>,
    pub(super) arrow_schema: SchemaRef,
}

impl PreparedIcebergScan {
    pub(super) fn open_batches(
        &self,
        batch_size: usize,
    ) -> iceberg_lite::Result<iceberg_lite::scan::ArrowRecordBatchIterator> {
        self.scan
            .to_arrow_with_shared_tasks_and_filter_and_batch_size(
                Arc::clone(&self.tasks),
                None,
                batch_size,
            )
    }
}

/// Statement-Begin-owned source after snapshot and file-task preparation.
///
/// The retained values contain no direct PostgreSQL plan/executor node,
/// Relation, MemoryContext, Datum, or borrowed backend pointer. `TableScan`
/// owns the exact snapshot/schema/overlay reader context used to produce
/// `tasks`; its `FileIO` may encapsulate a backend-thread storage service behind
/// the private Iceberg trait adapter. Consequently this value is not an
/// independently thread-safe capability even though the upstream trait bounds
/// require it to be `Send + Sync`.
#[derive(Debug)]
pub(crate) struct PreparedIcebergSource {
    scan: Arc<PreparedIcebergScan>,
}

impl PreparedIcebergSource {
    pub(super) fn new(input: PreparedQueryScanInput) -> Self {
        let scan = PreparedIcebergScan {
            scan: input.scan,
            tasks: input.tasks,
            arrow_schema: Arc::new(Schema::empty()),
        };
        Self {
            scan: Arc::new(scan),
        }
    }

    pub(super) fn open_stream(&self, batch_size: usize) -> IcebergArrowStream {
        IcebergArrowStream::new(Arc::clone(&self.scan), batch_size)
    }
}
