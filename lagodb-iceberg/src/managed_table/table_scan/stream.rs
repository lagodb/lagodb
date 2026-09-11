//! Run-local lazy Arrow stream for a planned Iceberg task set.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use iceberg_lite::expr::Predicate;
use iceberg_lite::scan::{
    ArrowRecordBatchIterator, FileScanTask, SharedTaskArrowReader,
};
use lagodb_arrow::query_source::{
    RuntimePredicateUpdate, ScanStreamOptions, TableScanStream,
};

use crate::error::IcebergError;

use super::{IcebergTableScanError, lifecycle::BoundIcebergScan};

enum IcebergBatchCursor {
    Pending,
    OpenAll(ArrowRecordBatchIterator),
    OpenTask(ArrowRecordBatchIterator),
    Finished,
}

enum IcebergReaderMode {
    Static {
        tasks: Arc<[FileScanTask]>,
        row_filter: Option<Predicate>,
    },
    Evolving(SharedTaskArrowReader),
}

pub(super) struct IcebergArrowStream {
    schema: SchemaRef,
    bound: Arc<BoundIcebergScan>,
    options: ScanStreamOptions,
    batch_size: usize,
    reader: IcebergReaderMode,
    predicate_generation: u64,
    cursor: IcebergBatchCursor,
}

impl IcebergArrowStream {
    pub(super) fn new(
        bound: Arc<BoundIcebergScan>,
        tasks: Arc<[FileScanTask]>,
        row_filter: Option<Predicate>,
        schema: SchemaRef,
        batch_size: usize,
        options: ScanStreamOptions,
    ) -> Result<Self, IcebergTableScanError> {
        let reader = if options.has_evolving_predicate() {
            IcebergReaderMode::Evolving(
                bound
                    .scan
                    .shared_task_arrow_reader(
                        Arc::clone(&tasks),
                        batch_size,
                        row_filter,
                    )
                    .map_err(IcebergError::from)
                    .map_err(IcebergTableScanError::from)?,
            )
        } else {
            IcebergReaderMode::Static { tasks, row_filter }
        };
        Ok(Self {
            schema,
            bound,
            options,
            batch_size,
            reader,
            predicate_generation: 0,
            cursor: IcebergBatchCursor::Pending,
        })
    }

    fn open_all(&mut self) -> Result<(), IcebergTableScanError> {
        let IcebergReaderMode::Static { tasks, row_filter } = &self.reader else {
            unreachable!("static cursor opening requires the static reader mode")
        };
        let cursor = self
            .bound
            .open_batches(Arc::clone(tasks), row_filter.clone(), self.batch_size)
            .map_err(IcebergError::from)
            .map_err(IcebergTableScanError::from)?;
        self.cursor = IcebergBatchCursor::OpenAll(cursor);
        Ok(())
    }

    fn open_next_task(&mut self) -> Result<(), IcebergTableScanError> {
        let IcebergReaderMode::Evolving(reader) = &mut self.reader else {
            unreachable!("task cursor opening requires the evolving reader mode")
        };
        if let Some(update) = self
            .options
            .runtime_predicate_update(self.predicate_generation)?
        {
            let generation = update.generation();
            let evolving = match update {
                RuntimePredicateUpdate::Replace { predicate, .. } => {
                    self.bound.plan_predicate(&predicate)?.into_predicate()
                }
                RuntimePredicateUpdate::Clear { .. } => None,
            };
            reader
                .replace_supplemental_filter(evolving)
                .map_err(IcebergError::from)?;
            self.predicate_generation = generation;
        }
        let cursor = match reader
            .read_next_task()
            .map_err(IcebergError::from)
            .map_err(IcebergTableScanError::from)
        {
            Ok(Some(cursor)) => cursor,
            Ok(None) => {
                self.cursor = IcebergBatchCursor::Finished;
                return Ok(());
            }
            Err(error) => {
                self.cursor = IcebergBatchCursor::Finished;
                return Err(error);
            }
        };
        self.cursor = IcebergBatchCursor::OpenTask(cursor);
        Ok(())
    }

    fn open_if_needed(&mut self) -> Result<(), IcebergTableScanError> {
        if !matches!(&self.cursor, IcebergBatchCursor::Pending) {
            return Ok(());
        }
        if matches!(&self.reader, IcebergReaderMode::Evolving(_)) {
            self.open_next_task()
        } else {
            self.open_all()
        }
    }
}

impl TableScanStream for IcebergArrowStream {
    type Error = IcebergTableScanError;

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error> {
        loop {
            self.open_if_needed()?;
            let next = match &mut self.cursor {
                IcebergBatchCursor::OpenAll(cursor)
                | IcebergBatchCursor::OpenTask(cursor) => cursor.next(),
                IcebergBatchCursor::Finished => return Ok(None),
                IcebergBatchCursor::Pending => {
                    unreachable!("open_if_needed resolves the pending state")
                }
            };
            match next {
                Some(Ok(batch)) => return Ok(Some(batch)),
                Some(Err(error)) => {
                    self.cursor = IcebergBatchCursor::Finished;
                    return Err(IcebergTableScanError::from(IcebergError::from(
                        error,
                    )));
                }
                None if matches!(&self.cursor, IcebergBatchCursor::OpenTask(_)) => {
                    self.cursor = IcebergBatchCursor::Pending;
                }
                None => {
                    self.cursor = IcebergBatchCursor::Finished;
                    return Ok(None);
                }
            }
        }
    }
}
