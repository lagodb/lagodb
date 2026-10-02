//! Run-local lazy Arrow stream for a planned Iceberg task set.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use iceberg_lite::expr::Predicate;
use iceberg_lite::scan::{
    ArrowRecordBatchIterator, FileScanTask, SharedTaskArrowReader,
};
use lagodb_arrow::scan::{
    RuntimePredicateUpdate, ScanStreamOptions, TableScanStream,
};

use crate::error::IcebergError;
use crate::scan::ScanError;

use super::lifecycle::StatementScan;

enum BatchCursor {
    Pending,
    OpenAll(ArrowRecordBatchIterator),
    OpenTask(ArrowRecordBatchIterator),
    Finished,
}

enum ReaderMode {
    Static {
        tasks: Arc<[FileScanTask]>,
        row_filter: Option<Predicate>,
    },
    Evolving(SharedTaskArrowReader),
}

pub(crate) struct ArrowStream {
    schema: SchemaRef,
    bound: Arc<StatementScan>,
    options: ScanStreamOptions,
    batch_size: usize,
    reader: ReaderMode,
    predicate_generation: u64,
    cursor: BatchCursor,
}

impl ArrowStream {
    pub(super) fn new(
        bound: Arc<StatementScan>,
        tasks: Arc<[FileScanTask]>,
        row_filter: Option<Predicate>,
        schema: SchemaRef,
        batch_size: usize,
        options: ScanStreamOptions,
    ) -> Result<Self, ScanError> {
        let reader = if options.has_evolving_predicate() {
            ReaderMode::Evolving(
                bound
                    .scan
                    .shared_task_arrow_reader(
                        Arc::clone(&tasks),
                        batch_size,
                        row_filter,
                    )
                    .map_err(IcebergError::from)
                    .map_err(ScanError::from)?,
            )
        } else {
            ReaderMode::Static { tasks, row_filter }
        };
        Ok(Self {
            schema,
            bound,
            options,
            batch_size,
            reader,
            predicate_generation: 0,
            cursor: BatchCursor::Pending,
        })
    }

    fn open_all(&mut self) -> Result<(), ScanError> {
        let ReaderMode::Static { tasks, row_filter } = &self.reader else {
            unreachable!("static cursor opening requires the static reader mode")
        };
        let cursor = self
            .bound
            .open_batches(Arc::clone(tasks), row_filter.clone(), self.batch_size)
            .map_err(IcebergError::from)
            .map_err(ScanError::from)?;
        self.cursor = BatchCursor::OpenAll(cursor);
        Ok(())
    }

    fn open_next_task(&mut self) -> Result<(), ScanError> {
        let ReaderMode::Evolving(reader) = &mut self.reader else {
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
            .map_err(ScanError::from)
        {
            Ok(Some(cursor)) => cursor,
            Ok(None) => {
                self.cursor = BatchCursor::Finished;
                return Ok(());
            }
            Err(error) => {
                self.cursor = BatchCursor::Finished;
                return Err(error);
            }
        };
        self.cursor = BatchCursor::OpenTask(cursor);
        Ok(())
    }

    fn open_if_needed(&mut self) -> Result<(), ScanError> {
        if !matches!(&self.cursor, BatchCursor::Pending) {
            return Ok(());
        }
        if matches!(&self.reader, ReaderMode::Evolving(_)) {
            self.open_next_task()
        } else {
            self.open_all()
        }
    }
}

impl TableScanStream for ArrowStream {
    type Error = ScanError;

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error> {
        loop {
            self.open_if_needed()?;
            let next = match &mut self.cursor {
                BatchCursor::OpenAll(cursor) | BatchCursor::OpenTask(cursor) => {
                    cursor.next()
                }
                BatchCursor::Finished => return Ok(None),
                BatchCursor::Pending => {
                    unreachable!("open_if_needed resolves the pending state")
                }
            };
            match next {
                Some(Ok(batch)) => return Ok(Some(batch)),
                Some(Err(error)) => {
                    self.cursor = BatchCursor::Finished;
                    return Err(ScanError::from(IcebergError::from(error)));
                }
                None if matches!(&self.cursor, BatchCursor::OpenTask(_)) => {
                    self.cursor = BatchCursor::Pending;
                }
                None => {
                    self.cursor = BatchCursor::Finished;
                    return Ok(None);
                }
            }
        }
    }
}
