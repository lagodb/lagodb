//! Run-local Arrow streams over planned Parquet splits.

use std::rc::Rc;
use std::sync::Arc;
use std::vec::IntoIter;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use lagodb_arrow::scan::{
    RuntimePredicateUpdate, ScanStreamOptions, TableScanStream,
};

use crate::format::parquet::{
    ParquetBatchSource, ParquetBoundPredicate, ParquetColumnarPlan,
    ParquetRuntimePlanner, ParquetSplit, ParquetSplits,
};
use crate::storage::StorageInput;

use super::BackendLocal;
use super::columns::ScanColumns;
use super::error::Error;
use super::scan::TaskSplit;

/// Re-plans the engine's evolving runtime predicate before each split.
pub(super) struct EvolvingFilters {
    options: ScanStreamOptions,
    generation: u64,
    /// The bound schema the runtime predicate's columns index.
    schema: SchemaRef,
    planned: Box<[ParquetBoundPredicate]>,
}

impl EvolvingFilters {
    pub(super) fn new(
        options: ScanStreamOptions,
        schema: SchemaRef,
        planned: Box<[ParquetBoundPredicate]>,
    ) -> Self {
        Self {
            options,
            generation: 0,
            schema,
            planned,
        }
    }

    /// The planned predicates plus the newest supplemental one, when the
    /// engine published a new generation.
    fn update(&mut self) -> Result<Option<Box<[ParquetBoundPredicate]>>, Error> {
        let Some(update) = self.options.runtime_predicate_update(self.generation)?
        else {
            return Ok(None);
        };
        let generation = update.generation();
        let supplemental = match update {
            RuntimePredicateUpdate::Replace { predicate, .. } => {
                ParquetRuntimePlanner::new(&self.schema)
                    .plan(&predicate)?
                    .into_predicate()
            }
            RuntimePredicateUpdate::Clear { .. } => None,
        };
        self.generation = generation;
        Ok(Some(
            self.planned.iter().cloned().chain(supplemental).collect(),
        ))
    }
}

/// Splits of the statement inventory read by one stream.
pub(super) struct TaskSplits {
    input: Rc<StorageInput>,
    splits: IntoIter<TaskSplit>,
    evolving: Option<EvolvingFilters>,
}

impl TaskSplits {
    pub(super) fn new(
        input: Rc<StorageInput>,
        splits: Vec<TaskSplit>,
        evolving: Option<EvolvingFilters>,
    ) -> Self {
        Self {
            input,
            splits: splits.into_iter(),
            evolving,
        }
    }

    fn open(&mut self, split: TaskSplit) -> Result<ParquetSplit, Error> {
        let replace_filters = match &mut self.evolving {
            Some(evolving) => evolving.update()?,
            None => None,
        };
        Ok(ParquetSplit {
            file: self.input.open_member(split.member)?,
            range: split.range,
            replace_filters,
        })
    }
}

impl ParquetSplits for TaskSplits {
    type Error = Error;

    fn next_split(&mut self) -> Option<Result<ParquetSplit, Error>> {
        let split = self.splits.next()?;
        Some(self.open(split))
    }
}

pub(super) struct Stream {
    schema: SchemaRef,
    source: BackendLocal<ParquetBatchSource<TaskSplits, ParquetColumnarPlan>>,
}

impl Stream {
    pub(super) fn open(
        columns: &ScanColumns,
        splits: TaskSplits,
        filters: Box<[ParquetBoundPredicate]>,
        batch_rows: u64,
    ) -> Result<Self, Error> {
        let batch_size = usize::try_from(batch_rows)
            .map_err(|_| Error::BatchRowLimit { value: batch_rows })?;
        let source = ParquetBatchSource::open_with_batch_size(
            splits,
            filters,
            batch_size,
            |file| columns.bind(file),
        )?;
        Ok(Self {
            schema: Arc::clone(columns.schema()),
            source: BackendLocal::new(source),
        })
    }
}

impl TableScanStream for Stream {
    type Error = Error;

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error> {
        self.source.get_mut().next_batch()
    }
}
