//! Synchronous file-set reader shared by scans, sampling, COPY FROM and
//! query offload.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use lagodb_arrow::BoundBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::ParquetMetaData;

use crate::error::ConnectorError;
use crate::format::FormatKind;
use crate::storage::{InputFile, InputFiles, ReadProgress};

use super::read_plan::ParquetReadPlan;
use super::reader::ParquetObjectReader;
use super::{ParquetBoundPredicate, ParquetFilePredicate};

const PARQUET_BATCH_SIZE: usize = 8_192;

/// One file to read, optionally restricted to the row groups it owns.
pub(crate) struct ParquetSplit {
    pub(crate) file: InputFile,
    /// A byte range owning each row group whose midpoint it contains, as
    /// parquet-mr and iceberg-lite assign split row groups.
    pub(crate) range: Option<Range<u64>>,
    /// Predicates replacing the current ones from this split on.
    pub(crate) replace_filters: Option<Box<[ParquetBoundPredicate]>>,
}

impl ParquetSplit {
    pub(crate) const fn whole(file: InputFile) -> Self {
        Self {
            file,
            range: None,
            replace_filters: None,
        }
    }
}

/// Ordered splits read by one [`ParquetBatchSource`].
pub(crate) trait ParquetSplits {
    type Error: From<ConnectorError>;

    fn next_split(&mut self) -> Option<Result<ParquetSplit, Self::Error>>;
}

impl ParquetSplits for InputFiles {
    type Error = ConnectorError;

    fn next_split(&mut self) -> Option<Result<ParquetSplit, ConnectorError>> {
        self.next().map(|file| file.map(ParquetSplit::whole))
    }
}

/// Schema-bound conversion of projected Parquet batches.
pub(crate) trait ParquetBatchBinding {
    type Batch;

    /// Root columns the reader projects, ascending.
    fn roots(&self) -> &[usize];

    /// # Safety
    ///
    /// `batch` comes from this binding's root projection of a file whose
    /// root fields equal those of the schema the binding was built from.
    unsafe fn bind_batch(
        &self,
        batch: RecordBatch,
    ) -> Result<Self::Batch, ConnectorError>;
}

impl ParquetBatchBinding for ParquetReadPlan {
    type Batch = BoundBatch;

    fn roots(&self) -> &[usize] {
        ParquetReadPlan::roots(self)
    }

    unsafe fn bind_batch(
        &self,
        batch: RecordBatch,
    ) -> Result<BoundBatch, ConnectorError> {
        Ok(unsafe { ParquetReadPlan::bind_batch(self, batch) })
    }
}

struct SourceBinding<B> {
    schema: Arc<Schema>,
    plan: B,
}

pub(crate) struct ParquetBatchSource<S = InputFiles, B = ParquetReadPlan> {
    splits: S,
    binding: Option<SourceBinding<B>>,
    reader: Option<ParquetRecordBatchReader>,
    filters: Box<[ParquetBoundPredicate]>,
    batch_size: usize,
    completed_bytes: u64,
    progress: Option<ReadProgress>,
}

impl<S: ParquetSplits, B: ParquetBatchBinding> ParquetBatchSource<S, B> {
    pub(crate) fn open(
        splits: S,
        filters: Box<[ParquetBoundPredicate]>,
        bind: impl FnOnce(&Schema) -> Result<B, ConnectorError>,
    ) -> Result<Self, S::Error> {
        Self::open_with_batch_size(splits, filters, PARQUET_BATCH_SIZE, bind)
    }

    /// Bind the plan from the first split's schema. Every later split must
    /// have the same root fields; batches are not checked again.
    pub(crate) fn open_with_batch_size(
        splits: S,
        filters: Box<[ParquetBoundPredicate]>,
        batch_size: usize,
        bind: impl FnOnce(&Schema) -> Result<B, ConnectorError>,
    ) -> Result<Self, S::Error> {
        let mut source = Self {
            splits,
            binding: None,
            reader: None,
            filters,
            batch_size,
            completed_bytes: 0,
            progress: None,
        };
        let Some(first) = source.splits.next_split() else {
            return Ok(source);
        };
        let (builder, range) = source.begin_split(first?)?;
        let schema = Arc::clone(builder.schema());
        let plan = bind(&schema)?;
        source.reader = source.build_reader(builder, plan.roots(), range)?;
        source.binding = Some(SourceBinding { schema, plan });
        Ok(source)
    }

    /// Adopt the split's predicates and open its footer.
    fn begin_split(
        &mut self,
        split: ParquetSplit,
    ) -> Result<
        (
            ParquetRecordBatchReaderBuilder<ParquetObjectReader>,
            Option<Range<u64>>,
        ),
        ConnectorError,
    > {
        let ParquetSplit {
            file,
            range,
            replace_filters,
        } = split;
        if let Some(filters) = replace_filters {
            self.filters = filters;
        }
        let (object, progress) = ParquetObjectReader::with_progress(file);
        self.progress = Some(progress);
        Ok((ParquetRecordBatchReaderBuilder::try_new(object)?, range))
    }

    /// `None` when no row group of the split can match.
    fn build_reader(
        &self,
        builder: ParquetRecordBatchReaderBuilder<ParquetObjectReader>,
        roots: &[usize],
        range: Option<Range<u64>>,
    ) -> Result<Option<ParquetRecordBatchReader>, ConnectorError> {
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
        let mut builder = builder.with_projection(projection);
        let mut row_groups =
            range.map(|range| Self::owned_row_groups(builder.metadata(), range));
        if !self.filters.is_empty() {
            let predicate = ParquetFilePredicate::try_new(
                &self.filters,
                builder.parquet_schema(),
                builder.schema(),
            )?;
            let metadata = builder.metadata();
            let selected = match row_groups {
                Some(owned) => owned
                    .into_iter()
                    .filter(|index| {
                        predicate.row_group_might_match(metadata.row_group(*index))
                    })
                    .collect(),
                None => predicate.selected_row_groups(metadata),
            };
            row_groups = Some(selected);
            if let Some(row_filter) = predicate.into_row_filter() {
                builder = builder.with_row_filter(row_filter);
            }
        }
        if let Some(row_groups) = row_groups {
            if row_groups.is_empty() {
                return Ok(None);
            }
            builder = builder.with_row_groups(row_groups);
        }
        Ok(Some(builder.with_batch_size(self.batch_size).build()?))
    }

    /// Row groups are stored sequentially after the 4-byte magic header; a
    /// range owns each row group whose midpoint it contains.
    fn owned_row_groups(metadata: &ParquetMetaData, range: Range<u64>) -> Vec<usize> {
        let mut offset = 4_u64;
        let mut owned = Vec::new();
        for (index, row_group) in metadata.row_groups().iter().enumerate() {
            let size = u64::try_from(row_group.compressed_size()).unwrap_or(0);
            if range.contains(&offset.saturating_add(size / 2)) {
                owned.push(index);
            }
            offset = offset.saturating_add(size);
        }
        owned
    }

    fn open_next_reader(&mut self) -> Result<bool, S::Error> {
        let Some(split) = self.splits.next_split() else {
            return Ok(false);
        };
        let (builder, range) = self.begin_split(split?)?;
        let binding = self
            .binding
            .as_ref()
            .expect("non-empty input has a bound schema");
        if builder.schema().fields() != binding.schema.fields() {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "objects under one prefix do not share the same Arrow schema",
            )
            .into());
        }
        self.reader = self.build_reader(builder, binding.plan.roots(), range)?;
        Ok(true)
    }

    pub(crate) fn next_batch(&mut self) -> Result<Option<B::Batch>, S::Error> {
        loop {
            if let Some(reader) = self.reader.as_mut()
                && let Some(batch) = reader.next()
            {
                let batch = batch.map_err(ConnectorError::from)?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let binding =
                    self.binding.as_ref().expect("a reader has a bound plan");
                // SAFETY: every split's root fields are checked at open; the
                // reader applies exactly this plan's root projection.
                return Ok(Some(unsafe { binding.plan.bind_batch(batch) }?));
            }
            self.close_reader();
            if !self.open_next_reader()? {
                return Ok(None);
            }
        }
    }

    pub(crate) fn close_reader(&mut self) {
        self.reader = None;
        if let Some(progress) = self.progress.take() {
            self.completed_bytes =
                self.completed_bytes.saturating_add(progress.bytes());
        }
    }

    pub(crate) fn bytes_consumed(&self) -> u64 {
        self.completed_bytes
            .saturating_add(self.progress.as_ref().map_or(0, ReadProgress::bytes))
    }
}

impl<B: ParquetBatchBinding> ParquetBatchSource<InputFiles, B> {
    pub(crate) fn reset(&mut self, filters: Option<Box<[ParquetBoundPredicate]>>) {
        self.close_reader();
        self.completed_bytes = 0;
        self.splits.reset();
        if let Some(filters) = filters {
            self.filters = filters;
        }
    }
}
