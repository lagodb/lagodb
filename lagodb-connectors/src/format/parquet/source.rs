//! Synchronous file-set reader shared by scans, sampling and COPY FROM.

use std::sync::Arc;

use arrow_schema::Schema;
use lagodb_arrow::BoundBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};

use crate::error::ConnectorError;
use crate::format::FormatKind;
use crate::storage::{InputFiles, ReadProgress};

use super::read_plan::ParquetReadPlan;
use super::reader::ParquetObjectReader;
use super::{ParquetBoundPredicate, ParquetFilePredicate};

const PARQUET_BATCH_SIZE: usize = 8_192;

struct SourceBinding {
    schema: Arc<Schema>,
    plan: ParquetReadPlan,
}

pub(crate) struct ParquetBatchSource {
    files: InputFiles,
    binding: Option<SourceBinding>,
    reader: Option<ParquetRecordBatchReader>,
    filters: Box<[ParquetBoundPredicate]>,
    completed_bytes: u64,
    progress: Option<ReadProgress>,
}

impl ParquetBatchSource {
    pub(crate) fn open(
        mut files: InputFiles,
        filters: Box<[ParquetBoundPredicate]>,
        bind: impl FnOnce(&Schema) -> Result<ParquetReadPlan, ConnectorError>,
    ) -> Result<Self, ConnectorError> {
        let Some(first) = files.next() else {
            return Ok(Self {
                files,
                binding: None,
                reader: None,
                filters,
                completed_bytes: 0,
                progress: None,
            });
        };
        let (object, progress) = ParquetObjectReader::with_progress(first?);
        let builder = ParquetRecordBatchReaderBuilder::try_new(object)?;
        let schema = Arc::clone(builder.schema());
        let plan = bind(&schema)?;
        let reader = Self::build_reader(builder, &plan, &filters)?;
        Ok(Self {
            files,
            binding: Some(SourceBinding { schema, plan }),
            reader: Some(reader),
            filters,
            completed_bytes: 0,
            progress: Some(progress),
        })
    }

    fn build_reader(
        builder: ParquetRecordBatchReaderBuilder<ParquetObjectReader>,
        plan: &ParquetReadPlan,
        filters: &[ParquetBoundPredicate],
    ) -> Result<ParquetRecordBatchReader, ConnectorError> {
        let projection = ProjectionMask::roots(
            builder.parquet_schema(),
            plan.roots().iter().copied(),
        );
        let mut builder = builder.with_projection(projection);
        if !filters.is_empty() {
            let predicate = ParquetFilePredicate::try_new(
                filters,
                builder.parquet_schema(),
                builder.schema(),
            )?;
            let selected = predicate.selected_row_groups(builder.metadata());
            builder = builder.with_row_groups(selected);
            builder = builder.with_row_filter(predicate.into_row_filter());
        }
        Ok(builder.with_batch_size(PARQUET_BATCH_SIZE).build()?)
    }

    fn open_next_reader(&mut self) -> Result<bool, ConnectorError> {
        let Some(file) = self.files.next() else {
            return Ok(false);
        };
        let (object, progress) = ParquetObjectReader::with_progress(file?);
        let builder = ParquetRecordBatchReaderBuilder::try_new(object)?;
        let binding = self
            .binding
            .as_ref()
            .expect("non-empty input has a bound schema");
        if builder.schema().fields() != binding.schema.fields() {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "objects under one prefix do not share the same Arrow schema",
            ));
        }
        self.reader =
            Some(Self::build_reader(builder, &binding.plan, &self.filters)?);
        self.progress = Some(progress);
        Ok(true)
    }

    pub(crate) fn next_batch(
        &mut self,
    ) -> Result<Option<BoundBatch>, ConnectorError> {
        loop {
            if let Some(reader) = self.reader.as_mut()
                && let Some(batch) = reader.next()
            {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let binding =
                    self.binding.as_ref().expect("a reader has a bound plan");
                // SAFETY: every file's schema is checked at open; the reader
                // applies exactly this plan's root projection to every batch.
                return Ok(Some(unsafe { binding.plan.bind_batch(batch) }?));
            }
            self.close_reader();
            if !self.open_next_reader()? {
                return Ok(None);
            }
        }
    }

    pub(crate) fn reset(&mut self, filters: Option<Box<[ParquetBoundPredicate]>>) {
        self.close_reader();
        self.completed_bytes = 0;
        self.files.reset();
        if let Some(filters) = filters {
            self.filters = filters;
        }
    }

    pub(crate) fn close_reader(&mut self) {
        self.reader = None;
        if let Some(progress) = self.progress.take() {
            self.completed_bytes =
                self.completed_bytes.saturating_add(progress.bytes());
        }
    }

    pub(super) fn bytes_consumed(&self) -> u64 {
        self.completed_bytes
            .saturating_add(self.progress.as_ref().map_or(0, ReadProgress::bytes))
    }
}
