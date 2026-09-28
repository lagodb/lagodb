//! Parquet-to-Datum source for PostgreSQL COPY FROM.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use arrow_schema::Schema;
use lagodb_arrow::{ColumnReader, ColumnRule, PgColumnType, resolve_column_rule};
use lagodb_core::copy::{
    CopyColumnLayout, CopyDatumCoercion, CopyDatumSource, CopyError, CopyInputRow,
    CopyRowOutcome,
};
use lagodb_core::diag::PgReportError;
use lagodb_core::tuple::{ColumnDatumCodec, ColumnDatumTarget, numeric_typmod};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use pgrx::PgTryBuilder;

use crate::error::ConnectorError;
use crate::format::{FormatKind, ParquetObjectReader};
use crate::storage::{ObjectFiles, ReadProgress};

use super::super::super::copy::{FormatCopyInput, FormatCopySource};

const PARQUET_BATCH_SIZE: usize = 8_192;
struct CopyColumnPlan {
    source: usize,
    rule: ColumnRule,
    codec: ColumnDatumCodec,
    coercion: CopyDatumCoercion,
}

struct BoundCopyBatch {
    columns: Box<[ColumnReader]>,
    rows: usize,
}

struct CopyColumnBindings {
    projection_roots: Box<[usize]>,
    columns: Box<[CopyColumnPlan]>,
}

struct BoundParquetCopy {
    layout: CopyColumnLayout,
    expected_schema: Arc<Schema>,
    projection_roots: Box<[usize]>,
    columns: Box<[CopyColumnPlan]>,
}

enum ParquetCopyBinding {
    Empty { layout: CopyColumnLayout },
    Bound(BoundParquetCopy),
}

impl ParquetCopyBinding {
    fn layout(&self) -> &CopyColumnLayout {
        match self {
            Self::Empty { layout } => layout,
            Self::Bound(binding) => &binding.layout,
        }
    }

    fn bound(&self) -> Option<&BoundParquetCopy> {
        match self {
            Self::Empty { .. } => None,
            Self::Bound(binding) => Some(binding),
        }
    }

    fn bound_mut(&mut self) -> Option<&mut BoundParquetCopy> {
        match self {
            Self::Empty { .. } => None,
            Self::Bound(binding) => Some(binding),
        }
    }
}

pub(in crate::format) struct ParquetCopySource {
    files: ObjectFiles,
    binding: ParquetCopyBinding,
    reader: Option<ParquetRecordBatchReader>,
    batch: Option<BoundCopyBatch>,
    row: usize,
    logical_row: u64,
    completed_bytes: u64,
    current_progress: Option<ReadProgress>,
}

impl ParquetCopySource {
    pub(in crate::format) fn new(
        mut files: ObjectFiles,
        layout: &CopyColumnLayout,
    ) -> Result<Self, CopyError> {
        let Some(first) = files.next() else {
            return Ok(Self {
                files,
                binding: ParquetCopyBinding::Empty {
                    layout: layout.clone(),
                },
                reader: None,
                batch: None,
                row: 0,
                logical_row: 0,
                completed_bytes: 0,
                current_progress: None,
            });
        };
        let first = first?;
        let (object, progress) = ParquetObjectReader::with_progress(first);
        let builder = ParquetRecordBatchReaderBuilder::try_new(object)
            .map_err(ConnectorError::from)?;
        let expected_schema = builder.schema().clone();
        let bindings = Self::bind_columns(&expected_schema, layout)?;
        let reader = Self::build_reader(builder, &bindings.projection_roots)
            .map_err(CopyError::from)?;
        Ok(Self {
            files,
            binding: ParquetCopyBinding::Bound(BoundParquetCopy {
                layout: layout.clone(),
                expected_schema,
                projection_roots: bindings.projection_roots,
                columns: bindings.columns,
            }),
            reader: Some(reader),
            batch: None,
            row: 0,
            logical_row: 0,
            completed_bytes: 0,
            current_progress: Some(progress),
        })
    }

    fn bind_columns(
        schema: &Arc<Schema>,
        layout: &CopyColumnLayout,
    ) -> Result<CopyColumnBindings, CopyError> {
        let mut roots = Vec::with_capacity(layout.len());
        let mut pending = Vec::with_capacity(layout.len());
        let bind = PgTryBuilder::new(AssertUnwindSafe(|| {
            for column in layout.columns() {
                let name = column.name().to_str().map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        "COPY column names must be valid UTF-8 for Parquet",
                    )
                })?;
                let source = schema.index_of(name).map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        format!(
                            "COPY target column {:?} is missing from the Parquet schema",
                            name
                        ),
                    )
                })?;
                let pg = PgColumnType::from_pg_type(column.type_oid()).ok_or_else(|| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        format!(
                            "PostgreSQL type OID {} has no Arrow conversion",
                            column.type_oid()
                        ),
                    )
                })?;
                let rule = resolve_column_rule(schema.field(source).data_type(), pg)?;
                let codec =
                    ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(column.type_oid()))?;
                // Decimal decoding already applies its schema typmod through
                // numeric_recv. Reuse that guarantee when the target matches.
                let source_typmod = match &rule {
                    ColumnRule::Decimal128 { precision, scale } => {
                        numeric_typmod(*precision, *scale as i32)
                    }
                    _ => -1,
                };
                let coercion = CopyDatumCoercion::bind(
                    column.type_oid(),
                    source_typmod,
                    column.type_mod(),
                )
                .map_err(PgReportError::from_pg_error)?;
                roots.push(source);
                pending.push((source, rule, codec, coercion));
            }
            Ok::<(), ConnectorError>(())
        }))
        .catch_others(|error| {
            Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
        })
        .execute();
        bind.map_err(CopyError::from)?;

        roots.sort_unstable();
        roots.dedup();
        let columns = pending
            .into_iter()
            .map(|(source, rule, codec, coercion)| CopyColumnPlan {
                source: roots
                    .binary_search(&source)
                    .expect("projected Parquet source was retained"),
                rule,
                codec,
                coercion,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(CopyColumnBindings {
            projection_roots: roots.into_boxed_slice(),
            columns,
        })
    }

    fn build_reader(
        builder: ParquetRecordBatchReaderBuilder<ParquetObjectReader>,
        roots: &[usize],
    ) -> Result<ParquetRecordBatchReader, ConnectorError> {
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
        builder
            .with_projection(projection)
            .with_batch_size(PARQUET_BATCH_SIZE)
            .build()
            .map_err(ConnectorError::from)
    }

    fn open_next_reader(&mut self) -> Result<bool, ConnectorError> {
        let Some(file) = self.files.next() else {
            return Ok(false);
        };
        let (object, progress) = ParquetObjectReader::with_progress(file?);
        let builder = ParquetRecordBatchReaderBuilder::try_new(object)
            .map_err(ConnectorError::from)?;
        let binding = self
            .binding
            .bound()
            .expect("only a bound Parquet source opens object readers");
        if builder.schema().fields() != binding.expected_schema.fields() {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "objects under one prefix do not share the same Arrow schema",
            ));
        }
        self.reader = Some(Self::build_reader(builder, &binding.projection_roots)?);
        self.current_progress = Some(progress);
        Ok(true)
    }

    fn finish_current_reader(&mut self) {
        if let Some(progress) = self.current_progress.take() {
            self.completed_bytes =
                self.completed_bytes.saturating_add(progress.bytes());
        }
    }

    fn bind_batch(
        &self,
        batch: arrow_array::RecordBatch,
    ) -> Result<BoundCopyBatch, ConnectorError> {
        let binding = self
            .binding
            .bound()
            .expect("only a bound Parquet source binds record batches");
        let columns = binding
            .columns
            .iter()
            .map(|plan| {
                ColumnReader::bind(&plan.rule, batch.column(plan.source).as_ref())
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(ConnectorError::from)?;
        Ok(BoundCopyBatch {
            columns: columns.into_boxed_slice(),
            rows: batch.num_rows(),
        })
    }

    fn next_batch(&mut self) -> Result<bool, ConnectorError> {
        loop {
            if let Some(reader) = self.reader.as_mut()
                && let Some(batch) = reader.next()
            {
                self.batch = Some(self.bind_batch(batch?)?);
                self.row = 0;
                return Ok(true);
            }
            self.reader = None;
            self.finish_current_reader();
            if !self.open_next_reader()? {
                return Ok(false);
            }
        }
    }
}

impl CopyDatumSource for ParquetCopySource {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
        if layout != self.binding.layout() {
            return Err(CopyError::invalid_column_layout(
                "Parquet source was bound to a different COPY layout",
            ));
        }
        Ok(())
    }

    fn next_row(
        &mut self,
        row: CopyInputRow<'_>,
    ) -> Result<CopyRowOutcome, CopyError> {
        if matches!(&self.binding, ParquetCopyBinding::Empty { .. }) {
            return Ok(CopyRowOutcome::End);
        }
        if self
            .batch
            .as_ref()
            .is_none_or(|batch| self.row >= batch.rows)
        {
            self.batch = None;
            if !self.next_batch().map_err(CopyError::from)? {
                return Ok(CopyRowOutcome::End);
            }
        }
        let mut active_column = 0;
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                let batch = self.batch.as_ref().expect("batch was loaded");
                let binding = self
                    .binding
                    .bound_mut()
                    .expect("a loaded Parquet batch has bound columns");
                for (column_index, ((target, plan), column)) in row
                    .columns()
                    .zip(binding.columns.iter_mut())
                    .zip(batch.columns.iter())
                    .enumerate()
                {
                    active_column = column_index;
                    let value = column.read_datum_unchecked(self.row, plan.codec)?;
                    target.set(plan.coercion.apply(value));
                }
                Ok::<(), ConnectorError>(())
            }))
            .catch_others(|error| {
                Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
            })
            .execute()
        };
        self.row += 1;
        self.logical_row += 1;
        match result {
            Ok(()) => Ok(CopyRowOutcome::Row),
            Err(error) => error
                .into_copy_row_rejection(
                    Some(active_column),
                    format!("Parquet row {}", self.logical_row),
                )
                .map(CopyRowOutcome::Rejected),
        }
    }

    fn bytes_consumed(&self) -> u64 {
        self.completed_bytes.saturating_add(
            self.current_progress
                .as_ref()
                .map_or(0, ReadProgress::bytes),
        )
    }
}

impl FormatCopySource for ParquetCopySource {
    fn input(&mut self) -> FormatCopyInput<'_> {
        FormatCopyInput::Datums(self)
    }
}
