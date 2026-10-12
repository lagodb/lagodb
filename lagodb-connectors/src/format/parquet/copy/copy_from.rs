//! Parquet-to-Datum source for PostgreSQL COPY FROM.

use std::panic::AssertUnwindSafe;

use lagodb_arrow::BoundBatch;
use lagodb_core::copy::{
    CopyColumnLayout, CopyDatumCoercion, CopyDatumSource, CopyError, CopyInputRow,
    CopyRowOutcome,
};
use lagodb_core::diag::PgReportError;
use lagodb_core::tuple::PgType;
use pgrx::PgTryBuilder;

use crate::error::ConnectorError;
use crate::storage::InputFiles;

use super::super::read_plan::ParquetReadPlan;
use super::super::source::ParquetBatchSource;
use super::super::type_mapping::{ParquetReadMode, ParquetTypeMapping};

pub(in crate::format) struct ParquetCopySource {
    source: ParquetBatchSource,
    coercions: Box<[CopyDatumCoercion]>,
    batch: Option<BoundBatch>,
    row: usize,
    logical_row: u64,
}

impl ParquetCopySource {
    pub(in crate::format) fn new(
        files: InputFiles,
        layout: &CopyColumnLayout,
    ) -> Result<Self, CopyError> {
        let mut coercions = Vec::with_capacity(layout.len());
        let source = PgTryBuilder::new(AssertUnwindSafe(|| {
            ParquetBatchSource::open(files, Box::new([]), |schema| {
                let plan = ParquetReadPlan::bind(
                    schema,
                    layout.columns().iter().enumerate().map(|(index, column)| {
                        (
                            column.name(),
                            PgType::new(column.type_oid(), column.type_mod()),
                            index,
                        )
                    }),
                    ParquetReadMode::Copy,
                )?;
                for column in layout.columns() {
                    let name = column
                        .name()
                        .to_str()
                        .expect("read planning validated the column name");
                    let source = schema
                        .index_of(name)
                        .expect("read planning bound every COPY column");
                    let source_typmod =
                        ParquetTypeMapping::decoded_typmod(schema.field(source));
                    coercions.push(
                        CopyDatumCoercion::bind(
                            column.type_oid(),
                            source_typmod,
                            column.type_mod(),
                        )
                        .map_err(PgReportError::from_pg_error)?,
                    );
                }
                Ok(plan)
            })
        }))
        .catch_others(|error| {
            Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
        })
        .execute()?;
        Ok(Self {
            source,
            coercions: coercions.into_boxed_slice(),
            batch: None,
            row: 0,
            logical_row: 0,
        })
    }
}

impl CopyDatumSource for ParquetCopySource {
    fn next_row(
        &mut self,
        row: CopyInputRow<'_>,
    ) -> Result<CopyRowOutcome, CopyError> {
        if self
            .batch
            .as_ref()
            .is_none_or(|batch| self.row >= batch.num_rows())
        {
            self.batch = None;
            self.batch = self.source.next_batch()?;
            self.row = 0;
        }
        let Some(batch) = self.batch.as_ref() else {
            return Ok(CopyRowOutcome::End);
        };
        let mut active_column = 0;
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                for (index, ((target, coercion), column)) in row
                    .columns()
                    .zip(self.coercions.iter_mut())
                    .zip(batch.columns())
                    .enumerate()
                {
                    active_column = index;
                    // SAFETY: the batch is non-empty and row was checked above.
                    // COPY supplies the bound targets and current row context.
                    let value = column.read_datum_unchecked(self.row)?;
                    target.set(coercion.apply(value));
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
        self.source.bytes_consumed()
    }
}
