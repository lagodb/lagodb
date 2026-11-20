//! Parquet-to-Datum source for PostgreSQL COPY FROM.

use lagodb_arrow::BoundBatch;
use lagodb_core::copy::{
    CopyColumnLayout, CopyDatumCoercion, CopyDatumSource, CopyError, CopyInputRow,
    CopyRowOutcome,
};
use lagodb_core::datum::PgType;

use crate::error::ConnectorError;
use crate::format::FormatKind;
use crate::format::parquet::{
    ParquetBatchSource, ParquetReadMode, ParquetReadPlan, ParquetTypeMapping,
};
use crate::storage::ResolvedStorageLocation;

/// Parquet-to-Datum source for PostgreSQL COPY FROM over one object or prefix.
pub(in crate::copy) struct ParquetCopySource {
    source: ParquetBatchSource,
    coercions: Box<[CopyDatumCoercion]>,
    batch: Option<BoundBatch>,
    row: usize,
    logical_row: u64,
}

impl ParquetCopySource {
    pub(in crate::copy) fn open(
        location: &ResolvedStorageLocation,
        layout: &CopyColumnLayout,
    ) -> Result<Self, CopyError> {
        let files = FormatKind::Parquet.input(location)?.open();
        let mut coercions = Vec::with_capacity(layout.len());
        let source = ParquetBatchSource::open(files, Box::new([]), |schema| {
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
                coercions.push(CopyDatumCoercion::bind(
                    column.type_oid(),
                    source_typmod,
                    column.type_mod(),
                ));
            }
            Ok(plan)
        })?;
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
        let current = self.row;
        self.row += 1;
        self.logical_row += 1;
        for (index, ((target, coercion), column)) in row
            .columns()
            .zip(self.coercions.iter_mut())
            .zip(batch.columns())
            .enumerate()
        {
            // SAFETY: the batch is non-empty and the row was checked above.
            // COPY supplies the bound targets and current row context.
            let value = match unsafe { column.read_datum_unchecked(current) } {
                Ok(value) => value,
                Err(error) => {
                    return ConnectorError::from(error)
                        .into_copy_row_rejection(
                            Some(index),
                            format!("Parquet row {}", self.logical_row),
                        )
                        .map(CopyRowOutcome::Rejected);
                }
            };
            // PostgreSQL typmod coercion errors unwind to the COPY boundary.
            target.set(unsafe { coercion.apply(value) });
        }
        Ok(CopyRowOutcome::Row)
    }

    fn bytes_consumed(&self) -> u64 {
        self.source.bytes_consumed()
    }
}
