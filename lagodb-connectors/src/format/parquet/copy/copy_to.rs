//! Tuple-slot-to-Parquet destination for PostgreSQL COPY TO.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use arrow_schema::{Field, Schema};
use lagodb_arrow::{
    BoundDatumBuffer, BoundDatumColumnPlan, PgColumnType, resolve_column_rule,
};
use lagodb_core::batch::BatchBuffer;
use lagodb_core::copy::{
    CopyColumnLayout, CopyError, CopyOutputRow, CopyTupleDestination,
};
use lagodb_core::diag::PgReportError;
use pgrx::PgTryBuilder;

use crate::error::ConnectorError;
use crate::format::{
    FormatKind, ParquetObjectWriter, ParquetWriteCompression, parquet_arrow_type,
};
use crate::storage::ObjectOutput;

use super::super::super::copy::{FormatCopyDestination, FormatCopyOutput};

const COPY_TO_BATCH_BYTES: usize = 8 * 1024 * 1024;

struct ReadyParquetCopyDestination {
    buffer: BoundDatumBuffer,
    writer: ParquetObjectWriter,
}

impl ReadyParquetCopyDestination {
    fn append_row(&mut self, row: CopyOutputRow<'_>) -> Result<(), ConnectorError> {
        // SAFETY: the buffer plans and row layout were bound from the same
        // final COPY output descriptor during destination initialization.
        unsafe {
            self.buffer
                .append_row_unchecked(row.datums().map(|datum| datum.value()))?;
        }
        if self.buffer.should_flush(COPY_TO_BATCH_BYTES) {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<(), ConnectorError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = self.buffer.finish_batch()?;
        self.writer.write_batch(&batch)
    }

    fn finish(&mut self) -> Result<(), ConnectorError> {
        self.flush_batch()?;
        self.writer.finish(true)
    }

    fn bytes_written(&self) -> u64 {
        self.writer.bytes_written()
    }
}

/// COPY TO destination initialized from PostgreSQL's actual output TupleDesc.
pub(in crate::format) struct ParquetCopyDestination {
    output: Option<ObjectOutput>,
    compression: ParquetWriteCompression,
    ready: Option<ReadyParquetCopyDestination>,
    completed: bool,
}

impl ParquetCopyDestination {
    pub(in crate::format) fn new(
        output: ObjectOutput,
        compression: ParquetWriteCompression,
    ) -> Self {
        Self {
            output: Some(output),
            compression,
            ready: None,
            completed: false,
        }
    }

    pub(in crate::format) fn finish(mut self) -> Result<(), CopyError> {
        self.finish_inner()
    }

    fn finish_inner(&mut self) -> Result<(), CopyError> {
        if self.completed {
            return Ok(());
        }
        self.ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows")
            .finish()
            .map_err(CopyError::from)?;
        self.completed = true;
        Ok(())
    }

    fn initialize_inner(
        &mut self,
        layout: &CopyColumnLayout,
    ) -> Result<(), CopyError> {
        if layout.is_empty() {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "Parquet COPY TO requires at least one output column",
            )
            .into());
        }
        let mut fields = Vec::with_capacity(layout.len());
        let mut datum_plans = Vec::with_capacity(layout.len());
        let result = PgTryBuilder::new(AssertUnwindSafe(|| {
            for column in layout.columns() {
                let name = column.name().to_str().map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        "COPY column names must be valid UTF-8 for Parquet",
                    )
                })?;
                if fields.iter().any(|field: &Field| field.name() == name) {
                    return Err(ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        "Parquet COPY TO output column names must be unique",
                    ));
                }
                let data_type =
                    parquet_arrow_type(column.type_oid(), column.type_mod())?;
                let pg = PgColumnType::from_pg_type(column.type_oid()).ok_or_else(
                    || {
                        ConnectorError::invalid_object_schema(
                            FormatKind::Parquet,
                            format!(
                                "PostgreSQL type OID {} has no Arrow conversion",
                                column.type_oid()
                            ),
                        )
                    },
                )?;
                let rule = resolve_column_rule(&data_type, pg)?;
                datum_plans
                    .push(BoundDatumColumnPlan::bind(rule, column.type_oid())?);

                fields.push(Field::new(name, data_type, true));
            }
            Ok::<(), ConnectorError>(())
        }))
        .catch_others(|error| {
            Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
        })
        .execute();
        result.map_err(CopyError::from)?;

        let schema = Arc::new(Schema::new(fields));
        let buffer = BoundDatumBuffer::new(
            Arc::clone(&schema),
            datum_plans.into_boxed_slice(),
        )
        .map_err(ConnectorError::from)?;
        let output = self
            .output
            .take()
            .expect("COPY TO initializes its destination exactly once");
        self.ready = Some(ReadyParquetCopyDestination {
            buffer,
            writer: ParquetObjectWriter::new(output, schema, self.compression),
        });
        Ok(())
    }
}

impl CopyTupleDestination for ParquetCopyDestination {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
        self.initialize_inner(layout)
    }

    fn write_slot(&mut self, row: CopyOutputRow<'_>) -> Result<(), CopyError> {
        let ready = self
            .ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows");
        ready.append_row(row).map_err(CopyError::from)
    }

    fn bytes_produced(&self) -> u64 {
        self.ready
            .as_ref()
            .map_or(0, ReadyParquetCopyDestination::bytes_written)
    }

    fn finish(&mut self) -> Result<(), CopyError> {
        self.finish_inner()
    }

    fn abort(&mut self) {
        self.ready = None;
        self.output = None;
    }
}

impl FormatCopyDestination for ParquetCopyDestination {
    fn output(&mut self) -> FormatCopyOutput<'_> {
        FormatCopyOutput::Tuples(self)
    }

    fn finish(self: Box<Self>) -> Result<(), CopyError> {
        let destination = *self;
        destination.finish()
    }
}
