//! Tuple-slot-to-Parquet destination for PostgreSQL COPY TO.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use lagodb_arrow::BoundDatumBuffer;
use lagodb_core::batch::BatchBuffer;
use lagodb_core::copy::{
    CopyColumnLayout, CopyError, CopyOutputRow, CopyTupleDestination,
};
use lagodb_core::datum::PgType;
use lagodb_core::diag::PgReportError;
use pgrx::PgTryBuilder;

use crate::error::ConnectorError;
use crate::format::{FormatKind, ParquetWriteCompression};
use crate::storage::StorageOutput;

use super::super::super::copy::{FormatCopyDestination, FormatCopyOutput};
use super::super::output_writer::ParquetOutputWriter;
use super::super::write_plan::ParquetWritePlan;

const COPY_TO_BATCH_BYTES: usize = 8 * 1024 * 1024;

struct ReadyParquetCopyDestination {
    buffer: BoundDatumBuffer,
    writer: ParquetOutputWriter,
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
    output: Option<StorageOutput>,
    compression: ParquetWriteCompression,
    ready: Option<ReadyParquetCopyDestination>,
}

impl ParquetCopyDestination {
    pub(in crate::format) fn new(
        output: StorageOutput,
        compression: ParquetWriteCompression,
    ) -> Self {
        Self {
            output: Some(output),
            compression,
            ready: None,
        }
    }
}

impl CopyTupleDestination for ParquetCopyDestination {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
        if layout.is_empty() {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "Parquet COPY TO requires at least one output column",
            )
            .into());
        }
        let plan = PgTryBuilder::new(AssertUnwindSafe(|| {
            ParquetWritePlan::bind(layout.columns().iter().enumerate().map(
                |(index, column)| {
                    (
                        column.name(),
                        PgType::new(column.type_oid(), column.type_mod()),
                        index,
                    )
                },
            ))
        }))
        .catch_others(|error| {
            Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
        })
        .execute()?;
        let schema = Arc::clone(plan.schema());
        let buffer = plan.into_datum_buffer()?;
        let output = self
            .output
            .take()
            .expect("COPY TO initializes its destination exactly once");
        self.ready = Some(ReadyParquetCopyDestination {
            buffer,
            writer: ParquetOutputWriter::new(output, schema, self.compression)?,
        });
        Ok(())
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
        self.ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows")
            .finish()
            .map_err(CopyError::from)?;
        Ok(())
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
}
