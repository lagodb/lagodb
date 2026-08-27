//! Canonical-CSV-to-Parquet destination for PostgreSQL COPY TO.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use arrow_schema::{Field, Schema};
use lagodb_core::batch::BatchBuffer;
use lagodb_core::copy::{CopyColumnLayout, CopyDataDestination, CopyError};
use lagodb_core::diag::PgReportError;
use pg_arrow_conv::{
    BoundDatumBuffer, BoundDatumColumnPlan, PgColumnType, resolve_column_rule,
};
use pgrx::memcxt::PgMemoryContexts;
use pgrx::{PgTryBuilder, pg_sys};

use crate::error::ConnectorError;
use crate::format::{
    FormatKind, ParquetObjectWriter, ParquetWriteCompression, parquet_arrow_type,
};
use crate::storage::ObjectOutput;

use super::super::super::copy::{CanonicalCsvRow, FormatCopyDestination};

const COPY_TO_BATCH_BYTES: usize = 8 * 1024 * 1024;

struct CopyInputPlan {
    input_function: pg_sys::Oid,
    type_io_param: pg_sys::Oid,
    type_mod: i32,
}

struct ReadyParquetCopyDestination {
    columns: Box<[CopyInputPlan]>,
    buffer: BoundDatumBuffer,
    writer: ParquetObjectWriter,
}

impl ReadyParquetCopyDestination {
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
}

/// COPY TO destination initialized from PostgreSQL's actual output TupleDesc.
pub(in crate::format) struct ParquetCopyDestination {
    output: Option<ObjectOutput>,
    compression: ParquetWriteCompression,
    ready: Option<ReadyParquetCopyDestination>,
    row: CanonicalCsvRow,
    datum_context: PgMemoryContexts,
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
            row: CanonicalCsvRow::new(),
            datum_context: PgMemoryContexts::new("lagodb parquet copy to bridge"),
        }
    }

    pub(in crate::format) fn finish(mut self) -> Result<(), CopyError> {
        self.ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows")
            .finish()
            .map_err(CopyError::from)
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
        let mut input_plans = Vec::with_capacity(layout.len());
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
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
                    let pg = PgColumnType::from_pg_type(column.type_oid())
                        .ok_or_else(|| {
                            ConnectorError::invalid_object_schema(
                                FormatKind::Parquet,
                                format!(
                                    "PostgreSQL type OID {} has no Arrow conversion",
                                    column.type_oid()
                                ),
                            )
                        })?;
                    let rule = resolve_column_rule(&data_type, pg)?;
                    datum_plans
                        .push(BoundDatumColumnPlan::bind(rule, column.type_oid())?);

                    let mut input_function = pg_sys::InvalidOid;
                    let mut type_io_param = pg_sys::InvalidOid;
                    pg_sys::getTypeInputInfo(
                        column.type_oid(),
                        &mut input_function,
                        &mut type_io_param,
                    );
                    input_plans.push(CopyInputPlan {
                        input_function,
                        type_io_param,
                        type_mod: column.type_mod(),
                    });
                    fields.push(Field::new(name, data_type, true));
                }
                Ok::<(), ConnectorError>(())
            }))
            .catch_others(|error| {
                Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
            })
            .execute()
        };
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
            columns: input_plans.into_boxed_slice(),
            buffer,
            writer: ParquetObjectWriter::new(output, schema, self.compression),
        });
        Ok(())
    }
}

impl CopyDataDestination for ParquetCopyDestination {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
        self.initialize_inner(layout)
    }

    fn write_row(&mut self, data: &[u8]) -> Result<(), CopyError> {
        let ready = self
            .ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows");
        self.row.parse(data, ready.columns.len())?;

        unsafe { self.datum_context.reset() };
        let datum_context = self.datum_context.value();
        unsafe {
            PgMemoryContexts::For(datum_context).switch_to(|_| {
                let values = self.row.fields().zip(ready.columns.iter()).map(
                    |(field, plan)| {
                        field.map(|value| {
                            // Intentional CSV bridge; see canonical_csv's accepted
                            // native-format performance trade-off.
                            pg_sys::OidInputFunctionCall(
                                plan.input_function,
                                value.as_ptr().cast_mut(),
                                plan.type_io_param,
                                plan.type_mod,
                            )
                        })
                    },
                );
                ready.buffer.append_row_unchecked(values)?;
                if ready.buffer.should_flush(COPY_TO_BATCH_BYTES) {
                    ready.flush_batch()?;
                }
                Ok::<(), ConnectorError>(())
            })
        }
        .map_err(CopyError::from)
    }
}

impl FormatCopyDestination for ParquetCopyDestination {
    fn destination(&mut self) -> &mut dyn CopyDataDestination {
        self
    }

    fn finish(self: Box<Self>) -> Result<(), CopyError> {
        let destination = *self;
        destination.finish()
    }
}
