//! Direct Datum/slot adapters for Avro COPY FROM and COPY TO.

use std::panic::AssertUnwindSafe;

use apache_avro::types::Value;
use apache_avro::{Reader, Schema};
use lagodb_core::copy::{
    CopyColumnLayout, CopyDatumCoercion, CopyDatumSource, CopyError, CopyInputRow,
    CopyOutputRow, CopyRowOutcome, CopyTupleDestination,
};
use lagodb_core::diag::PgReportError;
use pgrx::PgTryBuilder;

use crate::error::ConnectorError;
use crate::format::copy::{
    FormatCopyDestination, FormatCopyInput, FormatCopyOutput, FormatCopySource,
};
use crate::format::{AvroWriteCompression, FormatKind};
use crate::storage::{ObjectFiles, ObjectOutput, ReadProgress};

use super::read::{AvroObjectReader, AvroReadColumn};
use super::write::{AvroDatumRow, AvroObjectWriter, AvroWritePlan};

struct CopyReadColumn {
    reader: AvroReadColumn,
    coercion: CopyDatumCoercion,
}

/// Avro-to-Datum source for PostgreSQL COPY FROM.
pub(in crate::format) struct AvroCopySource {
    reader: Reader<'static, AvroObjectReader>,
    columns: Box<[CopyReadColumn]>,
    logical_row: u64,
    progress: ReadProgress,
}

impl AvroCopySource {
    pub(in crate::format) fn new(
        mut files: ObjectFiles,
        layout: &CopyColumnLayout,
    ) -> Result<Self, CopyError> {
        let first = files.next().expect(
            "Avro COPY FROM resolves one exact object before opening its source",
        );
        let (object, progress) = AvroObjectReader::with_progress(first?);
        let reader = Reader::new(object).map_err(ConnectorError::from)?;
        let schema = reader.writer_schema().clone();
        let Schema::Record(record) = &schema else {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "the Avro container writer schema must be a record",
            )
            .into());
        };
        let mut columns = Vec::with_capacity(layout.len());
        let bind = PgTryBuilder::new(AssertUnwindSafe(|| {
            for column in layout.columns() {
                let name = column.name().to_str().map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "COPY column names must be valid UTF-8 for Avro",
                    )
                })?;
                let source = record
                    .fields
                    .iter()
                    .position(|field| field.name == name)
                    .ok_or_else(|| {
                        ConnectorError::invalid_object_schema(
                            FormatKind::Avro,
                            format!("COPY target column {name:?} is missing from the Avro schema"),
                        )
                    })?;
                // Decode according to the writer schema, then let PostgreSQL
                // assign the result to the target typmod. COPY does not require
                // numeric precision/scale to match as a foreign scan does.
                let reader = AvroReadColumn::bind(
                    source,
                    &record.fields[source].schema,
                    column.type_oid(),
                    -1,
                )?;
                let coercion = CopyDatumCoercion::bind(
                    column.type_oid(),
                    reader.datum_typmod(),
                    column.type_mod(),
                )
                .map_err(PgReportError::from_pg_error)?;
                columns.push(CopyReadColumn { reader, coercion });
            }
            Ok::<(), ConnectorError>(())
        }))
        .catch_others(|error| Err(ConnectorError::Postgres(PgReportError::from_caught(error))))
        .execute();
        bind.map_err(CopyError::from)?;
        Ok(Self {
            reader,
            columns: columns.into_boxed_slice(),
            logical_row: 0,
            progress,
        })
    }
}

impl CopyDatumSource for AvroCopySource {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
        if layout.len() != self.columns.len() {
            return Err(CopyError::invalid_column_layout(
                "Avro source was bound to a different COPY layout",
            ));
        }
        Ok(())
    }

    fn next_row(
        &mut self,
        row: CopyInputRow<'_>,
    ) -> Result<CopyRowOutcome, CopyError> {
        let Some(value) = self.reader.next() else {
            return Ok(CopyRowOutcome::End);
        };
        let value = value.map_err(ConnectorError::from)?;
        let Value::Record(fields) = value else {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "the Avro container yielded a non-record value",
            )
            .into());
        };
        let mut active_column = 0;
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                for (column_index, (target, column)) in
                    row.columns().zip(self.columns.iter_mut()).enumerate()
                {
                    active_column = column_index;
                    // SAFETY: bind resolved this index against the exact writer
                    // schema and the decoded record remains live for this call.
                    let source = fields.get_unchecked(column.reader.source());
                    let value = column.reader.datum(&source.1)?;
                    target.set(column.coercion.apply(value));
                }
                Ok::<(), ConnectorError>(())
            }))
            .catch_others(|error| {
                Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
            })
            .execute()
        };
        self.logical_row += 1;
        match result {
            Ok(()) => Ok(CopyRowOutcome::Row),
            Err(error) => error
                .into_copy_row_rejection(
                    Some(active_column),
                    format!("Avro record {}", self.logical_row),
                )
                .map(CopyRowOutcome::Rejected),
        }
    }

    fn bytes_consumed(&self) -> u64 {
        self.progress.bytes()
    }
}

impl FormatCopySource for AvroCopySource {
    fn input(&mut self) -> FormatCopyInput<'_> {
        FormatCopyInput::Datums(self)
    }
}

struct ReadyAvroCopyDestination {
    row: AvroDatumRow,
    writer: AvroObjectWriter,
}

/// Tuple-slot-to-Avro destination for PostgreSQL COPY TO.
pub(in crate::format) struct AvroCopyDestination {
    output: Option<ObjectOutput>,
    compression: AvroWriteCompression,
    ready: Option<ReadyAvroCopyDestination>,
}

impl AvroCopyDestination {
    pub(in crate::format) fn new(
        output: ObjectOutput,
        compression: AvroWriteCompression,
    ) -> Self {
        Self {
            output: Some(output),
            compression,
            ready: None,
        }
    }

    pub(in crate::format) fn finish(mut self) -> Result<(), CopyError> {
        self.ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows")
            .writer
            .finish(true)
            .map_err(CopyError::from)
    }

    fn initialize_inner(
        &mut self,
        layout: &CopyColumnLayout,
    ) -> Result<(), CopyError> {
        let fields =
            layout.columns().iter().map(|column| {
                column.name().to_str().map(|name| {
                (name, column.type_oid(), column.type_mod())
            }).map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Avro,
                    "COPY TO output column names must be valid UTF-8 for Avro",
                )
            })
            });
        let plan = AvroWritePlan::from_copy_columns(fields, layout.len())
            .map_err(CopyError::from)?;
        let output = self
            .output
            .take()
            .expect("COPY TO initializes its destination exactly once");
        self.ready = Some(ReadyAvroCopyDestination {
            row: AvroDatumRow::new(layout.len()),
            writer: AvroObjectWriter::new(output, plan, self.compression),
        });
        Ok(())
    }
}

impl CopyTupleDestination for AvroCopyDestination {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
        self.initialize_inner(layout)
    }

    fn write_slot(&mut self, row: CopyOutputRow<'_>) -> Result<(), CopyError> {
        let ready = self
            .ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows");
        for (index, datum) in row.datums().enumerate() {
            unsafe { ready.row.set_at_bound(index, datum.value()) };
        }
        ready.writer.write_row(&ready.row).map_err(CopyError::from)
    }

    fn bytes_produced(&self) -> u64 {
        self.ready
            .as_ref()
            .map_or(0, |ready| ready.writer.bytes_written())
    }

    fn finish(&mut self) -> Result<(), CopyError> {
        self.ready
            .as_mut()
            .expect("COPY TO initializes its destination before completion")
            .writer
            .finish(true)
            .map_err(CopyError::from)
    }

    fn abort(&mut self) {
        self.ready = None;
        self.output = None;
    }
}

impl FormatCopyDestination for AvroCopyDestination {
    fn output(&mut self) -> FormatCopyOutput<'_> {
        FormatCopyOutput::Tuples(self)
    }

    fn finish(self: Box<Self>) -> Result<(), CopyError> {
        let destination = *self;
        destination.finish()
    }
}
