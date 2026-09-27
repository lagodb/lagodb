//! Direct Datum/slot adapters for NDJSON COPY FROM and COPY TO.

use std::panic::AssertUnwindSafe;

use lagodb_core::copy::{
    CopyColumnLayout, CopyDatumSource, CopyError, CopyInputRow, CopyOutputRow,
    CopyRowOutcome, CopyTupleDestination,
};
use lagodb_core::diag::PgReportError;
use lagodb_core::tuple::BoundJsonObjectEncoder;
use pgrx::PgTryBuilder;

use crate::error::ConnectorError;
use crate::format::json::{
    JsonColumnPlan, JsonInputValue, JsonRecordDecoder, JsonRecordStream,
};
use crate::format::{
    EmptyOutputPolicy, FormatKind, ObjectSetWriter, StreamCompression,
    StreamEncoderFactory, StreamFormat,
};
use crate::gucs::ReadConfig;
use crate::storage::{ObjectFiles, ObjectOutput};

use super::{
    FormatCopyDestination, FormatCopyInput, FormatCopyOutput, FormatCopySource,
};

/// NDJSON-to-Datum source for PostgreSQL COPY FROM.
pub(super) struct JsonCopySource {
    stream: JsonRecordStream,
    plan: JsonColumnPlan,
    decoder: JsonRecordDecoder,
}

impl JsonCopySource {
    pub(super) fn new(
        files: ObjectFiles,
        layout: &CopyColumnLayout,
        compression: StreamCompression,
    ) -> Result<Self, CopyError> {
        let plan = JsonColumnPlan::bind(
            layout
                .columns()
                .iter()
                .map(|column| {
                    let name = column.name().to_str().map_err(|_| {
                        ConnectorError::invalid_object_schema(
                            FormatKind::Json,
                            "COPY column names must be valid UTF-8 for JSON",
                        )
                    })?;
                    Ok((name, column.type_oid(), column.type_mod()))
                })
                .collect::<Result<Vec<_>, ConnectorError>>()?,
        )?;
        let max_record_bytes = ReadConfig::from_guc().json_max_record_bytes();
        Ok(Self {
            stream: JsonRecordStream::with_progress(
                files,
                compression,
                max_record_bytes,
            ),
            decoder: JsonRecordDecoder::new(plan.len()),
            plan,
        })
    }
}

impl CopyDatumSource for JsonCopySource {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
        if layout.len() != self.plan.len() {
            return Err(CopyError::invalid_column_layout(
                "JSON source was bound to a different COPY layout",
            ));
        }
        Ok(())
    }

    fn next_row(
        &mut self,
        row: CopyInputRow<'_>,
    ) -> Result<CopyRowOutcome, CopyError> {
        let (logical_line, record) = match self.stream.next_record() {
            Ok(Some(record)) => record,
            Ok(None) => return Ok(CopyRowOutcome::End),
            Err(error @ ConnectorError::JsonRecordTooLarge { line, .. }) => {
                return error
                    .into_copy_row_rejection(
                        None,
                        format!("NDJSON logical line {line}"),
                    )
                    .map(CopyRowOutcome::Rejected);
            }
            Err(error) => return Err(error.into()),
        };
        if let Err(error) = self.decoder.decode(&self.plan, record, logical_line) {
            return error
                .into_copy_row_rejection(
                    None,
                    format!("NDJSON logical line {logical_line}"),
                )
                .map(CopyRowOutcome::Rejected);
        }
        let mut active_column = 0;
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                for (index, (target, column)) in
                    row.columns().zip(self.plan.columns().iter()).enumerate()
                {
                    active_column = index;
                    let datum = match self.decoder.value(
                        record,
                        column,
                        index,
                        logical_line,
                    )? {
                        JsonInputValue::Null => None,
                        JsonInputValue::Bytes(value) => {
                            Some(column.input_bytes_datum(value))
                        }
                        JsonInputValue::CStr(value) => {
                            Some(column.input_datum(value))
                        }
                    };
                    target.set(datum);
                }
                Ok::<(), ConnectorError>(())
            }))
            .catch_others(|error| {
                Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
            })
            .execute()
        };
        match result {
            Ok(()) => Ok(CopyRowOutcome::Row),
            Err(error) => error
                .into_copy_row_rejection(
                    Some(active_column),
                    format!("NDJSON logical line {logical_line}"),
                )
                .map(CopyRowOutcome::Rejected),
        }
    }

    fn bytes_consumed(&self) -> u64 {
        self.stream.bytes_consumed()
    }
}

impl FormatCopySource for JsonCopySource {
    fn input(&mut self) -> FormatCopyInput<'_> {
        FormatCopyInput::Datums(self)
    }
}

struct ReadyJsonCopyDestination {
    encoder: BoundJsonObjectEncoder,
    writer: ObjectSetWriter<StreamEncoderFactory>,
}

/// Tuple-slot-to-NDJSON destination for PostgreSQL COPY TO.
pub(super) struct JsonCopyDestination {
    output: Option<ObjectOutput>,
    compression: StreamCompression,
    ready: Option<ReadyJsonCopyDestination>,
    completed: bool,
    bytes_produced: u64,
}

impl JsonCopyDestination {
    pub(super) fn new(output: ObjectOutput, compression: StreamCompression) -> Self {
        Self {
            output: Some(output),
            compression,
            ready: None,
            completed: false,
            bytes_produced: 0,
        }
    }

    pub(super) fn finish(mut self) -> Result<(), CopyError> {
        self.finish_inner()
    }

    fn finish_inner(&mut self) -> Result<(), CopyError> {
        if self.completed {
            return Ok(());
        }
        let ready = self
            .ready
            .take()
            .expect("COPY TO initializes its destination before completion");
        self.bytes_produced = ready
            .writer
            .finish_with_bytes(EmptyOutputPolicy::EmitFile)
            .map_err(CopyError::from)?;
        self.completed = true;
        Ok(())
    }

    fn initialize_inner(
        &mut self,
        layout: &CopyColumnLayout,
    ) -> Result<(), CopyError> {
        let fields = layout
            .columns()
            .iter()
            .map(|column| {
                let name = column.name().to_str().map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Json,
                        "COPY TO output column names must be valid UTF-8 for JSON",
                    )
                })?;
                Ok((name, column.type_oid(), column.type_mod()))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        let plan = JsonColumnPlan::bind(
            fields
                .iter()
                .map(|(name, oid, typmod)| (*name, *oid, *typmod)),
        )?;
        let encoder = BoundJsonObjectEncoder::bind(
            plan.columns()
                .iter()
                .map(|column| (column.name(), column.output_encoder())),
        )
        .map_err(ConnectorError::json_datum)?;
        let output = self
            .output
            .take()
            .expect("COPY TO initializes its destination exactly once");
        self.ready = Some(ReadyJsonCopyDestination {
            encoder,
            writer: ObjectSetWriter::new(
                output,
                StreamEncoderFactory::new(StreamFormat::Json, self.compression),
            ),
        });
        Ok(())
    }
}

impl CopyTupleDestination for JsonCopyDestination {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
        self.initialize_inner(layout)
    }

    fn write_slot(&mut self, row: CopyOutputRow<'_>) -> Result<(), CopyError> {
        let ready = self
            .ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows");
        let values = row.datums().map(|datum| datum.value());
        let encoded = unsafe { ready.encoder.encode_row(values) }
            .map_err(ConnectorError::json_datum)?;
        ready.writer.write(encoded).map_err(CopyError::from)
    }

    fn bytes_produced(&self) -> u64 {
        self.ready
            .as_ref()
            .map_or(self.bytes_produced, |ready| ready.writer.bytes_written())
    }

    fn finish(&mut self) -> Result<(), CopyError> {
        self.finish_inner()
    }

    fn abort(&mut self) {
        self.ready = None;
        self.output = None;
    }
}

impl FormatCopyDestination for JsonCopyDestination {
    fn output(&mut self) -> FormatCopyOutput<'_> {
        FormatCopyOutput::Tuples(self)
    }

    fn finish(self: Box<Self>) -> Result<(), CopyError> {
        let destination = *self;
        destination.finish()
    }
}
