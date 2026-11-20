//! Direct Datum/slot adapters for NDJSON COPY FROM and COPY TO.

use lagodb_core::copy::{
    CopyColumnLayout, CopyDatumSource, CopyError, CopyInputRow, CopyOutputRow,
    CopyRowOutcome, CopyTupleDestination,
};
use lagodb_core::datum::BoundJsonObjectEncoder;

use crate::error::ConnectorError;
use crate::format::json::{JsonColumnPlan, JsonRecordDecoder, JsonRecordStream};
use crate::format::{
    EmptyOutputPolicy, FormatKind, ObjectSetWriter, StreamCompression,
    StreamCompressionOptions, StreamEncoderFactory, StreamFormat,
};
use crate::gucs::ReadConfig;
use crate::storage::{ResolvedStorageLocation, StorageOutput};

use super::{FormatCopyDestination, FormatCopyOutput};

/// NDJSON-to-Datum source for PostgreSQL COPY FROM.
pub(in crate::copy) struct JsonCopySource {
    stream: JsonRecordStream,
    plan: JsonColumnPlan,
    decoder: JsonRecordDecoder,
}

impl JsonCopySource {
    /// The caller has validated that `location` names one exact object.
    pub(in crate::copy) fn open(
        location: &ResolvedStorageLocation,
        layout: &CopyColumnLayout,
        compression: StreamCompression,
    ) -> Result<Self, CopyError> {
        let files = FormatKind::Json.input(location)?.open();
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
                StreamCompressionOptions::new(Some(compression), None),
                max_record_bytes,
            ),
            decoder: JsonRecordDecoder::new(plan.len()),
            plan,
        })
    }
}

impl CopyDatumSource for JsonCopySource {
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
        // As in PostgreSQL's text COPY, ON_ERROR IGNORE saves soft type-input
        // errors; other PostgreSQL errors unwind to the COPY boundary.
        let soft_errors = row.soft_errors();
        for (index, (target, column)) in
            row.columns().zip(self.plan.columns().iter()).enumerate()
        {
            // SAFETY: record was decoded above with this plan, and COPY
            // selected its per-tuple context for this callback.
            let datum = unsafe {
                self.decoder
                    .datum(record, column, index, logical_line, soft_errors)
            };
            match datum {
                Ok(datum) => target.set(datum),
                Err(error) => {
                    return error
                        .into_copy_row_rejection(
                            Some(index),
                            format!("NDJSON logical line {logical_line}"),
                        )
                        .map(CopyRowOutcome::Rejected);
                }
            }
        }
        Ok(CopyRowOutcome::Row)
    }

    fn bytes_consumed(&self) -> u64 {
        self.stream.bytes_consumed()
    }
}

struct ReadyJsonCopyDestination {
    encoder: BoundJsonObjectEncoder,
    writer: ObjectSetWriter<StreamEncoderFactory>,
}

/// Tuple-slot-to-NDJSON destination for PostgreSQL COPY TO.
pub(super) struct JsonCopyDestination {
    output: Option<StorageOutput>,
    compression: StreamCompression,
    ready: Option<ReadyJsonCopyDestination>,
    bytes_produced: u64,
}

impl JsonCopyDestination {
    pub(super) fn new(output: StorageOutput, compression: StreamCompression) -> Self {
        Self {
            output: Some(output),
            compression,
            ready: None,
            bytes_produced: 0,
        }
    }
}

impl CopyTupleDestination for JsonCopyDestination {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
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
        .map_err(ConnectorError::from)?;
        let output = self
            .output
            .take()
            .expect("COPY TO initializes its destination exactly once");
        self.ready = Some(ReadyJsonCopyDestination {
            encoder,
            writer: ObjectSetWriter::new(
                output,
                StreamEncoderFactory::new(StreamFormat::Json, self.compression),
            )?,
        });
        Ok(())
    }

    fn write_slot(&mut self, row: CopyOutputRow<'_>) -> Result<(), CopyError> {
        let ready = self
            .ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows");
        let values = row.datums().map(|datum| datum.value());
        // SAFETY: the bound plan and slot values come from the same COPY
        // layout, and slot Datums remain live for this callback.
        let encoded = unsafe { ready.encoder.encode_row(values) };
        ready.writer.write(encoded).map_err(CopyError::from)
    }

    fn bytes_produced(&self) -> u64 {
        self.ready
            .as_ref()
            .map_or(self.bytes_produced, |ready| ready.writer.bytes_written())
    }

    fn finish(&mut self) -> Result<(), CopyError> {
        let ready = self
            .ready
            .take()
            .expect("COPY TO initializes its destination before completion");
        self.bytes_produced = ready
            .writer
            .finish_with_bytes(EmptyOutputPolicy::EmitFile)
            .map_err(CopyError::from)?;
        Ok(())
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
}
