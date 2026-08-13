//! Canonical-CSV bridges for NDJSON COPY FROM and COPY TO.

use std::panic::AssertUnwindSafe;

use pg_lakebase_core::copy::{
    CopyColumnLayout, CopyDataDestination, CopyDataSource, CopyError,
};
use pg_lakebase_core::diag::PgReportError;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::PgTryBuilder;

use crate::error::ConnectorError;
use crate::format::json::{
    JsonColumnPlan, JsonInputValue, JsonObjectEncoder, JsonRecordDecoder,
    JsonRecordStream,
};
use crate::format::{
    EmptyOutputPolicy, FormatKind, ObjectSetWriter, StreamCompression,
    StreamEncoderFactory,
};
use crate::gucs::ReadConfig;
use crate::storage::{ObjectFiles, ObjectOutput};

use super::{
    CanonicalCsv, CanonicalCsvRow, FormatCopyDestination, FormatCopySource,
};

const BRIDGE_BUFFER_TARGET: usize = 256 * 1024;

/// NDJSON-to-canonical-CSV source for PostgreSQL COPY FROM.
pub(super) struct JsonCopySource {
    stream: JsonRecordStream,
    plan: JsonColumnPlan,
    decoder: JsonRecordDecoder,
    bytes: Vec<u8>,
    position: usize,
}

impl JsonCopySource {
    pub(super) fn new(
        files: ObjectFiles,
        layout: &CopyColumnLayout,
        compression: StreamCompression,
    ) -> Result<Self, CopyError> {
        let plan = JsonColumnPlan::bind(layout.columns().iter().map(|column| {
            let name = column.name().to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Json,
                    "COPY column names must be valid UTF-8 for JSON",
                )
            })?;
            Ok((name, column.type_oid(), column.type_mod()))
        }).collect::<Result<Vec<_>, ConnectorError>>()?)?;
        let max_record_bytes = ReadConfig::from_guc().json_max_record_bytes();
        Ok(Self {
            stream: JsonRecordStream::new(files, compression, max_record_bytes),
            decoder: JsonRecordDecoder::new(plan.len()),
            plan,
            bytes: Vec::with_capacity(BRIDGE_BUFFER_TARGET),
            position: 0,
        })
    }

    fn fill_bytes(&mut self) -> Result<bool, CopyError> {
        self.bytes.clear();
        self.position = 0;
        while self.bytes.len() < BRIDGE_BUFFER_TARGET {
            let Some((logical_line, record)) = self.stream.next_record()? else {
                break;
            };
            self.decoder.decode(&self.plan, record, logical_line)?;
            for (index, column) in self.plan.columns().iter().enumerate() {
                if index > 0 {
                    self.bytes.push(b',');
                }
                match self.decoder.value(record, column, index, logical_line)? {
                    JsonInputValue::Null => {
                        self.bytes.extend_from_slice(CanonicalCsv::NULL);
                    }
                    JsonInputValue::Bytes(value) => {
                        CanonicalCsv::write_field(&mut self.bytes, value);
                    }
                }
            }
            self.bytes.push(b'\n');
        }
        Ok(!self.bytes.is_empty())
    }
}

impl CopyDataSource for JsonCopySource {
    fn read(&mut self, output: &mut [u8], min_read: usize) -> Result<usize, CopyError> {
        let mut written = 0;
        let target = min_read.max(1).min(output.len());
        while written < target {
            if self.position == self.bytes.len() && !self.fill_bytes()? {
                break;
            }
            let available = &self.bytes[self.position..];
            let count = available.len().min(output.len() - written);
            output[written..written + count].copy_from_slice(&available[..count]);
            self.position += count;
            written += count;
            if written == output.len() {
                break;
            }
        }
        Ok(written)
    }
}

impl FormatCopySource for JsonCopySource {
    fn source(&mut self) -> &mut dyn CopyDataSource {
        self
    }
}

struct ReadyJsonCopyDestination {
    plan: JsonColumnPlan,
    object: JsonObjectEncoder,
    writer: ObjectSetWriter<StreamEncoderFactory>,
}

/// Canonical-CSV-to-NDJSON destination for PostgreSQL COPY TO.
pub(super) struct JsonCopyDestination {
    output: Option<ObjectOutput>,
    compression: StreamCompression,
    ready: Option<ReadyJsonCopyDestination>,
    row: CanonicalCsvRow,
    datum_context: PgMemoryContexts,
}

impl JsonCopyDestination {
    pub(super) fn new(output: ObjectOutput, compression: StreamCompression) -> Self {
        Self {
            output: Some(output),
            compression,
            ready: None,
            row: CanonicalCsvRow::new(),
            datum_context: PgMemoryContexts::new(
                "lagodb JSON copy to bridge",
            ),
        }
    }

    pub(super) fn finish(mut self) -> Result<(), CopyError> {
        let ready = self
            .ready
            .take()
            .expect("COPY TO initializes its destination before completion");
        ready
            .writer
            .finish(EmptyOutputPolicy::EmitFile)
            .map_err(CopyError::from)
    }

    fn initialize_inner(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
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
        let object = JsonObjectEncoder::new(
            plan.columns().iter().map(|column| column.name()),
        )?;
        let output = self
            .output
            .take()
            .expect("COPY TO initializes its destination exactly once");
        self.ready = Some(ReadyJsonCopyDestination {
            plan,
            object,
            writer: ObjectSetWriter::new(
                output,
                StreamEncoderFactory::new(FormatKind::Json, self.compression),
            ),
        });
        Ok(())
    }
}

impl CopyDataDestination for JsonCopyDestination {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError> {
        self.initialize_inner(layout)
    }

    fn write_row(&mut self, data: &[u8]) -> Result<(), CopyError> {
        let ready = self
            .ready
            .as_mut()
            .expect("COPY TO initializes its destination before producing rows");
        self.row.parse(data, ready.plan.len())?;
        unsafe { self.datum_context.reset() };
        let datum_context = self.datum_context.value();
        unsafe {
            PgMemoryContexts::For(datum_context).switch_to(|_| {
                PgTryBuilder::new(AssertUnwindSafe(|| {
                    ready.object.begin_row();
                    for (index, (field, column)) in self
                        .row
                        .fields()
                        .zip(ready.plan.columns().iter())
                        .enumerate()
                    {
                        match field {
                            None => ready.object.write_value(index, None),
                            Some(value) => {
                                // SAFETY: CanonicalCsvRow returns a NUL-terminated
                                // field and the input plan was bound to this column.
                                let datum = unsafe { column.input_datum(value) };
                                // SAFETY: the datum came from the input function
                                // bound to this exact COPY output column.
                                let json = unsafe {
                                    column.output_encoder().encode(datum)
                                }
                                .map_err(ConnectorError::json_datum)?;
                                ready.object.write_value(
                                    index,
                                    Some(json.as_bytes().map_err(ConnectorError::json_datum)?),
                                );
                            }
                        }
                    }
                    ready.writer.write(ready.object.finish_row())?;
                    Ok::<(), ConnectorError>(())
                }))
                .catch_others(|error| {
                    Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
                })
                .execute()
            })
        }
        .map_err(CopyError::from)
    }
}

impl FormatCopyDestination for JsonCopyDestination {
    fn destination(&mut self) -> &mut dyn CopyDataDestination {
        self
    }

    fn finish(self: Box<Self>) -> Result<(), CopyError> {
        let destination = *self;
        destination.finish()
    }
}
