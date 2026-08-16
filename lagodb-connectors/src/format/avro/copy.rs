//! Canonical-CSV bridges for Avro COPY FROM and COPY TO.

use std::ffi::CStr;
use std::panic::AssertUnwindSafe;

use apache_avro::types::Value;
use apache_avro::{Reader, Schema};
use pg_lakebase_core::copy::{
    CopyColumnLayout, CopyDataDestination, CopyDataSource, CopyError,
};
use pg_lakebase_core::diag::PgReportError;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::{PgTryBuilder, pg_sys};

use crate::error::ConnectorError;
use crate::format::copy::{
    CanonicalCsv, CanonicalCsvRow, FormatCopyDestination, FormatCopySource,
};
use crate::format::{AvroWriteCompression, FormatKind};
use crate::storage::{ObjectFiles, ObjectOutput};

use super::read::{AvroObjectReader, AvroReadColumn};
use super::write::{AvroDatumRow, AvroObjectWriter, AvroWritePlan};

const BRIDGE_BUFFER_TARGET: usize = 256 * 1024;

struct CopyReadColumn {
    reader: AvroReadColumn,
    output_function: pg_sys::Oid,
}

/// Avro-to-canonical-CSV source for PostgreSQL COPY FROM.
pub(in crate::format) struct AvroCopySource {
    reader: Reader<'static, AvroObjectReader>,
    columns: Box<[CopyReadColumn]>,
    bytes: Vec<u8>,
    position: usize,
    datum_context: PgMemoryContexts,
}

impl AvroCopySource {
    pub(in crate::format) fn new(
        mut files: ObjectFiles,
        layout: &CopyColumnLayout,
    ) -> Result<Self, CopyError> {
        let first = files.next().expect(
            "Avro COPY FROM resolves one exact object before opening its source",
        );
        let reader = Reader::new(AvroObjectReader::new(first?))
            .map_err(ConnectorError::from)?;
        let schema = reader.writer_schema().clone();
        let Schema::Record(record) = &schema else {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "the Avro container writer schema must be a record",
            )
            .into());
        };
        let mut columns = Vec::with_capacity(layout.len());
        let bind = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
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
                    let mut output_function = pg_sys::InvalidOid;
                    let mut variable_length = false;
                    pg_sys::getTypeOutputInfo(
                        column.type_oid(),
                        &mut output_function,
                        &mut variable_length,
                    );
                    columns.push(CopyReadColumn {
                        reader: AvroReadColumn::bind(
                            source,
                            &record.fields[source].schema,
                            column.type_oid(),
                            column.type_mod(),
                        )?,
                        output_function,
                    });
                }
                Ok::<(), ConnectorError>(())
            }))
            .catch_others(|error| Err(ConnectorError::Postgres(PgReportError::from_caught(error))))
            .execute()
        };
        bind.map_err(CopyError::from)?;
        Ok(Self {
            reader,
            columns: columns.into_boxed_slice(),
            bytes: Vec::with_capacity(BRIDGE_BUFFER_TARGET),
            position: 0,
            datum_context: PgMemoryContexts::new("lagodb avro copy from bridge"),
        })
    }

    fn fill_bytes(&mut self) -> Result<bool, CopyError> {
        self.bytes.clear();
        self.position = 0;
        let datum_context = self.datum_context.value();
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                while self.bytes.len() < BRIDGE_BUFFER_TARGET {
                    let Some(value) = self.reader.next() else {
                        return Ok(false);
                    };
                    let Value::Record(fields) = value? else {
                        unreachable!(
                            "a record writer schema always decodes to a record value"
                        );
                    };
                    // One COPY row owns temporary PostgreSQL output text only
                    // until it is escaped into the bridge byte buffer.
                    pg_sys::MemoryContextReset(datum_context);
                    PgMemoryContexts::For(datum_context).switch_to(|_| {
                        for (index, column) in self.columns.iter().enumerate() {
                            if index > 0 {
                                self.bytes.push(b',');
                            }
                            // SAFETY: this source index was resolved against the
                            // exact Avro writer schema when the source opened.
                            let source = fields.get_unchecked(column.reader.source());
                            match column.reader.datum(&source.1)? {
                                None => {
                                    self.bytes.extend_from_slice(CanonicalCsv::NULL)
                                }
                                Some(datum) => {
                                    let value = pg_sys::OidOutputFunctionCall(
                                        column.output_function,
                                        datum,
                                    );
                                    let value = CStr::from_ptr(value);
                                    CanonicalCsv::write_field(
                                        &mut self.bytes,
                                        value.to_bytes(),
                                    );
                                }
                            }
                        }
                        self.bytes.push(b'\n');
                        Ok::<(), ConnectorError>(())
                    })?;
                }
                Ok::<bool, ConnectorError>(true)
            }))
            .catch_others(|error| {
                Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
            })
            .execute()
        };
        unsafe { self.datum_context.reset() };
        let more = result.map_err(CopyError::from)?;
        Ok(more || !self.bytes.is_empty())
    }
}

impl CopyDataSource for AvroCopySource {
    fn read(
        &mut self,
        output: &mut [u8],
        min_read: usize,
    ) -> Result<usize, CopyError> {
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

impl FormatCopySource for AvroCopySource {
    fn source(&mut self) -> &mut dyn CopyDataSource {
        self
    }
}

struct CopyInputPlan {
    input_function: pg_sys::Oid,
    type_io_param: pg_sys::Oid,
    type_mod: i32,
}

struct ReadyAvroCopyDestination {
    columns: Box<[CopyInputPlan]>,
    row: AvroDatumRow,
    writer: AvroObjectWriter,
}

/// Canonical-CSV-to-Avro destination for PostgreSQL COPY TO.
pub(in crate::format) struct AvroCopyDestination {
    output: Option<ObjectOutput>,
    compression: AvroWriteCompression,
    ready: Option<ReadyAvroCopyDestination>,
    row: CanonicalCsvRow,
    datum_context: PgMemoryContexts,
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
            row: CanonicalCsvRow::new(),
            datum_context: PgMemoryContexts::new("lagodb avro copy to bridge"),
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
        let mut input_plans = Vec::with_capacity(layout.len());
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                for column in layout.columns() {
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
                }
                Ok::<(), ConnectorError>(())
            }))
            .catch_others(|error| {
                Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
            })
            .execute()
        };
        result.map_err(CopyError::from)?;
        let output = self
            .output
            .take()
            .expect("COPY TO initializes its destination exactly once");
        self.ready = Some(ReadyAvroCopyDestination {
            row: AvroDatumRow::new(input_plans.len()),
            columns: input_plans.into_boxed_slice(),
            writer: AvroObjectWriter::new(output, plan, self.compression),
        });
        Ok(())
    }
}

impl CopyDataDestination for AvroCopyDestination {
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
                for (index, (field, plan)) in
                    self.row.fields().zip(ready.columns.iter()).enumerate()
                {
                    let datum = match field {
                        None => None,
                        Some(value) => {
                            let datum = pg_sys::OidInputFunctionCall(
                                plan.input_function,
                                value.as_ptr().cast_mut(),
                                plan.type_io_param,
                                plan.type_mod,
                            );
                            Some(datum)
                        }
                    };
                    // SAFETY: the row and input plans were allocated from the
                    // same COPY layout. A present Datum is allocated in this
                    // row's context and is consumed synchronously below.
                    ready.row.set_at_bound(index, datum);
                }
                ready.writer.write_row(&ready.row)?;
                Ok::<(), ConnectorError>(())
            })
        }
        .map_err(CopyError::from)
    }
}

impl FormatCopyDestination for AvroCopyDestination {
    fn destination(&mut self) -> &mut dyn CopyDataDestination {
        self
    }

    fn finish(self: Box<Self>) -> Result<(), CopyError> {
        let destination = *self;
        destination.finish()
    }
}
