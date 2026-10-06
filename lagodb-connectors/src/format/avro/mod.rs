//! Avro object-container format support.

mod copy;
mod read;
mod write;

use std::io::BufReader;

use crate::storage::InputFile;
use apache_avro::{Reader, Schema};
use lagodb_core::fdw::StartForeignScanContext;
use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::storage::{ObjectFiles, ObjectOutput};

use super::{
    AvroWriteCompression, FormatKind, FormatObject, FormatOption, FormatReader,
    FormatScanPlanner, FormatScanState, FormatSchemaReader, FormatWriteState,
    FormatWriter, InferredColumn, InferredSchema, PostgresType,
};

pub(super) use copy::{AvroCopyDestination, AvroCopySource};
use read::AvroScanState;
use write::{AvroValueKind, AvroWriteState};

/// Avro-format processor.
pub(crate) struct AvroFormat {
    write_compression: AvroWriteCompression,
}

impl AvroFormat {
    pub(crate) fn resolve(
        write_compression: AvroWriteCompression,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        if let Some(option) = options.first() {
            return Err(ConnectorError::invalid_option(
                option.name(),
                "is not valid for avro",
            ));
        }
        Ok(Self { write_compression })
    }
}

impl FormatObject for AvroFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Avro
    }
}

impl FormatReader for AvroFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(read::AvroScanPlanner::new())
    }

    fn begin(
        self: Box<Self>,
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: ObjectFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        Ok(Box::new(AvroScanState::begin(context, files)?))
    }
}

impl FormatWriter for AvroFormat {
    fn begin(
        self: Box<Self>,
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Ok(Box::new(AvroWriteState::begin(
            relation,
            output,
            self.write_compression,
        )?))
    }
}

impl FormatSchemaReader for AvroFormat {
    fn infer_schema(
        &self,
        file: &mut InputFile,
    ) -> Result<InferredSchema, ConnectorError> {
        // OCF headers contain byte-wise varints even though record payloads
        // are buffered internally by apache-avro.
        let reader = Reader::new(BufReader::new(file))?;
        let Schema::Record(record) = reader.writer_schema() else {
            return Err(ConnectorError::invalid_object_schema(
                self.kind(),
                "the Avro container writer schema must be a record",
            ));
        };
        let columns = record
            .fields
            .iter()
            .map(|field| {
                let postgres_type = self.postgres_type(&field.name, &field.schema)?;
                Ok(InferredColumn::new(&field.name, postgres_type))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        InferredSchema::new(self.kind(), columns)
    }
}

impl AvroFormat {
    fn postgres_type(
        &self,
        field_name: &str,
        schema: &Schema,
    ) -> Result<PostgresType, ConnectorError> {
        let plain = |oid| Ok(PostgresType::new(self.kind(), oid));
        match schema {
            Schema::Boolean => plain(pg_sys::BOOLOID),
            Schema::Int => plain(pg_sys::INT4OID),
            Schema::Long => plain(pg_sys::INT8OID),
            Schema::Float => plain(pg_sys::FLOAT4OID),
            Schema::Double => plain(pg_sys::FLOAT8OID),
            Schema::Bytes | Schema::Fixed(_) => plain(pg_sys::BYTEAOID),
            Schema::String | Schema::Enum(_) => plain(pg_sys::TEXTOID),
            Schema::Uuid => plain(pg_sys::UUIDOID),
            Schema::Date => plain(pg_sys::DATEOID),
            Schema::TimeMillis | Schema::TimeMicros => plain(pg_sys::TIMEOID),
            Schema::TimestampMillis | Schema::TimestampMicros => {
                plain(pg_sys::TIMESTAMPTZOID)
            }
            Schema::LocalTimestampMillis | Schema::LocalTimestampMicros => {
                plain(pg_sys::TIMESTAMPOID)
            }
            Schema::Decimal(decimal) => {
                AvroValueKind::from_schema(schema)?;
                let precision = i32::try_from(decimal.precision).map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        self.kind(),
                        "Avro decimal precision exceeds PostgreSQL typmod range",
                    )
                })?;
                let scale = i32::try_from(decimal.scale).map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        self.kind(),
                        "Avro decimal scale exceeds PostgreSQL typmod range",
                    )
                })?;
                Ok(PostgresType::numeric(self.kind(), precision, scale))
            }
            Schema::Union(union) => {
                let mut values = union
                    .variants()
                    .iter()
                    .filter(|variant| !matches!(variant, Schema::Null));
                let Some(value) = values.next() else {
                    return Err(self.unsupported_type(field_name, schema));
                };
                if values.next().is_some() {
                    return Err(self.unsupported_type(field_name, schema));
                }
                self.postgres_type(field_name, value)
            }
            Schema::Null
            | Schema::Array(_)
            | Schema::Map(_)
            | Schema::Record(_)
            | Schema::Ref { .. }
            | Schema::BigDecimal
            | Schema::Duration
            | Schema::TimestampNanos
            | Schema::LocalTimestampNanos => {
                Err(self.unsupported_type(field_name, schema))
            }
        }
    }

    fn unsupported_type(&self, field_name: &str, schema: &Schema) -> ConnectorError {
        ConnectorError::invalid_object_schema(
            self.kind(),
            format!("column {field_name:?} uses unsupported Avro schema {schema}"),
        )
    }
}
