//! Avro format object and container-header schema reader.

use apache_avro::{Reader, Schema};
use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::{
    AvroWriteCompression, FormatKind, FormatObject, FormatOption, FormatReader,
    FormatSchemaReader, FormatWriter, InferredColumn, InferredSchema, PostgresType,
    StorageFileReader,
};

/// Avro-format processor. The writer codec is validated now but is not stored
/// until the Avro write adapter exists.
pub(crate) struct AvroFormat;

impl AvroFormat {
    pub(crate) fn resolve(
        _write_compression: AvroWriteCompression,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        if let Some(option) = options.first() {
            return Err(ConnectorError::invalid_option(
                option.name(),
                "is not valid for avro",
            ));
        }
        Ok(Self)
    }
}

impl FormatObject for AvroFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Avro
    }
}

impl FormatReader for AvroFormat {}

impl FormatWriter for AvroFormat {}

impl FormatSchemaReader for AvroFormat {
    fn infer_schema(
        &self,
        file: &mut StorageFile,
    ) -> Result<InferredSchema, ConnectorError> {
        let reader = Reader::new(StorageFileReader::new(file))?;
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
            Schema::Null => Err(self.unsupported_type(field_name, schema)),
            Schema::Boolean => plain(pg_sys::BOOLOID),
            Schema::Int => plain(pg_sys::INT4OID),
            Schema::Long => plain(pg_sys::INT8OID),
            Schema::Float => plain(pg_sys::FLOAT4OID),
            Schema::Double => plain(pg_sys::FLOAT8OID),
            Schema::Bytes | Schema::Fixed(_) => plain(pg_sys::BYTEAOID),
            Schema::String | Schema::Enum(_) => plain(pg_sys::TEXTOID),
            Schema::Array(array) => self
                .postgres_type(field_name, &array.items)?
                .array(field_name),
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
            Schema::Decimal(decimal) => {
                let (Ok(precision), Ok(scale)) = (
                    i32::try_from(decimal.precision),
                    i32::try_from(decimal.scale),
                ) else {
                    return Err(self.unsupported_type(field_name, schema));
                };
                if precision > 1000 || scale > 1000 {
                    return Err(self.unsupported_type(field_name, schema));
                }
                Ok(PostgresType::numeric(self.kind(), precision, scale))
            }
            Schema::BigDecimal => plain(pg_sys::NUMERICOID),
            Schema::Uuid => plain(pg_sys::UUIDOID),
            Schema::Date => plain(pg_sys::DATEOID),
            Schema::TimeMillis | Schema::TimeMicros => plain(pg_sys::TIMEOID),
            Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos => plain(pg_sys::TIMESTAMPTZOID),
            Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => plain(pg_sys::TIMESTAMPOID),
            Schema::Duration => plain(pg_sys::INTERVALOID),
            Schema::Map(_) | Schema::Record(_) | Schema::Ref { .. } => {
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
