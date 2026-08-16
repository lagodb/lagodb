//! Avro schema construction and PostgreSQL-to-Avro field binding.

use apache_avro::Schema;
use pg_lakebase_core::handles::RelationColumn;
use pg_lakebase_core::tuple::{
    ColumnDatumTarget, Decimal128NumericCodec, numeric_precision_scale,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::FormatKind;

#[derive(Clone, Copy)]
pub(in crate::format::avro) enum AvroValueKind {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Name,
    Uuid,
    Date,
    TimeMicros,
    TimestampMicros,
    LocalTimestampMicros,
    Decimal(Decimal128NumericCodec),
}

impl AvroValueKind {
    pub(super) fn from_oid(
        oid: pg_sys::Oid,
        typmod: i32,
    ) -> Result<Self, ConnectorError> {
        match oid {
            pg_sys::BOOLOID => Ok(Self::Boolean),
            pg_sys::INT2OID | pg_sys::INT4OID => Ok(Self::Int),
            pg_sys::INT8OID => Ok(Self::Long),
            pg_sys::FLOAT4OID => Ok(Self::Float),
            pg_sys::FLOAT8OID => Ok(Self::Double),
            pg_sys::BYTEAOID => Ok(Self::Bytes),
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => {
                Ok(Self::String)
            }
            pg_sys::NAMEOID => Ok(Self::Name),
            pg_sys::UUIDOID => Ok(Self::Uuid),
            pg_sys::DATEOID => Ok(Self::Date),
            pg_sys::TIMEOID => Ok(Self::TimeMicros),
            pg_sys::TIMESTAMPTZOID => Ok(Self::TimestampMicros),
            pg_sys::TIMESTAMPOID => Ok(Self::LocalTimestampMicros),
            pg_sys::NUMERICOID => {
                let numeric = numeric_precision_scale(typmod).ok_or_else(|| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "unconstrained numeric cannot be represented losslessly as Avro decimal",
                    )
                })?;
                let scale = u32::try_from(numeric.scale).map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "numeric scale must be non-negative for Avro decimal",
                    )
                })?;
                Self::decimal(numeric.precision, scale)
            }
            _ => Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                format!("PostgreSQL type OID {oid} is not supported by Avro"),
            )),
        }
    }

    pub(super) const fn schema_name(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Long => "long",
            Self::Float => "float",
            Self::Double => "double",
            Self::Bytes => "bytes",
            Self::String | Self::Name => "string",
            Self::Uuid => "uuid",
            Self::Date => "date",
            Self::TimeMicros => "time-micros",
            Self::TimestampMicros => "timestamp-micros",
            Self::LocalTimestampMicros => "local-timestamp-micros",
            Self::Decimal(_) => "decimal",
        }
    }

    fn schema_json(self) -> serde_json::Value {
        match self {
            Self::Uuid => {
                serde_json::json!({ "type": "string", "logicalType": "uuid" })
            }
            Self::Date => serde_json::json!({ "type": "int", "logicalType": "date" }),
            Self::TimeMicros => {
                serde_json::json!({ "type": "long", "logicalType": "time-micros" })
            }
            Self::TimestampMicros => {
                serde_json::json!({ "type": "long", "logicalType": "timestamp-micros" })
            }
            Self::LocalTimestampMicros => serde_json::json!({
                "type": "long",
                "logicalType": "local-timestamp-micros",
            }),
            Self::Decimal(codec) => serde_json::json!({
                "type": "bytes",
                "logicalType": "decimal",
                "precision": codec.precision(),
                "scale": codec.scale(),
            }),
            primitive => {
                serde_json::Value::String(primitive.schema_name().to_owned())
            }
        }
    }

    pub(in crate::format::avro) fn from_schema(
        schema: &Schema,
    ) -> Result<Self, ConnectorError> {
        match schema {
            Schema::Boolean => Ok(Self::Boolean),
            Schema::Int => Ok(Self::Int),
            Schema::Long => Ok(Self::Long),
            Schema::Float => Ok(Self::Float),
            Schema::Double => Ok(Self::Double),
            Schema::Bytes | Schema::Fixed(_) => Ok(Self::Bytes),
            Schema::String | Schema::Enum(_) => Ok(Self::String),
            Schema::Uuid => Ok(Self::Uuid),
            Schema::Date => Ok(Self::Date),
            Schema::TimeMillis | Schema::TimeMicros => Ok(Self::TimeMicros),
            Schema::TimestampMillis | Schema::TimestampMicros => {
                Ok(Self::TimestampMicros)
            }
            Schema::LocalTimestampMillis | Schema::LocalTimestampMicros => {
                Ok(Self::LocalTimestampMicros)
            }
            Schema::Decimal(decimal) => {
                Self::decimal_metadata(decimal.precision, decimal.scale)
            }
            Schema::Union(union) => {
                let mut variants = union
                    .variants()
                    .iter()
                    .filter(|variant| !matches!(variant, Schema::Null));
                let Some(variant) = variants.next() else {
                    return Err(ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "an Avro union must contain one non-null variant",
                    ));
                };
                if variants.next().is_some() {
                    return Err(ConnectorError::invalid_object_schema(
                        FormatKind::Avro,
                        "an Avro union with multiple non-null variants is unsupported",
                    ));
                }
                Self::from_schema(variant)
            }
            _ => Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "the Avro field type is not supported",
            )),
        }
    }

    pub(in crate::format::avro) fn supports_target(self, oid: pg_sys::Oid) -> bool {
        match self {
            Self::Boolean => oid == pg_sys::BOOLOID,
            Self::Int | Self::Long => {
                oid == pg_sys::INT2OID
                    || oid == pg_sys::INT4OID
                    || oid == pg_sys::INT8OID
            }
            Self::Float => oid == pg_sys::FLOAT4OID,
            Self::Double => oid == pg_sys::FLOAT8OID,
            Self::Bytes => oid == pg_sys::BYTEAOID,
            Self::String => {
                oid == pg_sys::TEXTOID
                    || oid == pg_sys::VARCHAROID
                    || oid == pg_sys::BPCHAROID
                    || oid == pg_sys::NAMEOID
            }
            Self::Name => oid == pg_sys::NAMEOID,
            Self::Uuid => oid == pg_sys::UUIDOID,
            Self::Date => oid == pg_sys::DATEOID,
            Self::TimeMicros => oid == pg_sys::TIMEOID,
            Self::TimestampMicros => oid == pg_sys::TIMESTAMPTZOID,
            Self::LocalTimestampMicros => oid == pg_sys::TIMESTAMPOID,
            Self::Decimal(_) => oid == pg_sys::NUMERICOID,
        }
    }

    pub(super) fn value_out_of_range(self) -> ConnectorError {
        ConnectorError::invalid_object_schema(
            FormatKind::Avro,
            format!(
                "a {} value is outside the Avro epoch range",
                self.schema_name()
            ),
        )
    }

    fn decimal(precision: u32, scale: u32) -> Result<Self, ConnectorError> {
        Decimal128NumericCodec::new(precision, scale)
            .map(Self::Decimal)
            .map_err(ConnectorError::from)
    }

    fn decimal_metadata(
        precision: usize,
        scale: usize,
    ) -> Result<Self, ConnectorError> {
        let precision = u32::try_from(precision).map_err(|_| {
            ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "Avro decimal precision exceeds the supported range",
            )
        })?;
        let scale = u32::try_from(scale).map_err(|_| {
            ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "Avro decimal scale exceeds the supported range",
            )
        })?;
        Self::decimal(precision, scale)
    }
}

pub(in crate::format::avro) struct AvroWritePlan {
    schema: Schema,
    fields: Box<[AvroValueKind]>,
}

impl AvroWritePlan {
    pub(super) fn from_relation_columns(
        columns: &[RelationColumn],
    ) -> Result<Self, ConnectorError> {
        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            let name = column.name().to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Avro,
                    "PostgreSQL column names must be valid UTF-8 for Avro",
                )
            })?;
            fields.push((
                name.to_owned(),
                AvroValueKind::from_oid(column.type_oid(), column.type_mod())?,
            ));
        }
        Self::from_fields(fields)
    }

    pub(in crate::format::avro) fn from_copy_columns<'a>(
        columns: impl Iterator<Item = Result<(&'a str, pg_sys::Oid, i32), ConnectorError>>,
        count: usize,
    ) -> Result<Self, ConnectorError> {
        let mut fields = Vec::with_capacity(count);
        for column in columns {
            let (name, oid, typmod) = column?;
            fields.push((name.to_owned(), AvroValueKind::from_oid(oid, typmod)?));
        }
        Self::from_fields(fields)
    }

    fn from_fields(
        fields: Vec<(String, AvroValueKind)>,
    ) -> Result<Self, ConnectorError> {
        if fields.iter().any(|(_, kind)| {
            matches!(kind, AvroValueKind::String | AvroValueKind::Name)
        }) {
            ColumnDatumTarget::validate_utf8_server_encoding()?;
        }
        let kinds = fields
            .iter()
            .map(|(_, kind)| *kind)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let schema_fields = fields
            .into_iter()
            .map(|(name, kind)| {
                serde_json::json!({
                    "name": name,
                    "type": ["null", kind.schema_json()],
                    "default": null,
                })
            })
            .collect::<Vec<_>>();
        let schema = serde_json::json!({
            "type": "record",
            "name": "lakebase_record",
            "fields": schema_fields,
        });
        Ok(Self {
            schema: Schema::parse_str(&schema.to_string())?,
            fields: kinds,
        })
    }

    pub(super) fn into_parts(self) -> (Schema, Box<[AvroValueKind]>) {
        (self.schema, self.fields)
    }
}
