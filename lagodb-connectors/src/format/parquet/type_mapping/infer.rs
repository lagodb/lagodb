//! Default declarations for schema inference, without catalog type creation.

use arrow_schema::extension::{ExtensionType, Json, Uuid};
use arrow_schema::{DataType, Field, TimeUnit};
use lagodb_core::tuple::{PgType, numeric_typmod};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::ParquetTypeMapping;

impl ParquetTypeMapping {
    pub(in crate::format::parquet) fn canonical(
        field: &Field,
    ) -> Result<PgType, ConnectorError> {
        if let Some(extension) = field.extension_type_name() {
            match extension {
                Uuid::NAME => {
                    field.try_extension_type::<Uuid>()?;
                    return Ok(PgType::new(pg_sys::UUIDOID, -1));
                }
                Json::NAME => {
                    field.try_extension_type::<Json>()?;
                }
                _ => return Err(Self::unsupported(field)),
            }
        }
        let oid = match field.data_type() {
            DataType::Boolean => pg_sys::BOOLOID,
            DataType::Int16 => pg_sys::INT2OID,
            DataType::Int32 => pg_sys::INT4OID,
            DataType::Int64 => pg_sys::INT8OID,
            DataType::Float32 => pg_sys::FLOAT4OID,
            DataType::Float64 => pg_sys::FLOAT8OID,
            DataType::Utf8 | DataType::LargeUtf8 => pg_sys::TEXTOID,
            DataType::Binary | DataType::LargeBinary => pg_sys::BYTEAOID,
            DataType::FixedSizeBinary(width) if *width > 0 => pg_sys::BYTEAOID,
            DataType::Date32 => pg_sys::DATEOID,
            DataType::Time64(TimeUnit::Microsecond) => pg_sys::TIMEOID,
            DataType::Timestamp(
                TimeUnit::Microsecond | TimeUnit::Nanosecond,
                zone,
            ) => {
                if zone.is_some() {
                    pg_sys::TIMESTAMPTZOID
                } else {
                    pg_sys::TIMESTAMPOID
                }
            }
            DataType::Decimal128(precision, scale)
                if (1..=38).contains(precision)
                    && *scale >= 0
                    && *scale as u8 <= *precision =>
            {
                return Ok(PgType::new(
                    pg_sys::NUMERICOID,
                    numeric_typmod(u32::from(*precision), i32::from(*scale)),
                ));
            }
            DataType::List(element) => {
                // PG arrays have one element OID regardless of SQL's declared
                // dimension count. A second List is not another PG array type.
                if matches!(element.data_type(), DataType::List(_)) {
                    return Err(Self::unsupported(field));
                }
                let element = Self::canonical(element)?;
                let oid = unsafe { pg_sys::get_array_type(element.oid) };
                return Ok(PgType::new(oid, element.typmod));
            }
            DataType::Struct(_) => {
                return Err(ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "Parquet Struct requires an explicitly declared named PostgreSQL composite type",
                ));
            }
            _ => return Err(Self::unsupported(field)),
        };
        Ok(PgType::new(oid, -1))
    }
}
