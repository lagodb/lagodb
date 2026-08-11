//! Parquet footer and Arrow-to-PostgreSQL schema mapping.

use arrow_schema::{DataType, Field, TimeUnit};
use bytes::Bytes;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaDataReader;
use pg_lakebase_core::tuple::numeric_precision_scale;
use pg_lakebase_storage::StorageFile;
use pgrx::{PgBuiltInOids, PgOid, pg_sys};
use std::sync::Arc;

use crate::error::ConnectorError;

use super::super::{FormatKind, InferredSchema};

const INITIAL_FOOTER_READ: u64 = 64 * 1024;

pub(super) fn infer(
    file: &mut StorageFile,
) -> Result<InferredSchema, ConnectorError> {
    let file_size = file.size();
    if file_size == 0 {
        return Err(ConnectorError::invalid_object_schema(
            FormatKind::Parquet,
            "the object is empty",
        ));
    }

    let mut required = file_size.min(INITIAL_FOOTER_READ);
    let mut metadata_reader = ParquetMetaDataReader::new();
    loop {
        let read_len = u32::try_from(required).map_err(|_| {
            ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "the Parquet footer exceeds the supported read size",
            )
        })?;
        let tail = Bytes::from(file.read_at(file_size - required, read_len)?);
        match metadata_reader.try_parse_sized(&tail, file_size) {
            Ok(()) => break,
            Err(ParquetError::NeedMoreData(needed)) => {
                let needed = u64::try_from(needed).map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        "the Parquet footer size cannot be represented",
                    )
                })?;
                if needed <= required || needed > file_size {
                    return Err(ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        "the Parquet footer references bytes outside the object",
                    ));
                }
                required = needed;
            }
            Err(error) => return Err(error.into()),
        }
    }

    let metadata = metadata_reader.finish()?;
    let file_metadata = metadata.file_metadata();
    let schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;
    InferredSchema::from_arrow(FormatKind::Parquet, &schema)
}

pub(crate) fn parquet_arrow_type(
    oid: pg_sys::Oid,
    typmod: i32,
) -> Result<DataType, ConnectorError> {
    let data_type = match PgOid::from(oid) {
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => DataType::Boolean,
        PgOid::BuiltIn(PgBuiltInOids::INT2OID | PgBuiltInOids::INT4OID) => {
            DataType::Int32
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => DataType::Int64,
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => DataType::Float32,
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => DataType::Float64,
        PgOid::BuiltIn(
            PgBuiltInOids::TEXTOID
            | PgBuiltInOids::VARCHAROID
            | PgBuiltInOids::BPCHAROID
            | PgBuiltInOids::NAMEOID
            | PgBuiltInOids::JSONOID,
        ) => DataType::Utf8,
        PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => DataType::Binary,
        PgOid::BuiltIn(PgBuiltInOids::UUIDOID) => DataType::FixedSizeBinary(16),
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => DataType::Date32,
        PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
            DataType::Time64(TimeUnit::Microsecond)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
            let numeric = numeric_precision_scale(typmod).ok_or_else(|| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "unconstrained numeric cannot be represented losslessly as Parquet Decimal128",
                )
            })?;
            if numeric.precision > 38
                || numeric.scale < 0
                || numeric.scale as u32 > numeric.precision
            {
                return Err(ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "numeric precision/scale is outside Parquet Decimal128 limits",
                ));
            }
            DataType::Decimal128(numeric.precision as u8, numeric.scale as i8)
        }
        _ => {
            let element = unsafe { pg_sys::get_element_type(oid) };
            if element == pg_sys::InvalidOid {
                return Err(ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    format!(
                        "PostgreSQL type OID {oid} is not supported by the Parquet writer"
                    ),
                ));
            }
            let element_type = match PgOid::from(element) {
                PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => DataType::Boolean,
                PgOid::BuiltIn(PgBuiltInOids::INT2OID | PgBuiltInOids::INT4OID) => {
                    DataType::Int32
                }
                PgOid::BuiltIn(PgBuiltInOids::INT8OID) => DataType::Int64,
                PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => DataType::Float32,
                PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => DataType::Float64,
                PgOid::BuiltIn(
                    PgBuiltInOids::TEXTOID
                    | PgBuiltInOids::VARCHAROID
                    | PgBuiltInOids::BPCHAROID
                    | PgBuiltInOids::NAMEOID
                    | PgBuiltInOids::JSONOID,
                ) => DataType::Utf8,
                _ => {
                    return Err(ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        format!(
                            "PostgreSQL array element type OID {element} is not supported by the Parquet writer"
                        ),
                    ));
                }
            };
            DataType::List(Arc::new(Field::new("item", element_type, true)))
        }
    };
    Ok(data_type)
}
