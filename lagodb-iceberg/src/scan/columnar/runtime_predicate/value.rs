//! Source-bound runtime literals; physical binary width does not identify UUID.

use arrow_schema::{
    DataType,
    extension::{ExtensionType, Uuid as ArrowUuid},
};
use iceberg_lite::spec::Datum;
use lagodb_core::runtime_api::RuntimePredicateValue;
use uuid::Uuid;

use super::IcebergPredicatePlanner;
use crate::error::{IcebergError, IcebergResult};

impl IcebergPredicatePlanner<'_> {
    pub(super) fn comparison_datum(
        &self,
        column: usize,
        value: &RuntimePredicateValue<'_>,
    ) -> IcebergResult<Option<Datum>> {
        let field = self.arrow_schema.field(column);
        if let RuntimePredicateValue::Binary(bytes) = value {
            return Ok(match field.data_type() {
                DataType::FixedSizeBinary(width)
                    if bytes.len() == *width as usize =>
                {
                    if field.extension_type_name() == Some(ArrowUuid::NAME) {
                        let bytes = bytes
                            .as_ref()
                            .try_into()
                            .expect("UUID field has width 16");
                        Some(Datum::uuid(Uuid::from_bytes(bytes)))
                    } else {
                        Some(Datum::fixed(bytes.iter().copied()))
                    }
                }
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                    Some(Datum::binary(bytes.iter().copied()))
                }
                _ => None,
            });
        }
        Self::datum(value).map(Some)
    }

    pub(super) fn datum(value: &RuntimePredicateValue<'_>) -> IcebergResult<Datum> {
        Ok(match value {
            RuntimePredicateValue::Boolean(value) => Datum::bool(*value),
            RuntimePredicateValue::Int32(value) => Datum::int(*value),
            RuntimePredicateValue::Int64(value) => Datum::long(*value),
            RuntimePredicateValue::Date32(value) => Datum::date(*value),
            RuntimePredicateValue::TimestampMicrosecond(value) => {
                Datum::timestamp_micros(*value)
            }
            RuntimePredicateValue::TimestampTzMicrosecond(value) => {
                Datum::timestamptz_micros(*value)
            }
            RuntimePredicateValue::Decimal128 {
                coefficient,
                precision,
                scale,
            } => Datum::decimal_from_unscaled(
                *coefficient,
                u32::from(*precision),
                u32::try_from(*scale).map_err(|_| {
                    IcebergError::InvariantViolated(
                        "table-scan decimal predicate has a negative scale",
                    )
                })?,
            )?,
            RuntimePredicateValue::String(value) => Datum::string(value.as_ref()),
            RuntimePredicateValue::Binary(value) => {
                Datum::binary(value.iter().copied())
            }
        })
    }
}
