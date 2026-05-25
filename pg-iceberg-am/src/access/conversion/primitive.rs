use std::sync::Arc;

use super::traits::{ArrowToCell, RowsToArrow};
use crate::error::{IcebergError, IcebergResult};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampNanosecondType,
};
use arrow_array::{Array, ArrayRef};
use iceberg_lite::spec::PrimitiveType;
use pg_lakebase_core::tuple::{
    ByteaView, Cell, Decimal128NumericCodec, PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF,
    Row, StringView,
};
use pgrx::datum::Uuid;
use pgrx::prelude::{AnyNumeric, Date, Time, Timestamp, TimestampWithTimeZone};

struct TemporalCodec;

impl TemporalCodec {
    fn pg_date_from_arrow_days(arrow_days: i32) -> IcebergResult<Date> {
        let pg_days =
            arrow_days.checked_sub(PG_EPOCH_DAYS_DIFF).ok_or_else(|| {
                Self::invalid_datum(format!(
                    "Iceberg date value {arrow_days} days overflows PostgreSQL epoch"
                ))
            })?;

        Date::try_from(pg_days).map_err(|_| {
            Self::invalid_datum(format!(
                "Iceberg date value {arrow_days} days is outside PostgreSQL date range"
            ))
        })
    }

    fn arrow_days_from_pg_date(date: &Date) -> IcebergResult<i32> {
        date.to_pg_epoch_days()
            .checked_add(PG_EPOCH_DAYS_DIFF)
            .ok_or_else(|| {
                Self::invalid_datum(format!(
                    "PostgreSQL date value {} days overflows Unix epoch",
                    date.to_pg_epoch_days()
                ))
            })
    }

    fn time_from_micros(micros: i64) -> IcebergResult<Time> {
        Time::try_from(micros).map_err(|_| {
            Self::invalid_datum(format!(
                "Iceberg time value {micros} microseconds is outside PostgreSQL time range"
            ))
        })
    }

    fn timestamp_from_unix_micros(unix_micros: i64) -> IcebergResult<Timestamp> {
        let pg_micros = Self::unix_micros_to_pg_micros(unix_micros)?;
        Timestamp::try_from(pg_micros).map_err(|_| {
            Self::invalid_datum(format!(
                "Iceberg timestamp value {unix_micros} microseconds is outside PostgreSQL timestamp range"
            ))
        })
    }

    fn timestamptz_from_unix_micros(
        unix_micros: i64,
    ) -> IcebergResult<TimestampWithTimeZone> {
        let pg_micros = Self::unix_micros_to_pg_micros(unix_micros)?;
        TimestampWithTimeZone::try_from(pg_micros).map_err(|_| {
            Self::invalid_datum(format!(
                "Iceberg timestamptz value {unix_micros} microseconds is outside PostgreSQL timestamp range"
            ))
        })
    }

    fn unix_micros_from_timestamp(pg_micros: i64) -> IcebergResult<i64> {
        pg_micros.checked_add(PG_EPOCH_USECS_DIFF).ok_or_else(|| {
            Self::invalid_datum(format!(
                "PostgreSQL timestamp value {pg_micros} microseconds overflows Unix epoch"
            ))
        })
    }

    /// Convert a PostgreSQL-epoch microsecond timestamp to a Unix-epoch
    /// nanosecond timestamp. Used by the writer when materializing
    /// PostgreSQL `timestamp` / `timestamptz` values into Iceberg
    /// `timestamp_ns` / `timestamptz_ns` columns.
    ///
    /// PostgreSQL only stores microsecond resolution, so the produced
    /// nanosecond value always has its three least-significant digits
    /// equal to zero; no precision is lost going PG → Iceberg ns.
    fn unix_nanos_from_timestamp(pg_micros: i64) -> IcebergResult<i64> {
        let unix_micros = Self::unix_micros_from_timestamp(pg_micros)?;
        unix_micros.checked_mul(1_000).ok_or_else(|| {
            Self::invalid_datum(format!(
                "PostgreSQL timestamp value {pg_micros} microseconds overflows i64 \
                 nanosecond range required by Iceberg timestamp_ns"
            ))
        })
    }

    /// Convert a Unix-epoch nanosecond timestamp to Unix-epoch microseconds.
    ///
    /// Uses floor division (`div_euclid`) rather than truncating division so
    /// chronological order is preserved across the epoch: nanosecond values
    /// strictly before the Unix epoch round *down*, not toward zero.
    /// PostgreSQL's microsecond resolution can't represent the lost
    /// sub-microsecond nanos either way; flooring is the only behaviour that
    /// makes `read(write(x)) == x` hold for negative timestamps when paired
    /// with [`Self::unix_nanos_from_timestamp`].
    fn unix_micros_from_nanos(unix_nanos: i64) -> i64 {
        unix_nanos.div_euclid(1_000)
    }

    /// Read a single timestamp value from an Arrow timestamp column and
    /// normalize it to Unix microseconds.
    ///
    /// `is_nanos` selects the physical Arrow column type:
    /// - `false` → `TimestampMicrosecondType` (used by both Iceberg
    ///   `timestamp` and `timestamptz`)
    /// - `true`  → `TimestampNanosecondType` (used by both Iceberg
    ///   `timestamp_ns` and `timestamptz_ns`); the value is truncated to
    ///   microseconds via [`Self::unix_micros_from_nanos`].
    ///
    /// Output is always in Unix microseconds. Whether the surrounding column
    /// represents tz-naive or tz-aware data is the caller's concern; this
    /// helper deliberately ignores the timezone dimension because both
    /// Timestamp and Timestamptz Arrow columns are physically `i64` micros
    /// (or `i64` nanos when `is_nanos`).
    fn read_unix_micros_from_arrow(
        column: &dyn Array,
        row_idx: usize,
        is_nanos: bool,
    ) -> i64 {
        if is_nanos {
            let array = column.as_primitive::<TimestampNanosecondType>();
            Self::unix_micros_from_nanos(array.value(row_idx))
        } else {
            let array = column.as_primitive::<TimestampMicrosecondType>();
            array.value(row_idx)
        }
    }

    fn unix_micros_to_pg_micros(unix_micros: i64) -> IcebergResult<i64> {
        unix_micros.checked_sub(PG_EPOCH_USECS_DIFF).ok_or_else(|| {
            Self::invalid_datum(format!(
                "Iceberg timestamp value {unix_micros} microseconds overflows PostgreSQL epoch"
            ))
        })
    }

    fn invalid_datum(message: impl Into<String>) -> IcebergError {
        IcebergError::DatumConversionError(message.into())
    }
}

struct FixedCodec {
    len: usize,
}

impl FixedCodec {
    fn new(len: usize) -> Self {
        Self { len }
    }

    fn validate(&self, actual_len: usize) -> IcebergResult<()> {
        if actual_len == self.len {
            return Ok(());
        }

        Err(IcebergError::IncompatibleColumnType(
            format!("fixed[{}]", self.len),
            format!("BYTEA length {actual_len}"),
        ))
    }
}

struct DecimalCodec {
    precision: u32,
    scale: u32,
}

impl DecimalCodec {
    fn new(precision: u32, scale: u32) -> Self {
        Self { precision, scale }
    }

    fn encode(&self, value: &AnyNumeric) -> IcebergResult<i128> {
        let scale_factor = 10_i128.pow(self.scale);
        let scaled = value.clone() * scale_factor;
        let integral = scaled.floor();

        if integral != scaled {
            return Err(self.error(
                value,
                format!("has more than {} fractional digits", self.scale),
            ));
        }

        let encoded = i128::try_from(integral)
            .map_err(|_| self.error(value, "cannot be encoded as Decimal128"))?;

        if !self.fits_precision(encoded) {
            return Err(self.error(value, "exceeds target precision"));
        }

        Ok(encoded)
    }

    fn fits_precision(&self, value: i128) -> bool {
        let limit = self.precision_limit();
        (-limit..=limit).contains(&value)
    }

    fn precision_limit(&self) -> i128 {
        10_i128.pow(self.precision) - 1
    }

    fn error(&self, value: &AnyNumeric, reason: impl Into<String>) -> IcebergError {
        IcebergError::IncompatibleColumnType(
            format!("decimal({}, {})", self.precision, self.scale),
            format!("numeric value '{}' {}", value, reason.into()),
        )
    }
}

/// Implementation of ArrowToCell for PrimitiveType.
impl ArrowToCell for PrimitiveType {
    fn extract(
        &self,
        column: &dyn Array,
        row_idx: usize,
    ) -> IcebergResult<Option<Cell>> {
        match self {
            PrimitiveType::Boolean => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .ok_or(IcebergError::ArrowTypeMismatch("Boolean".to_string()))?;
                Ok(Some(Cell::Bool(array.value(row_idx))))
            }
            PrimitiveType::Int => {
                let array = column.as_primitive::<Int32Type>();
                Ok(Some(Cell::I32(array.value(row_idx))))
            }
            PrimitiveType::Long => {
                let array = column.as_primitive::<Int64Type>();
                Ok(Some(Cell::I64(array.value(row_idx))))
            }
            PrimitiveType::Float => {
                let array = column.as_primitive::<Float32Type>();
                Ok(Some(Cell::F32(array.value(row_idx))))
            }
            PrimitiveType::Double => {
                let array = column.as_primitive::<Float64Type>();
                Ok(Some(Cell::F64(array.value(row_idx))))
            }
            PrimitiveType::String => {
                let s = match column.data_type() {
                    arrow_schema::DataType::Utf8 => {
                        column.as_string::<i32>().value(row_idx)
                    }
                    arrow_schema::DataType::LargeUtf8 => {
                        column.as_string::<i64>().value(row_idx)
                    }
                    _ => {
                        return Err(IcebergError::ArrowTypeMismatch(format!(
                            "String or LargeString (actual: {:?})",
                            column.data_type()
                        )));
                    }
                };
                Ok(Some(Cell::StringView(StringView {
                    ptr: s.as_ptr(),
                    len: s.len(),
                })))
            }
            PrimitiveType::Binary => {
                let bytes = match column.data_type() {
                    arrow_schema::DataType::Binary => {
                        column.as_binary::<i32>().value(row_idx)
                    }
                    arrow_schema::DataType::LargeBinary => {
                        column.as_binary::<i64>().value(row_idx)
                    }
                    _ => {
                        return Err(IcebergError::ArrowTypeMismatch(format!(
                            "Binary or LargeBinary (actual: {:?})",
                            column.data_type()
                        )));
                    }
                };
                Ok(Some(Cell::ByteaView(ByteaView {
                    ptr: bytes.as_ptr(),
                    len: bytes.len(),
                })))
            }
            PrimitiveType::Fixed(_) => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                    .ok_or(IcebergError::ArrowTypeMismatch(
                        "FixedSizeBinary".to_string(),
                    ))?;
                let bytes = array.value(row_idx);
                Ok(Some(Cell::ByteaView(ByteaView {
                    ptr: bytes.as_ptr(),
                    len: bytes.len(),
                })))
            }
            PrimitiveType::Date => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::Date32Array>()
                    .ok_or(IcebergError::ArrowTypeMismatch("Date32".to_string()))?;
                let arrow_days = array.value(row_idx);
                Ok(Some(Cell::Date(TemporalCodec::pg_date_from_arrow_days(
                    arrow_days,
                )?)))
            }
            PrimitiveType::Time => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                    .ok_or(IcebergError::ArrowTypeMismatch(
                        "Time64Microsecond".to_string(),
                    ))?;
                let micros = array.value(row_idx);
                Ok(Some(Cell::Time(TemporalCodec::time_from_micros(micros)?)))
            }
            PrimitiveType::Timestamp | PrimitiveType::TimestampNs => {
                let unix_micros = TemporalCodec::read_unix_micros_from_arrow(
                    column,
                    row_idx,
                    matches!(self, PrimitiveType::TimestampNs),
                );
                Ok(Some(Cell::Timestamp(
                    TemporalCodec::timestamp_from_unix_micros(unix_micros)?,
                )))
            }
            PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                let unix_micros = TemporalCodec::read_unix_micros_from_arrow(
                    column,
                    row_idx,
                    matches!(self, PrimitiveType::TimestamptzNs),
                );
                Ok(Some(Cell::Timestamptz(
                    TemporalCodec::timestamptz_from_unix_micros(unix_micros)?,
                )))
            }
            PrimitiveType::Uuid => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                    .ok_or(IcebergError::ArrowTypeMismatch(
                        "FixedSizeBinary (UUID)".to_string(),
                    ))?;
                let bytes = array.value(row_idx);
                if bytes.len() != 16 {
                    return Err(IcebergError::ArrowTypeMismatch(
                        "UUID must be 16 bytes".to_string(),
                    ));
                }
                let mut uuid_bytes = [0u8; 16];
                // Iceberg/Arrow UUID bytes are RFC 4122 network-order bytes,
                // which is the order pgrx::Uuid expects here.
                uuid_bytes.copy_from_slice(bytes);
                Ok(Some(Cell::Uuid(Uuid::from_bytes(uuid_bytes))))
            }
            PrimitiveType::Decimal { precision, scale } => {
                let array =
                    column.as_primitive::<arrow_array::types::Decimal128Type>();
                // `From<DecimalCodecError> for IcebergError` routes each
                // codec-error variant to the right layer (datatype mismatch,
                // data exception, or codec bug). See `error.rs`.
                let codec = Decimal128NumericCodec::new(*precision, *scale)?;
                let numeric = codec.decode(array.value(row_idx))?;
                Ok(Some(Cell::Numeric(numeric)))
            }
        }
    }
}

/// Implementation of RowsToArrow for PrimitiveType.
impl RowsToArrow for PrimitiveType {
    fn build(&self, rows: &[Row], col_idx: usize) -> IcebergResult<ArrayRef> {
        macro_rules! build_array {
            ($builder_ty:ty, [ $($pattern:pat => $value:expr),+ ]) => {{
                let mut builder = <$builder_ty>::with_capacity(rows.len());
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        $($pattern => builder.append_value($value),)+
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }};
        }

        match self {
            PrimitiveType::Boolean => build_array!(
                arrow_array::builder::BooleanBuilder,
                [Some(Cell::Bool(v)) => *v]
            ),
            PrimitiveType::Int => build_array!(
                arrow_array::builder::Int32Builder,
                [
                    Some(Cell::I32(v)) => *v,
                    Some(Cell::I16(v)) => *v as i32,
                    Some(Cell::I8(v)) => *v as i32
                ]
            ),
            PrimitiveType::Long => build_array!(
                arrow_array::builder::Int64Builder,
                [
                    Some(Cell::I64(v)) => *v,
                    Some(Cell::I32(v)) => *v as i64,
                    Some(Cell::I16(v)) => *v as i64
                ]
            ),
            PrimitiveType::Float => build_array!(
                arrow_array::builder::Float32Builder,
                [Some(Cell::F32(v)) => *v]
            ),
            PrimitiveType::Double => build_array!(
                arrow_array::builder::Float64Builder,
                [
                    Some(Cell::F64(v)) => *v,
                    Some(Cell::F32(v)) => *v as f64
                ]
            ),
            PrimitiveType::String => {
                let mut builder = arrow_array::builder::StringBuilder::with_capacity(
                    rows.len(),
                    1024,
                );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::String(v)) => builder.append_value(v),
                        Some(Cell::StringView(v)) => {
                            builder.append_value(unsafe { v.as_str() })
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Binary => {
                let mut builder =
                    arrow_array::builder::LargeBinaryBuilder::with_capacity(
                        rows.len(),
                        1024,
                    );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Bytea(bytes)) => builder.append_value(bytes),
                        Some(Cell::ByteaView(bytes)) => {
                            builder.append_value(unsafe { bytes.as_slice() });
                        }
                        Some(Cell::Json(bytes)) => {
                            builder.append_value(bytes);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Fixed(len) => {
                // `ValidateSupported` (in `schema.rs`) rejects `Fixed(len)`
                // whose width does not fit `i32` *before* a batch builder is
                // constructed, so this `try_from` never fails on a
                // well-formed pipeline. Surface the failure as an invariant
                // violation rather than silently truncating with `as i32`, so
                // any future regression in the validator (or a write path
                // reached without going through it) shows up as a clear
                // error instead of a wrong-width column.
                let fixed_len_i32 = i32::try_from(*len).map_err(|_| {
                    IcebergError::InvariantViolated(
                        "Fixed(len) exceeds i32 reached the writer; \
                         ValidateSupported must reject this earlier",
                    )
                })?;
                let fixed_len = fixed_len_i32 as usize;
                let fixed = FixedCodec::new(fixed_len);
                let mut builder =
                    arrow_array::builder::FixedSizeBinaryBuilder::with_capacity(
                        rows.len(),
                        fixed_len_i32,
                    );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Bytea(bytes)) => {
                            fixed.validate(bytes.len())?;
                            builder
                                .append_value(bytes)
                                .map_err(IcebergError::ArrowError)?;
                        }
                        Some(Cell::ByteaView(bytes)) => {
                            let bytes = unsafe { bytes.as_slice() };
                            fixed.validate(bytes.len())?;
                            builder
                                .append_value(bytes)
                                .map_err(IcebergError::ArrowError)?;
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Date => build_array!(
                arrow_array::builder::Date32Builder,
                [Some(Cell::Date(d)) => {
                    TemporalCodec::arrow_days_from_pg_date(d)?
                }]
            ),
            PrimitiveType::Time => build_array!(
                arrow_array::builder::Time64MicrosecondBuilder,
                [Some(Cell::Time(t)) => {
                    let pg_micros: i64 = (*t).into();
                    pg_micros
                }]
            ),
            PrimitiveType::Timestamp => build_array!(
                arrow_array::builder::TimestampMicrosecondBuilder,
                [Some(Cell::Timestamp(ts)) => {
                    let pg_micros: i64 = (*ts).into();
                    TemporalCodec::unix_micros_from_timestamp(pg_micros)?
                }]
            ),
            PrimitiveType::TimestampNs => build_array!(
                arrow_array::builder::TimestampNanosecondBuilder,
                [Some(Cell::Timestamp(ts)) => {
                    let pg_micros: i64 = (*ts).into();
                    TemporalCodec::unix_nanos_from_timestamp(pg_micros)?
                }]
            ),
            PrimitiveType::Timestamptz => {
                let mut builder =
                    arrow_array::builder::TimestampMicrosecondBuilder::with_capacity(
                        rows.len(),
                    );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Timestamptz(ts)) => {
                            let pg_micros: i64 = (*ts).into();
                            builder.append_value(
                                TemporalCodec::unix_micros_from_timestamp(pg_micros)?,
                            );
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish().with_timezone("+00:00")))
            }
            PrimitiveType::TimestamptzNs => {
                let mut builder =
                    arrow_array::builder::TimestampNanosecondBuilder::with_capacity(
                        rows.len(),
                    );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Timestamptz(ts)) => {
                            let pg_micros: i64 = (*ts).into();
                            builder.append_value(
                                TemporalCodec::unix_nanos_from_timestamp(pg_micros)?,
                            );
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish().with_timezone("+00:00")))
            }
            PrimitiveType::Uuid => {
                let mut builder =
                    arrow_array::builder::FixedSizeBinaryBuilder::with_capacity(
                        rows.len(),
                        16,
                    );
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Uuid(u)) => {
                            builder
                                .append_value(u.as_bytes())
                                .map_err(IcebergError::ArrowError)?;
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            PrimitiveType::Decimal { precision, scale } => {
                let decimal = DecimalCodec::new(*precision, *scale);
                let mut builder =
                    arrow_array::builder::Decimal128Builder::with_capacity(
                        rows.len(),
                    )
                    .with_precision_and_scale(*precision as u8, *scale as i8)
                    .map_err(IcebergError::ArrowError)?;
                for row in rows {
                    match row.get(col_idx).and_then(|c| c.as_ref()) {
                        Some(Cell::Numeric(n)) => {
                            let encoded = decimal.encode(n)?;
                            builder.append_value(encoded);
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
        }
    }
}
