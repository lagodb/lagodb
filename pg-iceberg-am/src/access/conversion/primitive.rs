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

/// Convert PostgreSQL-epoch days (the raw `DateADT` value, i.e. days since
/// 2000-01-01) to Unix-epoch days (the Arrow/Iceberg `date` encoding, days
/// since 1970-01-01).
///
/// This is the single PG→Unix day-offset conversion shared by the storage
/// write side ([`TemporalCodec::arrow_days_from_pg_date`]) and the runtime
/// predicate translator (`customscan::predicate_translator::IcebergDatumDecoder::decode`), so a
/// pushed `date` bound is encoded with the *same* offset as the stored
/// manifest bounds (Requirement 3.5). It reuses the shared
/// [`PG_EPOCH_DAYS_DIFF`] constant rather than re-deriving the offset.
///
/// Returns `None` on `i32` overflow, which both call sites surface as a
/// structured error (the value is not representable as an Iceberg `date`).
pub(crate) fn pg_epoch_days_to_unix_days(pg_days: i32) -> Option<i32> {
    pg_days.checked_add(PG_EPOCH_DAYS_DIFF)
}

/// Convert PostgreSQL-epoch microseconds (microseconds since 2000-01-01) to
/// Unix-epoch microseconds (the Arrow/Iceberg `timestamp` / `timestamptz`
/// encoding, microseconds since 1970-01-01).
///
/// The single PG→Unix microsecond-offset conversion shared by the storage
/// write side ([`TemporalCodec::unix_micros_from_timestamp`]) and the runtime
/// predicate translator, so a pushed `timestamp` / `timestamptz` bound aligns
/// with the stored manifest bounds (Requirement 3.5). Reuses the shared
/// [`PG_EPOCH_USECS_DIFF`] constant.
///
/// Returns `None` on `i64` overflow.
pub(crate) fn pg_epoch_micros_to_unix_micros(pg_micros: i64) -> Option<i64> {
    pg_micros.checked_add(PG_EPOCH_USECS_DIFF)
}

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
        pg_epoch_days_to_unix_days(date.to_pg_epoch_days()).ok_or_else(|| {
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
        pg_epoch_micros_to_unix_micros(pg_micros).ok_or_else(|| {
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

// =============================================================================
// Task 4.1 — Round-trip / epoch-consistency: translator pushed bounds vs the
// storage write side (`TemporalCodec`) for `date` / `timestamp` / `timestamptz`.
//
// Feature: pushdown-capability-mismatch, Task 4.1 (integration / round-trip)
//
// The runtime predicate translator (`customscan::predicate_translator::IcebergDatumDecoder::decode`)
// encodes a pushed `date` / `timestamp` / `timestamptz` bound into an iceberg
// `Datum`, and the storage write side (`TemporalCodec` in this module) encodes
// the *stored* column values into the Arrow/Iceberg manifest representation.
// For Iceberg-side pruning to be sound, BOTH ends must apply the SAME PG->Unix
// epoch offset (`PG_EPOCH_DAYS_DIFF` / `PG_EPOCH_USECS_DIFF`) — otherwise a
// pushed predicate bound would be compared against stored manifest bounds on a
// different epoch and prune the wrong files (Requirement 3.5).
//
// These tests drive the SAME raw PG `Datum` through BOTH ends and assert the
// translator's pushed `Datum` equals the value produced by the write side's
// `TemporalCodec` conversion — proving the two share one offset without needing
// a full end-to-end table write+scan. They are host `#[test]` / `proptest`s
// because every conversion they touch is pure pgrx datum arithmetic
// (`Date`/`Timestamp`/`TimestampWithTimeZone::from_datum`, the shared
// `pg_epoch_*` helpers, and the checked-add `TemporalCodec` math) — none of
// which call into a live PG backend (per `docs/testing.md`, mirroring the
// existing `decode_date_applies_shared_epoch_offset` host tests in
// `customscan/predicate_translator/datum_decoder.rs`).
// =============================================================================
#[cfg(test)]
mod epoch_consistency_tests {
    use super::{
        TemporalCodec, pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros,
    };
    use crate::customscan::predicate_translator::IcebergDatumDecoder;
    use iceberg_lite::spec::Datum;
    use pg_lakebase_core::tuple::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
    use pgrx::prelude::{Date, Timestamp, TimestampWithTimeZone};
    use pgrx::{FromDatum, pg_sys};
    use proptest::prelude::*;

    /// Buffer (in PG-epoch days) kept away from both ends of the `i32` range so
    /// that (a) we never generate the `±infinity` date sentinels
    /// (`i32::MIN` / `i32::MAX`) and (b) adding `PG_EPOCH_DAYS_DIFF` (10957)
    /// can never overflow `i32`. Any value in the generated range is therefore
    /// a finite, representable date at BOTH ends.
    const DATE_GUARD: i32 = 20_000;

    /// PostgreSQL's minimum valid finite `timestamp` / `timestamptz` value in
    /// PG-epoch microseconds (`pgrx`'s `MIN_TIMESTAMP_USEC`, i.e. 4714-11-24 BC).
    /// `Timestamp` / `TimestampWithTimeZone::from_datum` reject (panic on) any
    /// value below this, so the generator must stay at or above it to model
    /// only *representable* timestamps (the task's domain).
    const MIN_PG_TS_USEC: i64 = -211_813_488_000_000_000;

    /// PostgreSQL's maximum valid finite `timestamp` / `timestamptz` value in
    /// PG-epoch microseconds (`pgrx`'s `MAX_TIMESTAMP_USEC`, i.e. 294276 AD).
    const MAX_PG_TS_USEC: i64 = 9_223_371_331_199_999_999;

    /// The largest PG-epoch micros value for which adding `PG_EPOCH_USECS_DIFF`
    /// stays within `i64` (so BOTH ends produce a `Datum` rather than the
    /// shared overflow → not-representable result). Near the very top of PG's
    /// valid range the offset addition would overflow `i64`; both the
    /// translator and the write side return an error there (consistently), but
    /// to exercise the *agreement on a produced bound* we cap the generator
    /// below the overflow point.
    const TS_OFFSET_SAFE_MAX: i64 = i64::MAX - PG_EPOCH_USECS_DIFF;

    /// Upper bound of the timestamp generator: the tighter of PG's max valid
    /// value and the offset-overflow-safe max. Any value in
    /// `MIN_PG_TS_USEC..=TS_GEN_MAX` is finite, representable by
    /// `from_datum`, and offset-convertible at BOTH ends.
    const TS_GEN_MAX: i64 = if MAX_PG_TS_USEC < TS_OFFSET_SAFE_MAX {
        MAX_PG_TS_USEC
    } else {
        TS_OFFSET_SAFE_MAX
    };

    /// Build a PG `date` `Datum` directly from a raw `DateADT` (PG-epoch days).
    /// This is exactly what `Date::into_datum` produces (`Datum::from(self.0)`),
    /// so both the translator decode and `Date::from_datum` round-trip it.
    fn date_datum(pg_days: i32) -> pg_sys::Datum {
        pg_sys::Datum::from(pg_days)
    }

    /// Build a PG `timestamp` / `timestamptz` `Datum` directly from raw
    /// PG-epoch microseconds.
    fn ts_datum(pg_micros: i64) -> pg_sys::Datum {
        pg_sys::Datum::from(pg_micros)
    }

    // -------------------------------------------------------------------------
    // Concrete cross-checks (unit tests): pin the offset agreement at the Unix
    // epoch and at an arbitrary representable instant.
    // -------------------------------------------------------------------------

    /// At the Unix epoch the translator and the write side agree: the PG-epoch
    /// day `-PG_EPOCH_DAYS_DIFF` (1970-01-01) is iceberg day 0 on BOTH ends.
    #[test]
    fn date_epoch_consistency_at_unix_epoch() {
        let pg_days = -PG_EPOCH_DAYS_DIFF;
        let datum = date_datum(pg_days);

        // Translator (pushed bound).
        let pushed = unsafe { IcebergDatumDecoder::decode(pg_sys::DATEOID, datum) }
            .expect("epoch date must decode on the translator side");

        // Storage write side (stored manifest bound).
        let date = unsafe { Date::from_datum(datum, false) }
            .expect("epoch date must decode into a pgrx Date");
        let write_arrow_days = TemporalCodec::arrow_days_from_pg_date(&date)
            .expect("epoch date must encode on the write side");

        assert_eq!(write_arrow_days, 0, "Unix epoch must be iceberg day 0");
        assert_eq!(
            pushed,
            Datum::date(write_arrow_days),
            "pushed date bound must equal the write side's stored bound",
        );
    }

    /// At the Unix epoch the timestamp translator and write side agree on micros
    /// 0 (PG-epoch micros `-PG_EPOCH_USECS_DIFF`).
    #[test]
    fn timestamp_epoch_consistency_at_unix_epoch() {
        let pg_micros = -PG_EPOCH_USECS_DIFF;
        let datum = ts_datum(pg_micros);

        let pushed =
            unsafe { IcebergDatumDecoder::decode(pg_sys::TIMESTAMPOID, datum) }
                .expect("epoch timestamp must decode on the translator side");

        let ts = unsafe { Timestamp::from_datum(datum, false) }
            .expect("epoch timestamp must decode into a pgrx Timestamp");
        let write_unix_micros = TemporalCodec::unix_micros_from_timestamp(ts.into())
            .expect("epoch timestamp must encode on the write side");

        assert_eq!(write_unix_micros, 0, "Unix epoch must be 0 micros");
        assert_eq!(
            pushed,
            Datum::timestamp_micros(write_unix_micros),
            "pushed timestamp bound must equal the write side's stored bound",
        );
    }

    // -------------------------------------------------------------------------
    // Property: for every representable date / timestamp / timestamptz value,
    // the translator's pushed `Datum` uses the SAME epoch offset as the storage
    // write side's `TemporalCodec` conversion (Requirement 3.5).
    // -------------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig {
            // 256 cases per run, matching the host PBTs in `translator.rs` and
            // `pg-lakebase-core` (well above the >=100 floor). Failing examples
            // persist to `proptest-regressions/<test>.txt` for replay.
            cases: 256,
            ..ProptestConfig::default()
        })]

        /// Property (Requirement 3.5): for every representable PG `date`, the
        /// translator's pushed iceberg `Datum::date` equals
        /// `Datum::date(TemporalCodec::arrow_days_from_pg_date(date))` — i.e.
        /// both ends apply the shared `PG_EPOCH_DAYS_DIFF` offset, so a pushed
        /// `date` predicate bound aligns with stored manifest bounds.
        ///
        /// Generator domain: finite, in-range PG-epoch days (excludes the
        /// `±infinity` sentinels and any value whose offset would overflow).
        ///
        /// **Validates: Requirements 3.5**
        #[test]
        fn pushed_date_bound_matches_write_side_offset(
            pg_days in (i32::MIN + DATE_GUARD)..=(i32::MAX - DATE_GUARD),
        ) {
            let datum = date_datum(pg_days);

            // Translator: the pushed predicate bound.
            let pushed = unsafe { IcebergDatumDecoder::decode(pg_sys::DATEOID, datum) }
                .expect("a representable date must decode on the translator side");

            // Storage write side: the stored manifest bound.
            let date = unsafe { Date::from_datum(datum, false) }
                .expect("a representable date must decode into a pgrx Date");
            let write_arrow_days = TemporalCodec::arrow_days_from_pg_date(&date)
                .expect("a representable date must encode on the write side");

            // Both ends must apply the SAME shared offset.
            prop_assert_eq!(
                write_arrow_days,
                pg_epoch_days_to_unix_days(pg_days)
                    .expect("offset must not overflow for a guarded value"),
            );
            prop_assert_eq!(pushed, Datum::date(write_arrow_days));
        }

        /// Property (Requirement 3.5): for every representable PG `timestamp`,
        /// the translator's pushed `Datum::timestamp_micros` equals
        /// `Datum::timestamp_micros(TemporalCodec::unix_micros_from_timestamp(..))`
        /// — both ends apply the shared `PG_EPOCH_USECS_DIFF` offset.
        ///
        /// **Validates: Requirements 3.5**
        #[test]
        fn pushed_timestamp_bound_matches_write_side_offset(
            pg_micros in MIN_PG_TS_USEC..=TS_GEN_MAX,
        ) {
            let datum = ts_datum(pg_micros);

            let pushed = unsafe { IcebergDatumDecoder::decode(pg_sys::TIMESTAMPOID, datum) }
                .expect("a representable timestamp must decode on the translator side");

            let ts = unsafe { Timestamp::from_datum(datum, false) }
                .expect("a representable timestamp must decode into a pgrx Timestamp");
            let write_unix_micros =
                TemporalCodec::unix_micros_from_timestamp(ts.into())
                    .expect("a representable timestamp must encode on the write side");

            prop_assert_eq!(
                write_unix_micros,
                pg_epoch_micros_to_unix_micros(pg_micros)
                    .expect("offset must not overflow for a guarded value"),
            );
            prop_assert_eq!(pushed, Datum::timestamp_micros(write_unix_micros));
        }

        /// Property (Requirement 3.5): for every representable PG `timestamptz`,
        /// the translator's pushed `Datum::timestamptz_micros` equals
        /// `Datum::timestamptz_micros(TemporalCodec::unix_micros_from_timestamp(..))`.
        /// PG stores `timestamptz` as UTC microseconds, and the write side uses
        /// the same `unix_micros_from_timestamp` conversion as `timestamp`
        /// (see the `Timestamptz` build arm), so the shared
        /// `PG_EPOCH_USECS_DIFF` offset must hold here too.
        ///
        /// **Validates: Requirements 3.5**
        #[test]
        fn pushed_timestamptz_bound_matches_write_side_offset(
            pg_micros in MIN_PG_TS_USEC..=TS_GEN_MAX,
        ) {
            let datum = ts_datum(pg_micros);

            let pushed = unsafe { IcebergDatumDecoder::decode(pg_sys::TIMESTAMPTZOID, datum) }
                .expect("a representable timestamptz must decode on the translator side");

            let ts = unsafe { TimestampWithTimeZone::from_datum(datum, false) }
                .expect("a representable timestamptz must decode into a pgrx value");
            let write_unix_micros =
                TemporalCodec::unix_micros_from_timestamp(ts.into())
                    .expect("a representable timestamptz must encode on the write side");

            prop_assert_eq!(
                write_unix_micros,
                pg_epoch_micros_to_unix_micros(pg_micros)
                    .expect("offset must not overflow for a guarded value"),
            );
            prop_assert_eq!(pushed, Datum::timestamptz_micros(write_unix_micros));
        }
    }
}
