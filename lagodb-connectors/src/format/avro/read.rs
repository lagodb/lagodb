//! Bound Avro column decoding shared by FDW scans and COPY FROM.

use apache_avro::Schema;
use apache_avro::types::Value;
use lagodb_core::datum::{
    BoundColumnDatumCodec, ByteaView, Cell, ColumnDatumTarget, PG_EPOCH_DAYS_DIFF,
    PG_EPOCH_USECS_DIFF, StringView, numeric_precision_scale,
};
use pgrx::datum::{USECS_PER_DAY, Uuid as PgUuid};
use pgrx::pg_sys;
use pgrx::prelude::{Date, Time, Timestamp, TimestampWithTimeZone};

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::AvroValueKind;

#[derive(Clone, Copy)]
pub(crate) struct AvroReadColumn {
    source: usize,
    kind: AvroValueKind,
    codec: BoundColumnDatumCodec,
}

impl AvroReadColumn {
    pub(crate) fn bind(
        source: usize,
        schema: &Schema,
        target_oid: pg_sys::Oid,
        target_typmod: i32,
    ) -> Result<Self, ConnectorError> {
        let kind = AvroValueKind::from_schema(schema)?;
        if !kind.supports_target(target_oid) {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                format!(
                    "Avro field type cannot be represented by PostgreSQL type OID {target_oid}",
                ),
            ));
        }
        if let AvroValueKind::Decimal(codec) = kind
            && let Some(target) = numeric_precision_scale(target_typmod)
            && (target.precision != codec.precision()
                || target.scale != codec.scale() as i32)
        {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "Avro decimal precision and scale must match the PostgreSQL numeric column",
            ));
        }
        Ok(Self {
            source,
            kind,
            codec: BoundColumnDatumCodec::bind(ColumnDatumTarget::from_oid(
                target_oid,
            ))?,
        })
    }

    pub(crate) const fn source(self) -> usize {
        self.source
    }

    /// Typmod already enforced when materializing this source datum.
    pub(super) fn datum_typmod(self) -> i32 {
        match self.kind {
            AvroValueKind::Decimal(codec) => codec.typmod(),
            _ => -1,
        }
    }

    /// Decodes a field that was already bound against this object's writer schema.
    pub(super) fn decode(
        self,
        value: &Value,
    ) -> Result<Option<Cell>, ConnectorError> {
        let value = match value {
            Value::Union(_, value) => value.as_ref(),
            value => value,
        };
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        match (self.kind, value) {
            (AvroValueKind::Boolean, Value::Boolean(value)) => {
                Ok(Some(Cell::Bool(*value)))
            }
            (AvroValueKind::Int, Value::Int(value)) => Ok(Some(Cell::I32(*value))),
            (AvroValueKind::Long, Value::Long(value)) => Ok(Some(Cell::I64(*value))),
            (AvroValueKind::Float, Value::Float(value)) => {
                Ok(Some(Cell::F32(*value)))
            }
            (AvroValueKind::Double, Value::Double(value)) => {
                Ok(Some(Cell::F64(*value)))
            }
            (AvroValueKind::Bytes, Value::Bytes(value)) => {
                // SAFETY: the datum conversion immediately copies this view into
                // PostgreSQL-owned memory before the decoded Avro record drops.
                Ok(Some(Cell::ByteaView(unsafe {
                    ByteaView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::Bytes, Value::Fixed(_, value)) => {
                // SAFETY: identical lifetime argument as the bytes branch.
                Ok(Some(Cell::ByteaView(unsafe {
                    ByteaView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::String, Value::String(value)) => {
                // SAFETY: Avro strings are UTF-8 and the datum conversion copies
                // the view before this record is advanced.
                Ok(Some(Cell::StringView(unsafe {
                    StringView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::String, Value::Enum(_, value)) => {
                // SAFETY: Avro enum symbols are UTF-8 schema strings.
                Ok(Some(Cell::StringView(unsafe {
                    StringView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::Uuid, Value::Uuid(value)) => {
                Ok(Some(Cell::Uuid(PgUuid::from_bytes(*value.as_bytes()))))
            }
            (AvroValueKind::Date, Value::Date(value)) => {
                let days = value
                    .checked_sub(PG_EPOCH_DAYS_DIFF)
                    .ok_or_else(|| self.out_of_range())?;
                if days == i32::MIN || days == i32::MAX {
                    return Err(self.out_of_range());
                }
                let date = Date::try_from(days).map_err(|_| self.out_of_range())?;
                Ok(Some(Cell::Date(date)))
            }
            (AvroValueKind::TimeMicros, Value::TimeMillis(value)) => {
                let micros = i64::from(*value)
                    .checked_mul(1_000)
                    .ok_or_else(|| self.out_of_range())?;
                self.time(micros)
            }
            (AvroValueKind::TimeMicros, Value::TimeMicros(value)) => {
                self.time(*value)
            }
            (AvroValueKind::TimestampMicros, Value::TimestampMillis(value)) => self
                .timestamp(
                    (*value)
                        .checked_mul(1_000)
                        .ok_or_else(|| self.out_of_range())?,
                ),
            (AvroValueKind::TimestampMicros, Value::TimestampMicros(value)) => {
                self.timestamp(*value)
            }
            (
                AvroValueKind::LocalTimestampMicros,
                Value::LocalTimestampMillis(value),
            ) => self.local_timestamp(
                (*value)
                    .checked_mul(1_000)
                    .ok_or_else(|| self.out_of_range())?,
            ),
            (
                AvroValueKind::LocalTimestampMicros,
                Value::LocalTimestampMicros(value),
            ) => self.local_timestamp(*value),
            (AvroValueKind::Decimal(codec), Value::Decimal(value)) => {
                let bytes = Vec::try_from(value).map_err(ConnectorError::from)?;
                Ok(Some(Cell::Numeric(codec.decode_signed_be_bytes(&bytes)?)))
            }
            _ => Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "a decoded Avro datum does not match its writer-schema field",
            )),
        }
    }

    fn timestamp(self, unix_micros: i64) -> Result<Option<Cell>, ConnectorError> {
        let pg_micros = unix_micros
            .checked_sub(PG_EPOCH_USECS_DIFF)
            .ok_or_else(|| self.out_of_range())?;
        if pg_micros == i64::MIN || pg_micros == i64::MAX {
            return Err(self.out_of_range());
        }
        let timestamp = TimestampWithTimeZone::try_from(pg_micros)
            .map_err(|_| self.out_of_range())?;
        Ok(Some(Cell::Timestamptz(timestamp)))
    }

    fn time(self, micros: i64) -> Result<Option<Cell>, ConnectorError> {
        if !(0..USECS_PER_DAY).contains(&micros) {
            return Err(self.out_of_range());
        }
        let time = Time::try_from(micros).map_err(|_| self.out_of_range())?;
        Ok(Some(Cell::Time(time)))
    }

    fn local_timestamp(
        self,
        unix_micros: i64,
    ) -> Result<Option<Cell>, ConnectorError> {
        let pg_micros = unix_micros
            .checked_sub(PG_EPOCH_USECS_DIFF)
            .ok_or_else(|| self.out_of_range())?;
        if pg_micros == i64::MIN || pg_micros == i64::MAX {
            return Err(self.out_of_range());
        }
        let timestamp =
            Timestamp::try_from(pg_micros).map_err(|_| self.out_of_range())?;
        Ok(Some(Cell::Timestamp(timestamp)))
    }

    fn out_of_range(self) -> ConnectorError {
        ConnectorError::row_conversion(
            FormatKind::Avro,
            "an Avro temporal value is outside the PostgreSQL range",
        )
    }

    pub(crate) unsafe fn datum(
        self,
        value: &Value,
    ) -> Result<Option<pg_sys::Datum>, ConnectorError> {
        let Some(cell) = self.decode(value)? else {
            return Ok(None);
        };
        // SAFETY: the FDW scan callback owns the current PostgreSQL memory
        // context, and the bound codec targets this output column's OID.
        unsafe {
            self.codec
                .cell_to_datum(cell)
                .map(Some)
                .map_err(ConnectorError::from)
        }
    }
}
