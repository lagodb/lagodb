//! Conversion of bound Avro fields to PostgreSQL datums.

use apache_avro::Schema;
use lagodb_core::datum::{
    BoundColumnDatumCodec, ByteaView, Cell, ColumnDatumTarget, PG_EPOCH_DAYS_DIFF,
    PG_EPOCH_USECS_DIFF, StringView, numeric_precision_scale,
};
use pgrx::datum::{USECS_PER_DAY, Uuid as PgUuid};
use pgrx::pg_sys;
use pgrx::prelude::{Date, Time, Timestamp, TimestampWithTimeZone};

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::super::AvroValueKind;
use super::AvroRow;
use super::decoder::AvroDatum;

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
    pub(crate) fn datum_typmod(self) -> i32 {
        match self.kind {
            AvroValueKind::Decimal(codec) => codec.typmod(),
            _ => -1,
        }
    }

    /// Converts a field decoded with this column's writer-schema binding.
    fn decode(self, value: AvroDatum<'_>) -> Result<Option<Cell>, ConnectorError> {
        if matches!(value, AvroDatum::Null) {
            return Ok(None);
        }
        match (self.kind, value) {
            (AvroValueKind::Boolean, AvroDatum::Boolean(value)) => {
                Ok(Some(Cell::Bool(value)))
            }
            (AvroValueKind::Int, AvroDatum::Int(value)) => Ok(Some(Cell::I32(value))),
            (AvroValueKind::Long, AvroDatum::Long(value)) => {
                Ok(Some(Cell::I64(value)))
            }
            (AvroValueKind::Float, AvroDatum::Float(value)) => {
                Ok(Some(Cell::F32(value)))
            }
            (AvroValueKind::Double, AvroDatum::Double(value)) => {
                Ok(Some(Cell::F64(value)))
            }
            (AvroValueKind::Bytes, AvroDatum::Bytes(value)) => {
                // SAFETY: the datum conversion copies this view of the block
                // into PostgreSQL-owned memory before the reader advances.
                Ok(Some(Cell::ByteaView(unsafe {
                    ByteaView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::String, AvroDatum::String(value)) => {
                // SAFETY: a `str` is UTF-8, and the datum conversion copies the
                // view before the reader advances.
                Ok(Some(Cell::StringView(unsafe {
                    StringView::from_raw_parts(value.as_ptr(), value.len())
                })))
            }
            (AvroValueKind::Uuid, AvroDatum::Uuid(value)) => {
                Ok(Some(Cell::Uuid(PgUuid::from_bytes(value))))
            }
            (AvroValueKind::Date, AvroDatum::Date(value)) => {
                let days = value
                    .checked_sub(PG_EPOCH_DAYS_DIFF)
                    .ok_or_else(|| self.out_of_range())?;
                if days == i32::MIN || days == i32::MAX {
                    return Err(self.out_of_range());
                }
                let date = Date::try_from(days).map_err(|_| self.out_of_range())?;
                Ok(Some(Cell::Date(date)))
            }
            (AvroValueKind::TimeMicros, AvroDatum::TimeMillis(value)) => {
                let micros = i64::from(value)
                    .checked_mul(1_000)
                    .ok_or_else(|| self.out_of_range())?;
                self.time(micros)
            }
            (AvroValueKind::TimeMicros, AvroDatum::TimeMicros(value)) => {
                self.time(value)
            }
            (AvroValueKind::TimestampMicros, AvroDatum::TimestampMillis(value)) => {
                self.timestamp(
                    value
                        .checked_mul(1_000)
                        .ok_or_else(|| self.out_of_range())?,
                )
            }
            (AvroValueKind::TimestampMicros, AvroDatum::TimestampMicros(value)) => {
                self.timestamp(value)
            }
            (
                AvroValueKind::LocalTimestampMicros,
                AvroDatum::LocalTimestampMillis(value),
            ) => self.local_timestamp(
                value
                    .checked_mul(1_000)
                    .ok_or_else(|| self.out_of_range())?,
            ),
            (
                AvroValueKind::LocalTimestampMicros,
                AvroDatum::LocalTimestampMicros(value),
            ) => self.local_timestamp(value),
            (AvroValueKind::Decimal(codec), AvroDatum::Decimal(value)) => {
                Ok(Some(Cell::Numeric(codec.decode_signed_be_bytes(value)?)))
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

    /// Converts this column's field of `row`.
    ///
    /// # Safety
    ///
    /// The caller must have selected the PostgreSQL memory context that owns
    /// the produced datum, and `row` must come from the reader whose plan
    /// bound this column's source field.
    pub(crate) unsafe fn datum(
        self,
        row: &AvroRow<'_>,
    ) -> Result<Option<pg_sys::Datum>, ConnectorError> {
        let Some(cell) = self.decode(row.datum(self.source)?)? else {
            return Ok(None);
        };
        // SAFETY: the caller selected the memory context for this datum, and
        // the bound codec targets this column's OID.
        unsafe {
            self.codec
                .cell_to_datum(cell)
                .map(Some)
                .map_err(ConnectorError::from)
        }
    }
}
