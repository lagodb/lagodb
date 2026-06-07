//! `Decimal128` conversion: the codec that scales a PostgreSQL `NUMERIC` into
//! the `i128` an Arrow `Decimal128(precision, scale)` column stores, plus the
//! read and write paths built on it.

use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, Decimal128Builder};
use arrow_array::{Array, ArrayRef, Decimal128Array};
use pg_lakebase_core::tuple::{Cell, Decimal128NumericCodec, PgDatumRef};
use pgrx::prelude::AnyNumeric;
use pgrx::{FromDatum, pg_sys};

use super::{ColumnAppend, cell_type_mismatch, downcast, read_oid};
use crate::error::{ConvError, ConvResult};

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

pub(crate) struct DecimalCodec {
    precision: u32,
    scale: u32,
}

impl DecimalCodec {
    pub(crate) fn new(precision: u32, scale: u32) -> Self {
        Self { precision, scale }
    }

    pub(crate) fn encode(&self, value: &AnyNumeric) -> ConvResult<i128> {
        let scaled = value.clone() * 10_i128.pow(self.scale);
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
        let limit = 10_i128.pow(self.precision) - 1;
        (-limit..=limit).contains(&value)
    }

    fn error(&self, value: &AnyNumeric, reason: impl Into<String>) -> ConvError {
        ConvError::IncompatibleColumnType(
            format!("decimal({}, {})", self.precision, self.scale),
            format!("numeric value '{}' {}", value, reason.into()),
        )
    }
}

// ---------------------------------------------------------------------------
// Write encoder
// ---------------------------------------------------------------------------

pub(crate) struct Decimal128Encoder {
    builder: Decimal128Builder,
    codec: DecimalCodec,
    precision: u32,
    scale: u32,
    bytes: usize,
}

impl Decimal128Encoder {
    pub(crate) fn with_capacity(capacity: usize, precision: u32, scale: u32) -> Self {
        Self {
            builder: Decimal128Builder::with_capacity(capacity),
            codec: DecimalCodec::new(precision, scale),
            precision,
            scale,
            bytes: 0,
        }
    }

    fn append_scaled(&mut self, value: &AnyNumeric) -> ConvResult<()> {
        self.builder.append_value(self.codec.encode(value)?);
        self.bytes += std::mem::size_of::<i128>();
        Ok(())
    }
}

impl ColumnAppend for Decimal128Encoder {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<()> {
        let n = unsafe {
            read_oid(
                datum,
                pg_sys::NUMERICOID,
                AnyNumeric::from_datum,
                "Decimal128 encoder: datum source type is not numeric",
            )
        }?;
        self.append_scaled(&n)
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        let Cell::Numeric(n) = cell else {
            return Err(cell_type_mismatch("numeric"));
        };
        self.append_scaled(n)
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ConvResult<ArrayRef> {
        self.bytes = 0;
        // The builder is untyped; the precision/scale tag must be applied to the
        // finished array.
        Ok(Arc::new(self.builder.finish().with_precision_and_scale(
            self.precision as u8,
            self.scale as i8,
        )?))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn estimated_size(&self) -> usize {
        self.bytes
    }
}

// ---------------------------------------------------------------------------
// Read (Arrow → Cell)
// ---------------------------------------------------------------------------

pub(crate) fn extract(
    column: &dyn Array,
    row_idx: usize,
    precision: u32,
    scale: u32,
) -> ConvResult<Cell> {
    let raw = downcast::<Decimal128Array>(column, "Decimal128")?.value(row_idx);
    let codec = Decimal128NumericCodec::new(precision, scale)?;
    Ok(Cell::Numeric(codec.decode(raw)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Decimal128Array};
    use arrow_schema::DataType;

    // The numeric->i128 scaling needs a backend, but `finish` must carry the
    // scaled values through untouched and tag the array with precision/scale.
    #[test]
    fn finish_preserves_scaled_values_and_tags_precision_scale() {
        let mut encoder = Decimal128Encoder::with_capacity(4, 10, 2);
        encoder.builder.append_value(12_345);
        encoder.builder.append_null();
        encoder.builder.append_value(-6_789);

        let array = encoder.finish().expect("finish");
        assert_eq!(array.data_type(), &DataType::Decimal128(10, 2));
        let decimals = array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128Array");
        assert_eq!(decimals.value(0), 12_345);
        assert!(decimals.is_null(1));
        assert_eq!(decimals.value(2), -6_789);
    }
}
