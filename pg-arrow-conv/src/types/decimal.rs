//! `Decimal128` conversion: the codec that scales a PostgreSQL `NUMERIC` into
//! the `i128` an Arrow `Decimal128(precision, scale)` column stores, plus the
//! read and write paths built on it.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, Decimal128Builder};
use pg_lakebase_core::tuple::Cell;
use pgrx::prelude::AnyNumeric;
use pgrx::{FromDatum, pg_sys};

use super::{ColumnAppend, cell_type_mismatch, read_bound};
use crate::error::{ArrowConversionError, ArrowConversionResult};

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

    pub(crate) fn encode(&self, value: &AnyNumeric) -> ArrowConversionResult<i128> {
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

    fn error(
        &self,
        value: &AnyNumeric,
        reason: impl Into<String>,
    ) -> ArrowConversionError {
        ArrowConversionError::IncompatibleColumnType(
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
}

impl Decimal128Encoder {
    pub(crate) fn with_capacity(capacity: usize, precision: u32, scale: u32) -> Self {
        Self {
            builder: Decimal128Builder::with_capacity(capacity),
            codec: DecimalCodec::new(precision, scale),
            precision,
            scale,
        }
    }

    fn append_scaled(&mut self, value: &AnyNumeric) -> ArrowConversionResult<()> {
        self.builder.append_value(self.codec.encode(value)?);
        Ok(())
    }

    /// Append a non-NULL PostgreSQL `numeric` datum after encoding its scale.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `numeric` Datum.
    pub(super) unsafe fn append_bound(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let numeric = unsafe {
            read_bound(
                datum,
                AnyNumeric::from_datum,
                "Decimal128 encoder: present numeric datum read as null",
            )
        }?;
        self.append_scaled(&numeric)?;
        Ok(std::mem::size_of::<i128>())
    }
}

impl ColumnAppend for Decimal128Encoder {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        let Cell::Numeric(n) = cell else {
            return Err(cell_type_mismatch("numeric"));
        };
        self.append_scaled(n)
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
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
}

// ---------------------------------------------------------------------------
// Read (Arrow → Cell): handled by the bound `ColumnReader` in `crate::read`,
// which builds the `Decimal128NumericCodec` once per batch (not per value).
// ---------------------------------------------------------------------------

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
