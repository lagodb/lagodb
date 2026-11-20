//! Binary Decimal128 decoding into PostgreSQL numeric values.

use pgrx::{AnyNumeric, pg_sys};

use super::{
    Decimal128NumericCodec, DecimalCodecError, MAX_NDIGITS, NumericExternal,
    receive_numeric_wire, receive_numeric_wire_datum,
};

impl Decimal128NumericCodec {
    /// Decodes an unscaled Decimal128 value into a freshly allocated
    /// `AnyNumeric`, going through PostgreSQL's binary `numeric_recv` path.
    ///
    /// This avoids the `to_string` + `numeric_in` round-trip that pgrx's
    /// `AnyNumeric::try_from(&str)` and `From<i128>` implementations use,
    /// which is the dominant cost of decimal columns on million-row scans.
    ///
    /// A value that does not fit `NUMERIC(precision, scale)` is reported as
    /// [`DecimalCodecError::ValueOutOfRange`] before `numeric_recv`, whose
    /// `apply_typmod` would reject it with a hard ERROR. Other PG errors
    /// unwind to the enclosing provider/FFI boundary.
    pub fn decode(&self, unscaled: i128) -> Result<AnyNumeric, DecimalCodecError> {
        self.check_precision(unscaled)?;
        let external = NumericExternal::from_decimal128(unscaled, self.scale);
        let mut wire = [0u8; 8 + MAX_NDIGITS * 2];
        let len = external.wire_byte_len();
        external.write_be_bytes(&mut wire[..len]);
        // SAFETY: the codec produced a complete numeric_recv wire value that
        // satisfies this typmod; decoding runs on a PostgreSQL backend thread.
        Ok(unsafe { receive_numeric_wire(&mut wire[..len], self.typmod()) })
    }

    /// Decode directly into the current memory context without an AnyNumeric
    /// copy. Callers such as the JSONB builder own the enclosing error boundary.
    ///
    /// # Safety
    /// Must run on a PostgreSQL backend thread with the destination context
    /// selected. PostgreSQL errors must be captured by the caller.
    pub unsafe fn decode_datum(&self, unscaled: i128) -> pg_sys::Datum {
        // SAFETY: this entry point uses the source type as the target type.
        unsafe { self.decode_datum_with_typmod(unscaled, self.typmod()) }
    }

    /// Decode into a declared PostgreSQL numeric type without a text round-trip.
    /// The source scale interprets the coefficient; the target typmod constrains
    /// the resulting value through numeric_recv's existing apply_typmod path.
    ///
    /// # Safety
    /// The same backend, memory-context and error-boundary requirements as
    /// [`Self::decode_datum`] apply. `target_typmod` must be a PG numeric typmod
    /// or -1 for unconstrained numeric.
    pub unsafe fn decode_datum_with_typmod(
        &self,
        unscaled: i128,
        target_typmod: i32,
    ) -> pg_sys::Datum {
        let external = NumericExternal::from_decimal128(unscaled, self.scale);
        let mut wire = [0u8; 8 + MAX_NDIGITS * 2];
        let len = external.wire_byte_len();
        external.write_be_bytes(&mut wire[..len]);
        // SAFETY: the codec produced a complete numeric_recv wire value.
        unsafe { receive_numeric_wire_datum(&mut wire[..len], target_typmod) }
    }
}
