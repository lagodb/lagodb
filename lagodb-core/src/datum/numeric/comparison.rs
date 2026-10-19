//! PostgreSQL NUMERIC values bound against finite Decimal128 columns.

use pgrx::{pg_sys, varlena_to_byte_slice};

use super::{
    Decimal128NumericCodec, DecimalCodecError, NUMERIC_NEG, NUMERIC_POS,
    pg_numeric_send,
};

const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;

/// A PostgreSQL NUMERIC comparison value relative to a finite Decimal128 column.
///
/// Decimal128 storage cannot contain PostgreSQL's special NUMERIC values, but
/// a parameter with the same declared typmod can. Keeping those cases typed
/// lets exact predicate binders simplify comparisons instead of failing after
/// planning accepted the predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decimal128ComparisonValue {
    /// An exactly scaled Decimal128 coefficient.
    Finite(i128),
    /// PostgreSQL numeric negative infinity, below every finite value.
    NegativeInfinity,
    /// PostgreSQL numeric positive infinity, above every finite value.
    PositiveInfinity,
    /// PostgreSQL numeric NaN, above every finite value and positive infinity.
    NaN,
}

impl Decimal128NumericCodec {
    /// Bind any PostgreSQL NUMERIC comparison value against this finite
    /// Decimal128 domain.
    ///
    /// # Safety
    ///
    /// The caller must run on the current PostgreSQL backend thread and pass a
    /// valid, non-NULL NUMERIC datum that remains live for this call.
    pub unsafe fn encode_comparison_datum(
        &self,
        datum: pg_sys::Datum,
    ) -> Result<Decimal128ComparisonValue, DecimalCodecError> {
        // SAFETY: identical NUMERIC datum contract to `encode_bound_datum`.
        let output = unsafe {
            pg_sys::DirectFunctionCall1Coll(
                Some(pg_numeric_send),
                pg_sys::InvalidOid,
                datum,
            )
        };
        // SAFETY: numeric_send always returns a non-NULL bytea Datum.
        let bytes = unsafe { varlena_to_byte_slice(output.cast_mut_ptr()) };
        let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
        let result = match sign {
            NUMERIC_POS | NUMERIC_NEG => self
                .numeric_send_to_i128(bytes)
                .map(Decimal128ComparisonValue::Finite),
            NUMERIC_NINF => Ok(Decimal128ComparisonValue::NegativeInfinity),
            NUMERIC_PINF => Ok(Decimal128ComparisonValue::PositiveInfinity),
            NUMERIC_NAN => Ok(Decimal128ComparisonValue::NaN),
            _ => Err(self.value_out_of_range(
                "PostgreSQL numeric value has an unknown binary sign code",
            )),
        };
        // SAFETY: DirectFunctionCall1Coll returned a fresh palloc'd bytea.
        unsafe { pg_sys::pfree(output.cast_mut_ptr()) };
        result
    }
}
