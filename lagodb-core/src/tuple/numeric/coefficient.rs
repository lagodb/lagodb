//! Fixed-width aggregate coefficient to PostgreSQL NUMERIC materialization.

use std::panic::AssertUnwindSafe;

use pgrx::prelude::PgSqlErrorCode;
use pgrx::{AnyNumeric, PgTryBuilder, pg_sys};

use crate::diag::PgReportError;

use super::super::varlena::DetoastedVarlena;
use super::{
    NUMERIC_DSCALE_MASK, NUMERIC_NEG, NUMERIC_POS, receive_numeric_wire,
    receive_numeric_wire_datum,
};

const DECIMAL_DIGITS_PER_GROUP: i32 = 4;
const NUMERIC_BASE: u64 = 10_000;
const I256_BYTES: usize = 32;
const I256_LIMBS: usize = 4;
const I256_DECIMAL_DIGITS: usize = 77;
const I256_NUMERIC_DIGITS: usize = I256_DECIMAL_DIGITS / 4 + 1;
const I256_WIRE_BYTES: usize = 8 + I256_NUMERIC_DIGITS * 2;

/// Binary materializer for exact aggregate coefficients.
///
/// The input is Arrow `i256`'s signed big-endian two's-complement byte form.
/// Keeping that representation at the API boundary avoids coupling
/// `lagodb-core` to Arrow while the conversion and PostgreSQL binary protocol
/// remain owned by the shared tuple codec layer.
pub struct PostgresNumericCodec;

impl PostgresNumericCodec {
    pub fn numeric_from_i256_be_bytes(
        coefficient: [u8; I256_BYTES],
        scale: u32,
    ) -> Result<AnyNumeric, PgReportError> {
        let mut wire = Self::wire(coefficient, scale)?;
        let len = wire.len;
        PgTryBuilder::new(AssertUnwindSafe(move || {
            // SAFETY: NumericWire constructs one complete numeric_recv external
            // value in stack storage and this runs during backend execution.
            Ok(unsafe { receive_numeric_wire(&mut wire.bytes[..len], -1) })
        }))
        .catch_others(|error| Err(PgReportError::from_caught(error)))
        .execute()
    }

    /// Materialize a SUM result directly into the owned varlena bytes expected
    /// by the DataFusion Binary output, avoiding the intermediate AnyNumeric
    /// copy and a second PostgreSQL allocation.
    pub fn varlena_from_i256_be_bytes(
        coefficient: [u8; I256_BYTES],
        scale: u32,
    ) -> Result<Vec<u8>, PgReportError> {
        let mut wire = Self::wire(coefficient, scale)?;
        let len = wire.len;
        PgTryBuilder::new(AssertUnwindSafe(move || {
            // SAFETY: the wire is complete stack-owned numeric_recv input.
            let datum =
                unsafe { receive_numeric_wire_datum(&mut wire.bytes[..len], -1) };
            let bytes = {
                // SAFETY: numeric_recv returned a live PostgreSQL varlena datum.
                let numeric = unsafe { DetoastedVarlena::from_datum(datum) };
                numeric.full_varlena_bytes().to_vec()
            };
            // SAFETY: the datum is copied into owned bytes and remains allocated
            // by PostgreSQL until this matching release.
            unsafe { pg_sys::pfree(datum.cast_mut_ptr()) };
            Ok(bytes)
        }))
        .catch_others(|error| Err(PgReportError::from_caught(error)))
        .execute()
    }

    fn wire(
        coefficient: [u8; I256_BYTES],
        scale: u32,
    ) -> Result<NumericWire, PgReportError> {
        let scale = u16::try_from(scale)
            .ok()
            .filter(|scale| *scale <= NUMERIC_DSCALE_MASK)
            .ok_or_else(|| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                    "numeric aggregate scale exceeds PostgreSQL's binary NUMERIC limit",
                )
            })?;
        Ok(NumericWire::from_coefficient(
            SignedCoefficient::from_be_bytes(coefficient),
            scale,
        ))
    }
}

#[derive(Clone, Copy)]
struct SignedCoefficient {
    magnitude: Unsigned256,
    negative: bool,
}

impl SignedCoefficient {
    fn from_be_bytes(mut bytes: [u8; I256_BYTES]) -> Self {
        let negative = bytes[0] & 0x80 != 0;
        if negative {
            for byte in &mut bytes {
                *byte = !*byte;
            }
            for byte in bytes.iter_mut().rev() {
                let (next, carry) = byte.overflowing_add(1);
                *byte = next;
                if !carry {
                    break;
                }
            }
        }
        let magnitude = Unsigned256::from_be_bytes(bytes);
        Self {
            magnitude,
            negative: negative && !magnitude.is_zero(),
        }
    }
}

#[derive(Clone, Copy)]
struct Unsigned256 {
    /// Little-endian limbs, so division walks this array in reverse.
    limbs: [u64; I256_LIMBS],
}

impl Unsigned256 {
    fn from_be_bytes(bytes: [u8; I256_BYTES]) -> Self {
        let limbs = std::array::from_fn(|index| {
            let start = I256_BYTES - (index + 1) * std::mem::size_of::<u64>();
            u64::from_be_bytes(
                bytes[start..start + std::mem::size_of::<u64>()]
                    .try_into()
                    .expect("a fixed i256 byte array contains four u64 limbs"),
            )
        });
        Self { limbs }
    }

    fn is_zero(self) -> bool {
        self.limbs.iter().all(|limb| *limb == 0)
    }

    fn decimal_digit_count(self) -> i32 {
        let mut value = self;
        let mut groups = 0;
        let mut highest = 0;
        while !value.is_zero() {
            highest = value.div_rem(NUMERIC_BASE);
            groups += 1;
        }
        (groups - 1) * DECIMAL_DIGITS_PER_GROUP + Self::group_digit_count(highest)
    }

    const fn group_digit_count(group: u64) -> i32 {
        if group >= 1_000 {
            4
        } else if group >= 100 {
            3
        } else if group >= 10 {
            2
        } else {
            1
        }
    }

    fn div_rem(&mut self, divisor: u64) -> u64 {
        let mut remainder = 0u128;
        for limb in self.limbs.iter_mut().rev() {
            let value = (remainder << u64::BITS) | u128::from(*limb);
            *limb = u64::try_from(value / u128::from(divisor))
                .expect("u256 long division produces one u64 quotient limb");
            remainder = value % u128::from(divisor);
        }
        u64::try_from(remainder)
            .expect("u256 long-division remainder is smaller than its u64 divisor")
    }
}

struct NumericWire {
    bytes: [u8; I256_WIRE_BYTES],
    len: usize,
}

impl NumericWire {
    fn from_coefficient(coefficient: SignedCoefficient, scale: u16) -> Self {
        if coefficient.magnitude.is_zero() {
            return Self::encode(&[], 0, NUMERIC_POS, scale);
        }

        let decimal_digits = coefficient.magnitude.decimal_digit_count();
        let decimal_weight = decimal_digits - i32::from(scale) - 1;
        let weight = if decimal_weight >= 0 {
            (decimal_weight + DECIMAL_DIGITS_PER_GROUP) / DECIMAL_DIGITS_PER_GROUP - 1
        } else {
            -((-decimal_weight - 1) / DECIMAL_DIGITS_PER_GROUP + 1)
        };
        let offset = (weight + 1) * DECIMAL_DIGITS_PER_GROUP - decimal_weight - 1;
        let padding =
            u32::try_from((offset + decimal_digits) % DECIMAL_DIGITS_PER_GROUP)
                .expect("base-10000 alignment padding is in 0..4");

        let mut value = coefficient.magnitude;
        let mut digits = [0u16; I256_NUMERIC_DIGITS];
        let mut count = 0usize;
        if padding != 0 {
            let factor = 10_u64.pow(padding);
            let remainder = value.div_rem(factor);
            count += 1;
            digits[I256_NUMERIC_DIGITS - count] =
                u16::try_from(remainder * 10_u64.pow(4 - padding))
                    .expect("one aligned PostgreSQL NUMERIC digit is below 10000");
        }
        while !value.is_zero() {
            count += 1;
            digits[I256_NUMERIC_DIGITS - count] =
                u16::try_from(value.div_rem(NUMERIC_BASE))
                    .expect("one PostgreSQL NUMERIC digit is below 10000");
        }

        let sign = if coefficient.negative {
            NUMERIC_NEG
        } else {
            NUMERIC_POS
        };
        let weight = i16::try_from(weight)
            .expect("i256 coefficient and PostgreSQL dscale fit an i16 weight");
        Self::encode(&digits[I256_NUMERIC_DIGITS - count..], weight, sign, scale)
    }

    fn encode(digits: &[u16], weight: i16, sign: u16, scale: u16) -> Self {
        let len = 8 + digits.len() * 2;
        let mut bytes = [0u8; I256_WIRE_BYTES];
        let count = u16::try_from(digits.len())
            .expect("fixed i256 NUMERIC wire has at most twenty digits");
        bytes[0..2].copy_from_slice(&count.to_be_bytes());
        bytes[2..4].copy_from_slice(&weight.to_be_bytes());
        bytes[4..6].copy_from_slice(&sign.to_be_bytes());
        bytes[6..8].copy_from_slice(&scale.to_be_bytes());
        for (index, digit) in digits.iter().enumerate() {
            let start = 8 + index * 2;
            bytes[start..start + 2].copy_from_slice(&digit.to_be_bytes());
        }
        Self { bytes, len }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed_i256(value: i128) -> [u8; I256_BYTES] {
        let mut bytes = [if value.is_negative() { 0xff } else { 0 }; I256_BYTES];
        bytes[I256_BYTES - std::mem::size_of::<i128>()..]
            .copy_from_slice(&value.to_be_bytes());
        bytes
    }

    fn field(bytes: &[u8], offset: usize) -> u16 {
        u16::from_be_bytes([bytes[offset], bytes[offset + 1]])
    }

    fn signed_field(bytes: &[u8], offset: usize) -> i16 {
        i16::from_be_bytes([bytes[offset], bytes[offset + 1]])
    }

    fn wire(value: i128, scale: u16) -> NumericWire {
        NumericWire::from_coefficient(
            SignedCoefficient::from_be_bytes(signed_i256(value)),
            scale,
        )
    }

    #[test]
    fn aligns_coefficient_to_postgres_base_10000_groups() {
        let wire = wire(12_345, 2);
        assert_eq!(field(&wire.bytes, 0), 2);
        assert_eq!(signed_field(&wire.bytes, 2), 0);
        assert_eq!(field(&wire.bytes, 4), NUMERIC_POS);
        assert_eq!(field(&wire.bytes, 6), 2);
        assert_eq!(field(&wire.bytes, 8), 123);
        assert_eq!(field(&wire.bytes, 10), 4_500);
        assert_eq!(wire.len, 12);
    }

    #[test]
    fn preserves_fractional_weight_and_negative_sign() {
        let wire = wire(-1, 4);
        assert_eq!(field(&wire.bytes, 0), 1);
        assert_eq!(signed_field(&wire.bytes, 2), -1);
        assert_eq!(field(&wire.bytes, 4), NUMERIC_NEG);
        assert_eq!(field(&wire.bytes, 6), 4);
        assert_eq!(field(&wire.bytes, 8), 1);
    }

    #[test]
    fn zero_retains_aggregate_display_scale() {
        let wire = wire(0, 18);
        assert_eq!(field(&wire.bytes, 0), 0);
        assert_eq!(field(&wire.bytes, 2), 0);
        assert_eq!(field(&wire.bytes, 4), NUMERIC_POS);
        assert_eq!(field(&wire.bytes, 6), 18);
        assert_eq!(wire.len, 8);
    }

    #[test]
    fn signed_i256_minimum_fits_the_fixed_wire_buffer() {
        let mut minimum = [0; I256_BYTES];
        minimum[0] = 0x80;
        let wire = NumericWire::from_coefficient(
            SignedCoefficient::from_be_bytes(minimum),
            0,
        );
        assert_eq!(usize::from(field(&wire.bytes, 0)), I256_NUMERIC_DIGITS);
        assert_eq!(field(&wire.bytes, 4), NUMERIC_NEG);
        assert_eq!(wire.len, I256_WIRE_BYTES);
    }
}
