//! PostgreSQL NUMERIC values bound to fixed-point domains of up to 76 digits.
//!
//! This is the PostgreSQL side of a fixed-width decimal engine value: the
//! coefficient is exchanged as a signed big-endian 256-bit integer, so the
//! core layer stays independent of Arrow and a domain may exceed the engine's
//! 128-bit coefficients, as the bound of a comparison over them does.

use pgrx::{pg_sys, varlena_to_byte_slice};

use super::coefficient::{I256_BYTES, Unsigned256};
use super::{
    DecimalCodecError, NUMERIC_NAN, NUMERIC_NEG, NUMERIC_NINF, NUMERIC_PINF,
    NUMERIC_POS, numeric_precision_scale, pg_numeric_send,
};

const DECIMAL_DIGITS_PER_GROUP: i32 = 4;
const NUMERIC_BASE: u64 = 10_000;

/// A finite `NUMERIC(precision, scale)` domain whose coefficients fit a
/// signed 256-bit integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumericScaledDomain {
    precision: u8,
    scale: u8,
}

/// Direction that selects a domain value for a value between two of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericRounding {
    Floor,
    Ceiling,
}

/// One PostgreSQL NUMERIC value placed in a [`NumericScaledDomain`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericScaledValue {
    /// The rounded coefficient as signed big-endian two's-complement bytes.
    /// `exact` is false when the value lies strictly between two domain
    /// values.
    Finite {
        coefficient: [u8; I256_BYTES],
        exact: bool,
    },
    /// Below every domain value: negative infinity, or a finite value whose
    /// rounded magnitude needs more than the domain precision.
    Below,
    /// Above every domain value: positive infinity, NaN (which PostgreSQL
    /// orders above every other value), or a finite value too large for the
    /// domain precision.
    Above,
}

/// A finite magnitude scaled to a domain, before its sign is applied.
enum ScaledMagnitude {
    Fits { magnitude: Unsigned256, exact: bool },
    TooLarge,
}

struct NumericSendValue<'a> {
    weight: i32,
    sign: u16,
    dscale: u16,
    digits: &'a [u8],
}

impl<'a> NumericSendValue<'a> {
    /// Parse `numeric_send` output: four big-endian u16/i16 header fields and
    /// `ndigits` base-10000 digits.
    fn parse(bytes: &'a [u8]) -> Self {
        let field = |offset: usize| [bytes[offset], bytes[offset + 1]];
        let ndigits = usize::from(u16::from_be_bytes(field(0)));
        Self {
            weight: i32::from(i16::from_be_bytes(field(2))),
            sign: u16::from_be_bytes(field(4)),
            dscale: u16::from_be_bytes(field(6)),
            digits: &bytes[8..8 + ndigits * 2],
        }
    }

    /// Scale the absolute value by `10^scale`, truncating toward zero.
    fn scaled_magnitude(&self, scale: u8, limit: Unsigned256) -> ScaledMagnitude {
        let mut magnitude = Unsigned256::ZERO;
        let mut exact = true;
        // Decimal exponent of the last retained group in the coefficient.
        let mut trailing = 0i32;
        for (index, &group) in self.digits.as_chunks::<2>().0.iter().enumerate() {
            let group = u64::from(u16::from_be_bytes(group));
            let exponent = (self.weight - index as i32) * DECIMAL_DIGITS_PER_GROUP
                + i32::from(scale);
            if exponent <= -DECIMAL_DIGITS_PER_GROUP {
                exact &= group == 0;
                continue;
            }
            let (multiplier, retained) = if exponent < 0 {
                let divisor = 10_u64.pow(exponent.unsigned_abs());
                exact &= group % divisor == 0;
                (NUMERIC_BASE / divisor, group / divisor)
            } else {
                (NUMERIC_BASE, group)
            };
            match magnitude.checked_mul_add(multiplier, retained) {
                Some(next) if next < limit => magnitude = next,
                _ => return ScaledMagnitude::TooLarge,
            }
            trailing = exponent.max(0);
        }
        for _ in 0..trailing {
            match magnitude.checked_mul_add(10, 0) {
                Some(next) if next < limit => magnitude = next,
                _ => return ScaledMagnitude::TooLarge,
            }
        }
        ScaledMagnitude::Fits { magnitude, exact }
    }
}

impl NumericScaledDomain {
    /// Largest decimal precision of a signed 256-bit coefficient
    /// (10^76 < 2^255), matching Arrow `Decimal256`.
    pub const MAX_PRECISION: u8 = 76;

    #[must_use]
    pub const fn new(precision: u8, scale: u8) -> Option<Self> {
        if precision == 0 || precision > Self::MAX_PRECISION || scale > precision {
            None
        } else {
            Some(Self { precision, scale })
        }
    }

    /// The fixed decimal holding every value of a `numeric(p, s)` column.
    /// PostgreSQL keeps `p - s` integer digits and a display scale of
    /// `max(s, 0)` (`numeric.c` `apply_typmod`): a negative scale stores
    /// multiples of `10^-s` as `(p - s, 0)`, and a scale above the precision
    /// stores values below `10^(p - s)` within `(s, s)`. Both domains are the
    /// narrowest supersets of PostgreSQL's value set. `None` for an unbounded
    /// typmod or a domain beyond 76 digits.
    #[must_use]
    pub fn for_typmod(typmod: i32) -> Option<Self> {
        let typmod = numeric_precision_scale(typmod)?;
        let (precision, scale) = match u32::try_from(typmod.scale) {
            Ok(scale) => (typmod.precision.max(scale), scale),
            Err(_) => (
                typmod.precision.checked_add(typmod.scale.unsigned_abs())?,
                0,
            ),
        };
        Self::new(u8::try_from(precision).ok()?, u8::try_from(scale).ok()?)
    }

    #[inline]
    pub const fn precision(self) -> u8 {
        self.precision
    }

    #[inline]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// The narrowest domain holding a finite NUMERIC value exactly at its own
    /// display scale. PostgreSQL arithmetic derives result scales from these
    /// display scales, so a constant keeps its written trailing zeros. `None`
    /// for NaN, an infinity, or a value needing more than 76 digits.
    ///
    /// # Safety
    ///
    /// Must run on a PostgreSQL backend thread with a valid, non-NULL NUMERIC
    /// datum that remains live for this call.
    #[must_use]
    pub unsafe fn of_datum(datum: pg_sys::Datum) -> Option<Self> {
        // SAFETY: forwarded caller contract.
        unsafe {
            Self::with_send_value(datum, |value| {
                if !matches!(value.sign, NUMERIC_POS | NUMERIC_NEG) {
                    return None;
                }
                let scale = u8::try_from(value.dscale)
                    .ok()
                    .filter(|scale| *scale <= Self::MAX_PRECISION)?;
                let ScaledMagnitude::Fits { magnitude, exact } = value
                    .scaled_magnitude(
                        scale,
                        Unsigned256::pow10(u32::from(Self::MAX_PRECISION)),
                    )
                else {
                    return None;
                };
                // PostgreSQL rounds every NUMERIC to its display scale.
                debug_assert!(exact);
                let digits = if magnitude.is_zero() {
                    1
                } else {
                    u8::try_from(magnitude.decimal_digit_count()).ok()?
                };
                Self::new(digits.max(scale), scale)
            })
        }
    }

    /// Place a NUMERIC value in this domain, rounding a value between two
    /// domain values in the requested direction.
    ///
    /// # Safety
    ///
    /// Must run on a PostgreSQL backend thread with a valid, non-NULL NUMERIC
    /// datum that remains live for this call.
    pub unsafe fn bind_datum(
        self,
        datum: pg_sys::Datum,
        rounding: NumericRounding,
    ) -> Result<NumericScaledValue, DecimalCodecError> {
        // SAFETY: forwarded caller contract.
        unsafe { Self::with_send_value(datum, |value| self.bind(&value, rounding)) }
    }

    fn bind(
        self,
        value: &NumericSendValue<'_>,
        rounding: NumericRounding,
    ) -> Result<NumericScaledValue, DecimalCodecError> {
        let negative = match value.sign {
            NUMERIC_POS => false,
            NUMERIC_NEG => true,
            NUMERIC_PINF | NUMERIC_NAN => return Ok(NumericScaledValue::Above),
            NUMERIC_NINF => return Ok(NumericScaledValue::Below),
            _ => {
                return Err(DecimalCodecError::InvalidBinaryRepresentation {
                    message:
                        "PostgreSQL numeric value has an unknown binary sign code"
                            .to_owned(),
                });
            }
        };
        let outside = if negative {
            NumericScaledValue::Below
        } else {
            NumericScaledValue::Above
        };
        let limit = Unsigned256::pow10(u32::from(self.precision));
        let ScaledMagnitude::Fits {
            mut magnitude,
            exact,
        } = value.scaled_magnitude(self.scale, limit)
        else {
            return Ok(outside);
        };
        let away_from_zero = match rounding {
            NumericRounding::Floor => negative,
            NumericRounding::Ceiling => !negative,
        };
        if !exact && away_from_zero {
            match magnitude.checked_mul_add(1, 1) {
                Some(next) if next < limit => magnitude = next,
                _ => return Ok(outside),
            }
        }
        Ok(NumericScaledValue::Finite {
            coefficient: magnitude.to_signed_be_bytes(negative),
            exact,
        })
    }

    /// # Safety
    ///
    /// Must run on a PostgreSQL backend thread with a valid, non-NULL NUMERIC
    /// datum that remains live for this call.
    unsafe fn with_send_value<R>(
        datum: pg_sys::Datum,
        read: impl FnOnce(NumericSendValue<'_>) -> R,
    ) -> R {
        // SAFETY: the caller supplies the bound NUMERIC datum required by
        // `numeric_send`, which returns a palloc'd bytea.
        let output = unsafe {
            pg_sys::DirectFunctionCall1Coll(
                Some(pg_numeric_send),
                pg_sys::InvalidOid,
                datum,
            )
        };
        // SAFETY: numeric_send always returns a non-NULL bytea Datum.
        let bytes = unsafe { varlena_to_byte_slice(output.cast_mut_ptr()) };
        let result = read(NumericSendValue::parse(bytes));
        // SAFETY: DirectFunctionCall1Coll returned a fresh palloc'd bytea.
        unsafe { pg_sys::pfree(output.cast_mut_ptr()) };
        result
    }
}
