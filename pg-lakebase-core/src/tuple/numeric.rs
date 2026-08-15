//! NUMERIC typmod encoding/decoding, PG/Unix epoch constants, and a fast
//! `Decimal128 -> AnyNumeric` codec used by columnar read paths.

use pgrx::pg_sys::{self, POSTGRES_EPOCH_JDATE, UNIX_EPOCH_JDATE};
use pgrx::{AnyNumeric, IntoDatum, varlena_to_byte_slice};

/// PostgreSQL epoch (2000-01-01) minus Unix epoch (1970-01-01) in days.
pub const PG_EPOCH_DAYS_DIFF: i32 = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i32;

/// PostgreSQL epoch (2000-01-01) minus Unix epoch (1970-01-01) in microseconds.
pub const PG_EPOCH_USECS_DIFF: i64 =
    (PG_EPOCH_DAYS_DIFF as i64) * pgrx::datum::USECS_PER_DAY;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericTypmod {
    pub precision: u32,
    pub scale: i32,
}

/// Encodes precision and scale into a PostgreSQL NUMERIC type modifier.
///
/// PostgreSQL stores scale as an 11-bit signed value.
pub fn numeric_typmod(precision: u32, scale: i32) -> i32 {
    (((precision as i32) << 16) | (scale & 0x7FF)) + pg_sys::VARHDRSZ as i32
}

/// Decodes precision and scale from a PostgreSQL NUMERIC type modifier.
///
/// Returns `None` if the typmod is not a valid numeric typmod. PostgreSQL
/// supports negative NUMERIC scales, so the scale is sign-extended from the
/// lower 11 bits.
pub fn numeric_precision_scale(typmod: i32) -> Option<NumericTypmod> {
    if typmod < pg_sys::VARHDRSZ as i32 {
        return None;
    }

    let adjusted = typmod - pg_sys::VARHDRSZ as i32;
    let precision = ((adjusted >> 16) & 0xFFFF) as u32;
    let scale = ((adjusted & 0x7FF) ^ 1024) - 1024;
    Some(NumericTypmod { precision, scale })
}

// ============================================================================
//  Decimal128 -> AnyNumeric codec
// ============================================================================

/// PostgreSQL NUMERIC base-10000 digit (`int16` in `numeric.c`).
type NumericDigit = i16;

/// `numeric.c` packs four decimal digits per `NumericDigit` (NBASE = 10000).
const DEC_DIGITS: u32 = 4;
const NBASE: i128 = 10_000;

/// Decimal128 columns can hold up to 38 decimal digits. Adding one extra
/// `NumericDigit` (4 decimal digits) covers any partial leading group.
const MAX_DEC_DIGITS: usize = 38;
const MAX_NDIGITS: usize = MAX_DEC_DIGITS / DEC_DIGITS as usize + 1;

/// Wire-format constants from `numeric.h`.
///
/// `numeric_recv` consumes the same on-the-wire representation that
/// `numeric_send` produces (see `numeric.c`), so these match the binary COPY
/// and protocol format and are stable across supported PostgreSQL versions.
const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;

/// Maximum `dscale` accepted by `numeric_recv` (`numeric.c`: NUMERIC_DSCALE_MASK).
const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;

/// Errors raised by [`Decimal128NumericCodec`].
#[derive(Debug, thiserror::Error)]
pub enum DecimalCodecError {
    #[error(
        "decimal precision {precision} is not valid for Decimal128: must be 1..={MAX_DEC_DIGITS}"
    )]
    PrecisionOutOfRange { precision: u32 },

    #[error(
        "decimal scale {scale} is not valid for Decimal128(precision={precision}): \
         scale must satisfy 0 <= scale <= precision <= {MAX_DEC_DIGITS}"
    )]
    ScaleOutOfRange { precision: u32, scale: u32 },

    /// `numeric_recv` rejected the value, e.g. because it exceeds the typmod
    /// `(precision, scale)` constraint. The message is the PostgreSQL ereport
    /// text. Other categories of `ereport(ERROR, ...)` (OOM, query cancel,
    /// internal errors, ...) are *not* mapped here; they propagate.
    #[error(
        "Decimal128 value out of range for NUMERIC({precision}, {scale}): {message}"
    )]
    ValueOutOfRange {
        precision: u32,
        scale: u32,
        message: String,
    },

    /// `numeric_recv` rejected the wire bytes as malformed. This indicates a
    /// codec bug rather than a data error and is surfaced as its own variant
    /// so the caller can react accordingly (typically: log + abort).
    #[error(
        "Decimal128 codec produced an invalid NUMERIC binary representation: {message}"
    )]
    InvalidBinaryRepresentation { message: String },
}

/// PostgreSQL NUMERIC binary external representation, as produced by
/// `numeric_send` and consumed by `numeric_recv`.
///
/// Holding this struct by value (no heap allocation) lets the encoder
/// build the value on the stack and hand it to PostgreSQL in one shot.
#[derive(Debug, Clone, Copy)]
struct NumericExternal {
    /// Number of base-10000 digits.
    ndigits: u16,
    /// Signed digit weight: `ndigits[0]` represents `NBASE^weight`.
    weight: i16,
    /// `NUMERIC_POS`, `NUMERIC_NEG`, `NUMERIC_NAN`, etc. Decimal128 only
    /// produces POS/NEG.
    sign: u16,
    /// Display scale (number of fractional decimal digits).
    dscale: u16,
    /// `ndigits` base-10000 digits in big-endian order (most significant first).
    digits: [NumericDigit; MAX_NDIGITS],
}

impl NumericExternal {
    /// Builds the external form of `unscaled * 10^(-scale)`.
    ///
    /// This is the algorithm used by `int128_to_numericvar_with_scale` in
    /// CMU/Greenplum vector code and `numeric.c::int128_to_numericvar`: walk
    /// `unscaled` in base-NBASE (10000) chunks, then place the decimal point
    /// according to `scale` to derive `weight` and the leading-digit padding.
    ///
    /// Callers must ensure `scale <= NUMERIC_DSCALE_MASK`; the public codec
    /// performs that check at construction time so this routine cannot fail.
    fn from_decimal128(unscaled: i128, scale: u32) -> Self {
        debug_assert!(scale <= NUMERIC_DSCALE_MASK as u32);

        let dscale = scale as u16;

        if unscaled == 0 {
            return Self {
                ndigits: 0,
                weight: 0,
                sign: NUMERIC_POS,
                dscale,
                digits: [0; MAX_NDIGITS],
            };
        }

        let (sign, magnitude) = if unscaled < 0 {
            // Negate via unsigned to avoid `i128::MIN.unsigned_abs()` panic.
            (NUMERIC_NEG, unscaled.unsigned_abs())
        } else {
            (NUMERIC_POS, unscaled as u128)
        };

        Self::from_unsigned(magnitude, sign, scale, dscale)
    }

    /// Splits a non-zero unsigned magnitude into base-NBASE digits and
    /// computes the NUMERIC weight/dscale fields.
    ///
    /// `nweight` is the count of significant decimal digits in `magnitude`.
    /// `dweight = nweight - scale - 1` is the decimal weight of the
    /// most-significant digit (`numeric.c::int64_to_numericvar`). From there
    /// the base-NBASE weight and the partial leading-digit padding follow.
    fn from_unsigned(magnitude: u128, sign: u16, scale: u32, dscale: u16) -> Self {
        debug_assert!(magnitude != 0);

        // Decimal-digit weight of the most-significant digit.
        let nweight = decimal_digit_count(magnitude) as i32;
        let dweight = nweight - scale as i32 - 1;

        // base-NBASE weight: number of NBASE digits to the left of the point,
        // minus one. Mirrors numeric.c's
        //   (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1
        // which is the standard ceiling-division idiom (avoids the unstable
        // `i32::div_ceil`).
        let weight = if dweight >= 0 {
            (dweight + DEC_DIGITS as i32) / DEC_DIGITS as i32 - 1
        } else {
            -((-dweight - 1) / DEC_DIGITS as i32 + 1)
        };

        // Number of leading zero decimal digits implied by the chosen weight.
        let offset = (weight + 1) * DEC_DIGITS as i32 - (dweight + 1);
        let scale_padding = ((offset + nweight) % DEC_DIGITS as i32) as u32;

        // Walk `magnitude` from least- to most-significant base-NBASE digit,
        // filling `digits` from the back. The first iteration may consume a
        // partial group when `scale_padding != 0`.
        let mut digits = [0 as NumericDigit; MAX_NDIGITS];
        let mut value = magnitude;
        let mut ndigits = 0usize;
        let mut padding_done = scale_padding == 0;

        while value != 0 {
            let digit = if !padding_done {
                let pad_factor = 10u128.pow(scale_padding);
                let remain_factor = 10u128.pow(DEC_DIGITS - scale_padding);
                let high = value / pad_factor;
                let low = value - high * pad_factor;
                value = high;
                padding_done = true;
                (low * remain_factor) as NumericDigit
            } else {
                let high = value / NBASE as u128;
                let low = value - high * NBASE as u128;
                value = high;
                low as NumericDigit
            };

            ndigits += 1;
            digits[MAX_NDIGITS - ndigits] = digit;
        }

        // Compact: shift the populated digits to the start of the buffer so
        // `digits[..ndigits]` is the canonical big-endian sequence.
        let mut compact = [0 as NumericDigit; MAX_NDIGITS];
        compact[..ndigits]
            .copy_from_slice(&digits[MAX_NDIGITS - ndigits..MAX_NDIGITS]);

        Self {
            ndigits: ndigits as u16,
            weight: weight as i16,
            sign,
            dscale,
            digits: compact,
        }
    }

    /// Length, in bytes, of the wire representation that `numeric_recv` reads.
    fn wire_byte_len(&self) -> usize {
        // ndigits + weight + sign + dscale + digits.
        4 * std::mem::size_of::<u16>() + (self.ndigits as usize) * 2
    }

    /// Writes the wire representation in network byte order, matching
    /// `numeric_send` in `numeric.c`.
    fn write_be_bytes(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), self.wire_byte_len());
        out[0..2].copy_from_slice(&self.ndigits.to_be_bytes());
        out[2..4].copy_from_slice(&(self.weight as u16).to_be_bytes());
        out[4..6].copy_from_slice(&self.sign.to_be_bytes());
        out[6..8].copy_from_slice(&self.dscale.to_be_bytes());
        for (i, digit) in self.digits[..self.ndigits as usize].iter().enumerate() {
            let off = 8 + i * 2;
            out[off..off + 2].copy_from_slice(&(*digit as u16).to_be_bytes());
        }
    }
}

/// Returns the number of decimal digits in a non-zero unsigned value.
fn decimal_digit_count(mut value: u128) -> u32 {
    debug_assert!(value != 0);
    let mut count = 0u32;
    while value != 0 {
        value /= 10;
        count += 1;
    }
    count
}

/// Schema-bound encoder/decoder between fixed-scale signed `i128` values and
/// PostgreSQL `NUMERIC`.
///
/// `decode` avoids the Rust-string and `numeric_in` text parser path that
/// pgrx's `AnyNumeric::try_from(&str)` and `From<i128>` fall back to for
/// out-of-`i64` values.
///
/// The struct is `Copy` and the constructor is two integer comparisons, so
/// reconstructing it per cell from a `PrimitiveType::Decimal { precision,
/// scale }` is effectively free. Hoisting it to a per-column extractor plan
/// in `IcebergScan::scan_begin` is the cleaner architectural shape (build a
/// `Vec<ColumnExtractor>` once, then dispatch per-cell without re-reading
/// the schema), but that touches the `ArrowToCell` trait surface and is
/// tracked as a separate refactor.
#[derive(Debug, Clone, Copy)]
pub struct Decimal128NumericCodec {
    precision: u32,
    scale: u32,
}

impl Decimal128NumericCodec {
    /// Builds a codec for `DECIMAL(precision, scale)` as understood by both
    /// Arrow `Decimal128` and PostgreSQL `NUMERIC`.
    ///
    /// Validation rules:
    /// - `1 <= precision <= 38` (Arrow `Decimal128` and PostgreSQL `NUMERIC`
    ///   share this upper bound for lossless `i128` representation).
    /// - `0 <= scale <= precision` (Arrow `Decimal128`'s validity rule;
    ///   `Decimal128Builder::with_precision_and_scale` enforces the same).
    ///
    /// Negative scales are not accepted: Iceberg decimals require
    /// `0 <= scale <= precision`, and `numeric_recv`'s wire format encodes
    /// `dscale` as an unsigned 14-bit value.
    pub fn new(precision: u32, scale: u32) -> Result<Self, DecimalCodecError> {
        if precision == 0 || precision as usize > MAX_DEC_DIGITS {
            return Err(DecimalCodecError::PrecisionOutOfRange { precision });
        }
        if scale > precision {
            return Err(DecimalCodecError::ScaleOutOfRange { precision, scale });
        }
        // `scale <= precision <= MAX_DEC_DIGITS` already implies the
        // `numeric_recv` dscale bound, but assert it explicitly so future
        // changes cannot silently widen the constructor.
        debug_assert!(scale <= NUMERIC_DSCALE_MASK as u32);
        Ok(Self { precision, scale })
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    /// PostgreSQL NUMERIC typmod (`numeric_typmod(precision, scale)`)
    /// suitable for `numeric_recv` and `ALTER TABLE ... TYPE NUMERIC(p, s)`.
    pub fn typmod(&self) -> i32 {
        numeric_typmod(self.precision, self.scale as i32)
    }

    /// Decodes an unscaled Decimal128 value into a freshly allocated
    /// `AnyNumeric`, going through PostgreSQL's binary `numeric_recv` path.
    ///
    /// This avoids the `to_string` + `numeric_in` round-trip that pgrx's
    /// `AnyNumeric::try_from(&str)` and `From<i128>` implementations use,
    /// which is the dominant cost of decimal columns on million-row scans.
    ///
    /// `numeric_recv` also runs `apply_typmod`, so a value that does not fit
    /// `NUMERIC(precision, scale)` is reported as
    /// [`DecimalCodecError::ValueOutOfRange`] rather than silently truncated.
    pub fn decode(
        &self,
        unscaled: i128,
    ) -> Result<AnyNumeric, DecimalCodecError> {
        let external = NumericExternal::from_decimal128(unscaled, self.scale);
        // SAFETY: only callable from a PostgreSQL backend thread; see the
        // safety section on `numeric_recv_external`.
        unsafe { numeric_recv_external(&external, self.typmod(), self) }
    }

    /// Decodes an Avro decimal's signed big-endian two's-complement integer.
    ///
    /// Avro decimals permit arbitrary-width byte arrays. This codec admits
    /// only values representable by this Decimal128 contract, so redundant
    /// sign-extension is accepted while a wider magnitude is rejected before
    /// reaching PostgreSQL.
    pub fn decode_signed_be_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<AnyNumeric, DecimalCodecError> {
        self.decode(self.signed_be_bytes_to_i128(bytes)?)
    }

    /// Encodes a relation-bound PostgreSQL `NUMERIC` datum as the unscaled
    /// integer of this fixed-scale Decimal128 contract.
    ///
    /// # Safety
    ///
    /// The caller must run on the current PostgreSQL backend thread and pass a
    /// valid, non-NULL `NUMERIC` datum that remains live for this call.
    pub unsafe fn encode_bound_datum(
        &self,
        datum: pg_sys::Datum,
    ) -> Result<i128, DecimalCodecError> {
        // SAFETY: the caller supplies the bound NUMERIC datum required by
        // `numeric_send`. PostgreSQL builds the FunctionCallInfo on its stack
        // and returns a palloc'd bytea in the current memory context.
        let output = unsafe {
            pg_sys::DirectFunctionCall1Coll(
                Some(pg_sys::numeric_send),
                pg_sys::InvalidOid,
                datum,
            )
        };
        // SAFETY: numeric_send always returns a non-NULL bytea Datum.
        let bytes = unsafe { varlena_to_byte_slice(output.cast_mut_ptr()) };
        let result = self.numeric_send_to_i128(bytes);
        // SAFETY: DirectFunctionCall1Coll returned a fresh palloc'd bytea.
        unsafe { pg_sys::pfree(output.cast_mut_ptr()) };
        result
    }

    /// Encodes an owned pgrx numeric value.
    ///
    /// This cold row-world interface copies the owned value into PostgreSQL
    /// memory. Relation-bound writers should call [`Self::encode_bound_datum`]
    /// directly to avoid that allocation and copy.
    pub fn encode(&self, value: &AnyNumeric) -> Result<i128, DecimalCodecError> {
        // SAFETY: into_datum produces a valid, non-NULL NUMERIC datum in the
        // current backend memory context. It remains live through the delegated
        // call and is freed before returning.
        unsafe {
            let input = value
                .clone()
                .into_datum()
                .expect("AnyNumeric always converts to a non-NULL datum");
            let result = self.encode_bound_datum(input);
            pg_sys::pfree(input.cast_mut_ptr());
            result
        }
    }

    fn signed_be_bytes_to_i128(&self, bytes: &[u8]) -> Result<i128, DecimalCodecError> {
        if bytes.is_empty() {
            return Ok(0);
        }

        let sign = bytes[0] & 0x80 != 0;
        let sign_extension = if sign { 0xFF } else { 0x00 };
        let retained = bytes.len().saturating_sub(std::mem::size_of::<i128>());
        if retained > 0
            && (!bytes[..retained].iter().all(|byte| *byte == sign_extension)
                || (bytes[retained] & 0x80 != 0) != sign)
        {
            return Err(self.value_out_of_range(
                "Avro decimal value cannot be represented as Decimal128",
            ));
        }

        let mut output = [sign_extension; std::mem::size_of::<i128>()];
        let source = &bytes[retained..];
        output[output.len() - source.len()..].copy_from_slice(source);
        Ok(i128::from_be_bytes(output))
    }

    fn numeric_send_to_i128(&self, bytes: &[u8]) -> Result<i128, DecimalCodecError> {
        let ndigits = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        debug_assert_eq!(bytes.len(), 8 + ndigits * 2);
        let weight = i16::from_be_bytes([bytes[2], bytes[3]]) as i32;
        let sign = u16::from_be_bytes([bytes[4], bytes[5]]);

        if sign != NUMERIC_POS && sign != NUMERIC_NEG {
            return Err(self.value_out_of_range(
                "PostgreSQL numeric value is not finite and cannot be represented as Decimal128",
            ));
        }

        let mut coefficient = 0i128;
        for digit in bytes[8..].chunks_exact(2) {
            let digit = i128::from(u16::from_be_bytes([digit[0], digit[1]]));
            coefficient = coefficient
                .checked_mul(NBASE)
                .and_then(|value| value.checked_add(digit))
                .ok_or_else(|| self.value_out_of_range("numeric value exceeds Decimal128"))?;
        }

        let exponent = (weight + 1 - ndigits as i32) * DEC_DIGITS as i32 + self.scale as i32;
        let unscaled = if exponent >= 0 {
            let factor = 10_i128
                .checked_pow(exponent as u32)
                .ok_or_else(|| self.value_out_of_range("numeric value exceeds Decimal128"))?;
            coefficient
                .checked_mul(factor)
                .ok_or_else(|| self.value_out_of_range("numeric value exceeds Decimal128"))?
        } else {
            let factor = 10_i128
                .checked_pow((-exponent) as u32)
                .ok_or_else(|| self.value_out_of_range("numeric value exceeds Decimal128"))?;
            if coefficient % factor != 0 {
                return Err(self.value_out_of_range(
                    "numeric value has more fractional digits than the decimal scale",
                ));
            }
            coefficient / factor
        };

        let unscaled = if sign == NUMERIC_NEG {
            unscaled
                .checked_neg()
                .ok_or_else(|| self.value_out_of_range("numeric value exceeds Decimal128"))?
        } else {
            unscaled
        };
        let limit = 10_i128.pow(self.precision) - 1;
        if !(-limit..=limit).contains(&unscaled) {
            return Err(self.value_out_of_range("numeric value exceeds decimal precision"));
        }
        Ok(unscaled)
    }

    fn value_out_of_range(&self, message: impl Into<String>) -> DecimalCodecError {
        DecimalCodecError::ValueOutOfRange {
            precision: self.precision,
            scale: self.scale,
            message: message.into(),
        }
    }
}

/// Calls `numeric_recv` with a stack-built `StringInfo` and converts the
/// returned datum into an owned `AnyNumeric`.
///
/// Error policy: only the two SQL states that `numeric_recv` itself produces
/// for malformed input or a typmod-violating value are mapped to
/// [`DecimalCodecError`]. Other categories (`ERRCODE_OUT_OF_MEMORY`,
/// `ERRCODE_QUERY_CANCELED`, internal errors, ...) are *not* swallowed; they
/// re-raise so the surrounding query aborts with the original SQL state.
///
/// # Safety
///
/// `numeric_recv` reads from the `StringInfo` it is handed and may call
/// `ereport(ERROR, ...)`. We wrap the call in `PgTryBuilder` so the long-jump
/// is converted to a Rust `Result`. The function is internally `unsafe`
/// because it dereferences raw pointers; callers must only invoke it from a
/// PostgreSQL backend thread, which is the only place pgrx datum APIs are
/// valid.
unsafe fn numeric_recv_external(
    external: &NumericExternal,
    typmod: i32,
    codec: &Decimal128NumericCodec,
) -> Result<pgrx::AnyNumeric, DecimalCodecError> {
    use pgrx::fcinfo::direct_function_call_as_datum;
    use pgrx::pg_sys::{self, errcodes::PgSqlErrorCode, panic::CaughtError};
    use pgrx::{AnyNumeric, FromDatum, IntoDatum, PgTryBuilder};

    let external = *external; // Copy; closure becomes UnwindSafe.
    let precision = codec.precision;
    let scale = codec.scale;

    unsafe {
        PgTryBuilder::new(move || {
            // Build StringInfoData on the call stack: numeric_recv only reads
            // it, and palloc'ing per-row would defeat the optimization.
            let len = external.wire_byte_len();
            let mut buf = [0u8; 8 + MAX_NDIGITS * 2];
            external.write_be_bytes(&mut buf[..len]);
            let mut string_info = pg_sys::StringInfoData {
                data: buf.as_mut_ptr().cast(),
                len: len as i32,
                maxlen: len as i32,
                cursor: 0,
            };

            let datum = direct_function_call_as_datum(
                pg_sys::numeric_recv,
                &[
                    Some(pg_sys::Datum::from(&mut string_info as *mut _)),
                    pg_sys::InvalidOid.into_datum(),
                    typmod.into_datum(),
                ],
            )
            .expect("numeric_recv must not return SQL NULL");
            let any = AnyNumeric::from_datum(datum, false)
                .expect("numeric_recv produced a non-NULL datum");
            // Free the palloc'd numeric now that AnyNumeric copied it.
            pg_sys::pfree(datum.cast_mut_ptr());
            Ok(any)
        })
        .catch_when(
            PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            move |e| {
                // Only Postgres-side ereport(ERROR, ...) is converted; anything
                // raised by Rust is rethrown so callers see the original cause.
                let CaughtError::PostgresError(ref ereport) = e else {
                    e.rethrow();
                };
                Err(DecimalCodecError::ValueOutOfRange {
                    precision,
                    scale,
                    message: ereport.message().to_string(),
                })
            },
        )
        .catch_when(
            PgSqlErrorCode::ERRCODE_INVALID_BINARY_REPRESENTATION,
            move |e| {
                let CaughtError::PostgresError(ref ereport) = e else {
                    e.rethrow();
                };
                Err(DecimalCodecError::InvalidBinaryRepresentation {
                    message: ereport.message().to_string(),
                })
            },
        )
        .execute()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn external(unscaled: i128, scale: u32) -> NumericExternal {
        NumericExternal::from_decimal128(unscaled, scale)
    }

    /// Reconstructs the unscaled magnitude implied by a `NumericExternal`,
    /// independent of the encoder, so failures isolate which side is wrong.
    fn reconstruct_unscaled(ext: &NumericExternal) -> i128 {
        if ext.ndigits == 0 {
            return 0;
        }
        let mut value: i128 = 0;
        for &digit in &ext.digits[..ext.ndigits as usize] {
            value = value * NBASE + digit as i128;
        }
        // value now represents the integer described by the digits with the
        // implicit decimal point at position (ndigits - weight - 1) groups.
        let trailing_groups = (ext.ndigits as i32 - 1) - ext.weight as i32;
        let trailing_decimal_digits = trailing_groups * DEC_DIGITS as i32;
        // Adjust so the value matches `unscaled = magnitude * 10^(scale - trailing)`.
        let exponent = ext.dscale as i32 - trailing_decimal_digits;
        if exponent >= 0 {
            value *= 10i128.pow(exponent as u32);
        } else {
            value /= 10i128.pow((-exponent) as u32);
        }
        if ext.sign == NUMERIC_NEG {
            -value
        } else {
            value
        }
    }

    #[test]
    fn zero_is_canonical() {
        let ext = external(0, 4);
        assert_eq!(ext.ndigits, 0);
        assert_eq!(ext.weight, 0);
        assert_eq!(ext.sign, NUMERIC_POS);
        assert_eq!(ext.dscale, 4);
    }

    #[test]
    fn positive_with_fraction_round_trips() {
        // 12345.6789 with scale=4 -> unscaled 123456789
        let ext = external(123_456_789, 4);
        assert_eq!(ext.sign, NUMERIC_POS);
        assert_eq!(ext.dscale, 4);
        assert_eq!(reconstruct_unscaled(&ext), 123_456_789);
    }

    #[test]
    fn negative_round_trips() {
        let ext = external(-123_456_789, 4);
        assert_eq!(ext.sign, NUMERIC_NEG);
        assert_eq!(reconstruct_unscaled(&ext), -123_456_789);
    }

    #[test]
    fn integer_value_round_trips() {
        // 100, scale 0 -> unscaled 100
        let ext = external(100, 0);
        assert_eq!(ext.dscale, 0);
        assert_eq!(reconstruct_unscaled(&ext), 100);
    }

    #[test]
    fn small_fraction_round_trips() {
        // 0.0001 with scale=4 -> unscaled 1
        let ext = external(1, 4);
        assert_eq!(ext.sign, NUMERIC_POS);
        assert_eq!(ext.dscale, 4);
        assert_eq!(reconstruct_unscaled(&ext), 1);
    }

    #[test]
    fn i128_min_does_not_panic() {
        // i128::MIN is the classic abs() overflow trap; codec uses
        // unsigned magnitude so this must succeed.
        let ext = external(i128::MIN, 0);
        assert_eq!(ext.sign, NUMERIC_NEG);
    }

    #[test]
    fn codec_rejects_invalid_decimal128_scale() {
        // Public codec API: scale must satisfy `scale <= precision`, which is
        // tighter than the bare `numeric_recv` dscale bound and matches Arrow
        // `Decimal128Builder::with_precision_and_scale`.
        assert!(matches!(
            Decimal128NumericCodec::new(5, 6),
            Err(DecimalCodecError::ScaleOutOfRange {
                precision: 5,
                scale: 6
            })
        ));
        // scale > MAX_DEC_DIGITS implies scale > precision; covered.
        assert!(matches!(
            Decimal128NumericCodec::new(38, 39),
            Err(DecimalCodecError::ScaleOutOfRange {
                precision: 38,
                scale: 39
            })
        ));
    }

    #[test]
    fn codec_validates_precision() {
        assert!(Decimal128NumericCodec::new(38, 9).is_ok());
        assert!(Decimal128NumericCodec::new(38, 38).is_ok());
        assert!(Decimal128NumericCodec::new(1, 0).is_ok());
        assert!(matches!(
            Decimal128NumericCodec::new(0, 0),
            Err(DecimalCodecError::PrecisionOutOfRange { precision: 0 })
        ));
        assert!(matches!(
            Decimal128NumericCodec::new(40, 0),
            Err(DecimalCodecError::PrecisionOutOfRange { precision: 40 })
        ));
    }

    #[test]
    fn wire_byte_layout_matches_numeric_send() {
        let ext = external(123_456_789, 4);
        let mut buf = vec![0u8; ext.wire_byte_len()];
        ext.write_be_bytes(&mut buf);
        // Header: 4 big-endian u16 fields.
        assert_eq!(&buf[0..2], &ext.ndigits.to_be_bytes());
        assert_eq!(&buf[2..4], &(ext.weight as u16).to_be_bytes());
        assert_eq!(&buf[4..6], &ext.sign.to_be_bytes());
        assert_eq!(&buf[6..8], &ext.dscale.to_be_bytes());
    }
}
