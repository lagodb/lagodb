//! NUMERIC typmod encoding/decoding and PG/Unix epoch constants.

use pgrx::pg_sys::{self, POSTGRES_EPOCH_JDATE, UNIX_EPOCH_JDATE};

/// PostgreSQL epoch (2000-01-01) minus Unix epoch (1970-01-01) in days.
pub const PG_EPOCH_DAYS_DIFF: i32 = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i32;

/// PostgreSQL epoch (2000-01-01) minus Unix epoch (1970-01-01) in microseconds.
pub const PG_EPOCH_USECS_DIFF: i64 =
    (PG_EPOCH_DAYS_DIFF as i64) * (pgrx::datum::USECS_PER_DAY as i64);

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
