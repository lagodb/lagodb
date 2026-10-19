//! Parse the JSONB calendar representation without losing nanoseconds.
//!
//! Keep the wire-format gate here and calendar interpretation in PG's input
//! functions (ParseDateTime/DecodeDateTime). JSONPath's parse_datetime is not
//! a nanosecond parser either, and its template path copies both input and
//! template to C strings. pgrx's FromStr additionally owns a per-value error
//! boundary. This path uses stack storage and the enclosing provider/FFI boundary.

use pgrx::pg_sys::{self, Datum};

use crate::datum::{JsonValueError, PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};

unsafe extern "C-unwind" {
    // As in datum/numeric.rs and wrapper/json.rs, DirectFunctionCall requires
    // native C entry points; pg_sys exposes guarded Rust functions. PG supplies
    // the stack call frame, so do not replace this with pgrx's allocating
    // direct_function_call_as_datum or a second implementation of LOCAL_FCINFO.
    #[link_name = "date_in"]
    fn pg_date_in(fcinfo: pg_sys::FunctionCallInfo) -> Datum;
    #[link_name = "time_in"]
    fn pg_time_in(fcinfo: pg_sys::FunctionCallInfo) -> Datum;
    #[link_name = "timestamp_in"]
    fn pg_timestamp_in(fcinfo: pg_sys::FunctionCallInfo) -> Datum;
    #[link_name = "timestamptz_in"]
    fn pg_timestamptz_in(fcinfo: pg_sys::FunctionCallInfo) -> Datum;
}

/// A schema-bound parser. PG errors unwind to the enclosing provider/FFI boundary.
#[derive(Clone, Copy)]
pub enum JsonbTemporalParser {
    Date,
    Time,
    Timestamp { nanos: bool, tz: bool },
}

impl JsonbTemporalParser {
    /// Parse the canonical ISO strings emitted by JsonbBuilder. Requiring the
    /// structural ISO form prevents DateStyle, TimeZone, and relative date
    /// keywords from changing the meaning of a stored JSONB value. The gate
    /// checks shape and precision only; PG validates the calendar and range.
    ///
    /// # Safety
    /// A PG backend/row context must be active. PG calls use pgrx FFI guards.
    pub unsafe fn parse(self, text: &str) -> Result<i64, JsonValueError> {
        let invalid = || JsonValueError::InvalidDatetime;
        let bytes = text.as_bytes();
        if bytes.len() > pg_sys::MAXDATELEN as usize {
            return Err(invalid());
        }
        let mut body = text;
        let bc = body.ends_with(" BC");
        if bc {
            body = &body[..body.len() - 3];
        }
        let (date, time, nanos, function): (_, _, _, pg_sys::PGFunction) = match self
        {
            Self::Date => (Some(body), None, false, Some(pg_date_in)),
            Self::Time => (None, Some(body), false, Some(pg_time_in)),
            Self::Timestamp { nanos, tz } => {
                if tz {
                    body = body.strip_suffix("+00:00").ok_or_else(invalid)?;
                }
                let (date, time) = body.split_once('T').ok_or_else(invalid)?;
                (
                    Some(date),
                    Some(time),
                    nanos,
                    if tz {
                        Some(pg_timestamptz_in)
                    } else {
                        Some(pg_timestamp_in)
                    },
                )
            }
        };
        if let Some(date) = date {
            let mut parts = date.split('-');
            let year = parts.next().ok_or_else(invalid)?;
            let month = parts.next().ok_or_else(invalid)?;
            let day = parts.next().ok_or_else(invalid)?;
            if year.len() < 4
                || month.len() != 2
                || day.len() != 2
                || parts.next().is_some()
                || !year
                    .bytes()
                    .chain(month.bytes())
                    .chain(day.bytes())
                    .all(|b| b.is_ascii_digit())
            {
                return Err(invalid());
            }
        }
        let mut fraction = 0_i64;
        let mut fraction_range = None;
        if let Some(time) = time {
            let (clock, digits) = time.split_once('.').unwrap_or((time, ""));
            let c = clock.as_bytes();
            if c.len() != 8
                || c[2] != b':'
                || c[5] != b':'
                || !c
                    .iter()
                    .enumerate()
                    .all(|(i, b)| i == 2 || i == 5 || b.is_ascii_digit())
                || digits.len() > if nanos { 9 } else { 6 }
                || !digits.bytes().all(|b| b.is_ascii_digit())
                || (time.contains('.') && digits.is_empty())
            {
                return Err(invalid());
            }
            if nanos && !digits.is_empty() {
                for b in digits.bytes() {
                    fraction = fraction * 10 + i64::from(b - b'0');
                }
                fraction *= 10_i64.pow(9 - digits.len() as u32);
                if fraction != 0 {
                    let pair = |offset: usize| {
                        i32::from(c[offset] - b'0') * 10
                            + i32::from(c[offset + 1] - b'0')
                    };
                    // The PG call below sees whole seconds. Before removing
                    // the fraction, preserve PG's end-of-day range check:
                    // 24:00:00.000000001 must not become valid 24:00:00.
                    // time_overflows compares an integral clock with 24h;
                    // for any fraction in (0, 1s), one microsecond has the
                    // same range-check result. It is never used as the value.
                    // SAFETY: the clock contains three checked digit pairs;
                    // time_overflows is PG's allocation-free range predicate.
                    if unsafe { pg_sys::time_overflows(pair(0), pair(3), pair(6), 1) }
                    {
                        return Err(invalid());
                    }
                }
                let start = text.find('.').expect("fraction has a separator");
                fraction_range = Some(start..start + 1 + digits.len());
            }
        }
        if bc && matches!(self, Self::Time) {
            return Err(invalid());
        }
        let mut buffer = [0_u8; pg_sys::MAXDATELEN as usize + 1];
        if let Some(range) = fraction_range {
            buffer[..range.start].copy_from_slice(&bytes[..range.start]);
            let suffix = &bytes[range.end..];
            buffer[range.start..range.start + suffix.len()].copy_from_slice(suffix);
        } else {
            buffer[..bytes.len()].copy_from_slice(bytes);
        }
        // SAFETY: a checked, NUL-terminated ISO value is live in the stack
        // buffer. PG validates the calendar and range; no per-leaf catch/report.
        let value = unsafe {
            pg_sys::DirectFunctionCall3Coll(
                function,
                pg_sys::InvalidOid,
                Datum::from(buffer.as_ptr()),
                Datum::from(pg_sys::InvalidOid),
                Datum::from(-1_i32),
            )
        };
        match self {
            Self::Date => (value.value() as i32)
                .checked_add(PG_EPOCH_DAYS_DIFF)
                .map(i64::from)
                .ok_or_else(invalid),
            Self::Time => {
                let micros = value.value() as i64;
                if (0..86_400_000_000).contains(&micros) {
                    Ok(micros)
                } else {
                    Err(invalid())
                }
            }
            Self::Timestamp { nanos: false, .. } => (value.value() as i64)
                .checked_add(PG_EPOCH_USECS_DIFF)
                .ok_or_else(invalid),
            Self::Timestamp { nanos: true, .. } => {
                // Use i128 so the whole-second component of i64::MIN nanos
                // need not itself fit i64 after multiplication.
                let unix = i128::from(value.value() as i64)
                    + i128::from(PG_EPOCH_USECS_DIFF);
                i64::try_from(unix * 1000 + i128::from(fraction))
                    .map_err(|_| invalid())
            }
        }
    }
}
