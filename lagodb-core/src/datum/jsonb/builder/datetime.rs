//! JSONB date/time encoding: PostgreSQL ISO dates and explicit UTC offsets.

use std::ffi::CStr;
use std::io::Write;
use std::ptr;

use pgrx::IntoDatum;
use pgrx::pg_sys::{self, Datum};

use super::{JsonbBuilder, JsonbPosition};
use crate::datum::{JsonValueError, PG_EPOCH_USECS_DIFF};

impl JsonbBuilder<'_> {
    /// Append a PostgreSQL date/time using its ISO JSON formatter, in UTC for
    /// timestamptz. This preserves PostgreSQL's full date/time range.
    ///
    /// # Safety
    /// Has `string`'s token/backend requirements. `value` must be a finite,
    /// non-null date, time, timestamp or timestamptz.
    pub unsafe fn datetime<T: IntoDatum>(
        &mut self,
        position: JsonbPosition,
        value: T,
    ) {
        let datum = value
            .into_datum()
            .expect("finite date/time returns a datum");
        let utc = 0;
        // SAFETY: the caller supplies the matching PG date/time datum. Passing
        // UTC explicitly avoids dependence on the session TimeZone setting.
        let text = unsafe {
            pg_sys::JsonEncodeDateTime(ptr::null_mut(), datum, T::type_oid(), &utc)
        };
        // SAFETY: PG returns a NUL-terminated string in the current context.
        let len = unsafe { CStr::from_ptr(text) }.to_bytes().len();
        // SAFETY: PG's ISO output is ASCII, bounded by MAXDATELEN, and owned by
        // the current context until JSONB serialization completes.
        unsafe { self.string_pointer(position, text, len) };
    }

    /// Append Unix nanoseconds with nine fractional digits and, for a zoned
    /// timestamp, the same `+00:00` suffix as PostgreSQL's UTC JSON formatter.
    ///
    /// # Safety
    /// Has `string`'s token/backend requirements.
    pub unsafe fn timestamp_nanos(
        &mut self,
        position: JsonbPosition,
        unix_nanos: i64,
        with_timezone: bool,
    ) -> Result<(), JsonValueError> {
        let seconds = unix_nanos.div_euclid(1_000_000_000);
        let fraction = unix_nanos.rem_euclid(1_000_000_000);
        // All i64 Unix nanoseconds lie in 1677..=2262. Converting their whole
        // seconds to PG microseconds cannot overflow and yields a finite date.
        let pg_micros = seconds * 1_000_000 - PG_EPOCH_USECS_DIFF;
        let mut buffer = [0_u8; pg_sys::MAXDATELEN as usize + 1];
        // SAFETY: PG receives a finite timestamp and MAXDATELEN + 1 bytes.
        // Encode only the whole-second calendar part, without a timezone;
        // TIMESTAMPOID makes it independent of the session TimeZone. PG's
        // JSON formatter forces ISO dates and omits a zero fractional part.
        unsafe {
            pg_sys::JsonEncodeDateTime(
                buffer.as_mut_ptr().cast(),
                Datum::from(pg_micros),
                pg_sys::TIMESTAMPOID,
                ptr::null(),
            );
        }
        // SAFETY: JsonEncodeDateTime NUL-terminates the supplied buffer.
        let prefix_len = unsafe { CStr::from_ptr(buffer.as_ptr().cast()) }
            .to_bytes()
            .len();
        let mut remaining = &mut buffer[prefix_len..];
        write!(
            remaining,
            ".{fraction:09}{}",
            if with_timezone { "+00:00" } else { "" },
        )
        .map_err(|_| JsonValueError::OutputTooLarge)?;
        let len = pg_sys::MAXDATELEN as usize + 1 - remaining.len();
        // SAFETY: PG's ISO prefix and the suffix are ASCII without NUL. The
        // complete timestamp is at most 35 bytes; copy it before the stack
        // buffer expires because pushJsonbValue retains string pointers.
        unsafe { self.copy_string(position, &buffer[..len]) };
        Ok(())
    }
}
