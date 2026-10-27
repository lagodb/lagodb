//! Database-encoding capability used by semantic text codecs.

use std::ffi::CStr;

use pgrx::pg_sys;

use super::error::Utf8ServerEncodingError;

/// PostgreSQL's table of character-encoding names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgEncodingNames;

impl PgEncodingNames {
    /// Whether PostgreSQL resolves `name`, as COPY's `ENCODING` option does.
    pub fn contains(name: &CStr) -> bool {
        // SAFETY: `name` is NUL-terminated and outlives this table lookup.
        #[cfg(feature = "pg16")]
        let encoding = unsafe { pg_sys::pg_char_to_encoding(name.as_ptr()) };
        // SAFETY: as above.
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        let encoding = unsafe { pg_sys::pg_char_to_encoding_private(name.as_ptr()) };
        encoding >= 0
    }
}

/// Proof that PostgreSQL text datums can be consumed by UTF-8 APIs.
///
/// The database encoding is fixed for a database. Plans retain this zero-sized
/// proof so row paths do not repeat the check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Utf8ServerEncoding(());

impl Utf8ServerEncoding {
    pub fn resolve() -> Result<Self, Utf8ServerEncodingError> {
        let encoding = unsafe { pg_sys::GetDatabaseEncoding() };
        if encoding != pg_sys::pg_enc::PG_UTF8 as i32 {
            return Err(Utf8ServerEncodingError { encoding });
        }
        Ok(Self(()))
    }
}
