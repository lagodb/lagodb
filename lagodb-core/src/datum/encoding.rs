//! Database-encoding capability used by semantic text codecs.

use pgrx::pg_sys;

use super::error::Utf8ServerEncodingError;

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
