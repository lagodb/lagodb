//! PostgreSQL COPY raw-field parsing for cold-path consumers.

use std::ffi::{CStr, c_char, c_void};
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::ptr::NonNull;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

use super::io::{CopyDataSource, SourceGuard, source_callback};
use super::{CopyError, pg};

/// A raw text/CSV record returned by PostgreSQL's COPY parser.
///
/// Field pointers remain valid only until the next call to
/// [`CopyRawFieldReader::next`]. The record's borrow prevents that call while
/// field values are still borrowed.
pub struct CopyRawRecord<'reader> {
    fields: *mut *mut c_char,
    field_count: usize,
    _lifetime: PhantomData<&'reader ()>,
}

impl CopyRawRecord<'_> {
    pub fn len(&self) -> usize {
        self.field_count
    }

    pub fn is_empty(&self) -> bool {
        self.field_count == 0
    }

    /// Returns `None` for an out-of-bounds field and `Some(None)` for a COPY
    /// NULL field.
    pub fn field(&self, index: usize) -> Option<Option<&CStr>> {
        if index >= self.field_count {
            return None;
        }
        // SAFETY: PostgreSQL allocated `fields` for the reported field count,
        // and this record borrows its reader so the parser cannot advance
        // before the returned C string is released.
        let field = unsafe { *self.fields.add(index) };
        Some((!field.is_null()).then(|| unsafe { CStr::from_ptr(field) }))
    }

    pub fn fields(&self) -> CopyRawFields<'_> {
        CopyRawFields {
            fields: self.fields,
            field_count: self.field_count,
            index: 0,
            _lifetime: PhantomData,
        }
    }
}

/// Iterator over one raw record's fields, preserving COPY NULLs.
pub struct CopyRawFields<'record> {
    fields: *mut *mut c_char,
    field_count: usize,
    index: usize,
    _lifetime: PhantomData<&'record CStr>,
}

impl<'record> Iterator for CopyRawFields<'record> {
    type Item = Option<&'record CStr>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.field_count {
            return None;
        }
        // The bound above is within PostgreSQL's reported field count. The
        // iterator lifetime is bounded by the record, so the parser cannot
        // advance while the C string is borrowed.
        let field = unsafe { *self.fields.add(self.index) };
        self.index += 1;
        Some((!field.is_null()).then(|| unsafe { CStr::from_ptr(field) }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.field_count - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for CopyRawFields<'_> {}

/// A synchronous PostgreSQL COPY parser that exposes raw fields.
///
/// The reader installs the same callback guard used by COPY scans. Its parser
/// state is ended before that guard releases the borrowed source.
pub struct CopyRawFieldReader<'source> {
    state: Option<NonNull<c_void>>,
    _source_guard: SourceGuard<'source>,
    _not_send_sync: PhantomData<*mut ()>,
}

impl<'source> CopyRawFieldReader<'source> {
    pub fn begin(
        options: *mut pg_sys::List,
        source: &'source mut dyn CopyDataSource,
    ) -> Result<Self, CopyError> {
        let source_guard = SourceGuard::install(source);
        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::begin_raw_field_reader(
                    source_callback(),
                    options,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(CopyError::from)?;
        // SAFETY: the C bridge allocates this object with palloc, which either
        // returns a valid address or reports a PostgreSQL ERROR caught above.
        let state = unsafe { NonNull::new_unchecked(state) };
        Ok(Self {
            state: Some(state),
            _source_guard: source_guard,
            _not_send_sync: PhantomData,
        })
    }

    pub fn next(&mut self) -> Result<Option<CopyRawRecord<'_>>, CopyError> {
        let state = self.state.ok_or(CopyError::RawFieldReaderFinished)?;
        let mut fields = std::ptr::null_mut();
        let mut field_count = 0;
        let found = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::next_raw_fields(
                    state.as_ptr(),
                    &mut fields,
                    &mut field_count,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(CopyError::from)?;
        Ok(found.then_some(CopyRawRecord {
            fields,
            field_count,
            _lifetime: PhantomData,
        }))
    }

    pub fn finish(&mut self) -> Result<(), CopyError> {
        let Some(state) = self.state.take() else {
            return Ok(());
        };
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::end_raw_field_reader(state.as_ptr());
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(CopyError::from)
    }
}

impl Drop for CopyRawFieldReader<'_> {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

/// Reusable safe PostgreSQL text-input validation for one type OID.
pub struct CopyTextInputValidator {
    state: Option<NonNull<c_void>>,
    _not_send_sync: PhantomData<*mut ()>,
}

impl CopyTextInputValidator {
    pub fn new(type_oid: pg_sys::Oid) -> Result<Self, CopyError> {
        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::begin_text_input_validator(type_oid))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(CopyError::from)?;
        // SAFETY: the C bridge uses palloc and reports allocation failure as a
        // PostgreSQL ERROR, so a successful return is non-null.
        let state = unsafe { NonNull::new_unchecked(state) };
        Ok(Self {
            state: Some(state),
            _not_send_sync: PhantomData,
        })
    }

    pub fn accepts(&mut self, value: &CStr) -> Result<bool, CopyError> {
        let state = self.state.ok_or(CopyError::TextInputValidatorFinished)?;
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::text_input_accepts(
                    state.as_ptr(),
                    value.as_ptr(),
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(CopyError::from)
    }

    pub fn finish(&mut self) -> Result<(), CopyError> {
        let Some(state) = self.state.take() else {
            return Ok(());
        };
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::end_text_input_validator(state.as_ptr());
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(CopyError::from)
    }
}

impl Drop for CopyTextInputValidator {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}
