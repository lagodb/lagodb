//! PG17-native COPY text/CSV row encoding for relation-bound providers.

use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

use super::error::CopyError;
use super::pg;

/// A relation-bound PostgreSQL text/CSV serializer.
///
/// Rows are encoded into PostgreSQL's reusable COPY buffer. The slice returned
/// from [`Self::header`] or [`Self::row`] remains valid only until the next
/// encoding call or [`Self::finish`].
pub struct CopyRowEncoder {
    state: Option<pg_sys::CopyToState>,
}

impl CopyRowEncoder {
    /// Bind PostgreSQL output functions and COPY format options once.
    ///
    /// # Safety
    ///
    /// `relation` must remain live for this encoder's lifetime. `options` must
    /// be a valid PostgreSQL text or CSV COPY TO option list in the current
    /// memory context. Every slot passed to [`Self::row`] must have the same
    /// relation-shaped attribute layout.
    pub unsafe fn begin(
        relation: pg_sys::Relation,
        options: *mut pg_sys::List,
    ) -> Result<Self, CopyError> {
        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg::CopyBridge::begin_row_encoder(relation, options))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;
        Ok(Self { state: Some(state) })
    }

    /// Encode the configured column names as one header row without a line
    /// terminator.
    pub fn header(&mut self) -> Result<&[u8], CopyError> {
        self.encode(std::ptr::null_mut(), true)
    }

    /// Encode one relation-shaped slot without a line terminator.
    ///
    /// # Safety
    ///
    /// `slot` must contain a live virtual or physical tuple with the
    /// relation-shaped descriptor bound at [`Self::begin`].
    pub unsafe fn row(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<&[u8], CopyError> {
        self.encode(slot, false)
    }

    fn encode(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
        header: bool,
    ) -> Result<&[u8], CopyError> {
        let state = self.state.ok_or_else(CopyError::encoder_finished)?;
        let mut data = std::ptr::null();
        let mut len = 0;
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                if header {
                    pg::CopyBridge::encode_copy_header(state, &mut data, &mut len);
                } else {
                    pg::CopyBridge::encode_copy_row(state, slot, &mut data, &mut len);
                }
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(CopyError::from)?;
        // SAFETY: the C bridge initializes its StringInfo before every encode,
        // returns its nonnegative length, and retains the buffer until this
        // encoder is called again or dropped.
        Ok(unsafe { std::slice::from_raw_parts(data.cast(), len as usize) })
    }

    /// Release PostgreSQL COPY state. Repeated calls are harmless.
    pub fn finish(&mut self) -> Result<(), CopyError> {
        let Some(state) = self.state.take() else {
            return Ok(());
        };
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                pg::CopyBridge::end_row_encoder(state);
                Ok(())
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(CopyError::from)
    }
}

impl Drop for CopyRowEncoder {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}
