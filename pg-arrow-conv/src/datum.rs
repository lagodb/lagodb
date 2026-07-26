//! Datum codecs selected by a provider's bound column mapping.
//!
//! `ColumnDatumTarget` covers the framework's normal semantic `Cell` → Datum
//! conversion. The other variants are explicit physical codecs for formats
//! whose bytes have already been validated by the producer. Keeping them here
//! prevents an Arrow type or a PostgreSQL OID from silently selecting a
//! provider-specific representation.

use std::ffi::c_char;
use std::ptr;

use pg_lakebase_core::diag::PgError;
use pg_lakebase_core::tuple::{ColumnDatumCodec, ColumnDatumTarget};
use pgrx::{PgTryBuilder, pg_sys};

use crate::error::{ArrowConversionError, ArrowConversionResult};

/// The datum representation bound to one Arrow column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatumCodec {
    kind: DatumCodecKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatumCodecKind {
    Standard(ColumnDatumCodec),
    PrevalidatedJsonText,
    PostgresJsonbVarlena,
}

impl DatumCodec {
    /// Bind the framework's standard semantic conversion for a destination OID.
    pub fn standard(oid: pg_sys::Oid) -> ArrowConversionResult<Self> {
        let target = ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(oid))
            .map_err(ArrowConversionError::from)?;
        Ok(Self {
            kind: DatumCodecKind::Standard(target),
        })
    }

    /// Bind the provider's prevalidated JSON text codec.
    ///
    /// # Safety
    ///
    /// Every value passed to the resulting decoder must already be valid JSON
    /// text. The constructor is unsafe because the decoder cannot validate
    /// this provider-owned contract per value. Bind it only to a `json` target
    /// through [`DecodedColumn::new`](crate::DecodedColumn::new).
    pub unsafe fn prevalidated_json_text() -> Self {
        Self {
            kind: DatumCodecKind::PrevalidatedJsonText,
        }
    }

    /// Bind the provider's complete internal JSONB varlena codec.
    ///
    /// # Safety
    ///
    /// Every value passed to the resulting decoder must be a complete, valid
    /// PostgreSQL JSONB varlena. The constructor is unsafe because the decoder
    /// cannot validate this provider-owned contract per value. Bind it only to
    /// a `jsonb` target through
    /// [`DecodedColumn::new`](crate::DecodedColumn::new).
    pub unsafe fn postgres_jsonb_varlena() -> Self {
        Self {
            kind: DatumCodecKind::PostgresJsonbVarlena,
        }
    }

    pub(crate) fn is_prevalidated_json_text(self) -> bool {
        matches!(self.kind, DatumCodecKind::PrevalidatedJsonText)
    }

    pub(crate) fn is_postgres_jsonb_varlena(self) -> bool {
        matches!(self.kind, DatumCodecKind::PostgresJsonbVarlena)
    }

    pub(crate) fn standard_target(self) -> Option<ColumnDatumCodec> {
        match self.kind {
            DatumCodecKind::Standard(target) => Some(target),
            DatumCodecKind::PrevalidatedJsonText
            | DatumCodecKind::PostgresJsonbVarlena => None,
        }
    }

    pub(crate) fn validate_target_oid(
        self,
        target_oid: pg_sys::Oid,
    ) -> ArrowConversionResult<()> {
        let valid = match self.kind {
            DatumCodecKind::Standard(target) => target.oid() == target_oid,
            DatumCodecKind::PrevalidatedJsonText => target_oid == pg_sys::JSONOID,
            DatumCodecKind::PostgresJsonbVarlena => target_oid == pg_sys::JSONBOID,
        };
        if valid {
            Ok(())
        } else {
            Err(ArrowConversionError::InvariantViolated(
                "datum codec does not match the bound target attribute OID",
            ))
        }
    }

    /// Copy text owned by a bound prevalidated JSON reader into the
    /// current PostgreSQL memory context.
    ///
    /// # Safety
    ///
    /// `text` must be valid JSON established by the bound provider codec, and
    /// PostgreSQL must be running with the destination memory context selected.
    pub(crate) unsafe fn copy_prevalidated_json_text(
        text: &str,
    ) -> ArrowConversionResult<pg_sys::Datum> {
        let len = i32::try_from(text.len()).map_err(|_| {
            ArrowConversionError::ValueOutOfRange(
                "JSON text is too large for PostgreSQL varlena input".to_string(),
            )
        })?;
        unsafe { Self::copy_json_text(text.as_ptr(), len) }
    }

    /// Copy bytes owned by a bound PostgreSQL JSONB varlena reader into the
    /// current PostgreSQL memory context.
    ///
    /// # Safety
    ///
    /// `bytes` must be a complete, valid PostgreSQL JSONB varlena, including
    /// its header, established by the bound provider codec. PostgreSQL must be
    /// running with the destination memory context selected.
    pub(crate) unsafe fn copy_postgres_jsonb_varlena(
        bytes: &[u8],
    ) -> ArrowConversionResult<pg_sys::Datum> {
        unsafe { Self::copy_internal_jsonb(bytes) }
    }

    unsafe fn copy_json_text(
        ptr: *const u8,
        len: i32,
    ) -> ArrowConversionResult<pg_sys::Datum> {
        unsafe {
            PgTryBuilder::new(move || {
                let text_ptr =
                    pg_sys::cstring_to_text_with_len(ptr as *const c_char, len);
                Ok(pg_sys::Datum::from(text_ptr))
            })
            .catch_others(|error| Err(PgError::from(error)))
            .execute()
        }
        .map_err(ArrowConversionError::Postgres)
    }

    unsafe fn copy_internal_jsonb(
        bytes: &[u8],
    ) -> ArrowConversionResult<pg_sys::Datum> {
        let ptr = bytes.as_ptr();
        let len = bytes.len();
        unsafe {
            PgTryBuilder::new(move || {
                let new_ptr = pg_sys::palloc(len);
                ptr::copy_nonoverlapping(ptr, new_ptr as *mut u8, len);
                Ok(pg_sys::Datum::from(new_ptr))
            })
            .catch_others(|error| Err(PgError::from(error)))
            .execute()
        }
        .map_err(ArrowConversionError::Postgres)
    }
}
