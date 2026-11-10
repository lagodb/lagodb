//! Datum codecs selected by a bound column mapping.
//!
//! `ColumnDatumTarget` covers the framework's normal semantic conversion. The
//! other variants are explicit physical codecs selected by a provider or the
//! query engine. Keeping them here prevents an Arrow type or a PostgreSQL OID
//! from silently selecting a representation.

use std::ffi::c_char;
use std::ptr;

use lagodb_core::datum::{BoundColumnDatumCodec, ColumnDatumTarget};
use pgrx::{AnyNumeric, IntoDatum, pg_sys};

use crate::error::{ArrowConversionError, ArrowConversionResult};

/// The datum representation bound to one Arrow column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatumCodec {
    pub(super) kind: DatumCodecKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DatumCodecKind {
    Standard(BoundColumnDatumCodec),
    PrevalidatedJsonText,
    PostgresNumericVarlena,
    NumericFromFloat64,
}

impl DatumCodec {
    /// Bind the framework's standard semantic conversion for a destination OID.
    pub fn standard(oid: pg_sys::Oid) -> ArrowConversionResult<Self> {
        let target = BoundColumnDatumCodec::bind(ColumnDatumTarget::from_oid(oid))?;
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
    /// through [`DecodedColumn::new`](crate::DecodedColumn::new) or the
    /// recursive [`BoundDatumPlan::bind_with_prevalidated_json_text`](crate::BoundDatumPlan::bind_with_prevalidated_json_text)
    /// entry point.
    pub unsafe fn prevalidated_json_text() -> Self {
        Self {
            kind: DatumCodecKind::PrevalidatedJsonText,
        }
    }

    /// Bind complete PostgreSQL NUMERIC internal-varlena bytes produced by a
    /// trusted engine finalizer.
    ///
    /// # Safety
    ///
    /// Every value must be a complete, detoasted PostgreSQL NUMERIC varlena.
    /// Bind it only to a `numeric` target through `DecodedColumn::new`.
    pub unsafe fn postgres_numeric_varlena() -> Self {
        Self {
            kind: DatumCodecKind::PostgresNumericVarlena,
        }
    }

    /// Bind a native Float64 aggregate result to PostgreSQL NUMERIC.
    pub const fn numeric_from_float64() -> Self {
        Self {
            kind: DatumCodecKind::NumericFromFloat64,
        }
    }

    pub(super) fn validate_target_oid(
        self,
        target_oid: pg_sys::Oid,
    ) -> ArrowConversionResult<()> {
        let valid = match self.kind {
            DatumCodecKind::Standard(target) => target.oid() == target_oid,
            DatumCodecKind::PrevalidatedJsonText => target_oid == pg_sys::JSONOID,
            DatumCodecKind::PostgresNumericVarlena
            | DatumCodecKind::NumericFromFloat64 => target_oid == pg_sys::NUMERICOID,
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
    pub(super) unsafe fn copy_prevalidated_json_text(
        text: &str,
    ) -> ArrowConversionResult<pg_sys::Datum> {
        let len = i32::try_from(text.len()).map_err(|_| {
            ArrowConversionError::ValueOutOfRange(
                "JSON text is too large for PostgreSQL varlena input".to_string(),
            )
        })?;
        // SAFETY: `text` provides `len` readable bytes and the current
        // PostgreSQL memory context owns the returned allocation.
        let text_ptr = unsafe {
            pg_sys::cstring_to_text_with_len(text.as_ptr() as *const c_char, len)
        };
        Ok(pg_sys::Datum::from(text_ptr))
    }

    /// Copy trusted complete PostgreSQL NUMERIC varlena bytes into the current
    /// destination memory context.
    ///
    /// # Safety
    ///
    /// `bytes` must be a complete, detoasted NUMERIC varlena, and PostgreSQL
    /// must be running with the destination memory context selected.
    pub(super) unsafe fn copy_postgres_numeric_varlena(
        bytes: &[u8],
    ) -> pg_sys::Datum {
        let len = bytes.len();
        // SAFETY: `bytes` contains `len` initialized bytes and `palloc`
        // returns a live allocation in the current PostgreSQL memory context.
        unsafe {
            let new_ptr = pg_sys::palloc(len);
            ptr::copy_nonoverlapping(bytes.as_ptr(), new_ptr.cast(), len);
            pg_sys::Datum::from(new_ptr)
        }
    }

    pub(super) fn numeric_datum_from_float64(
        value: f64,
    ) -> ArrowConversionResult<pg_sys::Datum> {
        AnyNumeric::try_from(value)?.into_datum().ok_or(
            ArrowConversionError::InvariantViolated(
                "float to NUMERIC conversion returned SQL NULL",
            ),
        )
    }
}
