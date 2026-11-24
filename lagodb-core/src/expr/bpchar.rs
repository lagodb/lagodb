//! PostgreSQL `character(n)` equality over canonical blank-padded bytes.
//!
//! `bpchareq`/`bpcharne` compare values after `bcTruelen` strips trailing
//! blanks. PostgreSQL's assignment coercion pads every value stored in a
//! `character(n)` column to exactly `n` characters, so two stored values are
//! equal exactly when their bytes are equal under a deterministic collation.
//! A comparison value padded the same way by PostgreSQL's own `bpchar()`
//! length coercion then compares bytewise against that storage.
//!
//! Ordering is deliberately absent: `bpcharcmp` orders the trimmed values,
//! while byte order over padded values places a blank after every character
//! below it (for example a tab).

use pgrx::fcinfo;
use pgrx::pg_sys::{self, Datum};

use super::{
    ExprType, PgComparisonIdentity, PgComparisonKind, PgComparisonSignature,
    PgTextComparisonSemantics,
};
use crate::datum::PackedDetoastedVarlena;

/// Proof that a value compares by canonical bytes padded to `character(n)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PgBpcharSemantics {
    typmod: i32,
}

/// A comparison value normalized against a [`PgBpcharSemantics`] width.
pub enum PgBpcharValue {
    /// The value padded to exactly the declared width.
    Canonical(PgCanonicalBpchar),
    /// More non-blank characters than the width admits, so no stored value
    /// can equal it.
    OutsideWidth,
}

impl PgBpcharSemantics {
    /// The declared `character(n)` width of a type.
    ///
    /// `anychar_typmodin` rejects `n < 1`, so every declared width has a
    /// typmod above `VARHDRSZ`; unconstrained `bpchar` keeps the blanks it was
    /// given and has no canonical bytes.
    pub fn for_type(value_type: ExprType) -> Option<Self> {
        if value_type.type_oid != pg_sys::BPCHAROID {
            return None;
        }
        Self::for_typmod(value_type.typmod)
    }

    /// Reconstruct the semantics from a persisted typmod.
    pub fn for_typmod(typmod: i32) -> Option<Self> {
        (typmod > pg_sys::VARHDRSZ).then_some(Self { typmod })
    }

    /// Prove a built-in `bpchar = bpchar` or `bpchar <> bpchar` comparison
    /// over values of this width, returning its kind.
    ///
    /// # Safety
    ///
    /// Must run in a PostgreSQL backend during planning or plan reconstruction
    /// because the collation proof consults the live `pg_collation` catalog.
    pub unsafe fn comparison(
        self,
        identity: PgComparisonIdentity,
    ) -> Option<PgComparisonKind> {
        if identity.opcollid != pg_sys::Oid::INVALID {
            return None;
        }
        let signature = PgComparisonSignature::for_operator(identity.opno)?;
        if signature.left_type() != pg_sys::BPCHAROID
            || signature.right_type() != pg_sys::BPCHAROID
            || !matches!(
                signature.kind(),
                PgComparisonKind::Equal | PgComparisonKind::NotEqual
            )
        {
            return None;
        }
        unsafe {
            PgTextComparisonSemantics::for_equality_collation(identity.inputcollid)
        }?;
        Some(signature.kind())
    }

    #[inline]
    pub const fn typmod(self) -> i32 {
        self.typmod
    }

    /// The declared width `n` in characters.
    #[inline]
    pub const fn width(self) -> i32 {
        self.typmod - pg_sys::VARHDRSZ
    }

    /// Whether a `bpchar` value fits this width, so that some padded value
    /// can equal it.
    ///
    /// # Safety
    ///
    /// Must run on the current PostgreSQL backend thread with a valid,
    /// non-NULL `bpchar` datum.
    pub unsafe fn admits(self, datum: Datum) -> bool {
        // SAFETY: required by this method's contract.
        let source = unsafe { PackedDetoastedVarlena::from_datum(datum) };
        // SAFETY: `source` is a live detoasted bpchar on this backend thread.
        unsafe { self.admits_detoasted(&source) }
    }

    /// Pad a `bpchar` value to this width, as PostgreSQL's assignment to a
    /// `character(n)` column does.
    ///
    /// `bpcharlen` counts characters without trailing blanks, so every
    /// character `bpchar()` would clip beyond the width is a blank and the
    /// implicit coercion cannot raise its right-truncation error.
    ///
    /// # Safety
    ///
    /// Must run on the current PostgreSQL backend thread with a valid,
    /// non-NULL `bpchar` datum that stays live for the returned value.
    pub unsafe fn canonicalize(self, datum: Datum) -> PgBpcharValue {
        // SAFETY: required by this method's contract.
        let source = unsafe { PackedDetoastedVarlena::from_datum(datum) };
        // SAFETY: `source` is a live detoasted bpchar on this backend thread.
        if !unsafe { self.admits_detoasted(&source) } {
            return PgBpcharValue::OutsideWidth;
        }
        // SAFETY: bpchar(bpchar, int4, bool) returns its argument unchanged
        // or a value palloc'd in the current memory context.
        let canonical = unsafe {
            fcinfo::direct_function_call_as_datum(
                pg_sys::bpchar,
                &[
                    Some(source.as_datum()),
                    Some(Datum::from(self.typmod)),
                    Some(Datum::from(false)),
                ],
            )
        }
        .expect("strict bpchar returns non-NULL for a non-NULL value");
        PgBpcharValue::Canonical(PgCanonicalBpchar { source, canonical })
    }

    /// `bpcharlen` counts characters without trailing blanks, which a stored
    /// value of this width has at most `n` of.
    unsafe fn admits_detoasted(self, source: &PackedDetoastedVarlena) -> bool {
        // SAFETY: `source` is a detoasted bpchar; bpcharlen returns int4.
        let length = unsafe {
            fcinfo::direct_function_call_as_datum(
                pg_sys::bpcharlen,
                &[Some(source.as_datum())],
            )
        }
        .expect("strict bpcharlen returns non-NULL for a non-NULL value");
        // DatumGetInt32: the low 32 bits hold bpcharlen's int4 result.
        length.value() as i32 <= self.width()
    }
}

/// A canonical `bpchar` datum that may alias its detoasted source.
#[must_use]
pub struct PgCanonicalBpchar {
    source: PackedDetoastedVarlena,
    canonical: Datum,
}

impl PgCanonicalBpchar {
    /// The padded value as a PostgreSQL Datum for synchronous PG calls.
    #[inline]
    pub fn as_datum(&self) -> Datum {
        self.canonical
    }
}

impl Drop for PgCanonicalBpchar {
    fn drop(&mut self) {
        if self.canonical != self.source.as_datum() {
            // SAFETY: a result distinct from its argument is the palloc'd copy
            // bpchar() made for this value.
            unsafe { pg_sys::pfree(self.canonical.cast_mut_ptr()) };
        }
    }
}
