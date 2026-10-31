//! Shared PostgreSQL scalar-comparison capability proofs.
//!
//! Query planning and storage providers use these types as the single source
//! of truth for semantics that are independent of DataFusion, Iceberg, and
//! Parquet. Catalog access happens while planning or reconstructing a plan;
//! row execution consumes the typed plan without repeated collation lookup.

use pgrx::pg_sys::{self, ffi::pg_guard_ffi_boundary};

use super::{PgComparisonIdentity, PgComparisonKind, PgComparisonSignature};

unsafe extern "C-unwind" {
    fn lagodb_collation_is_c(collation: pg_sys::Oid) -> bool;
}

/// Raw UTF-8 string comparison semantics implemented by Arrow-backed engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgTextComparisonSemantics {
    /// Byte equality, which matches every deterministic PostgreSQL collation.
    Equality,
    /// Byte ordering under a collation PostgreSQL recognizes as C-equivalent.
    Ordering,
}

impl PgTextComparisonSemantics {
    /// Prove byte equality for a resolved PostgreSQL text collation.
    ///
    /// # Safety
    ///
    /// Must run in a PostgreSQL backend during planning or plan reconstruction
    /// because this consults the live `pg_collation` catalog.
    pub unsafe fn for_equality_collation(collation: pg_sys::Oid) -> Option<Self> {
        (collation != pg_sys::Oid::INVALID
            && unsafe { pg_sys::get_collation_isdeterministic(collation) })
        .then_some(Self::Equality)
    }

    /// Prove that a built-in text comparison can be evaluated over UTF-8 bytes.
    ///
    /// `opcollid` is the collation of the boolean result and must therefore be
    /// invalid. `inputcollid` is PostgreSQL's resolved input collation.
    ///
    /// # Safety
    ///
    /// Must run in a PostgreSQL backend during planning or plan reconstruction
    /// because equality and ordering proofs can consult PostgreSQL's live
    /// collation catalog and locale cache.
    pub unsafe fn for_comparison(
        identity: PgComparisonIdentity,
        operator: PgComparisonKind,
    ) -> Option<Self> {
        if identity.opcollid != pg_sys::Oid::INVALID {
            return None;
        }
        let signature = PgComparisonSignature::for_operator(identity.opno)?;
        if signature.left_type() != pg_sys::TEXTOID
            || signature.right_type() != pg_sys::TEXTOID
            || signature.kind() != operator
        {
            return None;
        }
        match operator {
            PgComparisonKind::Equal | PgComparisonKind::NotEqual => unsafe {
                Self::for_equality_collation(identity.inputcollid)
            },
            PgComparisonKind::Less
            | PgComparisonKind::LessEqual
            | PgComparisonKind::Greater
            | PgComparisonKind::GreaterEqual => {
                // SAFETY: this proof runs while PostgreSQL owns the live
                // planner or reconstructed plan and its catalog state.
                unsafe { Self::is_byte_ordered(identity.inputcollid) }
                    .then_some(Self::Ordering)
            }
        }
    }

    /// Whether PostgreSQL executes this collation as byte ordering.
    ///
    /// # Safety
    ///
    /// Must run in a PostgreSQL backend during planning or plan
    /// reconstruction because PostgreSQL may consult its locale cache and the
    /// live `pg_collation` catalog.
    #[inline]
    pub unsafe fn is_byte_ordered(collation: pg_sys::Oid) -> bool {
        // SAFETY: the caller guarantees a live PostgreSQL backend. The C
        // adapter handles InvalidOid uniformly and borrows cached locale state
        // only for the duration of the call.
        unsafe { pg_guard_ffi_boundary(|| lagodb_collation_is_c(collation)) }
    }
}
