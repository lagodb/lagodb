//! Shared PostgreSQL scalar-comparison capability proofs.
//!
//! Query planning and storage providers use these types as the single source
//! of truth for semantics that are independent of DataFusion, Iceberg, and
//! Parquet. Catalog access happens while planning; execution consumes the
//! resulting typed plan and performs no repeated collation lookup.

use pgrx::pg_sys;

use super::{PgComparisonIdentity, PgComparisonKind, PgComparisonSignature};

/// Raw UTF-8 string comparison semantics implemented by Arrow-backed engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgTextComparisonSemantics {
    /// Byte equality, which matches every deterministic PostgreSQL collation.
    Equality,
    /// Byte ordering, which is admitted only for built-in C/POSIX collations.
    Ordering,
}

impl PgTextComparisonSemantics {
    /// Prove byte equality for a resolved PostgreSQL text collation.
    ///
    /// # Safety
    ///
    /// Must run in a PostgreSQL backend during planning because this consults
    /// the live `pg_collation` catalog.
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
    /// Must run in a PostgreSQL backend during planning because deterministic
    /// equality consults the live `pg_collation` catalog.
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
                Self::is_byte_ordered(identity.inputcollid).then_some(Self::Ordering)
            }
        }
    }

    #[inline]
    pub const fn is_byte_ordered(collation: pg_sys::Oid) -> bool {
        collation.to_u32() == pg_sys::C_COLLATION_OID.to_u32()
            || collation.to_u32() == pg_sys::POSIX_COLLATION_OID.to_u32()
    }
}
