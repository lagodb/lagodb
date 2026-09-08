//! PostgreSQL built-in comparison signatures shared by query and providers.

mod catalog;

use pgrx::pg_sys;

use super::contract::PgComparisonOp;

/// Semantic class of a PostgreSQL comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgComparisonKind {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgNanComparison {
    IsNan,
    IsNotNan,
    StrictTrue,
    StrictFalse,
}

impl PgComparisonKind {
    /// Classify a PostgreSQL comparison with exactly one NaN operand.
    pub const fn with_nan_on(self, nan_on_left: bool) -> PgNanComparison {
        match (nan_on_left, self) {
            (_, Self::Equal) => PgNanComparison::IsNan,
            (_, Self::NotEqual) => PgNanComparison::IsNotNan,
            (false, Self::Less) => PgNanComparison::IsNotNan,
            (false, Self::LessEqual) => PgNanComparison::StrictTrue,
            (false, Self::Greater) => PgNanComparison::StrictFalse,
            (false, Self::GreaterEqual) => PgNanComparison::IsNan,
            (true, Self::Less) => PgNanComparison::StrictFalse,
            (true, Self::LessEqual) => PgNanComparison::IsNan,
            (true, Self::Greater) => PgNanComparison::IsNotNan,
            (true, Self::GreaterEqual) => PgNanComparison::StrictTrue,
        }
    }
}

/// Catalog identity of a built-in comparison operator.
///
/// This describes PostgreSQL facts only. A query engine or storage provider
/// must still apply its own type, collation, and exactness policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgComparisonSignature {
    operator_oid: u32,
    left_type: pg_sys::Oid,
    right_type: pg_sys::Oid,
    function_oid: u32,
    kind: PgComparisonKind,
}

impl PgComparisonSignature {
    pub fn for_operator(operator: pg_sys::Oid) -> Option<Self> {
        let operator = u32::from(operator);
        catalog::SIGNATURES
            .iter()
            .copied()
            .find(|signature| signature.operator_oid == operator)
    }

    pub fn for_types(
        left_type: pg_sys::Oid,
        right_type: pg_sys::Oid,
        kind: PgComparisonKind,
    ) -> Option<Self> {
        catalog::SIGNATURES.iter().copied().find(|signature| {
            signature.left_type == left_type
                && signature.right_type == right_type
                && signature.kind == kind
        })
    }

    #[inline]
    pub fn operator_oid(self) -> pg_sys::Oid {
        pg_sys::Oid::from(self.operator_oid)
    }

    #[inline]
    pub const fn left_type(self) -> pg_sys::Oid {
        self.left_type
    }

    #[inline]
    pub const fn right_type(self) -> pg_sys::Oid {
        self.right_type
    }

    #[inline]
    pub fn function_oid(self) -> pg_sys::Oid {
        pg_sys::Oid::from(self.function_oid)
    }

    #[inline]
    pub const fn kind(self) -> PgComparisonKind {
        self.kind
    }

    #[inline]
    pub fn matches(self, operator: PgComparisonOp) -> bool {
        operator.opno == self.operator_oid()
            && operator.opfuncid == self.function_oid()
            && operator.opresulttype == pg_sys::BOOLOID
    }
}

impl PgComparisonOp {
    #[inline]
    pub fn builtin_signature(self) -> Option<PgComparisonSignature> {
        PgComparisonSignature::for_operator(self.opno)
            .filter(|signature| signature.matches(self))
    }
}
