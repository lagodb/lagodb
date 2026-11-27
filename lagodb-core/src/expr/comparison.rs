//! PostgreSQL built-in comparison signatures shared by query and providers.

mod catalog;
mod temporal;

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

pub use temporal::{PgDateComparisonOutcome, PgDateTimestampComparison};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgNanComparison {
    IsNan,
    IsNotNan,
    StrictTrue,
    StrictFalse,
}

impl PgComparisonKind {
    /// Return the equivalent comparison after exchanging its operands.
    #[inline]
    pub const fn commuted(self) -> Self {
        match self {
            Self::Equal | Self::NotEqual => self,
            Self::Less => Self::Greater,
            Self::LessEqual => Self::GreaterEqual,
            Self::Greater => Self::Less,
            Self::GreaterEqual => Self::LessEqual,
        }
    }

    /// Return the comparison whose truth set complements `self` on non-NULL
    /// totally ordered operands.
    #[inline]
    pub const fn negated(self) -> Self {
        match self {
            Self::Equal => Self::NotEqual,
            Self::NotEqual => Self::Equal,
            Self::Less => Self::GreaterEqual,
            Self::LessEqual => Self::Greater,
            Self::Greater => Self::LessEqual,
            Self::GreaterEqual => Self::Less,
        }
    }

    /// Display symbol used by EXPLAIN output.
    #[inline]
    pub const fn explain_symbol(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::Less => "<",
            Self::LessEqual => "<=",
            Self::Greater => ">",
            Self::GreaterEqual => ">=",
        }
    }

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

    /// Build the operator of a comparison that LagoDB derives itself rather
    /// than reads from a PostgreSQL expression.
    ///
    /// The signature carries no expression collation, so this is valid only
    /// for non-collatable operand types such as boolean and date.
    #[inline]
    pub(crate) fn collationless_operator(self) -> PgComparisonOp {
        PgComparisonOp {
            opno: self.operator_oid(),
            opfuncid: self.function_oid(),
            opresulttype: pg_sys::BOOLOID,
            opcollid: pg_sys::InvalidOid,
            inputcollid: pg_sys::InvalidOid,
        }
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
