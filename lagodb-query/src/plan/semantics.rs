//! Scalar domains admitted by query planning and lowering.

use lagodb_core::expr::ExprType;
use lagodb_core::expr::{PgComparisonOp, PgComparisonSignature};
use pgrx::pg_sys;

pub use lagodb_core::expr::PgComparisonKind as ComparisonKind;

/// Query-layer scalar domains with distinct representation guarantees.
///
/// Provider pruning keeps its separate, smaller IR. Exact query execution may
/// additionally use PostgreSQL boolean operators and byte-semantic C/POSIX
/// text comparisons; HAVING also admits unbounded-typmod NUMERIC values
/// produced by integer SUM/AVG aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarSemantics {
    Integer,
    Exact,
    Having,
}

impl ScalarSemantics {
    #[inline]
    pub const fn supports_type(self, value_type: ExprType) -> bool {
        if value_type.typmod != -1 {
            return false;
        }
        match value_type.type_oid {
            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID => {
                value_type.collation.to_u32() == pg_sys::InvalidOid.to_u32()
            }
            pg_sys::BOOLOID => {
                !matches!(self, Self::Integer)
                    && value_type.collation.to_u32() == pg_sys::InvalidOid.to_u32()
            }
            pg_sys::TEXTOID => {
                matches!(self, Self::Exact)
                    && Self::is_byte_collation(value_type.collation)
            }
            pg_sys::NUMERICOID => {
                matches!(self, Self::Having)
                    && value_type.collation.to_u32() == pg_sys::InvalidOid.to_u32()
            }
            _ => false,
        }
    }

    pub fn comparison(
        self,
        operator: PgComparisonOp,
        left: ExprType,
        right: ExprType,
    ) -> Option<ComparisonKind> {
        if !self.supports_type(left) || !self.supports_type(right) {
            return None;
        }
        let signature = operator.builtin_signature()?;
        if left.type_oid != signature.left_type()
            || right.type_oid != signature.right_type()
        {
            return None;
        }
        if left.type_oid == pg_sys::TEXTOID {
            (operator.opcollid == operator.inputcollid
                && Self::is_byte_collation(operator.inputcollid))
            .then_some(signature.kind())
        } else {
            (operator.opcollid == pg_sys::InvalidOid
                && operator.inputcollid == pg_sys::InvalidOid)
                .then_some(signature.kind())
        }
    }

    /// PostgreSQL resolves GROUP BY equality and its optional sort operator
    /// through `get_sort_group_operators`. The engine currently implements
    /// keys only for the exact built-in int4/int8 families below.
    pub fn supports_grouping(
        self,
        value_type: ExprType,
        equality_operator: pg_sys::Oid,
        sort_operator: pg_sys::Oid,
        hashable: bool,
    ) -> bool {
        if self != Self::Integer
            || !hashable
            || !matches!(value_type.type_oid, pg_sys::INT4OID | pg_sys::INT8OID)
            || !self.supports_type(value_type)
        {
            return false;
        }
        let equality = PgComparisonSignature::for_types(
            value_type.type_oid,
            value_type.type_oid,
            ComparisonKind::Equal,
        );
        let ordering = PgComparisonSignature::for_types(
            value_type.type_oid,
            value_type.type_oid,
            ComparisonKind::Less,
        );
        equality
            .is_some_and(|signature| signature.operator_oid() == equality_operator)
            && ordering
                .is_some_and(|signature| signature.operator_oid() == sort_operator)
    }

    #[inline]
    const fn is_byte_collation(collation: pg_sys::Oid) -> bool {
        collation.to_u32() == pg_sys::C_COLLATION_OID.to_u32()
            || collation.to_u32() == pg_sys::POSIX_COLLATION_OID.to_u32()
    }
}
