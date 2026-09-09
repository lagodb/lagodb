//! Scalar domains admitted by query planning and lowering.

use lagodb_core::expr::ExprType;
use lagodb_core::expr::{
    PgComparisonOp, PgComparisonSignature, PgTextComparisonSemantics,
};
use lagodb_core::tuple::Utf8ServerEncoding;
use pgrx::pg_sys;

pub use lagodb_core::expr::PgComparisonKind as ComparisonKind;
pub use lagodb_core::tuple::Decimal128Semantics;

/// Query-layer scalar domains with distinct representation guarantees.
///
/// Provider pruning keeps its separate, smaller IR. Exact query execution may
/// additionally use PostgreSQL boolean operators, deterministic byte equality,
/// and byte-ordered C/POSIX text comparisons; HAVING also admits
/// unbounded-typmod NUMERIC values
/// produced by integer SUM/AVG aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarSemantics {
    Integer,
    Exact,
    Having,
}

impl ScalarSemantics {
    #[inline]
    pub fn supports_type(self, value_type: ExprType) -> bool {
        if value_type.type_oid == pg_sys::NUMERICOID {
            return value_type.collation == pg_sys::InvalidOid
                && if value_type.typmod == -1 {
                    matches!(self, Self::Having)
                } else {
                    !matches!(self, Self::Integer)
                        && Decimal128Semantics::for_type(value_type).is_some()
                };
        }
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
                    && Utf8ServerEncoding::resolve().is_ok()
                    && unsafe {
                        PgTextComparisonSemantics::for_equality_collation(
                            value_type.collation,
                        )
                    }
                    .is_some()
            }
            _ => false,
        }
    }

    /// Resolve a PostgreSQL NUMERIC comparison to the single fixed Decimal128
    /// representation used by both operands. One unbounded-typmod operand is
    /// admitted here only so the planner can prove that it is a direct Const
    /// exactly encodable in the bounded operand's representation.
    pub fn decimal_comparison(
        self,
        operator: PgComparisonOp,
        left: ExprType,
        right: ExprType,
    ) -> Option<(ComparisonKind, Decimal128Semantics)> {
        if matches!(self, Self::Integer)
            || left.type_oid != pg_sys::NUMERICOID
            || right.type_oid != pg_sys::NUMERICOID
            || left.collation != pg_sys::InvalidOid
            || right.collation != pg_sys::InvalidOid
            || operator.opcollid != pg_sys::InvalidOid
            || operator.inputcollid != pg_sys::InvalidOid
        {
            return None;
        }
        let signature = operator.builtin_signature()?;
        if signature.left_type() != pg_sys::NUMERICOID
            || signature.right_type() != pg_sys::NUMERICOID
        {
            return None;
        }
        let left = Decimal128Semantics::for_type(left);
        let right = Decimal128Semantics::for_type(right);
        let decimal = match (left, right) {
            (Some(left), Some(right)) if left == right => left,
            (Some(decimal), None) | (None, Some(decimal)) => decimal,
            _ => return None,
        };
        Some((signature.kind(), decimal))
    }

    pub fn comparison(
        self,
        operator: PgComparisonOp,
        left: ExprType,
        right: ExprType,
    ) -> Option<ComparisonKind> {
        if let Some((kind, _)) = self.decimal_comparison(operator, left, right) {
            return Some(kind);
        }
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
            unsafe {
                PgTextComparisonSemantics::for_comparison(
                    operator.identity(),
                    signature.kind(),
                )
            }
            .map(|_| signature.kind())
        } else {
            (operator.opcollid == pg_sys::InvalidOid
                && operator.inputcollid == pg_sys::InvalidOid)
                .then_some(signature.kind())
        }
    }

    /// Whether DataFusion's native hash grouping exactly implements this
    /// PostgreSQL scalar domain.
    ///
    /// Deterministic PostgreSQL TEXT/VARCHAR equality uses the same UTF-8 bytes
    /// stored in Arrow, independently of the collation's ordering. BPCHAR and
    /// NAME retain PostgreSQL-specific value semantics that the provider-neutral
    /// query contract does not prove. String ordering remains a separate,
    /// stricter capability owned by Sort.
    pub fn supports_hash_group_key(self, value_type: ExprType) -> bool {
        if self != Self::Exact {
            return false;
        }
        match value_type.type_oid {
            pg_sys::INT4OID | pg_sys::INT8OID => self.supports_type(value_type),
            pg_sys::TEXTOID | pg_sys::VARCHAROID => {
                let typmod_is_normalized = value_type.typmod == -1
                    || (value_type.type_oid == pg_sys::VARCHAROID
                        && value_type.typmod >= pg_sys::VARHDRSZ as i32);
                typmod_is_normalized
                    && Utf8ServerEncoding::resolve().is_ok()
                    && unsafe {
                        PgTextComparisonSemantics::for_equality_collation(
                            value_type.collation,
                        )
                    }
                    .is_some()
            }
            pg_sys::NUMERICOID => Decimal128Semantics::for_type(value_type).is_some(),
            _ => false,
        }
    }

    /// Prove PostgreSQL GROUP BY equality against the engine's native hash
    /// grouping. The optional GROUP BY sort operator is deliberately not part
    /// of this proof: it describes ordering, which hash grouping does not use.
    pub fn supports_grouping(
        self,
        value_type: ExprType,
        equality_operator: pg_sys::Oid,
        hashable: bool,
    ) -> bool {
        let equality_type = if value_type.type_oid == pg_sys::VARCHAROID {
            // VARCHAR is binary-coercible to TEXT and uses text_ops for GROUP
            // BY equality and hashing.
            pg_sys::TEXTOID
        } else {
            value_type.type_oid
        };
        hashable
            && self.supports_hash_group_key(value_type)
            && PgComparisonSignature::for_types(
                equality_type,
                equality_type,
                ComparisonKind::Equal,
            )
            .is_some_and(|signature| signature.operator_oid() == equality_operator)
    }
}
