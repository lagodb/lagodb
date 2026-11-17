//! Scalar domains admitted by query planning and lowering.

use lagodb_core::expr::ExprType;
use lagodb_core::expr::{
    PgBpcharSemantics, PgComparisonOp, PgComparisonSignature,
    PgTextComparisonSemantics,
};
use pgrx::pg_sys;

pub use lagodb_core::datum::Decimal128Semantics;
pub use lagodb_core::expr::PgComparisonKind as ComparisonKind;

/// Query-layer scalar domains with distinct representation guarantees.
///
/// Provider pruning keeps its separate, smaller IR. Exact query execution may
/// additionally use PostgreSQL boolean operators, deterministic byte equality,
/// and PostgreSQL-recognized C-equivalent text comparisons; HAVING also admits
/// the unbounded-typmod NUMERIC values produced by integer AVG aggregates.
/// Float comparisons and grouping inherit DataFusion's NaN semantics, an
/// accepted engine difference listed in docs/query-offload-semantics.md.
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
        if value_type.typmod != -1
            && !matches!(
                value_type.type_oid,
                pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID
            )
        {
            return false;
        }
        match value_type.type_oid {
            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID => {
                value_type.collation.to_u32() == pg_sys::InvalidOid.to_u32()
            }
            pg_sys::BYTEAOID
            | pg_sys::UUIDOID
            | pg_sys::DATEOID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID => {
                matches!(self, Self::Exact)
                    && value_type.collation == pg_sys::InvalidOid
            }
            pg_sys::BOOLOID
            // For floats, DataFusion 55 normalizes -0 to +0 before every
            // comparison kernel (datafusion-physical-expr-common `apply_cmp`),
            // so only NaN differs from PostgreSQL: Arrow's IEEE totalOrder
            // places a negative-sign NaN below every number and keeps NaN
            // payloads distinct, where PostgreSQL treats every NaN as one
            // value above +Infinity. Comparisons with a NaN constant are
            // lowered to exact NaN tests before reaching this domain.
            // DataFusion 55's IN-list skips that normalization and compares
            // float bits, so `f IN (0.0)` misses -0 rows. This is an
            // upstream-owned defect, not worked around here; it is fixed by
            // https://github.com/apache/datafusion/pull/25186 (merged after
            // DataFusion 55) and recorded in docs/query-offload-semantics.md.
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID => {
                !matches!(self, Self::Integer)
                    && value_type.collation == pg_sys::InvalidOid
            }
            pg_sys::TEXTOID => {
                matches!(self, Self::Exact)
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

    /// Resolve a `bpchar` comparison between a `character(n)` column and a
    /// value padded to the column width at execution
    /// ([`ExecutionExpr::BpcharValue`](super::ExecutionExpr::BpcharValue)).
    /// Only `=` / `<>` under a deterministic collation reduce to byte
    /// equality over the padded values.
    pub fn bpchar_comparison(
        self,
        operator: PgComparisonOp,
        column: ExprType,
        value: ExprType,
    ) -> Option<(ComparisonKind, PgBpcharSemantics)> {
        if self != Self::Exact || value.type_oid != pg_sys::BPCHAROID {
            return None;
        }
        operator.builtin_signature()?;
        let semantics = PgBpcharSemantics::for_type(column)?;
        let kind = unsafe { semantics.comparison(operator.identity()) }?;
        Some((kind, semantics))
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
        if !Self::has_exact_operand_coercion(left.type_oid, right.type_oid) {
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

    /// Whether DataFusion's implicit operand coercion preserves PostgreSQL's
    /// comparison for this catalog operand pair.
    ///
    /// Integer and float widths widen losslessly, as in PostgreSQL's cross-type
    /// operators. Every other cross-type pair must be normalized during
    /// planning; for example, DataFusion compares date with timestamp as
    /// nanosecond timestamps, which do not cover PostgreSQL's date range.
    fn has_exact_operand_coercion(left: pg_sys::Oid, right: pg_sys::Oid) -> bool {
        left == right
            || matches!(
                (left, right),
                (
                    pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID,
                    pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                ) | (
                    pg_sys::FLOAT4OID | pg_sys::FLOAT8OID,
                    pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
                )
            )
    }

    /// Whether DataFusion's native hash grouping implements this PostgreSQL
    /// scalar domain.
    ///
    /// Deterministic PostgreSQL TEXT/VARCHAR equality uses the same UTF-8 bytes
    /// stored in Arrow, independently of the collation's ordering. A positive
    /// `character(n)` typmod proves the value is padded to its width, so
    /// `bpchareq` is byte equality; unconstrained BPCHAR and NAME retain
    /// PostgreSQL-specific value semantics that the provider-neutral query
    /// contract does not prove. String ordering remains a separate, stricter
    /// capability owned by Sort. Float group values fold -0 into +0 but keep
    /// NaN payloads apart, the accepted engine difference shared with query
    /// DISTINCT.
    pub fn supports_hash_group_key(self, value_type: ExprType) -> bool {
        if self != Self::Exact {
            return false;
        }
        match value_type.type_oid {
            pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID => self.supports_type(value_type),
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID => {
                let typmod_is_normalized = match value_type.type_oid {
                    pg_sys::TEXTOID => value_type.typmod == -1,
                    pg_sys::VARCHAROID => {
                        value_type.typmod == -1
                            || value_type.typmod >= pg_sys::VARHDRSZ as i32
                    }
                    _ => PgBpcharSemantics::for_type(value_type).is_some(),
                };
                typmod_is_normalized
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

    /// Whether Arrow byte equality implements PostgreSQL equality for a
    /// text-family DISTINCT key or DISTINCT aggregate argument.
    ///
    /// Byte equality matches every deterministic collation. BPCHAR equality
    /// and hashing ignore trailing blanks (`bpchareq`, `hashbpchar`), so its
    /// bytes are canonical only under a declared `character(n)` length, to
    /// which PostgreSQL pads every assigned value. Unconstrained `bpchar`
    /// keeps the blanks it was given: `'a'` and `'a '` are one PostgreSQL
    /// value but two byte strings.
    pub fn supports_text_distinct_key(value_type: ExprType) -> bool {
        let canonical_bytes = match value_type.type_oid {
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::NAMEOID => true,
            pg_sys::BPCHAROID => PgBpcharSemantics::for_type(value_type).is_some(),
            _ => false,
        };
        canonical_bytes
            && unsafe {
                PgTextComparisonSemantics::for_equality_collation(
                    value_type.collation,
                )
            }
            .is_some()
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
