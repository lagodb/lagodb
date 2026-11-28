//! Scalar domains admitted by query planning and lowering.

use lagodb_core::expr::ExprType;
use lagodb_core::expr::{
    PgBpcharSemantics, PgComparisonOp, PgComparisonSignature,
    PgTextComparisonSemantics,
};
use pgrx::pg_sys;

pub use lagodb_core::datum::Decimal128Semantics;
pub use lagodb_core::expr::PgComparisonKind as ComparisonKind;

use super::FixedDecimalType;

/// Query-layer scalar domains with distinct representation guarantees.
///
/// Provider pruning keeps its separate, smaller IR. Exact query execution may
/// additionally use PostgreSQL boolean operators, deterministic byte equality,
/// and PostgreSQL-recognized C-equivalent text comparisons; HAVING also
/// compares the NUMERIC values the engine keeps in Float64
/// ([`NumericRepr::Float64`]). Float comparisons and grouping inherit
/// DataFusion's NaN semantics, an accepted engine difference listed in
/// docs/query-offload-semantics.md.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarSemantics {
    Integer,
    Exact,
    Having,
}

/// The engine representation of a PostgreSQL NUMERIC value. PostgreSQL gives
/// most NUMERIC results no typmod, while the engine computes each as a fixed
/// decimal, an integer or a Float64; a comparison is proven over these
/// ([`ScalarSemantics::numeric_repr_comparison`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericRepr {
    /// Decimal128 in a fixed precision and scale: bounded columns, constants,
    /// decimal arithmetic, integer casts, conditionals over these, and the
    /// decimal aggregates.
    Decimal(FixedDecimalType),
    /// An exact Int16, Int32 or Int64 value with its lossless decimal image
    /// ([`FixedDecimalType::for_integer`]): `extract(year ...)`
    /// ([`ScalarFunctionKind::ExtractYear`](super::ScalarFunctionKind::ExtractYear)),
    /// an int4 constant bound from a NUMERIC one, or the integer under
    /// PostgreSQL's integer-to-NUMERIC cast.
    Integer(FixedDecimalType),
    /// An approximate Float64 result: integer AVG, variance and standard
    /// deviation (FLOAT8 execution type), HAVING arithmetic over them, and
    /// conditionals whose branches are these or runtime values.
    Float64,
    /// A NUMERIC runtime value, which the engine reads as Float64.
    Float64Value,
}

impl NumericRepr {
    /// The representation of an engine value of `execution_type`: a fixed
    /// decimal, an integer, or FLOAT8. An unbounded NUMERIC execution type
    /// names no engine representation, so it has none.
    pub fn for_execution_type(execution_type: ExprType) -> Option<Self> {
        if let Some(decimal) = FixedDecimalType::for_type(execution_type) {
            return Some(Self::Decimal(decimal));
        }
        if execution_type.typmod != -1
            || execution_type.collation != pg_sys::InvalidOid
        {
            return None;
        }
        match execution_type.type_oid {
            pg_sys::FLOAT8OID => Some(Self::Float64),
            integer => FixedDecimalType::for_integer(integer).map(Self::Integer),
        }
    }
}

impl ScalarSemantics {
    #[inline]
    pub fn supports_type(self, value_type: ExprType) -> bool {
        // A NUMERIC value without a typmod has no type of its own; its
        // comparisons are proven over its engine representation
        // ([`Self::numeric_repr_comparison`]).
        if value_type.type_oid == pg_sys::NUMERICOID {
            return value_type.collation == pg_sys::InvalidOid
                && !matches!(self, Self::Integer)
                && Decimal128Semantics::for_type(value_type).is_some();
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
            // upstream-owned defect, not worked around here; it is fixed on
            // main by https://github.com/apache/datafusion/pull/25186, which
            // DataFusion 55.3 does not include, and recorded in
            // docs/query-offload-semantics.md.
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

    /// Resolve a PostgreSQL NUMERIC comparison to the fixed decimal types of
    /// its left and right operands. Two bounded operands may differ; they
    /// compare in their lossless common type ([`FixedDecimalType::common`]),
    /// which must exist within Decimal128. One unbounded-typmod operand is
    /// admitted only so the planner can prove that it is a direct Const
    /// exactly encodable in the bounded operand's representation, which both
    /// sides then share.
    pub fn decimal_comparison(
        self,
        operator: PgComparisonOp,
        left: ExprType,
        right: ExprType,
    ) -> Option<(ComparisonKind, FixedDecimalType, FixedDecimalType)> {
        if left.type_oid != pg_sys::NUMERICOID
            || right.type_oid != pg_sys::NUMERICOID
            || left.collation != pg_sys::InvalidOid
            || right.collation != pg_sys::InvalidOid
        {
            return None;
        }
        let (left, right) = match (
            FixedDecimalType::for_type(left),
            FixedDecimalType::for_type(right),
        ) {
            (Some(left), Some(right)) => (left, right),
            (Some(decimal), None) | (None, Some(decimal)) => (decimal, decimal),
            (None, None) => return None,
        };
        let kind = self.numeric_repr_comparison(
            operator,
            NumericRepr::Decimal(left),
            NumericRepr::Decimal(right),
        )?;
        Some((kind, left, right))
    }

    /// Resolve a built-in NUMERIC comparison between operands in the given
    /// engine representations. Two fixed decimals compare in their lossless
    /// common type, and two integers compare exactly in the wider integer;
    /// an integer compared with a fixed decimal is first converted to its
    /// decimal image, so it reaches here as a decimal. HAVING also compares in
    /// Float64 where an operand is an approximate Float64 result: integer
    /// AVG, variance and standard deviation are approximate in the engine, as
    /// docs/query-offload-semantics.md lists. Without such an operand a
    /// runtime value, which the engine reads as Float64, would make an exact
    /// PostgreSQL comparison approximate, so it declines, as does any other
    /// pair.
    pub fn numeric_repr_comparison(
        self,
        operator: PgComparisonOp,
        left: NumericRepr,
        right: NumericRepr,
    ) -> Option<ComparisonKind> {
        let kind = self.numeric_comparison(operator)?;
        let admitted = match (left, right) {
            (NumericRepr::Decimal(left), NumericRepr::Decimal(right)) => {
                left.common(right).is_some()
            }
            (NumericRepr::Integer(_), NumericRepr::Integer(_)) => true,
            (NumericRepr::Float64, _) | (_, NumericRepr::Float64) => {
                self == Self::Having
            }
            _ => false,
        };
        admitted.then_some(kind)
    }

    /// The comparison a built-in `numeric` operator performs in this domain.
    pub fn numeric_comparison(
        self,
        operator: PgComparisonOp,
    ) -> Option<ComparisonKind> {
        if matches!(self, Self::Integer)
            || operator.opcollid != pg_sys::InvalidOid
            || operator.inputcollid != pg_sys::InvalidOid
        {
            return None;
        }
        let signature = operator.builtin_signature()?;
        (signature.left_type() == pg_sys::NUMERICOID
            && signature.right_type() == pg_sys::NUMERICOID)
            .then(|| signature.kind())
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
        // A built-in NUMERIC operator compares only in a representation both
        // operands share; two fixed decimals without a Decimal128 common type
        // have none.
        if self.numeric_comparison(operator).is_some() {
            return self
                .decimal_comparison(operator, left, right)
                .map(|(kind, ..)| kind);
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

    /// Whether DataFusion's native hash grouping implements PostgreSQL
    /// equality for this scalar domain.
    ///
    /// GROUP BY and query DISTINCT both lower to DataFusion grouping keys and
    /// share this one domain. Fixed-width keys compare their Arrow values,
    /// which is PostgreSQL's equality for these types. Float keys fold -0 into
    /// one +0 group but keep distinct NaN payloads apart, where PostgreSQL
    /// treats every NaN as equal; see docs/query-offload-semantics.md.
    /// Text-family keys use Arrow byte equality
    /// ([`Self::supports_text_distinct_key`]). Timestamp OIDs cover both Arrow
    /// microseconds and nanoseconds; DataFusion admission declines a
    /// nanosecond grouping key, which does not preserve PostgreSQL equality.
    pub fn supports_hash_group_key(self, value_type: ExprType) -> bool {
        if self != Self::Exact {
            return false;
        }
        match value_type.type_oid {
            pg_sys::BOOLOID
            | pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID
            | pg_sys::BYTEAOID
            | pg_sys::UUIDOID
            | pg_sys::DATEOID
            | pg_sys::TIMEOID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID => {
                value_type.typmod == -1 && value_type.collation == pg_sys::InvalidOid
            }
            pg_sys::TEXTOID
            | pg_sys::VARCHAROID
            | pg_sys::BPCHAROID
            | pg_sys::NAMEOID => Self::supports_text_distinct_key(value_type),
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

    /// Prove that a GROUP BY key of `value_type` uses PostgreSQL's built-in
    /// hashable equality for that type. The lowered key's execution type must
    /// separately be a supported hash key
    /// ([`GroupExpr::try_new`](super::GroupExpr::try_new)): a NUMERIC key may
    /// execute as a fixed decimal or an integer. The optional GROUP BY sort
    /// operator is deliberately not part of this proof: it describes
    /// ordering, which hash grouping does not use.
    pub fn supports_group_equality(
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
            && self == Self::Exact
            && PgComparisonSignature::for_types(
                equality_type,
                equality_type,
                ComparisonKind::Equal,
            )
            .is_some_and(|signature| signature.operator_oid() == equality_operator)
    }
}
