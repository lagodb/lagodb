//! PostgreSQL-semantic join nodes for provider-neutral query plans.

use lagodb_core::expr::{ColumnRef, ExprType, PgComparisonKind, PgComparisonOp};
use lagodb_core::expr::{PgComparisonSignature, PgTextComparisonSemantics};
use lagodb_core::tuple::Utf8ServerEncoding;
use pgrx::pg_sys;

use super::ir::RowEstimate;
use super::{Decimal128Semantics, ExecutionExpr, QueryNode, QueryPlanError};

/// Join semantics represented by the provider-neutral query plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    LeftAnti,
    /// Synthetic mark join used to preserve NULL-sensitive SubPlan semantics.
    /// The compiler consumes the mark before producing the query output.
    LeftMark,
}

impl JoinType {
    pub(crate) const fn wire_id(self) -> i32 {
        match self {
            Self::Inner => 1,
            Self::Left => 2,
            Self::Right => 3,
            Self::Full => 4,
            Self::LeftSemi => 5,
            Self::LeftAnti => 6,
            Self::LeftMark => 7,
        }
    }

    pub(crate) const fn from_wire_id(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::Inner),
            2 => Some(Self::Left),
            3 => Some(Self::Right),
            4 => Some(Self::Full),
            5 => Some(Self::LeftSemi),
            6 => Some(Self::LeftAnti),
            7 => Some(Self::LeftMark),
            _ => None,
        }
    }

    #[inline]
    pub const fn emits_right(self) -> bool {
        !matches!(self, Self::LeftSemi | Self::LeftAnti | Self::LeftMark)
    }
}

/// The only supported Mark Join shape: test (or invert) the synthetic mark,
/// OR it with an outer-column IS NULL test, then discard the mark column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkFilter {
    null_test: ColumnRef,
    anti: bool,
}

impl MarkFilter {
    pub(crate) fn new(null_test: ColumnRef, anti: bool) -> Self {
        Self { null_test, anti }
    }

    #[inline]
    pub const fn null_test(self) -> ColumnRef {
        self.null_test
    }

    #[inline]
    pub const fn anti(self) -> bool {
        self.anti
    }
}

/// One exact, hashable PostgreSQL equality key with explicit side identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinKey {
    left: ColumnRef,
    right: ColumnRef,
    operator: PgComparisonOp,
}

impl JoinKey {
    pub fn try_new(
        left: ColumnRef,
        right: ColumnRef,
        operator: PgComparisonOp,
    ) -> Result<Self, QueryPlanError> {
        let key = Self {
            left,
            right,
            operator,
        };
        key.validate_semantics()?;
        Ok(key)
    }

    #[inline]
    pub const fn left(self) -> ColumnRef {
        self.left
    }

    #[inline]
    pub const fn right(self) -> ColumnRef {
        self.right
    }

    #[inline]
    pub const fn operator(self) -> PgComparisonOp {
        self.operator
    }

    pub(crate) fn validate_semantics(self) -> Result<ExprType, QueryPlanError> {
        let left = self.left.value_type;
        let right = self.right.value_type;
        // Keep the complete type contract, including NUMERIC typmod. Decimal
        // coefficients use each column's declared scale, so comparing
        // differently scaled coefficients without coercion is not PostgreSQL
        // numeric equality. Decline that shape until key coercion owns a common
        // representation.
        if left != right
            || !Self::supports_type(left)
            || !Self::supports_operator_semantics(left, self.operator)
            || PgComparisonSignature::for_operator(self.operator.opno)
                .filter(|signature| {
                    signature.left_type() == left.type_oid
                        && signature.right_type() == right.type_oid
                })
                .map(PgComparisonSignature::kind)
                != Some(PgComparisonKind::Equal)
        {
            return Err(QueryPlanError::UnsupportedJoinKey);
        }
        Ok(left)
    }

    fn supports_type(value_type: ExprType) -> bool {
        match value_type.type_oid {
            pg_sys::BOOLOID
            | pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::DATEOID
            | pg_sys::UUIDOID => {
                value_type.typmod == -1 && value_type.collation == pg_sys::InvalidOid
            }
            pg_sys::TEXTOID => {
                value_type.typmod == -1
                    && Utf8ServerEncoding::resolve().is_ok()
                    && value_type.collation != pg_sys::InvalidOid
                    // Hash keys use the stored UTF-8 bytes. Decline
                    // nondeterministic collations: PostgreSQL's texteq uses
                    // collation comparison there, so byte hashing is not an
                    // exact equality implementation.
                    && unsafe {
                        PgTextComparisonSemantics::for_equality_collation(
                            value_type.collation,
                        )
                    }
                    .is_some()
            }
            pg_sys::NUMERICOID => {
                // The managed Iceberg source exposes the finite Decimal128
                // domain and rejects NaN/Infinity at the provider boundary.
                // Equality and hashing are aligned for every value admitted by
                // this source ABI. Supporting the complete PostgreSQL NUMERIC
                // domain requires a separate provider representation change,
                // not join-local coercion or a weakened equality rule.
                Decimal128Semantics::for_type(value_type).is_some()
            }
            pg_sys::FLOAT4OID | pg_sys::FLOAT8OID => {
                // Keep the known correctness difference at the single join-key
                // gate. DataFusion 55 normalizes signed zero, but its hash keeps
                // NaN payload bits and its comparator uses IEEE totalOrder.
                // PostgreSQL's float4eq/float8eq considers every NaN equal, so
                // distinct NaN encodings can miss a match. The source contract
                // must establish a NaN-domain invariant or provide a vectorized
                // PostgreSQL-semantic key representation; do not add per-row
                // PostgreSQL callbacks.
                false
            }
            _ => false,
        }
    }

    fn supports_operator_semantics(
        value_type: ExprType,
        operator: PgComparisonOp,
    ) -> bool {
        if value_type.type_oid == pg_sys::TEXTOID {
            return operator.inputcollid == value_type.collation
                && unsafe {
                    PgTextComparisonSemantics::for_comparison(
                        operator.identity(),
                        PgComparisonKind::Equal,
                    )
                }
                .is_some();
        }
        operator.opcollid == pg_sys::InvalidOid
            && operator.inputcollid == pg_sys::InvalidOid
    }
}

/// Binary join with equi keys and an optional residual evaluated as part of ON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinNode {
    join_type: JoinType,
    left: Box<QueryNode>,
    right: Box<QueryNode>,
    keys: Box<[JoinKey]>,
    on_filter: Option<ExecutionExpr>,
    null_aware: bool,
    mark_filter: Option<MarkFilter>,
    estimated_key_rows: RowEstimate,
    estimated_rows: RowEstimate,
}

impl JoinNode {
    pub fn new(
        join_type: JoinType,
        left: QueryNode,
        right: QueryNode,
        keys: Box<[JoinKey]>,
        on_filter: Option<ExecutionExpr>,
        estimated_key_rows: f64,
        estimated_rows: f64,
    ) -> Result<Self, QueryPlanError> {
        let keyless_semi_anti = keys.is_empty()
            && matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti);
        if join_type == JoinType::LeftMark || (keys.is_empty() && !keyless_semi_anti)
        {
            return Err(QueryPlanError::EmptyJoinKeys);
        }
        Ok(Self {
            join_type,
            left: Box::new(left),
            right: Box::new(right),
            keys,
            on_filter,
            null_aware: false,
            mark_filter: None,
            estimated_key_rows: RowEstimate::try_new(estimated_key_rows)?,
            estimated_rows: RowEstimate::try_new(estimated_rows)?,
        })
    }

    pub fn new_null_aware_anti(
        left: QueryNode,
        right: QueryNode,
        key: JoinKey,
        estimated_key_rows: f64,
        estimated_rows: f64,
    ) -> Result<Self, QueryPlanError> {
        Ok(Self {
            join_type: JoinType::LeftAnti,
            left: Box::new(left),
            right: Box::new(right),
            keys: Box::new([key]),
            on_filter: None,
            null_aware: true,
            mark_filter: None,
            estimated_key_rows: RowEstimate::try_new(estimated_key_rows)?,
            estimated_rows: RowEstimate::try_new(estimated_rows)?,
        })
    }

    /// Construct the closed SubPlan Mark shape. The synthetic mark is consumed
    /// by the compiler and can never be referenced by a Project expression.
    pub fn new_filtered_mark_subplan(
        left: QueryNode,
        right: QueryNode,
        key: JoinKey,
        null_test: ColumnRef,
        anti: bool,
        estimated_key_rows: f64,
        estimated_rows: f64,
    ) -> Result<Self, QueryPlanError> {
        let filter = MarkFilter::new(null_test, anti);
        if !filter.null_test.same_storage_column(key.left()) {
            return Err(QueryPlanError::UnsupportedTopology);
        }
        Ok(Self {
            join_type: JoinType::LeftMark,
            left: Box::new(left),
            right: Box::new(right),
            keys: Box::new([key]),
            on_filter: None,
            null_aware: false,
            mark_filter: Some(filter),
            estimated_key_rows: RowEstimate::try_new(estimated_key_rows)?,
            estimated_rows: RowEstimate::try_new(estimated_rows)?,
        })
    }

    #[inline]
    pub const fn join_type(&self) -> JoinType {
        self.join_type
    }

    #[inline]
    pub fn left(&self) -> &QueryNode {
        &self.left
    }

    #[inline]
    pub fn right(&self) -> &QueryNode {
        &self.right
    }

    #[inline]
    pub fn keys(&self) -> &[JoinKey] {
        &self.keys
    }

    #[inline]
    pub const fn on_filter(&self) -> Option<&ExecutionExpr> {
        self.on_filter.as_ref()
    }

    #[inline]
    pub const fn null_aware(&self) -> bool {
        self.null_aware
    }

    #[inline]
    pub const fn mark_filter(&self) -> Option<MarkFilter> {
        self.mark_filter
    }

    /// Rows passing the equi keys before the join-level residual is applied.
    #[inline]
    pub const fn estimated_key_rows(&self) -> f64 {
        self.estimated_key_rows.get()
    }

    #[inline]
    pub const fn estimated_rows(&self) -> f64 {
        self.estimated_rows.get()
    }
}
