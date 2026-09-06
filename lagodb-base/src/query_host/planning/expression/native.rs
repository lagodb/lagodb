//! Catalog-bound PostgreSQL scalar-function classification.
//!
//! Native-first recursion is used, but the final decision is an exact
//! PostgreSQL function OID plus an explicit argument/result type contract. A
//! name lookup is never an execution identity.

use lagodb_query::plan::ScalarFunctionKind;
use pgrx::pg_sys;

use super::QueryExpressionPlanner;

pub(super) struct NativeScalarCall {
    pub(super) kind: ScalarFunctionKind,
    pub(super) arguments: Vec<*mut pg_sys::Node>,
    pub(super) input_collation: pg_sys::Oid,
}

impl NativeScalarCall {
    pub(super) unsafe fn classify(node: *mut pg_sys::Node) -> Option<Self> {
        match unsafe { (*node).type_ } {
            pg_sys::NodeTag::T_FuncExpr => unsafe {
                Self::classify_function(node.cast())
            },
            pg_sys::NodeTag::T_MinMaxExpr => unsafe {
                Self::classify_minmax(node.cast())
            },
            // Conditional nodes are lowered by the shape planner. DataFusion 55
            // lowers COALESCE to CASE and its physical CASE expression evaluates
            // branch expressions only for the selected row subset. Keep these
            // nodes out of ordinary function classification so that conditional
            // placement remains explicit rather than maintaining a second,
            // drifting per-function fallibility registry here.
            pg_sys::NodeTag::T_CaseExpr
            | pg_sys::NodeTag::T_CoalesceExpr
            | pg_sys::NodeTag::T_NullIfExpr => None,
            _ => None,
        }
    }

    unsafe fn classify_function(expression: *mut pg_sys::FuncExpr) -> Option<Self> {
        let arguments = unsafe { Self::arguments((*expression).args) };
        let argument_types = arguments
            .iter()
            .map(|argument| QueryExpressionPlanner::expr_type((*argument).cast()))
            .collect::<Vec<_>>();
        let signature = FunctionSignature::for_oid(unsafe { (*expression).funcid })?;
        let result_type = QueryExpressionPlanner::expr_type(expression.cast());
        let input_collation = unsafe { (*expression).inputcollid };
        if !signature
            .argument_types
            .iter()
            .copied()
            .eq(argument_types.iter().map(|argument| argument.type_oid))
            || signature.result_type != unsafe { (*expression).funcresulttype }
            || unsafe { pg_sys::exprTypmod(expression.cast()) } != -1
            || unsafe { (*expression).funcretset }
            || unsafe { (*expression).funcvariadic }
            || !signature.accepts_collation(input_collation)
            || !signature.kind.supports_signature(
                &argument_types,
                input_collation,
                result_type,
            )
        {
            return None;
        }
        Some(Self {
            kind: signature.kind,
            arguments,
            input_collation,
        })
    }

    unsafe fn classify_minmax(expression: *mut pg_sys::MinMaxExpr) -> Option<Self> {
        let arguments = unsafe { Self::arguments((*expression).args) };
        if arguments.is_empty() {
            return None;
        }
        let result_type = unsafe { (*expression).minmaxtype };
        if !matches!(
            result_type,
            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
        ) || arguments.iter().any(|argument| unsafe {
            pg_sys::exprType((*argument).cast()) != result_type
        }) || unsafe { (*expression).inputcollid } != pg_sys::InvalidOid
        {
            return None;
        }
        let kind = match unsafe { (*expression).op } {
            pg_sys::MinMaxOp::IS_GREATEST => ScalarFunctionKind::Greatest,
            pg_sys::MinMaxOp::IS_LEAST => ScalarFunctionKind::Least,
            _ => return None,
        };
        Some(Self {
            kind,
            arguments,
            input_collation: unsafe { (*expression).inputcollid },
        })
    }

    unsafe fn arguments(list: *mut pg_sys::List) -> Vec<*mut pg_sys::Node> {
        let count = unsafe { pg_sys::list_length(list) };
        (0..count)
            .map(|index| unsafe { pg_sys::list_nth(list, index) }.cast())
            .collect()
    }
}

struct FunctionSignature {
    kind: ScalarFunctionKind,
    argument_types: &'static [pg_sys::Oid],
    result_type: pg_sys::Oid,
    collation_policy: CollationPolicy,
}

#[derive(Clone, Copy)]
enum CollationPolicy {
    Ignored,
    Deterministic,
}

impl FunctionSignature {
    fn for_oid(oid: pg_sys::Oid) -> Option<Self> {
        let (kind, argument_types, result_type, collation_policy) =
            match u32::from(oid) {
                pg_sys::F_ASCII => (
                    ScalarFunctionKind::Ascii,
                    &[pg_sys::TEXTOID][..],
                    pg_sys::INT4OID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_REPEAT => (
                    // PostgreSQL repeat(text, int4) maps directly to DataFusion's
                    // vectorized repeat expression. PostgreSQL 17 rejects results
                    // above MaxAllocSize (1 GiB - 1), while DataFusion's Utf8
                    // kernel uses the larger i32 offset limit. This known
                    // allocation/error-semantics difference must be addressed by
                    // a PostgreSQL-compatible native wrapper, not by disabling
                    // conditional lazy evaluation or adding a fallibility table.
                    ScalarFunctionKind::Repeat,
                    &[pg_sys::TEXTOID, pg_sys::INT4OID][..],
                    pg_sys::TEXTOID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_STARTS_WITH => (
                    ScalarFunctionKind::StartsWith,
                    &[pg_sys::TEXTOID, pg_sys::TEXTOID][..],
                    pg_sys::BOOLOID,
                    CollationPolicy::Deterministic,
                ),
                pg_sys::F_REPLACE => (
                    // As with repeat, replace remains vectorized. Its allocation
                    // ceiling is not PostgreSQL's MaxAllocSize; a later semantic
                    // wrapper must close that known difference without moving
                    // the expression to the per-row PG fallback.
                    ScalarFunctionKind::Replace,
                    &[pg_sys::TEXTOID, pg_sys::TEXTOID, pg_sys::TEXTOID][..],
                    pg_sys::TEXTOID,
                    CollationPolicy::Deterministic,
                ),
                pg_sys::F_LENGTH_TEXT | pg_sys::F_CHAR_LENGTH_TEXT => (
                    ScalarFunctionKind::CharacterLength,
                    &[pg_sys::TEXTOID][..],
                    pg_sys::INT4OID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_SUBSTR_TEXT_INT4 | pg_sys::F_SUBSTRING_TEXT_INT4 => (
                    ScalarFunctionKind::Substring,
                    &[pg_sys::TEXTOID, pg_sys::INT4OID][..],
                    pg_sys::TEXTOID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_SUBSTR_TEXT_INT4_INT4
                | pg_sys::F_SUBSTRING_TEXT_INT4_INT4 => (
                    ScalarFunctionKind::Substring,
                    &[pg_sys::TEXTOID, pg_sys::INT4OID, pg_sys::INT4OID][..],
                    pg_sys::TEXTOID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_REVERSE => (
                    ScalarFunctionKind::Reverse,
                    &[pg_sys::TEXTOID][..],
                    pg_sys::TEXTOID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_ABS_INT2 => (
                    ScalarFunctionKind::Abs,
                    &[pg_sys::INT2OID][..],
                    pg_sys::INT2OID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_ABS_INT4 => (
                    ScalarFunctionKind::Abs,
                    &[pg_sys::INT4OID][..],
                    pg_sys::INT4OID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_ABS_INT8 => (
                    ScalarFunctionKind::Abs,
                    &[pg_sys::INT8OID][..],
                    pg_sys::INT8OID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_ABS_FLOAT4 => (
                    ScalarFunctionKind::Abs,
                    &[pg_sys::FLOAT4OID][..],
                    pg_sys::FLOAT4OID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_ABS_FLOAT8 => (
                    ScalarFunctionKind::Abs,
                    &[pg_sys::FLOAT8OID][..],
                    pg_sys::FLOAT8OID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_CEIL_FLOAT8 | pg_sys::F_CEILING_FLOAT8 => (
                    ScalarFunctionKind::Ceil,
                    &[pg_sys::FLOAT8OID][..],
                    pg_sys::FLOAT8OID,
                    CollationPolicy::Ignored,
                ),
                pg_sys::F_FLOOR_FLOAT8 => (
                    ScalarFunctionKind::Floor,
                    &[pg_sys::FLOAT8OID][..],
                    pg_sys::FLOAT8OID,
                    CollationPolicy::Ignored,
                ),
                _ => return None,
            };
        Some(Self {
            kind,
            argument_types,
            result_type,
            collation_policy,
        })
    }

    fn accepts_collation(&self, collation: pg_sys::Oid) -> bool {
        match self.collation_policy {
            CollationPolicy::Ignored => true,
            CollationPolicy::Deterministic => {
                collation != pg_sys::InvalidOid
                    && unsafe { pg_sys::get_collation_isdeterministic(collation) }
            }
        }
    }
}
