//! Executor-side expression and runtime-parameter errors.

use core::ffi::c_int;

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::diag::{PgError, SqlStateError};
use crate::expr::pg::PgStructuralError;

/// Executor-side failures while resolving PostgreSQL runtime parameters.
#[doc(hidden)]
#[derive(Debug, thiserror::Error)]
pub enum RuntimeParamError {
    #[error("no value found for parameter {param_id}")]
    NoValueFound { param_id: c_int },

    #[error(
        "type of parameter {param_id} ({runtime_type_name}) does not match \
         that when preparing the plan ({expected_type_name})"
    )]
    TypeMismatch {
        param_id: c_int,
        runtime_type_name: String,
        expected_type_name: String,
    },

    #[error("PostgreSQL failed to fetch external parameter {param_id}: {source}")]
    FetchExternal {
        param_id: c_int,
        #[source]
        source: PgError,
    },

    #[error("PostgreSQL failed to format type OID {type_oid}: {source}")]
    FormatType {
        type_oid: pg_sys::Oid,
        #[source]
        source: PgError,
    },

    #[error("PostgreSQL failed to materialize PARAM_EXEC values: {source}")]
    MaterializeExec {
        #[source]
        source: PgError,
    },
}

impl SqlStateError for RuntimeParamError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::NoValueFound { .. } => PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            Self::TypeMismatch { .. } => PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
            Self::FetchExternal { source, .. }
            | Self::FormatType { source, .. }
            | Self::MaterializeExec { source } => source.sql_error_code(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildPredicateError<E>
where
    E: std::error::Error + 'static,
{
    #[error(
        "PredicateBuilder::build_one: expr_index {expr_index} is out of range \
         (pushed len {pushed_len})"
    )]
    ExprIndexOutOfRange {
        expr_index: usize,
        pushed_len: usize,
    },
    #[error("PredicateBuilder: expression {expr_index} is NULL")]
    NullExpression { expr_index: usize },
    #[error(
        "PredicateBuilder: pushed expression {expr_index} is a scalar; \
         expected a boolean predicate"
    )]
    ExpectedPredicateAtTopLevel { expr_index: usize },
    #[error(
        "PredicateBuilder: scalar context for expression {expr_index} \
         received a predicate"
    )]
    UnexpectedPredicate { expr_index: usize },
    #[error(
        "PredicateBuilder: predicate context for expression {expr_index} \
         received a scalar"
    )]
    UnexpectedScalar { expr_index: usize },
    #[error(
        "PredicateBuilder: no column_refs entry for \
         (expr_index={expr_index}, attno={attno})"
    )]
    MissingColumnRef {
        expr_index: usize,
        attno: pg_sys::AttrNumber,
    },
    #[error(
        "PredicateBuilder: expression {expr_index} contains scan-relation \
         Var with unsupported attno {attno}; pushed predicates must reference \
         user columns only"
    )]
    UnsupportedScanVarAttno {
        expr_index: usize,
        attno: pg_sys::AttrNumber,
    },
    #[error(
        "PredicateBuilder: expression {expr_index} references projected scan \
         resno {resno}, outside layout width {width}"
    )]
    MappedScanVarOutOfRange {
        expr_index: usize,
        resno: pg_sys::AttrNumber,
        width: usize,
    },
    #[error(
        "PredicateBuilder: param ({paramkind:?}, id {param_id}) referenced \
         by expression {expr_index} is not in the resolved-params slice"
    )]
    MissingParam {
        expr_index: usize,
        paramkind: pg_sys::ParamKind::Type,
        param_id: c_int,
    },
    #[error(
        "PredicateBuilder: expression {expr_index} contains a Var with \
         varno={varno} != scan_relid={scan_relid}; \
         replace_nestloop_params should have rewritten it"
    )]
    OuterRelationVar {
        expr_index: usize,
        varno: c_int,
        scan_relid: c_int,
    },
    #[error("PredicateBuilder: empty AND/OR BoolExpr in expression {expr_index}")]
    EmptyBoolExpr { expr_index: usize },
    #[error(
        "PredicateBuilder: NOT in expression {expr_index} has arity \
         {arity}; expected exactly 1 child"
    )]
    MalformedNot { expr_index: usize, arity: usize },
    #[error("PredicateBuilder: NULL child pointer in expression {expr_index}")]
    NullChild { expr_index: usize },
    #[error(
        "PredicateBuilder: unknown BoolExprType {boolop} in expression \
         {expr_index}"
    )]
    UnknownBoolOp {
        expr_index: usize,
        boolop: pg_sys::BoolExprType::Type,
    },
    #[error(
        "PredicateBuilder: unsupported NodeTag {tag:?} in expression \
         {expr_index}"
    )]
    UnsupportedNodeTag {
        expr_index: usize,
        tag: pg_sys::NodeTag,
    },
    #[error("PredicateBuilder: malformed expression {expr_index}: {source}")]
    Structural {
        expr_index: usize,
        #[source]
        source: PgStructuralError,
    },
    #[error("PredicateBuilder: translator error: {0}")]
    Translator(#[source] E),
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("translator failed")]
    struct TranslatorError;

    #[test]
    fn translator_error_preserves_source_chain() {
        let err = BuildPredicateError::Translator(TranslatorError);
        assert_eq!(
            err.source().map(ToString::to_string).as_deref(),
            Some("translator failed")
        );
    }
}
