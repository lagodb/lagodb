//! Query-level ordering over materialized query outputs.

use lagodb_core::expr::ExprType;
use lagodb_core::query_contract::OutputId;
use pgrx::pg_sys;

use super::Decimal128Semantics;
use super::aggregate::SortDirection;
use super::ir::{QueryNode, QueryPlanError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortExpr {
    output: OutputId,
    result_type: ExprType,
    direction: SortDirection,
    nulls_first: bool,
}

impl SortExpr {
    pub fn try_new(
        output: OutputId,
        result_type: ExprType,
        direction: SortDirection,
        nulls_first: bool,
    ) -> Result<Self, QueryPlanError> {
        if !Self::supports_type(result_type) {
            return Err(QueryPlanError::UnsupportedSortKey);
        }
        Ok(Self {
            output,
            result_type,
            direction,
            nulls_first,
        })
    }

    fn supports_type(result_type: ExprType) -> bool {
        match result_type.type_oid {
            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID => {
                result_type.typmod == -1
                    && result_type.collation == pg_sys::InvalidOid
            }
            pg_sys::TEXTOID => {
                result_type.typmod == -1
                    && matches!(
                        result_type.collation,
                        pg_sys::C_COLLATION_OID | pg_sys::POSIX_COLLATION_OID
                    )
            }
            pg_sys::VARCHAROID => {
                result_type.typmod >= -1
                    && matches!(
                        result_type.collation,
                        pg_sys::C_COLLATION_OID | pg_sys::POSIX_COLLATION_OID
                    )
            }
            pg_sys::NUMERICOID => {
                Decimal128Semantics::for_type(result_type).is_some()
            }
            _ => false,
        }
    }

    #[inline]
    pub const fn output(self) -> OutputId {
        self.output
    }

    #[inline]
    pub const fn result_type(self) -> ExprType {
        self.result_type
    }

    #[inline]
    pub const fn direction(self) -> SortDirection {
        self.direction
    }

    #[inline]
    pub const fn nulls_first(self) -> bool {
        self.nulls_first
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortNode {
    input: Box<QueryNode>,
    keys: Box<[SortExpr]>,
}

impl SortNode {
    pub fn new(
        input: QueryNode,
        keys: Box<[SortExpr]>,
    ) -> Result<Self, QueryPlanError> {
        if keys.is_empty() {
            return Err(QueryPlanError::EmptySort);
        }
        Ok(Self {
            input: Box::new(input),
            keys,
        })
    }

    #[inline]
    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    #[inline]
    pub fn keys(&self) -> &[SortExpr] {
        &self.keys
    }
}
