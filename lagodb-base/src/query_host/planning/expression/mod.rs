//! Statement-level exact-expression planning for query offload.

mod decimal;
mod fallback;
mod native;
mod predicate;
mod shape;
mod state;

use std::ptr;

use lagodb_core::expr::pg::{PgExprRef, PgScalarExprRef};
use lagodb_core::expr::{
    ColumnRef, ExprType, PgComparisonOp, RuntimeValueExpr, RuntimeValueSource,
    RuntimeValueSpec,
};
use lagodb_core::query_contract::ScanId;
use lagodb_query::plan::{
    Decimal128Semantics, ExecutionExpr, ExecutionScalarRepr, JoinKey, ScalarSemantics,
};
use pgrx::pg_sys;

use fallback::PostgresExpressionBuilder;
use native::NativeScalarCall;
pub(in crate::query_host::planning) use state::{
    ExpressionDecline, ExpressionPlanResult,
};
pub(super) use state::{
    ExpressionScope, ExpressionSourceCatalog, OutputCatalog, ResolvedOutput,
};

pub(super) type PredicateDomain = ScalarSemantics;

pub(super) struct QueryExpressionPlanner {
    sources: ExpressionSourceCatalog,
    runtime_exprs: Vec<RuntimeValueExpr>,
    runtime_specs: Vec<RuntimeValueSpec>,
    columns_by_scan: Vec<Vec<Option<ColumnRef>>>,
    column_registrations: Vec<ColumnRegistration>,
}

struct PlanCheckpoint {
    runtime_count: usize,
    column_registration_count: usize,
}

#[derive(Clone, Copy)]
struct ColumnRegistration {
    scan: ScanId,
    index: usize,
}

impl QueryExpressionPlanner {
    pub(super) fn new(sources: ExpressionSourceCatalog) -> Self {
        Self {
            sources,
            runtime_exprs: Vec::new(),
            runtime_specs: Vec::new(),
            columns_by_scan: Vec::new(),
            column_registrations: Vec::new(),
        }
    }

    pub(super) fn add_source_relations(
        &mut self,
        root: *mut pg_sys::PlannerInfo,
        relations: &[(pg_sys::Index, ScanId)],
    ) -> Option<()> {
        self.sources.add_relations(root, relations, ptr::null_mut())
    }

    /// The sole capability/lowering entry point for exact execution.
    ///
    /// Every failed alternative rolls back newly registered columns and
    /// runtime values before another native shape or PostgreSQL fallback is
    /// attempted. A final error is a planner decline and is never reported as
    /// a PostgreSQL ERROR at this layer.
    pub(super) unsafe fn lower(
        &mut self,
        expression: *mut pg_sys::Node,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        self.attempt(|planner| unsafe { planner.lower_inner(expression, scope) })
    }

    unsafe fn lower_inner(
        &mut self,
        expression: *mut pg_sys::Node,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        match scope.predicate_domain() {
            Some(domain) => unsafe { self.lower_exact(expression, scope, domain) },
            None => unsafe { self.lower_scalar(expression.cast(), scope) },
        }
    }

    unsafe fn lower_scalar(
        &mut self,
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if let Ok(leaf) =
            self.attempt(|planner| unsafe { planner.lower_leaf(expression, scope) })
        {
            return Ok(leaf);
        }
        if unsafe { pg_sys::exprType(expression.cast()) } == pg_sys::BOOLOID {
            return unsafe {
                self.lower_exact(
                    expression.cast(),
                    scope.with_predicate(scope.scalar_domain()),
                    scope.scalar_domain(),
                )
            };
        }
        if let Ok(native) = self.attempt(|planner| unsafe {
            planner.lower_native_shape(expression, scope, scope.scalar_domain())
        }) {
            return Ok(native);
        }
        if let Ok(native) = self.attempt(|planner| unsafe {
            planner.lower_native_call(expression.cast(), scope)
        }) {
            return Ok(native);
        }
        unsafe { self.lower_postgres(expression.cast(), scope) }
    }

    unsafe fn lower_exact(
        &mut self,
        expression: *mut pg_sys::Node,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if unsafe { (*expression).type_ } == pg_sys::NodeTag::T_List {
            return unsafe {
                self.lower_implicit_and(expression.cast(), scope, domain)
            };
        }
        if unsafe { pg_sys::exprType(expression) } != pg_sys::BOOLOID {
            return Err(ExpressionDecline::UnsupportedType(unsafe {
                pg_sys::exprType(expression)
            }));
        }
        if let Ok(leaf) = self.attempt(|planner| unsafe {
            planner.lower_leaf(expression.cast(), scope)
        }) {
            return Ok(leaf);
        }
        if unsafe { (*expression).type_ } == pg_sys::NodeTag::T_BoolExpr
            && let Ok(boolean) = self.attempt(|planner| unsafe {
                planner.lower_boolean(expression.cast(), scope, domain)
            })
        {
            return Ok(boolean);
        }
        if let Ok(predicate) = self.attempt(|planner| unsafe {
            planner.lower_predicate_leaf(expression, scope, domain)
        }) {
            return Ok(predicate);
        }
        if let Ok(native) = self.attempt(|planner| unsafe {
            planner.lower_native_shape(expression.cast(), scope, domain)
        }) {
            return Ok(native);
        }
        if let Ok(native) = self.attempt(|planner| unsafe {
            planner.lower_native_call(expression, scope)
        }) {
            return Ok(native);
        }
        unsafe { self.lower_postgres(expression, scope) }
    }

    unsafe fn lower_native_call(
        &mut self,
        expression: *mut pg_sys::Node,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let call = unsafe { NativeScalarCall::classify(expression) }.ok_or(
            ExpressionDecline::UnsupportedNode(unsafe { (*expression).type_ }),
        )?;
        let arguments = call
            .arguments
            .iter()
            .map(|argument| unsafe {
                self.lower((*argument).cast(), scope.as_scalar())
            })
            .collect::<ExpressionPlanResult<Vec<_>>>()?;
        Ok(ExecutionExpr::Function {
            kind: call.kind,
            arguments: arguments.into_boxed_slice(),
            input_collation: call.input_collation,
            result_type: Self::expr_type(expression.cast()),
        })
    }

    unsafe fn lower_leaf(
        &mut self,
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if let Some(output) = scope
            .outputs()
            .and_then(|catalog| unsafe { catalog.resolve_output(expression) })
        {
            return Ok(ExecutionExpr::Output(output.output));
        }
        let expression_ref = unsafe { PgExprRef::from_raw(expression) };
        match PgScalarExprRef::parse(expression_ref).map_err(|_| {
            ExpressionDecline::UnsupportedNode(expression_ref.node_tag())
        })? {
            PgScalarExprRef::Var {
                node: var,
                expression,
            } => {
                if var.varattno() <= 0 {
                    return Err(ExpressionDecline::UnsupportedSource);
                }
                let Some(scan) =
                    self.sources.resolve(scope.source_root(), var.varno())
                else {
                    if var.varlevelsup() == 0
                        && self
                            .sources
                            .is_runtime_outer(scope.source_root(), var.varno())
                    {
                        return self.lower_runtime_value(
                            expression.as_ptr(),
                            RuntimeValueSource::OuterValue,
                        );
                    }
                    return Err(ExpressionDecline::UnsupportedSource);
                };
                let column = ColumnRef {
                    scan,
                    attno: var.varattno(),
                    declared_type: ExprType {
                        type_oid: var.vartype(),
                        typmod: var.vartypmod(),
                        collation: var.varcollid(),
                    },
                    value_type: Self::expr_type(expression.as_ptr()),
                };
                self.record_column(column)?;
                Ok(ExecutionExpr::Column(column))
            }
            PgScalarExprRef::WidenedIntegerVar {
                node: var,
                expression,
            } => {
                if var.varattno() <= 0 {
                    return Err(ExpressionDecline::UnsupportedSource);
                }
                let Some(scan) =
                    self.sources.resolve(scope.source_root(), var.varno())
                else {
                    return Err(ExpressionDecline::UnsupportedSource);
                };
                let declared_type = ExprType {
                    type_oid: var.vartype(),
                    typmod: var.vartypmod(),
                    collation: var.varcollid(),
                };
                let column = ColumnRef {
                    scan,
                    attno: var.varattno(),
                    declared_type,
                    value_type: declared_type,
                };
                self.record_column(column)?;
                Ok(ExecutionExpr::WidenInteger {
                    value: Box::new(ExecutionExpr::Column(column)),
                    result_type: Self::expr_type(expression.as_ptr()),
                })
            }
            PgScalarExprRef::Const { expression, .. } => self.lower_runtime_value(
                expression.as_ptr(),
                RuntimeValueSource::Constant,
            ),
            PgScalarExprRef::Param {
                node: parameter,
                expression,
            } => {
                let source = match parameter.paramkind() {
                    pg_sys::ParamKind::PARAM_EXTERN => {
                        RuntimeValueSource::ExternalParam
                    }
                    pg_sys::ParamKind::PARAM_EXEC => RuntimeValueSource::ExecParam,
                    _ => return Err(ExpressionDecline::UnsupportedRuntimeSource),
                };
                self.lower_runtime_value(expression.as_ptr(), source)
            }
        }
    }

    fn lower_runtime_value(
        &mut self,
        expression: *mut pg_sys::Expr,
        source_kind: RuntimeValueSource,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let value_type = Self::expr_type(expression);
        ExecutionScalarRepr::for_runtime_value(value_type)
            .ok_or(ExpressionDecline::UnsupportedType(value_type.type_oid))?;
        Ok(ExecutionExpr::Value(self.push_runtime(
            expression,
            RuntimeValueSpec {
                value_type,
                source_kind,
            },
        )))
    }

    unsafe fn lower_postgres(
        &mut self,
        expression: *mut pg_sys::Node,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let result_type = Self::expr_type(expression.cast());
        ExecutionScalarRepr::for_postgres_eval(result_type)
            .ok_or(ExpressionDecline::UnsupportedType(result_type.type_oid))?;
        unsafe {
            PostgresExpressionBuilder::lower(expression, |dependency| {
                let value_type = Self::expr_type(dependency.cast());
                ExecutionScalarRepr::for_postgres_eval(value_type)?;
                let lowered = if let Some(output) = scope
                    .outputs()
                    .and_then(|catalog| catalog.resolve_output(dependency.cast()))
                {
                    ExecutionExpr::Output(output.output)
                } else {
                    self.lower_leaf(dependency.cast(), scope).ok()?
                };
                Some((lowered, value_type))
            })
        }
        .ok_or(ExpressionDecline::UnsupportedPostgresFallback)
    }

    unsafe fn lower_implicit_and(
        &mut self,
        expressions: *mut pg_sys::List,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let count = unsafe { pg_sys::list_length(expressions) };
        if count == 0 {
            return Err(ExpressionDecline::InvalidShape);
        }
        let mut children = Vec::with_capacity(count as usize);
        for index in 0..count {
            let child = unsafe { pg_sys::list_nth(expressions, index) };
            children.push(unsafe { self.lower_exact(child.cast(), scope, domain) }?);
        }
        Ok(ExecutionExpr::And(children.into_boxed_slice()))
    }

    unsafe fn lower_boolean(
        &mut self,
        expression: *mut pg_sys::BoolExpr,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let arguments = unsafe { (*expression).args };
        let count = unsafe { pg_sys::list_length(arguments) };
        if count == 0 {
            return Err(ExpressionDecline::InvalidShape);
        }
        let mut children = Vec::with_capacity(count as usize);
        for index in 0..count {
            let child = unsafe { pg_sys::list_nth(arguments, index) };
            children.push(unsafe { self.lower_exact(child.cast(), scope, domain) }?);
        }
        match unsafe { (*expression).boolop } {
            pg_sys::BoolExprType::AND_EXPR => {
                Ok(ExecutionExpr::And(children.into_boxed_slice()))
            }
            pg_sys::BoolExprType::OR_EXPR => {
                Ok(ExecutionExpr::Or(children.into_boxed_slice()))
            }
            pg_sys::BoolExprType::NOT_EXPR if children.len() == 1 => {
                Ok(ExecutionExpr::Not(Box::new(children.remove(0))))
            }
            _ => Err(ExpressionDecline::InvalidShape),
        }
    }

    pub(super) unsafe fn unwrap_relabel(
        mut expression: *mut pg_sys::Expr,
    ) -> *mut pg_sys::Expr {
        while unsafe { (*expression).type_ } == pg_sys::NodeTag::T_RelabelType {
            expression = unsafe { (*expression.cast::<pg_sys::RelabelType>()).arg };
        }
        expression
    }

    /// Resolve the type used to select an exact predicate domain. PostgreSQL's
    /// declared type remains authoritative except when DataFusion preserves a
    /// bounded Decimal128 representation that the aggregate result typmod lost.
    unsafe fn predicate_type(
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
    ) -> ExprType {
        let declared_type = Self::expr_type(expression);
        scope
            .outputs()
            .and_then(|catalog| unsafe { catalog.resolve_output(expression) })
            .and_then(|output| {
                Decimal128Semantics::for_type(output.execution_type)
                    .map(|_| output.execution_type)
            })
            .unwrap_or(declared_type)
    }

    /// Inspect a direct join-key column through type-preserving RelabelType
    /// wrappers without changing the eventual scan projection. PlaceHolderVar
    /// remains outside the query executor contract.
    unsafe fn inspect_join_column(
        &self,
        source_root: *mut pg_sys::PlannerInfo,
        expression: *mut pg_sys::Expr,
    ) -> ExpressionPlanResult<ColumnRef> {
        let direct = unsafe { Self::unwrap_relabel(expression) };
        if unsafe { (*direct).type_ } != pg_sys::NodeTag::T_Var {
            return Err(ExpressionDecline::InvalidShape);
        }
        let var = unsafe { &*direct.cast::<pg_sys::Var>() };
        if var.varlevelsup != 0 || var.varattno <= 0 {
            return Err(ExpressionDecline::UnsupportedSource);
        }
        let scan = self
            .sources
            .resolve(source_root, var.varno)
            .ok_or(ExpressionDecline::UnsupportedSource)?;
        let declared_type = ExprType {
            type_oid: var.vartype,
            typmod: var.vartypmod,
            collation: var.varcollid,
        };
        let column = ColumnRef {
            scan,
            attno: var.varattno,
            declared_type,
            value_type: Self::expr_type(expression),
        };
        if !column.has_binary_compatible_value() {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        Ok(column)
    }

    /// Translate one PostgreSQL equality expression into the shared join-key
    /// contract without registering either input column. Relation-tree key
    /// orientation is the single commit point, including EC substitutions.
    pub(in crate::query_host::planning) unsafe fn lower_join_key(
        &self,
        source_root: *mut pg_sys::PlannerInfo,
        expression: *mut pg_sys::Node,
    ) -> ExpressionPlanResult<JoinKey> {
        if unsafe { (*expression).type_ } != pg_sys::NodeTag::T_OpExpr {
            return Err(ExpressionDecline::InvalidShape);
        }
        let operator = expression.cast::<pg_sys::OpExpr>();
        if unsafe { pg_sys::list_length((*operator).args) } != 2 {
            return Err(ExpressionDecline::InvalidShape);
        }
        let left = unsafe {
            self.inspect_join_column(
                source_root,
                pg_sys::list_nth((*operator).args, 0).cast(),
            )
        }?;
        let right = unsafe {
            self.inspect_join_column(
                source_root,
                pg_sys::list_nth((*operator).args, 1).cast(),
            )
        }?;
        if left.scan == right.scan
            || !unsafe {
                pg_sys::op_hashjoinable((*operator).opno, left.value_type.type_oid)
            }
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        JoinKey::try_new(
            left,
            right,
            PgComparisonOp {
                opno: unsafe { (*operator).opno },
                opfuncid: unsafe { (*operator).opfuncid },
                opresulttype: unsafe { (*operator).opresulttype },
                opcollid: unsafe { (*operator).opcollid },
                inputcollid: unsafe { (*operator).inputcollid },
            },
        )
        .map_err(|_| ExpressionDecline::UnsupportedSemantics)
    }

    /// Translate the equality carried by an ANY/IN SubPlan. PostgreSQL's
    /// `build_subplan` has replaced the parser's PARAM_SUBLINK with the
    /// PARAM_EXEC recorded in `SubPlan::paramIds`, so the two column endpoints
    /// must be validated and resolved in different PlannerInfo namespaces.
    ///
    /// # Safety
    ///
    /// Both planner roots, `subplan`, and `inner_output` must remain live for
    /// the current PostgreSQL planning callback.
    pub(in crate::query_host::planning) unsafe fn lower_subplan_join_key(
        &self,
        outer_root: *mut pg_sys::PlannerInfo,
        inner_root: *mut pg_sys::PlannerInfo,
        subplan: *mut pg_sys::SubPlan,
        inner_output: *mut pg_sys::Expr,
    ) -> ExpressionPlanResult<JoinKey> {
        if unsafe { pg_sys::list_length((*subplan).paramIds) } != 1 {
            return Err(ExpressionDecline::InvalidShape);
        }
        let test_expression = unsafe { (*subplan).testexpr };
        if test_expression.is_null()
            || unsafe { (*test_expression).type_ } != pg_sys::NodeTag::T_OpExpr
        {
            return Err(ExpressionDecline::InvalidShape);
        }
        let operator = test_expression.cast::<pg_sys::OpExpr>();
        if unsafe { pg_sys::list_length((*operator).args) } != 2 {
            return Err(ExpressionDecline::InvalidShape);
        }
        let first =
            unsafe { pg_sys::list_nth((*operator).args, 0).cast::<pg_sys::Expr>() };
        let second =
            unsafe { pg_sys::list_nth((*operator).args, 1).cast::<pg_sys::Expr>() };
        let first_direct = unsafe { Self::unwrap_relabel(first) };
        let second_direct = unsafe { Self::unwrap_relabel(second) };
        let subplan_param_id =
            unsafe { pg_sys::list_nth_int((*subplan).paramIds, 0) };
        let outer_expression = match (unsafe { (*first_direct).type_ }, unsafe {
            (*second_direct).type_
        }) {
            (pg_sys::NodeTag::T_Var, pg_sys::NodeTag::T_Param) => {
                let parameter = second_direct.cast::<pg_sys::Param>();
                if unsafe { (*parameter).paramkind } != pg_sys::ParamKind::PARAM_EXEC
                    || unsafe { (*parameter).paramid } != subplan_param_id
                {
                    return Err(ExpressionDecline::UnsupportedRuntimeSource);
                }
                first
            }
            (pg_sys::NodeTag::T_Param, pg_sys::NodeTag::T_Var) => {
                let parameter = first_direct.cast::<pg_sys::Param>();
                if unsafe { (*parameter).paramkind } != pg_sys::ParamKind::PARAM_EXEC
                    || unsafe { (*parameter).paramid } != subplan_param_id
                {
                    return Err(ExpressionDecline::UnsupportedRuntimeSource);
                }
                second
            }
            _ => return Err(ExpressionDecline::InvalidShape),
        };
        let outer =
            unsafe { self.inspect_join_column(outer_root, outer_expression) }?;
        let inner = unsafe { self.inspect_join_column(inner_root, inner_output) }?;
        if !unsafe {
            pg_sys::op_hashjoinable((*operator).opno, outer.value_type.type_oid)
        } {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        JoinKey::try_new(
            outer,
            inner,
            PgComparisonOp {
                opno: unsafe { (*operator).opno },
                opfuncid: unsafe { (*operator).opfuncid },
                opresulttype: unsafe { (*operator).opresulttype },
                opcollid: unsafe { (*operator).opcollid },
                inputcollid: unsafe { (*operator).inputcollid },
            },
        )
        .map_err(|_| ExpressionDecline::UnsupportedSemantics)
    }

    /// RestrictInfo-aware entry point used for predicates selected by the
    /// PostgreSQL join planner. The metadata check prevents an arbitrary
    /// equality expression from being reclassified as a planner join key.
    pub(in crate::query_host::planning) unsafe fn lower_restrictinfo_join_key(
        &self,
        source_root: *mut pg_sys::PlannerInfo,
        restriction: &pg_sys::RestrictInfo,
    ) -> ExpressionPlanResult<JoinKey> {
        if !restriction.can_join
            || restriction.hashjoinoperator == pg_sys::InvalidOid
            || unsafe { (*restriction.clause).type_ } != pg_sys::NodeTag::T_OpExpr
            || unsafe { (*restriction.clause.cast::<pg_sys::OpExpr>()).opno }
                != restriction.hashjoinoperator
        {
            return Err(ExpressionDecline::InvalidShape);
        }
        unsafe { self.lower_join_key(source_root, restriction.clause.cast()) }
    }

    /// Register both endpoints selected for one relational Join key.
    ///
    /// This is also the commit point for equivalence-class substitutions: if
    /// either endpoint conflicts with the scan projection contract, neither
    /// newly registered endpoint survives the failed planning attempt.
    pub(in crate::query_host::planning) fn record_join_key(
        &mut self,
        key: JoinKey,
    ) -> ExpressionPlanResult<()> {
        self.attempt(|planner| {
            planner.record_column(key.left())?;
            planner.record_column(key.right())
        })
    }
}
