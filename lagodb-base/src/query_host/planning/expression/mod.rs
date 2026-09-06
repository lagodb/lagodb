//! Statement-level exact-expression planning for query offload.

mod fallback;
mod native;
mod shape;
mod state;

use lagodb_core::expr::pg::{
    PgExprRef, PgNullTestKind, PgPredicateLeafRef, PgScalarExprRef,
};
use lagodb_core::expr::{
    ColumnRef, ExprType, RuntimeValueExpr, RuntimeValueSource, RuntimeValueSpec,
};
use lagodb_query::plan::{ExecutionExpr, ExecutionScalarRepr, ScalarSemantics};
use pgrx::pg_sys;

use fallback::PostgresExpressionBuilder;
use native::NativeScalarCall;
use state::{ExpressionDecline, ExpressionPlanResult};
pub(super) use state::{ExpressionScope, ExpressionSourceCatalog, OutputCatalog};

pub(super) type PredicateDomain = ScalarSemantics;

pub(super) struct QueryExpressionPlanner {
    sources: ExpressionSourceCatalog,
    runtime_exprs: Vec<RuntimeValueExpr>,
    runtime_specs: Vec<RuntimeValueSpec>,
    columns_by_scan: Vec<Vec<Option<ColumnRef>>>,
}

struct PlanCheckpoint {
    runtime_count: usize,
    columns_by_scan: Vec<Vec<Option<ColumnRef>>>,
}

impl QueryExpressionPlanner {
    pub(super) fn new(sources: ExpressionSourceCatalog) -> Self {
        Self {
            sources,
            runtime_exprs: Vec::new(),
            runtime_specs: Vec::new(),
            columns_by_scan: Vec::new(),
        }
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
        if let Ok(leaf) = self.attempt(|planner| unsafe {
            planner.lower_leaf(expression, scope.outputs())
        }) {
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
        unsafe { self.lower_postgres(expression.cast(), scope.outputs()) }
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
            planner.lower_leaf(expression.cast(), scope.outputs())
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
        unsafe { self.lower_postgres(expression, scope.outputs()) }
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
        outputs: Option<&dyn OutputCatalog>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if let Some(output) =
            outputs.and_then(|catalog| unsafe { catalog.resolve_output(expression) })
        {
            return Ok(ExecutionExpr::Output(output));
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
                let scan = self
                    .sources
                    .resolve(var.varno())
                    .ok_or(ExpressionDecline::UnsupportedSource)?;
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
            PgScalarExprRef::Const { expression, .. } => self.lower_runtime_value(
                expression.as_ptr(),
                RuntimeValueSource::Constant,
            ),
            PgScalarExprRef::Param {
                node: parameter,
                expression,
            } => {
                if parameter.paramkind() != pg_sys::ParamKind::PARAM_EXTERN {
                    return Err(ExpressionDecline::UnsupportedRuntimeSource);
                }
                self.lower_runtime_value(
                    expression.as_ptr(),
                    RuntimeValueSource::ExternalParam,
                )
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
        outputs: Option<&dyn OutputCatalog>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let result_type = Self::expr_type(expression.cast());
        ExecutionScalarRepr::for_postgres_eval(result_type)
            .ok_or(ExpressionDecline::UnsupportedType(result_type.type_oid))?;
        unsafe {
            PostgresExpressionBuilder::lower(expression, |dependency| {
                let value_type = Self::expr_type(dependency.cast());
                ExecutionScalarRepr::for_postgres_eval(value_type)?;
                let lowered = if let Some(output) = outputs
                    .and_then(|catalog| catalog.resolve_output(dependency.cast()))
                {
                    ExecutionExpr::Output(output)
                } else {
                    self.lower_leaf(dependency.cast(), None).ok()?
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

    unsafe fn lower_predicate_leaf(
        &mut self,
        expression: *mut pg_sys::Node,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let expression_ref = unsafe { PgExprRef::from_raw(expression.cast()) };
        match PgPredicateLeafRef::parse(expression_ref).map_err(|_| {
            ExpressionDecline::UnsupportedNode(expression_ref.node_tag())
        })? {
            PgPredicateLeafRef::Comparison { op, left, right } => {
                domain
                    .comparison(
                        op,
                        Self::expr_type(left.as_ptr()),
                        Self::expr_type(right.as_ptr()),
                    )
                    .ok_or(ExpressionDecline::UnsupportedSemantics)?;
                Ok(ExecutionExpr::Comparison {
                    operator: op,
                    left: Box::new(unsafe {
                        self.lower(left.as_ptr().cast(), scope.as_scalar())
                    }?),
                    right: Box::new(unsafe {
                        self.lower(right.as_ptr().cast(), scope.as_scalar())
                    }?),
                })
            }
            PgPredicateLeafRef::NullTest { kind, value } => {
                domain
                    .supports_type(Self::expr_type(value.as_ptr()))
                    .then_some(())
                    .ok_or(ExpressionDecline::UnsupportedSemantics)?;
                let value = Box::new(unsafe {
                    self.lower(value.as_ptr().cast(), scope.as_scalar())
                }?);
                match kind {
                    PgNullTestKind::IsNull => Ok(ExecutionExpr::IsNull(value)),
                    PgNullTestKind::IsNotNull => Ok(ExecutionExpr::IsNotNull(value)),
                }
            }
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
}
