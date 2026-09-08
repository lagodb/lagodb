//! PostgreSQL expression normalization into the shared scalar/predicate IR.

mod predicate;

use core::ffi::c_void;
use core::ptr;

use pgrx::pg_sys;

use crate::expr::pg::{
    PgBoolExpr, PgExprRef, PgNullTestKind, PgPredicateLeafRef, PgScalarExprRef,
};
use crate::expr::{
    ColumnRef, ExprType, RuntimeValueExpr, RuntimeValueId, RuntimeValueSource,
    RuntimeValueSpec,
};
use crate::tuple::Decimal128Semantics;

use super::scope::{RelationExpressionScope, VarResolution};
use super::{PredicateExpr, PredicateFragment, QueryExpressionScope, ScalarExpr};

/// A normalized predicate plus the PostgreSQL expressions backing its values.
#[derive(Debug, Clone)]
pub struct NormalizedPredicate {
    pub(crate) fragment: PredicateFragment,
    pub(crate) bindings: Vec<RuntimeValueExpr>,
    pub(crate) pushed_expr: *mut pg_sys::Expr,
}

impl NormalizedPredicate {
    #[inline]
    pub fn fragment(&self) -> &PredicateFragment {
        &self.fragment
    }

    #[inline]
    pub fn bindings(&self) -> &[RuntimeValueExpr] {
        &self.bindings
    }

    #[inline]
    pub fn pushed_expr(&self) -> *mut pg_sys::Expr {
        self.pushed_expr
    }

    pub fn into_parts(
        self,
    ) -> (PredicateFragment, Vec<RuntimeValueExpr>, *mut pg_sys::Expr) {
        (self.fragment, self.bindings, self.pushed_expr)
    }

    /// Combine independently normalized conjuncts, rebasing their value slots.
    /// Used by both relation filter negotiation and query scan pruning.
    pub(crate) unsafe fn combine_and(items: Vec<Self>) -> Option<Self> {
        unsafe { Self::combine(items, PredicateCombination::And) }
    }

    /// Combine normalized disjuncts only after every branch has supplied a
    /// safe exact or widened pruning candidate.
    pub(crate) unsafe fn combine_or(items: Vec<Self>) -> Option<Self> {
        unsafe { Self::combine(items, PredicateCombination::Or) }
    }

    unsafe fn combine(
        items: Vec<Self>,
        combination: PredicateCombination,
    ) -> Option<Self> {
        if items.is_empty() {
            return None;
        }
        if items.len() == 1 {
            return items.into_iter().next();
        }
        let value_count = items.iter().map(|item| item.bindings.len()).sum();
        let mut values = Vec::with_capacity(value_count);
        let mut bindings = Vec::with_capacity(value_count);
        let mut nodes = Vec::with_capacity(items.len());
        let mut pushed_args: *mut pg_sys::List = ptr::null_mut();
        for item in items {
            let offset = values.len();
            nodes.push(item.fragment.root().rebase_runtime_values(offset));
            values.extend_from_slice(item.fragment.values());
            bindings.extend(item.bindings);
            pushed_args = unsafe {
                pg_sys::lappend(pushed_args, item.pushed_expr.cast::<c_void>())
            };
        }
        let (root, boolop) = match combination {
            PredicateCombination::And => (
                PredicateExpr::And(nodes.into_boxed_slice()),
                pg_sys::BoolExprType::AND_EXPR,
            ),
            PredicateCombination::Or => (
                PredicateExpr::Or(nodes.into_boxed_slice()),
                pg_sys::BoolExprType::OR_EXPR,
            ),
        };
        Some(Self {
            fragment: PredicateFragment::new(root, values),
            bindings,
            pushed_expr: unsafe { pg_sys::makeBoolExpr(boolop, pushed_args, -1) },
        })
    }
}

#[derive(Clone, Copy)]
enum PredicateCombination {
    And,
    Or,
}

enum ExpressionScope<'a> {
    Relation(RelationExpressionScope),
    Query(&'a QueryExpressionScope),
}

struct ExpressionNormalizer<'a> {
    scope: ExpressionScope<'a>,
}

impl<'a> ExpressionNormalizer<'a> {
    unsafe fn normalize_predicate(
        &self,
        expr: *mut pg_sys::Expr,
    ) -> Option<NormalizedPredicate> {
        let expr = unsafe { PgExprRef::from_raw_opt(expr) }?;
        let mut bindings = Vec::new();
        let root = unsafe { self.normalize_node(expr, &mut bindings) }?;
        let values = bindings
            .iter()
            .copied()
            .map(RuntimeValueExpr::metadata)
            .collect();
        Some(NormalizedPredicate {
            fragment: PredicateFragment::new(root, values),
            bindings,
            pushed_expr: expr.as_ptr(),
        })
    }

    unsafe fn normalize_node(
        &self,
        expr: PgExprRef<'_>,
        bindings: &mut Vec<RuntimeValueExpr>,
    ) -> Option<PredicateExpr> {
        let expr = expr.without_relabels();
        if let Some(boolean) = PgBoolExpr::try_from_expr(expr) {
            let args = boolean.args_list();
            let length = unsafe { pg_sys::list_length(args) };
            if length == 0 {
                return None;
            }
            let mut children = Vec::with_capacity(length as usize);
            for index in 0..length {
                let child =
                    unsafe { pg_sys::list_nth(args, index) } as *mut pg_sys::Expr;
                let child = unsafe { PgExprRef::from_raw_opt(child) }?;
                children.push(unsafe { self.normalize_node(child, bindings) }?);
            }
            return match boolean.boolop() {
                pg_sys::BoolExprType::AND_EXPR => {
                    Some(PredicateExpr::And(children.into_boxed_slice()))
                }
                pg_sys::BoolExprType::OR_EXPR => {
                    Some(PredicateExpr::Or(children.into_boxed_slice()))
                }
                pg_sys::BoolExprType::NOT_EXPR if children.len() == 1 => {
                    Some(PredicateExpr::Not(Box::new(children.remove(0))))
                }
                _ => None,
            };
        }

        match PgPredicateLeafRef::parse(expr).ok()? {
            PgPredicateLeafRef::Comparison { op, left, right } => {
                if op.opno == pg_sys::Oid::from(pg_sys::OID_TEXT_LIKE_OP) {
                    return unsafe {
                        self.normalize_like_prefix(op, left, right, bindings)
                    };
                }
                if let Some(predicate) = unsafe {
                    self.normalize_nan_comparison(op, left, right, bindings)
                } {
                    return Some(predicate);
                }
                let left = self.normalize_scalar(left, bindings)?;
                let right = self.normalize_scalar(right, bindings)?;
                unsafe {
                    Self::specialize_decimal_constant(&left, &right, bindings);
                    Self::specialize_decimal_constant(&right, &left, bindings);
                }
                Some(PredicateExpr::Comparison {
                    operator: op,
                    left,
                    right,
                })
            }
            PgPredicateLeafRef::NullTest { kind, value } => {
                let value = self.normalize_scalar(value, bindings)?;
                match kind {
                    PgNullTestKind::IsNull => Some(PredicateExpr::IsNull(value)),
                    PgNullTestKind::IsNotNull => {
                        Some(PredicateExpr::IsNotNull(value))
                    }
                }
            }
            PgPredicateLeafRef::StartsWith {
                value,
                prefix,
                input_collation,
            } => unsafe {
                self.normalize_starts_with(value, prefix, input_collation, bindings)
            },
        }
    }

    fn normalize_scalar(
        &self,
        expression: PgExprRef<'_>,
        bindings: &mut Vec<RuntimeValueExpr>,
    ) -> Option<ScalarExpr> {
        let scalar = PgScalarExprRef::parse(expression).ok()?;
        match scalar {
            PgScalarExprRef::Var {
                node: var,
                expression,
            }
            | PgScalarExprRef::WidenedIntegerVar {
                node: var,
                expression,
            } if var.varattno() > 0 => match &self.scope {
                ExpressionScope::Relation(scope) => {
                    match scope.resolve_var(var.varno()) {
                        VarResolution::Column(scan) => {
                            Some(ScalarExpr::Column(ColumnRef {
                                scan,
                                attno: var.varattno(),
                                declared_type: ExprType {
                                    type_oid: var.vartype(),
                                    typmod: var.vartypmod(),
                                    collation: var.varcollid(),
                                },
                                value_type: Self::type_metadata(expression),
                            }))
                        }
                        VarResolution::OuterValue => Some(Self::push_binding(
                            bindings,
                            expression.as_ptr(),
                            RuntimeValueSpec {
                                value_type: Self::type_metadata(expression),
                                source_kind: RuntimeValueSource::OuterValue,
                            },
                        )),
                    }
                }
                ExpressionScope::Query(scope) => {
                    let scan = scope.resolve_var(var.varno())?;
                    Some(ScalarExpr::Column(ColumnRef {
                        scan,
                        attno: var.varattno(),
                        declared_type: ExprType {
                            type_oid: var.vartype(),
                            typmod: var.vartypmod(),
                            collation: var.varcollid(),
                        },
                        value_type: Self::type_metadata(expression),
                    }))
                }
            },
            PgScalarExprRef::Var { .. }
            | PgScalarExprRef::WidenedIntegerVar { .. } => None,
            PgScalarExprRef::Const { expression, .. } => Some(Self::push_binding(
                bindings,
                expression.as_ptr(),
                RuntimeValueSpec {
                    value_type: Self::type_metadata(expression),
                    source_kind: RuntimeValueSource::Constant,
                },
            )),
            PgScalarExprRef::Param {
                node: param,
                expression,
            } => {
                let source_kind = match &self.scope {
                    ExpressionScope::Relation(scope) => {
                        scope.resolve_param(param.paramkind())?
                    }
                    ExpressionScope::Query(_) => match param.paramkind() {
                        pg_sys::ParamKind::PARAM_EXTERN => {
                            RuntimeValueSource::ExternalParam
                        }
                        _ => return None,
                    },
                };
                Some(Self::push_binding(
                    bindings,
                    expression.as_ptr(),
                    RuntimeValueSpec {
                        value_type: Self::type_metadata(expression),
                        source_kind,
                    },
                ))
            }
        }
    }

    /// If a direct unbounded NUMERIC Const has a total comparison against the
    /// storage column's Decimal128 shape, retain that proof in the value slot.
    /// Provider planning can then stay datum-free while its Exact binder remains
    /// total for every admitted runtime value.
    ///
    /// # Safety
    ///
    /// Constant expressions and their pass-by-reference datums must remain
    /// live in the PostgreSQL planner context for this call.
    unsafe fn specialize_decimal_constant(
        column: &ScalarExpr,
        value: &ScalarExpr,
        bindings: &mut [RuntimeValueExpr],
    ) {
        let (ScalarExpr::Column(column), ScalarExpr::Value(value)) = (column, value)
        else {
            return;
        };
        let binding = &mut bindings[value.index()];
        let metadata = binding.metadata();
        if metadata.source_kind != RuntimeValueSource::Constant
            || metadata.value_type.type_oid != pg_sys::NUMERICOID
            || metadata.value_type.typmod != -1
            || metadata.value_type.collation != pg_sys::InvalidOid
        {
            return;
        }
        let Some(declared) = Decimal128Semantics::for_type(column.declared_type)
        else {
            return;
        };
        if Decimal128Semantics::for_type(column.value_type) != Some(declared) {
            return;
        }
        let expression = unsafe { PgExprRef::from_raw(binding.expr()) };
        let Ok(PgScalarExprRef::Const { node, .. }) =
            PgScalarExprRef::parse(expression)
        else {
            return;
        };
        let (type_oid, collation, datum, is_null) = node.parts();
        if type_oid != pg_sys::NUMERICOID
            || collation != pg_sys::InvalidOid
            || (!is_null
                && unsafe { declared.codec().encode_comparison_datum(datum) }
                    .is_err())
        {
            return;
        }
        binding.specialize_value_type(declared.value_type());
    }

    fn type_metadata(expression: PgExprRef<'_>) -> ExprType {
        ExprType {
            type_oid: expression.type_oid(),
            typmod: expression.typmod(),
            collation: expression.collation(),
        }
    }

    fn push_binding(
        bindings: &mut Vec<RuntimeValueExpr>,
        expr: *mut pg_sys::Expr,
        metadata: RuntimeValueSpec,
    ) -> ScalarExpr {
        let id = RuntimeValueId::new(bindings.len());
        bindings.push(RuntimeValueExpr::new(expr, metadata));
        ScalarExpr::Value(id)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RelationExpressionNormalizer {
    scope: RelationExpressionScope,
}

impl RelationExpressionNormalizer {
    pub(crate) const fn new(scan_relid: core::ffi::c_int) -> Self {
        Self {
            scope: RelationExpressionScope::new(scan_relid),
        }
    }

    pub(crate) unsafe fn normalize(
        self,
        expr: *mut pg_sys::Expr,
    ) -> Option<NormalizedPredicate> {
        unsafe {
            ExpressionNormalizer {
                scope: ExpressionScope::Relation(self.scope),
            }
            .normalize_predicate(expr)
        }
    }
}

pub struct QueryExpressionNormalizer {
    scope: QueryExpressionScope,
}

impl QueryExpressionNormalizer {
    pub fn new(scope: QueryExpressionScope) -> Self {
        Self { scope }
    }

    /// # Safety
    /// `qual` must be a live planner-owned expression tree or PostgreSQL's
    /// implicit-AND `List` representation of a qual.
    pub unsafe fn normalize_predicate(
        &self,
        qual: *mut pg_sys::Node,
    ) -> Option<NormalizedPredicate> {
        unsafe { self.normalize_qual(qual) }
    }

    unsafe fn normalize_qual(
        &self,
        qual: *mut pg_sys::Node,
    ) -> Option<NormalizedPredicate> {
        if qual.is_null() {
            return None;
        }
        let normalizer = ExpressionNormalizer {
            scope: ExpressionScope::Query(&self.scope),
        };
        if unsafe { (*qual).type_ } != pg_sys::NodeTag::T_List {
            return unsafe { normalizer.normalize_predicate(qual.cast()) };
        }

        let list = qual.cast::<pg_sys::List>();
        let count = unsafe { pg_sys::list_length(list) };
        let mut predicates = Vec::with_capacity(count as usize);
        for index in 0..count {
            let expression =
                unsafe { pg_sys::list_nth(list, index) }.cast::<pg_sys::Expr>();
            predicates.push(unsafe { normalizer.normalize_predicate(expression) }?);
        }
        unsafe { NormalizedPredicate::combine_and(predicates) }
    }
}
