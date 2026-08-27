//! PostgreSQL expression normalization into an owned [`FilterFragment`].

use core::ffi::c_void;
use core::ptr;
use pgrx::pg_sys;

use crate::expr::pg::{
    PgBoolExpr, PgExprRef, PgNullTestKind, PgPredicateLeafRef, PgScalarExprRef,
};

use super::{
    FilterBindingExpr, FilterColumn, FilterFragment, FilterNode, FilterScalar,
    FilterTypeMetadata, FilterValueSlot, FilterValueSlotId, FilterValueSourceKind,
};

/// A normalized fragment plus the PG expressions backing its local slots.
#[derive(Debug, Clone)]
pub(crate) struct NormalizedFilter {
    pub fragment: FilterFragment,
    pub bindings: Vec<FilterBindingExpr>,
    pub pushed_expr: *mut pg_sys::Expr,
}

impl NormalizedFilter {
    /// # Safety
    ///
    /// Every candidate expression must be live in the current planner memory
    /// context.
    pub(crate) unsafe fn combine_and(items: Vec<Self>) -> Option<Self> {
        unsafe { Self::combine(items, pg_sys::BoolExprType::AND_EXPR) }
    }

    /// # Safety
    ///
    /// Every candidate expression must be live in the current planner memory
    /// context.
    pub(crate) unsafe fn combine_or(items: Vec<Self>) -> Option<Self> {
        unsafe { Self::combine(items, pg_sys::BoolExprType::OR_EXPR) }
    }

    unsafe fn combine(
        items: Vec<Self>,
        boolop: pg_sys::BoolExprType::Type,
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
            nodes.push(Self::rebase_node(item.fragment.root(), offset));
            values.extend_from_slice(item.fragment.values());
            bindings.extend(item.bindings);
            pushed_args = unsafe {
                pg_sys::lappend(pushed_args, item.pushed_expr.cast::<c_void>())
            };
        }
        let root = match boolop {
            pg_sys::BoolExprType::AND_EXPR => {
                FilterNode::And(nodes.into_boxed_slice())
            }
            pg_sys::BoolExprType::OR_EXPR => FilterNode::Or(nodes.into_boxed_slice()),
            _ => unreachable!("filter candidates combine only AND or OR"),
        };
        Some(Self {
            fragment: FilterFragment::new(root, values),
            bindings,
            pushed_expr: unsafe { pg_sys::makeBoolExpr(boolop, pushed_args, -1) },
        })
    }

    fn rebase_node(node: &FilterNode, offset: usize) -> FilterNode {
        match node {
            FilterNode::Comparison {
                operator,
                left,
                right,
            } => FilterNode::Comparison {
                operator: *operator,
                left: Self::rebase_scalar(left, offset),
                right: Self::rebase_scalar(right, offset),
            },
            FilterNode::IsNull(value) => {
                FilterNode::IsNull(Self::rebase_scalar(value, offset))
            }
            FilterNode::IsNotNull(value) => {
                FilterNode::IsNotNull(Self::rebase_scalar(value, offset))
            }
            FilterNode::And(items) => FilterNode::And(
                items
                    .iter()
                    .map(|item| Self::rebase_node(item, offset))
                    .collect(),
            ),
            FilterNode::Or(items) => FilterNode::Or(
                items
                    .iter()
                    .map(|item| Self::rebase_node(item, offset))
                    .collect(),
            ),
            FilterNode::Not(item) => {
                FilterNode::Not(Box::new(Self::rebase_node(item, offset)))
            }
        }
    }

    fn rebase_scalar(value: &FilterScalar, offset: usize) -> FilterScalar {
        match value {
            FilterScalar::Column(column) => FilterScalar::Column(*column),
            FilterScalar::Value(id) => {
                FilterScalar::Value(FilterValueSlotId::new(id.index() + offset))
            }
        }
    }
}

/// Relation-scoped normalizer. It owns no PostgreSQL pointers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FilterNormalizer {
    rel_oid: pg_sys::Oid,
    scan_relid: core::ffi::c_int,
}

impl FilterNormalizer {
    pub(crate) const fn new(
        rel_oid: pg_sys::Oid,
        scan_relid: core::ffi::c_int,
    ) -> Self {
        Self {
            rel_oid,
            scan_relid,
        }
    }

    /// # Safety
    ///
    /// `expr` must be a live planner-owned expression tree.
    pub(crate) unsafe fn normalize(
        self,
        expr: *mut pg_sys::Expr,
    ) -> Option<NormalizedFilter> {
        let expr = unsafe { PgExprRef::from_raw_opt(expr) }?;
        let mut bindings = Vec::new();
        let root = unsafe { self.normalize_node(expr, &mut bindings) }?;
        let values = bindings.iter().map(|binding| binding.metadata).collect();
        Some(NormalizedFilter {
            fragment: FilterFragment::new(root, values),
            bindings,
            pushed_expr: expr.as_ptr(),
        })
    }

    unsafe fn normalize_node(
        self,
        expr: PgExprRef<'_>,
        bindings: &mut Vec<FilterBindingExpr>,
    ) -> Option<FilterNode> {
        let expr = expr.without_relabels();
        if let Some(boolean) = PgBoolExpr::try_from_expr(expr) {
            let args = boolean.args_list();
            let length = if args.is_null() {
                0
            } else {
                unsafe { pg_sys::list_length(args) }
            };
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
                    Some(FilterNode::And(children.into_boxed_slice()))
                }
                pg_sys::BoolExprType::OR_EXPR => {
                    Some(FilterNode::Or(children.into_boxed_slice()))
                }
                pg_sys::BoolExprType::NOT_EXPR if children.len() == 1 => {
                    Some(FilterNode::Not(Box::new(children.remove(0))))
                }
                _ => None,
            };
        }

        match PgPredicateLeafRef::parse(expr).ok()? {
            PgPredicateLeafRef::Comparison { op, left, right } => {
                Some(FilterNode::Comparison {
                    operator: op,
                    left: self.normalize_scalar(left, bindings)?,
                    right: self.normalize_scalar(right, bindings)?,
                })
            }
            PgPredicateLeafRef::NullTest { kind, value } => {
                let value = self.normalize_scalar(value, bindings)?;
                match kind {
                    PgNullTestKind::IsNull => Some(FilterNode::IsNull(value)),
                    PgNullTestKind::IsNotNull => Some(FilterNode::IsNotNull(value)),
                }
            }
        }
    }

    fn normalize_scalar(
        self,
        scalar: PgScalarExprRef<'_>,
        bindings: &mut Vec<FilterBindingExpr>,
    ) -> Option<FilterScalar> {
        match scalar {
            PgScalarExprRef::Var {
                node: var,
                expression,
            } if var.varno() == self.scan_relid => {
                (var.varattno() > 0).then_some(FilterScalar::Column(FilterColumn {
                    rel_oid: self.rel_oid,
                    attno: var.varattno(),
                    declared_type: FilterTypeMetadata {
                        type_oid: var.vartype(),
                        typmod: var.vartypmod(),
                        collation: var.varcollid(),
                    },
                    value_type: Self::type_metadata(expression),
                }))
            }
            PgScalarExprRef::Var {
                node: var,
                expression,
            } => (var.varattno() != 0).then(|| {
                self.push_binding(
                    bindings,
                    expression.as_ptr(),
                    FilterValueSlot {
                        value_type: Self::type_metadata(expression),
                        source_kind: FilterValueSourceKind::OuterValue,
                    },
                )
            }),
            PgScalarExprRef::Const { expression, .. } => Some(self.push_binding(
                bindings,
                expression.as_ptr(),
                FilterValueSlot {
                    value_type: Self::type_metadata(expression),
                    source_kind: FilterValueSourceKind::Constant,
                },
            )),
            PgScalarExprRef::Param {
                node: param,
                expression,
            } => {
                let source_kind = match param.paramkind() {
                    pg_sys::ParamKind::PARAM_EXTERN => {
                        FilterValueSourceKind::ExternalParam
                    }
                    pg_sys::ParamKind::PARAM_EXEC => FilterValueSourceKind::ExecParam,
                    _ => return None,
                };
                Some(self.push_binding(
                    bindings,
                    expression.as_ptr(),
                    FilterValueSlot {
                        value_type: Self::type_metadata(expression),
                        source_kind,
                    },
                ))
            }
        }
    }

    fn type_metadata(expression: PgExprRef<'_>) -> FilterTypeMetadata {
        FilterTypeMetadata {
            type_oid: expression.type_oid(),
            typmod: expression.typmod(),
            collation: expression.collation(),
        }
    }

    fn push_binding(
        self,
        bindings: &mut Vec<FilterBindingExpr>,
        expr: *mut pg_sys::Expr,
        metadata: FilterValueSlot,
    ) -> FilterScalar {
        let id = FilterValueSlotId::new(bindings.len());
        bindings.push(FilterBindingExpr { expr, metadata });
        FilterScalar::Value(id)
    }
}
