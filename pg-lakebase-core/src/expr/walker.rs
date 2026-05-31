//! Clause classification for CustomScan predicate pushdown.

use core::ptr;

use pgrx::pg_sys;

use crate::expr::inspect::contains_subplan;
use crate::expr::nodes::{PgBoolExpr, PgExprRef, PgRelabelType};
use crate::expr::predicate::{PlanPredicate, PlanPredicateContext};
use crate::expr::split::{PushdownContract, PushdownCosting, QualPushdownDecision};

pub use crate::expr::rewrite::rewrite_not;

/// One independently pushable fragment from clause classification.
#[derive(Debug, Clone, Copy)]
pub struct ClassifiedPushedPart {
    pub expr: *mut pg_sys::Expr,
    pub contract: PushdownContract,
    pub costing: PushdownCosting,
}

/// Per-clause classification consumed by [`PlanPushdownSplitter`](crate::expr::split::PlanPushdownSplitter).
#[derive(Debug, Clone)]
pub enum ClauseClassification {
    /// One or more pushed fragments. AND with mixed contracts keeps separate parts so a
    /// best-effort conservative child cannot drag down an exact sibling at runtime.
    Pushable {
        parts: Vec<ClassifiedPushedPart>,
        residual: Option<*mut pg_sys::Expr>,
    },

    PartialPush {
        pushed: *mut pg_sys::Expr,
        residual: *mut pg_sys::Expr,
    },

    Unsupported {
        residual: *mut pg_sys::Expr,
    },
}

/// Stateful classifier for one planner clause tree.
pub struct ClauseClassifier<'a, F> {
    predicate_ctx: &'a PlanPredicateContext,
    classify_leaf: &'a mut F,
}

impl<'a, F> ClauseClassifier<'a, F>
where
    F: FnMut(&PlanPredicate<'_>) -> QualPushdownDecision,
{
    #[inline]
    pub fn new(
        predicate_ctx: &'a PlanPredicateContext,
        classify_leaf: &'a mut F,
    ) -> Self {
        Self {
            predicate_ctx,
            classify_leaf,
        }
    }

    /// Classify one clause: volatile/SubPlan -> `Unsupported`, then AND/OR/NOT.
    ///
    /// # Safety
    ///
    /// `expr` must be NULL or a live PostgreSQL expression tree for
    /// `predicate_ctx`. The callback must not retain borrowed predicate views after
    /// returning.
    pub unsafe fn classify(
        &mut self,
        expr: *mut pg_sys::Expr,
    ) -> ClauseClassification {
        if expr.is_null() {
            return ClauseClassification::Unsupported { residual: expr };
        }
        if unsafe { subtree_is_unsafe_to_push(expr) } {
            return ClauseClassification::Unsupported { residual: expr };
        }
        unsafe { self.classify_subtree(expr) }
    }

    unsafe fn classify_subtree(
        &mut self,
        expr: *mut pg_sys::Expr,
    ) -> ClauseClassification {
        let r = unsafe { PgExprRef::from_raw(expr) };
        let tag = unsafe { r.node_tag() };

        match tag {
            pg_sys::NodeTag::T_OpExpr | pg_sys::NodeTag::T_NullTest => unsafe {
                self.classify_leaf_node(expr, r)
            },

            pg_sys::NodeTag::T_BoolExpr => {
                let be = unsafe { PgBoolExpr::try_from_expr(r) }
                    .expect("PgBoolExpr tag matched but downcast failed");
                let boolop = unsafe { be.boolop() };
                match boolop {
                    pg_sys::BoolExprType::AND_EXPR => unsafe {
                        self.classify_and(expr, be)
                    },
                    pg_sys::BoolExprType::OR_EXPR => unsafe {
                        self.classify_or(expr, be)
                    },
                    pg_sys::BoolExprType::NOT_EXPR => unsafe {
                        self.classify_not(expr, be)
                    },
                    _ => ClauseClassification::Unsupported { residual: expr },
                }
            }

            pg_sys::NodeTag::T_RelabelType => {
                let rl = unsafe { PgRelabelType::try_from_expr(r) }
                    .expect("PgRelabelType tag matched but downcast failed");
                match unsafe { rl.arg() } {
                    Some(inner) => unsafe { self.classify_subtree(inner.as_ptr()) },
                    None => ClauseClassification::Unsupported { residual: expr },
                }
            }

            _ => ClauseClassification::Unsupported { residual: expr },
        }
    }

    unsafe fn classify_leaf_node(
        &mut self,
        expr: *mut pg_sys::Expr,
        r: PgExprRef<'_>,
    ) -> ClauseClassification {
        let predicate = match unsafe { self.predicate_ctx.parse_leaf(r) } {
            Ok(p) => p,
            Err(_) => return ClauseClassification::Unsupported { residual: expr },
        };
        let decision = (self.classify_leaf)(&predicate);
        match decision {
            QualPushdownDecision::Pushable { contract, costing } => {
                let residual = if contract.requires_residual() {
                    Some(expr)
                } else {
                    None
                };
                Self::pushable_one(expr, contract, costing, residual)
            }
            QualPushdownDecision::Unsupported => {
                ClauseClassification::Unsupported { residual: expr }
            }
        }
    }

    unsafe fn classify_and(
        &mut self,
        original: *mut pg_sys::Expr,
        be: PgBoolExpr<'_>,
    ) -> ClauseClassification {
        let args = unsafe { be.args_list() };
        let len = if args.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(args) }
        };
        if len == 0 {
            return ClauseClassification::Unsupported { residual: original };
        }

        let mut children =
            ChildClassificationAccumulator::with_capacity(len as usize);
        for i in 0..len {
            let cell = unsafe { pg_sys::list_nth(args, i) } as *mut pg_sys::Expr;
            let child = unsafe { self.classify_subtree(cell) };
            children.absorb(child);
        }

        let location = unsafe { (*be.as_ptr()).location };
        unsafe { children.into_and_classification(original, location) }
    }

    unsafe fn classify_or(
        &mut self,
        original: *mut pg_sys::Expr,
        be: PgBoolExpr<'_>,
    ) -> ClauseClassification {
        let args = unsafe { be.args_list() };
        let len = if args.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(args) }
        };
        if len == 0 {
            return ClauseClassification::Unsupported { residual: original };
        }

        let mut child_results: Vec<ClauseClassification> =
            Vec::with_capacity(len as usize);
        let mut all_exact = true;
        for i in 0..len {
            let cell = unsafe { pg_sys::list_nth(args, i) } as *mut pg_sys::Expr;
            let child = unsafe { self.classify_subtree(cell) };
            if !Self::is_exact_pushable_without_residual(&child) {
                all_exact = false;
            }
            child_results.push(child);
        }

        let location = unsafe { (*be.as_ptr()).location };

        if all_exact {
            let mut branch_exprs: Vec<*mut pg_sys::Expr> =
                Vec::with_capacity(child_results.len());
            let mut all_costings: Vec<PushdownCosting> =
                Vec::with_capacity(child_results.len());
            for c in &child_results {
                match c {
                    ClauseClassification::Pushable { parts, residual }
                        if residual.is_none()
                            && !parts.is_empty()
                            && parts.iter().all(|p| {
                                p.contract == PushdownContract::ExactRowFilter
                            }) =>
                    {
                        branch_exprs.push(unsafe {
                            Self::combined_push_expr(parts, location)
                        });
                        all_costings.extend(parts.iter().map(|p| p.costing));
                    }
                    _ => {
                        debug_assert!(
                            false,
                            "classify_or: all_exact invariant broken"
                        );
                        return ClauseClassification::Unsupported {
                            residual: original,
                        };
                    }
                }
            }
            let pushed = unsafe { make_or(&branch_exprs, location) };
            return Self::pushable_one(
                pushed,
                PushdownContract::ExactRowFilter,
                Self::merge_costing_all_costed(&all_costings),
                None,
            );
        }

        // Widening: PartialPush is always uncosted at split time.
        let mut widenings: Vec<*mut pg_sys::Expr> =
            Vec::with_capacity(child_results.len());
        for c in &child_results {
            match c {
                ClauseClassification::Pushable { parts, .. } => {
                    widenings
                        .push(unsafe { Self::combined_push_expr(parts, location) });
                }
                ClauseClassification::PartialPush { pushed, .. } => {
                    widenings.push(*pushed);
                }
                ClauseClassification::Unsupported { .. } => {
                    return ClauseClassification::Unsupported { residual: original };
                }
            }
        }

        let pushed = unsafe { make_or(&widenings, location) };
        ClauseClassification::PartialPush {
            pushed,
            residual: original,
        }
    }

    unsafe fn classify_not(
        &mut self,
        original: *mut pg_sys::Expr,
        be: PgBoolExpr<'_>,
    ) -> ClauseClassification {
        let args = unsafe { be.args_list() };
        let len = if args.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(args) }
        };
        if len != 1 {
            return ClauseClassification::Unsupported { residual: original };
        }
        let child_ptr = unsafe { pg_sys::list_nth(args, 0) } as *mut pg_sys::Expr;
        let child = unsafe { self.classify_subtree(child_ptr) };

        match child {
            ClauseClassification::Pushable { parts, residual }
                if residual.is_none()
                    && parts.len() == 1
                    && parts[0].contract == PushdownContract::ExactRowFilter =>
            {
                let location = unsafe { (*be.as_ptr()).location };
                let not_node = unsafe { make_not(parts[0].expr, location) };
                Self::pushable_one(
                    not_node,
                    PushdownContract::ExactRowFilter,
                    parts[0].costing,
                    None,
                )
            }
            _ => ClauseClassification::Unsupported { residual: original },
        }
    }

    fn pushable_one(
        expr: *mut pg_sys::Expr,
        contract: PushdownContract,
        costing: PushdownCosting,
        residual: Option<*mut pg_sys::Expr>,
    ) -> ClauseClassification {
        ClauseClassification::Pushable {
            parts: vec![ClassifiedPushedPart {
                expr,
                contract,
                costing,
            }],
            residual,
        }
    }

    /// Compose child pushable parts into one expr for OR-widening (not for mixed-contract AND).
    unsafe fn combined_push_expr(
        parts: &[ClassifiedPushedPart],
        location: pg_sys::ParseLoc,
    ) -> *mut pg_sys::Expr {
        if parts.len() == 1 {
            parts[0].expr
        } else {
            let exprs: Vec<*mut pg_sys::Expr> =
                parts.iter().map(|p| p.expr).collect();
            unsafe { make_and(&exprs, location) }
        }
    }

    fn is_exact_pushable_without_residual(
        classification: &ClauseClassification,
    ) -> bool {
        match classification {
            ClauseClassification::Pushable { parts, residual } => {
                residual.is_none()
                    && !parts.is_empty()
                    && parts
                        .iter()
                        .all(|p| p.contract == PushdownContract::ExactRowFilter)
            }
            _ => false,
        }
    }

    /// AND/OR composition: costed only when every child is costed.
    fn merge_costing_all_costed(costings: &[PushdownCosting]) -> PushdownCosting {
        if costings.iter().all(|c| c.is_costed()) {
            PushdownCosting::CostedPruning
        } else {
            PushdownCosting::UncostedBestEffort
        }
    }
}

struct ChildClassificationAccumulator {
    pushed_parts: Vec<ClassifiedPushedPart>,
    residual_parts: Vec<*mut pg_sys::Expr>,
    all_exact_pushable: bool,
    any_pushed: bool,
}

impl ChildClassificationAccumulator {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            pushed_parts: Vec::with_capacity(capacity),
            residual_parts: Vec::with_capacity(capacity),
            all_exact_pushable: true,
            any_pushed: false,
        }
    }

    fn absorb(&mut self, child: ClauseClassification) {
        match child {
            ClauseClassification::Pushable { parts, residual } => {
                self.any_pushed = true;
                for part in parts {
                    if part.contract != PushdownContract::ExactRowFilter {
                        self.all_exact_pushable = false;
                    }
                    self.pushed_parts.push(part);
                }
                if let Some(res) = residual {
                    self.residual_parts.push(res);
                    self.all_exact_pushable = false;
                }
            }
            ClauseClassification::PartialPush { pushed, residual } => {
                self.pushed_parts.push(ClassifiedPushedPart {
                    expr: pushed,
                    contract: PushdownContract::ConservativePruning,
                    costing: PushdownCosting::UncostedBestEffort,
                });
                self.residual_parts.push(residual);
                self.any_pushed = true;
                self.all_exact_pushable = false;
            }
            ClauseClassification::Unsupported { residual } => {
                self.residual_parts.push(residual);
                self.all_exact_pushable = false;
            }
        }
    }

    unsafe fn into_and_classification(
        self,
        original: *mut pg_sys::Expr,
        location: pg_sys::ParseLoc,
    ) -> ClauseClassification {
        if !self.any_pushed {
            return ClauseClassification::Unsupported { residual: original };
        }

        if self.can_merge_as_exact() {
            let merged_costing = self.merge_part_costing();
            if self.pushed_parts.len() == 1 {
                return ClauseClassification::Pushable {
                    parts: self.pushed_parts,
                    residual: None,
                };
            }
            let exprs: Vec<*mut pg_sys::Expr> =
                self.pushed_parts.iter().map(|p| p.expr).collect();
            let merged = unsafe { make_and(&exprs, location) };
            return ClauseClassification::Pushable {
                parts: vec![ClassifiedPushedPart {
                    expr: merged,
                    contract: PushdownContract::ExactRowFilter,
                    costing: merged_costing,
                }],
                residual: None,
            };
        }

        let residual = unsafe { self.residual_expr(location) };
        ClauseClassification::Pushable {
            parts: self.pushed_parts,
            residual,
        }
    }

    fn can_merge_as_exact(&self) -> bool {
        self.all_exact_pushable && self.residual_parts.is_empty()
    }

    fn merge_part_costing(&self) -> PushdownCosting {
        if self.pushed_parts.iter().all(|p| p.costing.is_costed()) {
            PushdownCosting::CostedPruning
        } else {
            PushdownCosting::UncostedBestEffort
        }
    }

    unsafe fn residual_expr(
        &self,
        location: pg_sys::ParseLoc,
    ) -> Option<*mut pg_sys::Expr> {
        if self.residual_parts.is_empty() {
            None
        } else if self.residual_parts.len() == 1 {
            Some(self.residual_parts[0])
        } else {
            Some(unsafe { make_and(&self.residual_parts, location) })
        }
    }
}

/// Return true when a subtree contains volatile functions or SubPlans.
///
/// # Safety
///
/// `expr` must be NULL or a live PostgreSQL expression tree in the current
/// backend memory context.
pub(crate) unsafe fn subtree_is_unsafe_to_push(expr: *mut pg_sys::Expr) -> bool {
    if expr.is_null() {
        return false;
    }
    if unsafe { pg_sys::contain_volatile_functions(expr as *mut pg_sys::Node) } {
        return true;
    }
    unsafe { contains_subplan(expr.cast::<pg_sys::Node>()) }
}

unsafe fn make_and(
    parts: &[*mut pg_sys::Expr],
    location: pg_sys::ParseLoc,
) -> *mut pg_sys::Expr {
    if parts.len() == 1 {
        return parts[0];
    }
    let args = unsafe { build_expr_list(parts) };
    unsafe { pg_sys::makeBoolExpr(pg_sys::BoolExprType::AND_EXPR, args, location) }
}

unsafe fn make_or(
    parts: &[*mut pg_sys::Expr],
    location: pg_sys::ParseLoc,
) -> *mut pg_sys::Expr {
    if parts.len() == 1 {
        return parts[0];
    }
    let args = unsafe { build_expr_list(parts) };
    unsafe { pg_sys::makeBoolExpr(pg_sys::BoolExprType::OR_EXPR, args, location) }
}

unsafe fn make_not(
    child: *mut pg_sys::Expr,
    location: pg_sys::ParseLoc,
) -> *mut pg_sys::Expr {
    let mut args: *mut pg_sys::List = ptr::null_mut();
    args = unsafe { pg_sys::lappend(args, child as *mut core::ffi::c_void) };
    unsafe { pg_sys::makeBoolExpr(pg_sys::BoolExprType::NOT_EXPR, args, location) }
}

unsafe fn build_expr_list(parts: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
    let mut out: *mut pg_sys::List = ptr::null_mut();
    for &p in parts {
        out = unsafe { pg_sys::lappend(out, p as *mut core::ffi::c_void) };
    }
    out
}
