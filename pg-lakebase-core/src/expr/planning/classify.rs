//! Planner expression safety inspection and compositional pushdown classification.

use core::ptr;

use pgrx::pg_sys;

use crate::expr::contract::{
    PushdownContract, PushdownCosting, QualPushdownDecision,
};
use crate::expr::pg::{PgBoolExpr, PgExprRef, PgRelabelType};
use crate::expr::predicate::{PlanPredicate, PlanPredicateContext};

use super::inspect::subtree_is_unsafe_to_push;

/// One independently pushable fragment from clause classification.
#[derive(Debug, Clone, Copy)]
pub struct ClassifiedPushedPart {
    pub expr: *mut pg_sys::Expr,
    pub contract: PushdownContract,
    pub costing: PushdownCosting,
}

/// A residual expression obtained from the original PG clause tree.
///
/// The constructor is private so generated widening expressions cannot be
/// routed into the executor residual list by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OriginalExpr(*mut pg_sys::Expr);

impl OriginalExpr {
    #[inline]
    fn from_ptr(expr: *mut pg_sys::Expr) -> Self {
        Self(expr)
    }

    #[inline]
    pub fn as_ptr(self) -> *mut pg_sys::Expr {
        self.0
    }
}
/// Per-clause classification consumed by [`PlanPushdownSplitter`](super::split::PlanPushdownSplitter).
#[derive(Debug, Clone)]
pub enum ClauseClassification {
    /// One or more pushed fragments. AND with mixed contracts keeps separate parts so a
    /// best-effort conservative child cannot drag down an exact sibling at runtime.
    Pushable {
        parts: Vec<ClassifiedPushedPart>,
        /// Untouched PG-owned subtrees that must remain as residual quals.
        residuals: Vec<OriginalExpr>,
    },

    PartialPush {
        pushed: *mut pg_sys::Expr,
        residual: OriginalExpr,
    },

    Unsupported {
        residual: OriginalExpr,
    },
}

/// Stateful classifier for one planner clause tree.
pub struct ClauseClassifier<'a, F> {
    predicate_ctx: &'a PlanPredicateContext,
    classify_leaf: &'a mut F,
}

impl<'a, F> ClauseClassifier<'a, F>
where
    F: FnMut(&PlanPredicate) -> QualPushdownDecision,
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
            return ClauseClassification::Unsupported {
                residual: OriginalExpr::from_ptr(expr),
            };
        }
        if unsafe { subtree_is_unsafe_to_push(expr) } {
            return ClauseClassification::Unsupported {
                residual: OriginalExpr::from_ptr(expr),
            };
        }
        unsafe { self.classify_subtree(expr) }
    }

    unsafe fn classify_subtree(
        &mut self,
        expr: *mut pg_sys::Expr,
    ) -> ClauseClassification {
        let r = unsafe { PgExprRef::from_raw(expr) };
        let tag = r.node_tag();

        match tag {
            pg_sys::NodeTag::T_OpExpr | pg_sys::NodeTag::T_NullTest => unsafe {
                self.classify_leaf_node(expr, r)
            },

            pg_sys::NodeTag::T_BoolExpr => {
                let be = PgBoolExpr::try_from_expr(r)
                    .expect("PgBoolExpr tag matched but downcast failed");
                let boolop = be.boolop();
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
                    _ => ClauseClassification::Unsupported {
                        residual: OriginalExpr::from_ptr(expr),
                    },
                }
            }

            pg_sys::NodeTag::T_RelabelType => {
                let rl = PgRelabelType::try_from_expr(r)
                    .expect("PgRelabelType tag matched but downcast failed");
                match rl.arg() {
                    Some(inner) => unsafe { self.classify_subtree(inner.as_ptr()) },
                    None => ClauseClassification::Unsupported {
                        residual: OriginalExpr::from_ptr(expr),
                    },
                }
            }

            _ => ClauseClassification::Unsupported {
                residual: OriginalExpr::from_ptr(expr),
            },
        }
    }

    unsafe fn classify_leaf_node(
        &mut self,
        expr: *mut pg_sys::Expr,
        r: PgExprRef<'_>,
    ) -> ClauseClassification {
        let predicate = match self.predicate_ctx.parse_leaf(r) {
            Ok(p) => p,
            Err(_) => {
                return ClauseClassification::Unsupported {
                    residual: OriginalExpr::from_ptr(expr),
                };
            }
        };
        let decision = (self.classify_leaf)(&predicate);
        match decision {
            QualPushdownDecision::Pushable { contract, costing } => {
                let residuals = if contract.requires_residual() {
                    vec![OriginalExpr::from_ptr(expr)]
                } else {
                    Vec::new()
                };
                Self::pushable_one(expr, contract, costing, residuals)
            }
            QualPushdownDecision::Unsupported => ClauseClassification::Unsupported {
                residual: OriginalExpr::from_ptr(expr),
            },
        }
    }

    unsafe fn classify_and(
        &mut self,
        original: *mut pg_sys::Expr,
        be: PgBoolExpr<'_>,
    ) -> ClauseClassification {
        let args = be.args_list();
        let len = if args.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(args) }
        };
        if len == 0 {
            return ClauseClassification::Unsupported {
                residual: OriginalExpr::from_ptr(original),
            };
        }

        let mut children =
            ChildClassificationAccumulator::with_capacity(len as usize);
        for i in 0..len {
            let cell = unsafe { pg_sys::list_nth(args, i) } as *mut pg_sys::Expr;
            let child = unsafe { self.classify_subtree(cell) };
            children.absorb(child);
        }

        let location = be.location();
        unsafe { children.into_and_classification(original, location) }
    }

    unsafe fn classify_or(
        &mut self,
        original: *mut pg_sys::Expr,
        be: PgBoolExpr<'_>,
    ) -> ClauseClassification {
        let args = be.args_list();
        let len = if args.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(args) }
        };
        if len == 0 {
            return ClauseClassification::Unsupported {
                residual: OriginalExpr::from_ptr(original),
            };
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

        let location = be.location();

        if all_exact {
            let mut branch_exprs: Vec<*mut pg_sys::Expr> =
                Vec::with_capacity(child_results.len());
            let mut all_costings: Vec<PushdownCosting> =
                Vec::with_capacity(child_results.len());
            for c in &child_results {
                match c {
                    ClauseClassification::Pushable { parts, residuals }
                        if residuals.is_empty()
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
                            residual: OriginalExpr::from_ptr(original),
                        };
                    }
                }
            }
            let pushed = unsafe { make_or(&branch_exprs, location) };
            return Self::pushable_one(
                pushed,
                PushdownContract::ExactRowFilter,
                Self::merge_costing_all_costed(&all_costings),
                Vec::new(),
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
                    return ClauseClassification::Unsupported {
                        residual: OriginalExpr::from_ptr(original),
                    };
                }
            }
        }

        let pushed = unsafe { make_or(&widenings, location) };
        ClauseClassification::PartialPush {
            pushed,
            residual: OriginalExpr::from_ptr(original),
        }
    }

    unsafe fn classify_not(
        &mut self,
        original: *mut pg_sys::Expr,
        be: PgBoolExpr<'_>,
    ) -> ClauseClassification {
        let args = be.args_list();
        let len = if args.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(args) }
        };
        if len != 1 {
            return ClauseClassification::Unsupported {
                residual: OriginalExpr::from_ptr(original),
            };
        }
        let child_ptr = unsafe { pg_sys::list_nth(args, 0) } as *mut pg_sys::Expr;
        let child = unsafe { self.classify_subtree(child_ptr) };

        match child {
            ClauseClassification::Pushable { parts, residuals }
                if residuals.is_empty()
                    && parts.len() == 1
                    && parts[0].contract == PushdownContract::ExactRowFilter =>
            {
                let location = be.location();
                let not_node = unsafe { make_not(parts[0].expr, location) };
                Self::pushable_one(
                    not_node,
                    PushdownContract::ExactRowFilter,
                    parts[0].costing,
                    Vec::new(),
                )
            }
            _ => ClauseClassification::Unsupported {
                residual: OriginalExpr::from_ptr(original),
            },
        }
    }

    fn pushable_one(
        expr: *mut pg_sys::Expr,
        contract: PushdownContract,
        costing: PushdownCosting,
        residuals: Vec<OriginalExpr>,
    ) -> ClauseClassification {
        ClauseClassification::Pushable {
            parts: vec![ClassifiedPushedPart {
                expr,
                contract,
                costing,
            }],
            residuals,
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
            ClauseClassification::Pushable { parts, residuals } => {
                residuals.is_empty()
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
    residual_parts: Vec<OriginalExpr>,
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
            ClauseClassification::Pushable { parts, residuals } => {
                self.any_pushed = true;
                for part in parts {
                    if part.contract != PushdownContract::ExactRowFilter {
                        self.all_exact_pushable = false;
                    }
                    self.pushed_parts.push(part);
                }
                if !residuals.is_empty() {
                    self.residual_parts.extend(residuals);
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
            return ClauseClassification::Unsupported {
                residual: OriginalExpr::from_ptr(original),
            };
        }

        if self.can_merge_as_exact() {
            let merged_costing = self.merge_part_costing();
            if self.pushed_parts.len() == 1 {
                return ClauseClassification::Pushable {
                    parts: self.pushed_parts,
                    residuals: Vec::new(),
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
                residuals: Vec::new(),
            };
        }

        ClauseClassification::Pushable {
            parts: self.pushed_parts,
            residuals: self.residual_parts,
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
