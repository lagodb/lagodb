//! Plan-stage `RestrictInfo` unwrap, security/movability gates, and [`PlanPushdownSplitter`]
//! → [`PlanPushdownSplit`]. Expression pointers are PG-owned; encode in the
//! customscan plan-data envelope.

use pgrx::pg_sys;

use crate::expr::contract::{
    ColumnRef, PushdownContract, PushdownCosting, QualPushdownDecision,
};
use crate::expr::predicate::PlanPredicate;

/// One pushed PG expression with contract and costing metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushedExpr {
    pub expr: *mut pg_sys::Expr,
    pub contract: PushdownContract,
    pub costing: PushdownCosting,
}

/// Source of a planner clause list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScanClauseSource {
    /// `RelOptInfo.baserestrictinfo`; only the security gate is required.
    BaseRestriction,
    /// `joininfo` / `ParamPathInfo.ppi_clauses`; also requires movability.
    Movable,
}

impl ScanClauseSource {
    #[inline]
    fn requires_movability_gate(self) -> bool {
        matches!(self, Self::Movable)
    }
}

/// Plan-stage split: `residual` / `pushed` / `recheck` / `column_refs`.
#[derive(Debug, Clone)]
pub struct PlanPushdownSplit {
    pub residual: Vec<*mut pg_sys::Expr>,
    pub pushed: Vec<PushedExpr>,
    pub recheck: Vec<*mut pg_sys::Expr>,
    pub column_refs: Vec<ColumnRef>,
}

impl PlanPushdownSplit {
    /// All pushed PG expression pointers (plan encode / column-ref walk order).
    pub fn pushed_exprs(&self) -> impl Iterator<Item = *mut pg_sys::Expr> + '_ {
        self.pushed.iter().map(|p| p.expr)
    }

    /// Contracts aligned with [`Self::pushed`] order (private encode).
    pub fn pushed_contracts(&self) -> impl Iterator<Item = PushdownContract> + '_ {
        self.pushed.iter().map(|p| p.contract)
    }

    /// Pushed exprs eligible for path-stage scan-volume costing.
    pub fn costed_pruning_exprs(
        &self,
    ) -> impl Iterator<Item = *mut pg_sys::Expr> + '_ {
        self.pushed
            .iter()
            .filter(|p| p.costing.is_costed())
            .map(|p| p.expr)
    }

    /// Whether this split contributes any pushed predicate to a scan variant.
    #[inline]
    pub fn has_pushed_predicates(&self) -> bool {
        !self.pushed.is_empty()
    }

    /// Concatenate two splits while preserving `column_refs.expr_index` alignment.
    ///
    /// `column_refs` index into the pushed expression section. When the right
    /// split is appended after the left split, the right indexes must be offset
    /// by `left.pushed.len()` so runtime translation still points at the same
    /// expressions after `custom_exprs` concatenation.
    pub fn merged_with_rebased_expr_indexes(&self, right: &Self) -> Self {
        let left_pushed_len = self.pushed.len();

        let mut residual =
            Vec::with_capacity(self.residual.len() + right.residual.len());
        residual.extend_from_slice(&self.residual);
        residual.extend_from_slice(&right.residual);

        let mut pushed = Vec::with_capacity(self.pushed.len() + right.pushed.len());
        pushed.extend_from_slice(&self.pushed);
        pushed.extend_from_slice(&right.pushed);

        let mut recheck =
            Vec::with_capacity(self.recheck.len() + right.recheck.len());
        recheck.extend_from_slice(&self.recheck);
        recheck.extend_from_slice(&right.recheck);

        let mut column_refs =
            Vec::with_capacity(self.column_refs.len() + right.column_refs.len());
        column_refs.extend_from_slice(&self.column_refs);
        for cr in &right.column_refs {
            let mut rebased = cr.clone();
            rebased.expr_index = cr.expr_index + left_pushed_len;
            column_refs.push(rebased);
        }

        Self {
            residual,
            pushed,
            recheck,
            column_refs,
        }
    }
}

/// Bare `Expr` from `RestrictInfo.clause` plus the source `rinfo` for gates.
#[derive(Debug, Clone, Copy)]
struct UnwrappedClause {
    clause: *mut pg_sys::Expr,
    rinfo: *mut pg_sys::RestrictInfo,
}

use crate::expr::classify::{ClauseClassification, ClauseClassifier};
use crate::expr::relation::{ColumnRefCollector, PlanScanRelation};

/// Planner clause splitter for PG-preprocessed `RestrictInfo.clause` trees.
///
/// PostgreSQL runs quals through `preprocess_expression(EXPRKIND_QUAL)` and
/// `eval_const_expressions`, whose `NOT_EXPR` branch delegates to
/// `negate_clause`, before constructing these `RestrictInfo` nodes. This layer
/// therefore classifies the resulting PG semantics and never reimplements NOT
/// normalization.
pub struct PlanPushdownSplitter<'a, F> {
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    scan_clauses: *mut pg_sys::List,
    source: ScanClauseSource,
    classify_leaf: &'a mut F,
}

impl<'a, F> PlanPushdownSplitter<'a, F>
where
    F: FnMut(&PlanPredicate) -> QualPushdownDecision,
{
    #[inline]
    pub fn new(
        root: *mut pg_sys::PlannerInfo,
        baserel: *mut pg_sys::RelOptInfo,
        scan_clauses: *mut pg_sys::List,
        source: ScanClauseSource,
        classify_leaf: &'a mut F,
    ) -> Self {
        Self {
            root,
            baserel,
            scan_clauses,
            source,
            classify_leaf,
        }
    }

    /// # Safety
    ///
    /// Live planner pointers; `scan_clauses` is NULL or `List<RestrictInfo>`.
    pub unsafe fn split(&mut self) -> PlanPushdownSplit {
        let source = self.source;
        unsafe { self.split_with_source(|_| source) }
    }

    /// Split a final scan clause list whose entries may come from different
    /// planner sources.
    ///
    /// PostgreSQL passes `PlanCustomPath` a single ordered `scan_clauses` list.
    /// For parameterized base scans that list can contain both
    /// `baserestrictinfo` and `ParamPathInfo.ppi_clauses`, so callers that have
    /// source information must supply it per `RestrictInfo`.
    ///
    /// # Safety
    ///
    /// Live planner pointers; `scan_clauses` is NULL or `List<RestrictInfo>`.
    pub unsafe fn split_with_source<S>(
        &mut self,
        mut source_for: S,
    ) -> PlanPushdownSplit
    where
        S: FnMut(*mut pg_sys::RestrictInfo) -> ScanClauseSource,
    {
        let root = self.root;
        let baserel = self.baserel;
        let scan_clauses = self.scan_clauses;

        let scan_rel = unsafe { PlanScanRelation::new(root, baserel) };
        let predicate_ctx = scan_rel.predicate_context();
        let gates = PlannerClauseGate::for_relation(baserel);
        let mut classifier =
            ClauseClassifier::new(&predicate_ctx, &mut *self.classify_leaf);
        let mut out = SplitAccumulator::new();

        for clause in unsafe { RestrictInfoList::new(scan_clauses).unwrapped() } {
            let source = source_for(clause.rinfo);
            if !unsafe { gates.accepts(clause.rinfo, source) } {
                out.push_residual(clause.clause);
                continue;
            }

            let classification = unsafe { classifier.classify(clause.clause) };
            out.absorb_classification(clause.clause, classification);
        }

        let column_refs = unsafe {
            ColumnRefCollector::new(scan_rel)
                .collect_exprs(out.pushed.iter().map(|p| p.expr))
        };
        out.finish(column_refs)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PlannerClauseGate {
    baserel: *mut pg_sys::RelOptInfo,
}

impl PlannerClauseGate {
    #[inline]
    pub(crate) fn for_relation(baserel: *mut pg_sys::RelOptInfo) -> Self {
        Self { baserel }
    }

    /// `restriction_is_securely_promotable` — apply before provider classification.
    ///
    /// # Safety
    ///
    /// Live `RestrictInfo` and `RelOptInfo` in the planner context.
    #[inline]
    pub(crate) unsafe fn is_securely_promotable(
        self,
        rinfo: *mut pg_sys::RestrictInfo,
    ) -> bool {
        unsafe { pg_sys::restriction_is_securely_promotable(rinfo, self.baserel) }
    }

    /// `join_clause_is_movable_to` for `joininfo` / `ppi_clauses` sources.
    ///
    /// # Safety
    ///
    /// Live planner pointers.
    #[inline]
    pub(crate) unsafe fn is_movable_to_relation(
        self,
        rinfo: *mut pg_sys::RestrictInfo,
    ) -> bool {
        unsafe { pg_sys::join_clause_is_movable_to(rinfo, self.baserel) }
    }

    /// Apply all gates required by the source list.
    ///
    /// # Safety
    ///
    /// Live planner pointers.
    unsafe fn accepts(
        self,
        rinfo: *mut pg_sys::RestrictInfo,
        source: ScanClauseSource,
    ) -> bool {
        if !unsafe { self.is_securely_promotable(rinfo) } {
            return false;
        }
        if source.requires_movability_gate()
            && !unsafe { self.is_movable_to_relation(rinfo) }
        {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, Copy)]
struct RestrictInfoList {
    raw: *mut pg_sys::List,
}

impl RestrictInfoList {
    #[inline]
    fn new(raw: *mut pg_sys::List) -> Self {
        Self { raw }
    }

    /// Unwrap `List<RestrictInfo>` into bare `Expr`s, skipping pseudoconstants.
    ///
    /// # Safety
    ///
    /// `self.raw` is NULL or `List<RestrictInfo>`; nodes stay live in the planner context.
    unsafe fn unwrapped(self) -> Vec<UnwrappedClause> {
        if self.raw.is_null() {
            return Vec::new();
        }

        let len = unsafe { (*self.raw).length };
        if len <= 0 {
            return Vec::new();
        }

        let mut out = Vec::with_capacity(len as usize);
        for i in 0..len {
            let rinfo =
                unsafe { pg_sys::list_nth(self.raw, i) } as *mut pg_sys::RestrictInfo;
            if rinfo.is_null() {
                continue;
            }

            if unsafe { (*rinfo).pseudoconstant } {
                continue;
            }

            let clause = unsafe { (*rinfo).clause };
            if clause.is_null() {
                continue;
            }

            out.push(UnwrappedClause { clause, rinfo });
        }

        out
    }
}

#[derive(Debug)]
struct SplitAccumulator {
    residual: Vec<*mut pg_sys::Expr>,
    pushed: Vec<PushedExpr>,
    recheck: Vec<*mut pg_sys::Expr>,
}

impl SplitAccumulator {
    fn new() -> Self {
        Self {
            residual: Vec::new(),
            pushed: Vec::new(),
            recheck: Vec::new(),
        }
    }

    #[inline]
    fn push_residual(&mut self, expr: *mut pg_sys::Expr) {
        self.residual.push(expr);
    }

    fn absorb_classification(
        &mut self,
        original: *mut pg_sys::Expr,
        classification: ClauseClassification,
    ) {
        match classification {
            ClauseClassification::Pushable { parts, residuals } => {
                for part in parts {
                    self.push_pushed(PushedExpr {
                        expr: part.expr,
                        contract: part.contract,
                        costing: part.costing,
                    });
                }
                for residual in residuals {
                    // Every classifier residual is an untouched subtree of
                    // `original`; no semantic rewrite precedes classification.
                    self.push_residual(residual.as_ptr());
                }
            }

            ClauseClassification::PartialPush {
                pushed: p,
                residual: _,
            } => {
                self.push_pushed(PushedExpr {
                    expr: p,
                    contract: PushdownContract::ConservativePruning,
                    costing: PushdownCosting::UncostedBestEffort,
                });
                self.push_residual(original);
            }

            ClauseClassification::Unsupported { residual: _ } => {
                self.push_residual(original);
            }
        }
    }

    fn push_pushed(&mut self, pushed: PushedExpr) {
        if pushed.contract.requires_recheck() {
            self.recheck.push(pushed.expr);
        }
        self.pushed.push(pushed);
    }

    fn finish(self, column_refs: Vec<ColumnRef>) -> PlanPushdownSplit {
        PlanPushdownSplit {
            residual: self.residual,
            pushed: self.pushed,
            recheck: self.recheck,
            column_refs,
        }
    }
}
