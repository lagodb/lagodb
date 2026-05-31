//! CustomPath variant routing after provider matching.

use pgrx::pg_sys;

use crate::customscan::param_path::{
    ParamPathEnumerator, ParamPathGroup, ParamPathResolver,
};
use crate::customscan::path_clause::ProviderClauseSplitter;
use crate::customscan::provider::{ErasedProvider, PathVariantKind};
use crate::expr::split::{PlanPushdownSplit, ScanClauseSource};

pub(super) struct PathlistRouter {
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    provider: &'static dyn ErasedProvider,
    base_split: PlanPushdownSplit,
}

impl PathlistRouter {
    /// # Safety
    ///
    /// Planner pointers must be live for the current pathlist callback.
    pub(super) unsafe fn new(
        root: *mut pg_sys::PlannerInfo,
        rel: *mut pg_sys::RelOptInfo,
        provider: &'static dyn ErasedProvider,
    ) -> Self {
        let base_split = unsafe {
            ProviderClauseSplitter::new(root, rel, provider)
                .split((*rel).baserestrictinfo, ScanClauseSource::BaseRestriction)
        };
        Self {
            root,
            rel,
            provider,
            base_split,
        }
    }

    /// Emit Plain first, then useful JoinParameterized variants.
    ///
    /// # Safety
    ///
    /// Captured planner pointers must remain live.
    pub(super) unsafe fn emit_variants(&self) {
        unsafe {
            self.emit_plain_variant();
            self.emit_join_parameterized_variants();
        }
    }

    unsafe fn emit_plain_variant(&self) {
        let lateral_relids_copy =
            unsafe { pg_sys::bms_copy((*self.rel).lateral_relids) };

        if lateral_relids_copy.is_null() {
            unsafe {
                self.emit_path(
                    PathVariantKind::Plain,
                    lateral_relids_copy,
                    &self.base_split,
                );
            }
            return;
        }

        let lateral_ppi = unsafe {
            ParamPathResolver::new(self.root, self.rel, self.provider)
                .resolve_and_split(lateral_relids_copy)
        };
        let plain_split = self
            .base_split
            .merged_with_rebased_expr_indexes(&lateral_ppi.split);
        unsafe {
            self.emit_path(PathVariantKind::Plain, lateral_relids_copy, &plain_split);
        }
    }

    unsafe fn emit_join_parameterized_variants(&self) {
        let joininfo = unsafe { (*self.rel).joininfo };
        let groups = unsafe {
            ParamPathEnumerator::new(self.root, self.rel, self.provider)
                .enumerate(joininfo)
        };

        for group in groups {
            let variant = JoinParameterizedVariant::new(&self.base_split, &group);
            let Some(merged_split) = variant.merged_split() else {
                continue;
            };
            unsafe {
                self.emit_path(
                    PathVariantKind::JoinParameterized,
                    group.outer_relids,
                    &merged_split,
                );
            }
        }
    }

    unsafe fn emit_path(
        &self,
        kind: PathVariantKind,
        required_outer: *mut pg_sys::Bitmapset,
        split: &PlanPushdownSplit,
    ) {
        let ctx = crate::customscan::builder::EmitCustomPathContext {
            root: self.root,
            baserel: self.rel,
            kind,
            required_outer,
            split,
        };
        unsafe { self.provider.emit_path(&ctx) };
    }
}

struct JoinParameterizedVariant<'a> {
    base_split: &'a PlanPushdownSplit,
    group: &'a ParamPathGroup,
}

impl<'a> JoinParameterizedVariant<'a> {
    fn new(base_split: &'a PlanPushdownSplit, group: &'a ParamPathGroup) -> Self {
        Self { base_split, group }
    }

    fn merged_split(&self) -> Option<PlanPushdownSplit> {
        if !self.group.ppi_split.has_pushed_predicates() {
            return None;
        }
        Some(
            self.base_split
                .merged_with_rebased_expr_indexes(&self.group.ppi_split),
        )
    }
}

/// True when JoinParameterized variant adds no pushed clauses beyond Plain.
pub fn join_parameterized_variant_pushes_nothing(
    ppi_split: &crate::expr::split::PlanPushdownSplit,
) -> bool {
    !ppi_split.has_pushed_predicates()
}

#[cfg(test)]
/// Merge base and ppi splits; rebase `column_refs.expr_index` for concatenation.
fn merge_pushdown_splits(
    left: &crate::expr::split::PlanPushdownSplit,
    right: &crate::expr::split::PlanPushdownSplit,
) -> crate::expr::split::PlanPushdownSplit {
    left.merged_with_rebased_expr_indexes(right)
}

#[cfg(test)]
mod joinparam_noop_pbt {
    use proptest::prelude::*;

    use super::join_parameterized_variant_pushes_nothing;
    use crate::customscan::test_support::PushdownSplitFixture;
    use crate::expr::split::PlanPushdownSplit;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum EmitDecision {
        Emit,
        Skip,
    }

    fn emit_decision(
        _base: &PlanPushdownSplit,
        ppi: &PlanPushdownSplit,
    ) -> EmitDecision {
        if join_parameterized_variant_pushes_nothing(ppi) {
            EmitDecision::Skip
        } else {
            EmitDecision::Emit
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn skip_join_parameterized_when_ppi_pushes_nothing(
            base_pushed_len in 0usize..=4,
            base_residual_len in 0usize..=4,
            base_recheck_len in 0usize..=4,
            ppi_residual_len in 0usize..=4,
            ppi_recheck_len in 0usize..=4,
        ) {
            let base = PushdownSplitFixture::new(0).split_exact_counts(
                base_pushed_len,
                base_residual_len,
                base_recheck_len,
            );
            let ppi = PushdownSplitFixture::new(1).split_exact_counts(
                0,
                ppi_residual_len,
                ppi_recheck_len,
            );

            prop_assert!(ppi.pushed.is_empty());

            let decision = emit_decision(&base, &ppi);

            prop_assert_eq!(
                decision,
                EmitDecision::Skip,
                "empty ppi.pushed must skip JoinParameterized (base.pushed.len={})",
                base_pushed_len
            );
        }
    }
}

#[cfg(test)]
mod join_parameterized_noop_preservation_tests {
    use super::*;
    use crate::customscan::test_support::PushdownSplitFixture;
    use crate::expr::split::{PlanPushdownSplit, PushdownContract};
    use pgrx::pg_sys;
    use proptest::prelude::*;

    #[derive(Debug)]
    enum EmitDecision {
        Skip,
        Emit {
            merged: PlanPushdownSplit,
            required_outer: *mut pg_sys::Bitmapset,
        },
    }

    fn original_emit_decision(
        base: &PlanPushdownSplit,
        ppi: &PlanPushdownSplit,
        outer_relids: *mut pg_sys::Bitmapset,
    ) -> EmitDecision {
        let merged = merge_pushdown_splits(base, ppi);
        EmitDecision::Emit {
            merged,
            required_outer: outer_relids,
        }
    }

    fn fixed_emit_decision(
        base: &PlanPushdownSplit,
        ppi: &PlanPushdownSplit,
        outer_relids: *mut pg_sys::Bitmapset,
    ) -> EmitDecision {
        if ppi.pushed.is_empty() {
            return EmitDecision::Skip;
        }
        let merged = merge_pushdown_splits(base, ppi);
        EmitDecision::Emit {
            merged,
            required_outer: outer_relids,
        }
    }

    fn arb_guarantee() -> impl Strategy<Value = PushdownContract> {
        prop_oneof![
            Just(PushdownContract::ExactRowFilter),
            Just(PushdownContract::ConservativePruning),
        ]
    }

    fn arb_split(
        namespace: u64,
        pushed_min: usize,
        pushed_max: usize,
    ) -> impl Strategy<Value = PlanPushdownSplit> {
        (pushed_min..=pushed_max, 0usize..=3, 0usize..=3).prop_flat_map(
            move |(pushed_n, residual_n, recheck_n)| {
                let guarantees = prop::collection::vec(arb_guarantee(), pushed_n);
                let col_idxs = if pushed_n == 0 {
                    Just(Vec::<usize>::new()).boxed()
                } else {
                    prop::collection::vec(0..pushed_n, 0..=3usize).boxed()
                };
                (guarantees, col_idxs).prop_map(move |(guarantees, col_idxs)| {
                    PushdownSplitFixture::new(namespace).split_from_contracts(
                        residual_n,
                        recheck_n,
                        &guarantees,
                        &col_idxs,
                    )
                })
            },
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn prop2_preserves_useful_join_parameterized_variant(
            base in arb_split(0, 0, 3),
            ppi in arb_split(1, 1, 4),
            outer_tag in 0u64..32,
        ) {
            // Generator: non-empty ppi => must emit.
            prop_assert!(
                !ppi.pushed.is_empty(),
                "generator must produce a non-empty ppi.pushed",
            );

            let outer_relids = PushdownSplitFixture::relids(outer_tag);

            let original = original_emit_decision(&base, &ppi, outer_relids);
            let fixed = fixed_emit_decision(&base, &ppi, outer_relids);

            // Fixed loop must emit when ppi.pushed is non-empty.
            let EmitDecision::Emit {
                merged: merged_fixed,
                required_outer: outer_fixed,
            } = &fixed
            else {
                return Err(proptest::test_runner::TestCaseError::fail(
                    "fixed emit loop must not SKIP a group with \
                     non-empty ppi_split.pushed",
                ));
            };
            let EmitDecision::Emit {
                merged: merged_orig,
                required_outer: outer_orig,
            } = &original
            else {
                return Err(proptest::test_runner::TestCaseError::fail(
                    "original emit loop must always emit",
                ));
            };

            prop_assert_eq!(*outer_fixed, outer_relids);
            prop_assert_eq!(*outer_orig, outer_relids);

            let base_pushed_len = base.pushed.len();

            let mut exp_pushed = base.pushed.clone();
            exp_pushed.extend_from_slice(&ppi.pushed);
            let mut exp_residual = base.residual.clone();
            exp_residual.extend_from_slice(&ppi.residual);
            let mut exp_recheck = base.recheck.clone();
            exp_recheck.extend_from_slice(&ppi.recheck);
            let mut exp_guarantees: Vec<PushdownContract> =
                base.pushed_contracts().collect();
            exp_guarantees.extend(ppi.pushed_contracts());
            let mut exp_col_refs = base.column_refs.clone();
            for cr in &ppi.column_refs {
                let mut rebased = cr.clone();
                rebased.expr_index = cr.expr_index + base_pushed_len;
                exp_col_refs.push(rebased);
            }

            prop_assert_eq!(&merged_fixed.pushed, &exp_pushed);
            prop_assert_eq!(&merged_fixed.residual, &exp_residual);
            prop_assert_eq!(&merged_fixed.recheck, &exp_recheck);
            prop_assert_eq!(
                merged_fixed.pushed_contracts().collect::<Vec<_>>(),
                exp_guarantees
            );
            prop_assert_eq!(&merged_fixed.column_refs, &exp_col_refs);

            // 4. The fixed merged split is byte-for-byte identical to the
            //    original's, section by section.
            prop_assert_eq!(&merged_fixed.pushed, &merged_orig.pushed);
            prop_assert_eq!(&merged_fixed.residual, &merged_orig.residual);
            prop_assert_eq!(&merged_fixed.recheck, &merged_orig.recheck);
            prop_assert_eq!(
                merged_fixed.pushed_contracts().collect::<Vec<_>>(),
                merged_orig.pushed_contracts().collect::<Vec<_>>()
            );
            prop_assert_eq!(&merged_fixed.column_refs, &merged_orig.column_refs);

            // 5. Alignment invariant: pushed entries carry contracts in lockstep.
            prop_assert_eq!(merged_fixed.pushed.len(), exp_pushed.len());
        }
    }
}

#[cfg(test)]
mod joinparam_noop_unit_tests {
    use super::{join_parameterized_variant_pushes_nothing, merge_pushdown_splits};
    use crate::customscan::test_support::PushdownSplitFixture;
    use crate::expr::split::{PlanPushdownSplit, PushdownContract};

    fn make_split(
        tag: u64,
        pushed_n: usize,
        residual_n: usize,
        recheck_n: usize,
        col_idxs: &[usize],
    ) -> PlanPushdownSplit {
        PushdownSplitFixture::new(tag)
            .split_alternating_contracts(pushed_n, residual_n, recheck_n, col_idxs)
    }

    #[derive(Debug, PartialEq, Eq)]
    enum EmitDecision {
        Emit,
        Skip,
    }

    fn emit_decision(
        _base: &PlanPushdownSplit,
        ppi: &PlanPushdownSplit,
    ) -> EmitDecision {
        if join_parameterized_variant_pushes_nothing(ppi) {
            EmitDecision::Skip
        } else {
            EmitDecision::Emit
        }
    }

    fn emit_loop(
        base: &PlanPushdownSplit,
        groups: &[PlanPushdownSplit],
    ) -> Vec<PlanPushdownSplit> {
        let mut emitted = Vec::new();
        for ppi in groups {
            if join_parameterized_variant_pushes_nothing(ppi) {
                continue;
            }
            emitted.push(merge_pushdown_splits(base, ppi));
        }
        emitted
    }

    #[test]
    fn predicate_true_when_ppi_pushed_is_empty() {
        let ppi = make_split(1, 0, 2, 1, &[]);
        assert!(
            join_parameterized_variant_pushes_nothing(&ppi),
            "empty ppi_split.pushed => variant pushes nothing beyond Plain => skip",
        );
    }

    #[test]
    fn predicate_false_when_ppi_pushed_is_non_empty() {
        let ppi = make_split(1, 2, 1, 1, &[0]);
        assert!(
            !join_parameterized_variant_pushes_nothing(&ppi),
            "non-empty ppi_split.pushed => variant pushes a join qual => emit",
        );
    }

    #[test]
    fn decision_skips_empty_ppi_when_base_is_empty() {
        let base = make_split(0, 0, 0, 0, &[]);
        let ppi = make_split(1, 0, 1, 0, &[]);
        assert_eq!(emit_decision(&base, &ppi), EmitDecision::Skip);
    }

    #[test]
    fn decision_skips_empty_ppi_even_when_base_is_non_empty() {
        let base = make_split(0, 3, 1, 1, &[0, 1]);
        let ppi = make_split(1, 0, 2, 0, &[]);
        assert_eq!(
            emit_decision(&base, &ppi),
            EmitDecision::Skip,
            "non-empty base must NOT flip the decision to Emit when ppi pushes nothing",
        );
    }

    #[test]
    fn decision_emits_non_empty_ppi_when_base_is_empty() {
        let base = make_split(0, 0, 0, 0, &[]);
        let ppi = make_split(1, 2, 0, 0, &[0]);
        assert_eq!(emit_decision(&base, &ppi), EmitDecision::Emit);
    }

    #[test]
    fn decision_emits_non_empty_ppi_when_base_is_non_empty() {
        let base = make_split(0, 2, 1, 1, &[0]);
        let ppi = make_split(1, 1, 0, 0, &[0]);
        assert_eq!(emit_decision(&base, &ppi), EmitDecision::Emit);
    }

    #[test]
    fn merge_yields_base_union_ppi_section_wise() {
        let base = make_split(0, 2, 1, 1, &[0, 1]);
        let ppi = make_split(1, 3, 2, 1, &[0, 2]);
        let merged = merge_pushdown_splits(&base, &ppi);

        let mut exp_pushed = base.pushed.clone();
        exp_pushed.extend_from_slice(&ppi.pushed);
        assert_eq!(merged.pushed, exp_pushed);

        let mut exp_residual = base.residual.clone();
        exp_residual.extend_from_slice(&ppi.residual);
        assert_eq!(merged.residual, exp_residual);

        let mut exp_recheck = base.recheck.clone();
        exp_recheck.extend_from_slice(&ppi.recheck);
        assert_eq!(merged.recheck, exp_recheck);

        let mut exp_guarantees: Vec<PushdownContract> =
            base.pushed_contracts().collect();
        exp_guarantees.extend(ppi.pushed_contracts());
        assert_eq!(
            merged.pushed_contracts().collect::<Vec<_>>(),
            exp_guarantees
        );

        let base_pushed_len = base.pushed.len();
        let mut exp_col_refs = base.column_refs.clone();
        for cr in &ppi.column_refs {
            let mut rebased = cr.clone();
            rebased.expr_index = cr.expr_index + base_pushed_len;
            exp_col_refs.push(rebased);
        }
        assert_eq!(merged.column_refs, exp_col_refs);
    }

    #[test]
    fn merge_rebases_ppi_column_ref_expr_index_by_base_pushed_len() {
        let base = make_split(0, 2, 0, 0, &[1]);
        let ppi = make_split(1, 2, 0, 0, &[0, 1]);
        let merged = merge_pushdown_splits(&base, &ppi);

        assert_eq!(merged.column_refs[0].expr_index, 1);
        assert_eq!(merged.column_refs[1].expr_index, 2);
        assert_eq!(merged.column_refs[2].expr_index, 3);

        for cr in &merged.column_refs {
            assert!(
                cr.expr_index < merged.pushed.len(),
                "rebased expr_index {} out of bounds for merged.pushed.len() {}",
                cr.expr_index,
                merged.pushed.len(),
            );
        }
    }

    #[test]
    fn merge_preserves_pushed_guarantee_alignment() {
        let base = make_split(0, 3, 1, 0, &[]);
        let ppi = make_split(1, 2, 0, 1, &[]);
        let merged = merge_pushdown_splits(&base, &ppi);

        assert_eq!(merged.pushed.len(), base.pushed.len() + ppi.pushed.len());
    }

    #[test]
    fn merge_with_empty_base_yields_ppi_only() {
        let base = make_split(0, 0, 0, 0, &[]);
        let ppi = make_split(1, 2, 1, 0, &[0, 1]);
        let merged = merge_pushdown_splits(&base, &ppi);

        assert_eq!(merged.pushed, ppi.pushed);
        assert_eq!(
            merged.pushed_contracts().collect::<Vec<_>>(),
            ppi.pushed_contracts().collect::<Vec<_>>()
        );
        assert_eq!(merged.column_refs, ppi.column_refs);
        assert_eq!(
            merged.pushed.len(),
            ppi.pushed_contracts().collect::<Vec<_>>().len()
        );
    }

    #[test]
    fn zero_groups_emits_no_join_parameterized_path() {
        let base = make_split(0, 2, 1, 1, &[0]);
        let groups: Vec<PlanPushdownSplit> = Vec::new();
        let emitted = emit_loop(&base, &groups);
        assert!(
            emitted.is_empty(),
            "zero groups => loop body never runs => no JoinParameterized path",
        );
    }

    #[test]
    fn group_with_empty_ppi_and_empty_base_is_skipped_at_core_gate() {
        let base = make_split(0, 0, 0, 0, &[]);
        let ppi = make_split(1, 0, 0, 0, &[]);
        assert!(
            join_parameterized_variant_pushes_nothing(&ppi),
            "empty ppi.pushed must be reported as pushing nothing",
        );
        let emitted = emit_loop(&base, &[ppi]);
        assert!(
            emitted.is_empty(),
            "empty ppi AND empty base => skipped at the core gate, not the provider decline check",
        );
    }

    #[test]
    fn emit_loop_skips_empty_ppi_groups_and_emits_useful_ones() {
        let base = make_split(0, 1, 0, 0, &[0]);
        let groups = vec![
            make_split(1, 0, 1, 0, &[]),
            make_split(2, 2, 0, 0, &[0]),
            make_split(3, 0, 0, 0, &[]),
        ];
        let emitted = emit_loop(&base, &groups);
        assert_eq!(
            emitted.len(),
            1,
            "only the group with non-empty ppi_split.pushed should be emitted",
        );
        assert_eq!(emitted[0].pushed.len(), base.pushed.len() + 2);
        assert_eq!(
            emitted[0].pushed.len(),
            emitted[0].pushed_contracts().collect::<Vec<_>>().len()
        );
    }
}
