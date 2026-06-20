//! Property tests: pseudoconstant skip, security gating, and mixed-source
//! final scan-clause gating.
//! Rust-only model of path-stage and plan-stage `RestrictInfo` filters (no live PG).

use std::collections::HashSet;

use proptest::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LeafVerdict {
    ExactRowFilter,
    ConservativePruning,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ModelRestrictInfo {
    id: u32,
    pseudoconstant: bool,
    security_level: u32,
    leakproof: bool,
    verdict: LeafVerdict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ModelBaserel {
    baserestrict_min_security: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ModelClauseSource {
    BaseRestriction,
    Movable { movable_to_relation: bool },
}

/// Mirrors `restriction_is_securely_promotable` (`restrictinfo.c:425`).
#[inline]
fn securely_promotable(rinfo: ModelRestrictInfo, baserel: ModelBaserel) -> bool {
    rinfo.security_level <= baserel.baserestrict_min_security || rinfo.leakproof
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct PhaseSplit {
    pushed: Vec<u32>,
    residual: Vec<u32>,
}

struct SecurityGateModel;

impl SecurityGateModel {
    const fn path() -> Self {
        Self
    }

    const fn plan() -> Self {
        Self
    }

    fn classify(
        &self,
        clauses: &[ModelRestrictInfo],
        baserel: ModelBaserel,
    ) -> PhaseSplit {
        let mut split = PhaseSplit::default();
        for rinfo in clauses {
            if rinfo.pseudoconstant {
                continue;
            }
            if !securely_promotable(*rinfo, baserel) {
                split.residual.push(rinfo.id);
                continue;
            }
            match rinfo.verdict {
                LeafVerdict::ExactRowFilter => {
                    split.pushed.push(rinfo.id);
                }
                LeafVerdict::ConservativePruning => {
                    split.pushed.push(rinfo.id);
                    split.residual.push(rinfo.id);
                }
                LeafVerdict::Unsupported => {
                    split.residual.push(rinfo.id);
                }
            }
        }
        split
    }
}

fn classify_path_stage(
    clauses: &[ModelRestrictInfo],
    baserel: ModelBaserel,
) -> PhaseSplit {
    SecurityGateModel::path().classify(clauses, baserel)
}

fn classify_plan_stage(
    clauses: &[ModelRestrictInfo],
    baserel: ModelBaserel,
) -> PhaseSplit {
    SecurityGateModel::plan().classify(clauses, baserel)
}

fn classify_final_scan_clauses(
    clauses: &[(ModelRestrictInfo, ModelClauseSource)],
    baserel: ModelBaserel,
) -> PhaseSplit {
    let mut split = PhaseSplit::default();
    for (rinfo, source) in clauses {
        if rinfo.pseudoconstant {
            continue;
        }
        if !securely_promotable(*rinfo, baserel) {
            split.residual.push(rinfo.id);
            continue;
        }
        if matches!(
            source,
            ModelClauseSource::Movable {
                movable_to_relation: false
            }
        ) {
            split.residual.push(rinfo.id);
            continue;
        }
        match rinfo.verdict {
            LeafVerdict::ExactRowFilter => {
                split.pushed.push(rinfo.id);
            }
            LeafVerdict::ConservativePruning => {
                split.pushed.push(rinfo.id);
                split.residual.push(rinfo.id);
            }
            LeafVerdict::Unsupported => {
                split.residual.push(rinfo.id);
            }
        }
    }
    split
}

const MAX_SECURITY_LEVEL: u32 = 4;
const MAX_CLAUSES: usize = 8;

fn arb_verdict() -> impl Strategy<Value = LeafVerdict> {
    prop_oneof![
        Just(LeafVerdict::ExactRowFilter),
        Just(LeafVerdict::ConservativePruning),
        Just(LeafVerdict::Unsupported),
    ]
}

fn arb_clause_unkeyed() -> impl Strategy<Value = ModelRestrictInfo> {
    (
        any::<bool>(),
        0u32..=MAX_SECURITY_LEVEL + 1,
        any::<bool>(),
        arb_verdict(),
    )
        .prop_map(|(pseudoconstant, security_level, leakproof, verdict)| {
            ModelRestrictInfo {
                id: 0,
                pseudoconstant,
                security_level,
                leakproof,
                verdict,
            }
        })
}

fn arb_clauses() -> impl Strategy<Value = Vec<ModelRestrictInfo>> {
    proptest::collection::vec(arb_clause_unkeyed(), 1..=MAX_CLAUSES).prop_map(
        |mut clauses| {
            for (i, c) in clauses.iter_mut().enumerate() {
                c.id = i as u32;
            }
            clauses
        },
    )
}

fn arb_baserel() -> impl Strategy<Value = ModelBaserel> {
    (0u32..=MAX_SECURITY_LEVEL).prop_map(|baserestrict_min_security| ModelBaserel {
        baserestrict_min_security,
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn pbt4_pseudoconstant_skip_and_security_gating(
        clauses in arb_clauses(),
        baserel in arb_baserel(),
    ) {
        let path = classify_path_stage(&clauses, baserel);
        let plan = classify_plan_stage(&clauses, baserel);

        let path_pushed: HashSet<u32> = path.pushed.iter().copied().collect();
        let path_residual: HashSet<u32> = path.residual.iter().copied().collect();
        let plan_pushed: HashSet<u32> = plan.pushed.iter().copied().collect();
        let plan_residual: HashSet<u32> = plan.residual.iter().copied().collect();

        prop_assert_eq!(
            &path,
            &plan,
            "path-stage and plan-stage security/pseudoconstant gates must classify identically"
        );

        for rinfo in &clauses {
            if rinfo.pseudoconstant {
                prop_assert!(
                    !path_pushed.contains(&rinfo.id),
                    "pseudoconstant clause {} in path pushed",
                    rinfo.id
                );
                prop_assert!(
                    !path_residual.contains(&rinfo.id),
                    "pseudoconstant clause {} in path residual",
                    rinfo.id
                );
                prop_assert!(
                    !plan_pushed.contains(&rinfo.id),
                    "pseudoconstant clause {} in plan pushed",
                    rinfo.id
                );
                prop_assert!(
                    !plan_residual.contains(&rinfo.id),
                    "pseudoconstant clause {} in plan residual",
                    rinfo.id
                );
                continue;
            }

            if !securely_promotable(*rinfo, baserel) {
                prop_assert!(
                    !path_pushed.contains(&rinfo.id),
                    "gated clause {} in path pushed",
                    rinfo.id
                );
                prop_assert!(
                    !plan_pushed.contains(&rinfo.id),
                    "gated clause {} in plan pushed",
                    rinfo.id
                );
                prop_assert!(path_residual.contains(&rinfo.id));
                prop_assert!(plan_residual.contains(&rinfo.id));
                continue;
            }

            match rinfo.verdict {
                LeafVerdict::ExactRowFilter => {
                    prop_assert!(path_pushed.contains(&rinfo.id));
                    prop_assert!(!path_residual.contains(&rinfo.id));
                    prop_assert!(plan_pushed.contains(&rinfo.id));
                    prop_assert!(!plan_residual.contains(&rinfo.id));
                }
                LeafVerdict::ConservativePruning => {
                    prop_assert!(path_pushed.contains(&rinfo.id));
                    prop_assert!(path_residual.contains(&rinfo.id));
                    prop_assert!(plan_pushed.contains(&rinfo.id));
                    prop_assert!(plan_residual.contains(&rinfo.id));
                }
                LeafVerdict::Unsupported => {
                    prop_assert!(!path_pushed.contains(&rinfo.id));
                    prop_assert!(path_residual.contains(&rinfo.id));
                    prop_assert!(!plan_pushed.contains(&rinfo.id));
                    prop_assert!(plan_residual.contains(&rinfo.id));
                }
            }
        }
    }
}

#[cfg(test)]
mod model_smoke {
    use super::*;

    fn baserel(min: u32) -> ModelBaserel {
        ModelBaserel {
            baserestrict_min_security: min,
        }
    }

    fn clause(
        id: u32,
        pseudoconstant: bool,
        security_level: u32,
        leakproof: bool,
        verdict: LeafVerdict,
    ) -> ModelRestrictInfo {
        ModelRestrictInfo {
            id,
            pseudoconstant,
            security_level,
            leakproof,
            verdict,
        }
    }

    #[test]
    fn securely_promotable_below_min_passes() {
        let r = clause(0, false, 1, false, LeafVerdict::ExactRowFilter);
        assert!(securely_promotable(r, baserel(2)));
    }

    #[test]
    fn securely_promotable_at_min_passes() {
        let r = clause(0, false, 2, false, LeafVerdict::ExactRowFilter);
        assert!(securely_promotable(r, baserel(2)));
    }

    #[test]
    fn securely_promotable_above_min_fails_unless_leakproof() {
        let r = clause(0, false, 3, false, LeafVerdict::ExactRowFilter);
        assert!(!securely_promotable(r, baserel(2)));
    }

    #[test]
    fn securely_promotable_leakproof_overrides_level() {
        let r = clause(0, false, 99, true, LeafVerdict::ExactRowFilter);
        assert!(securely_promotable(r, baserel(0)));
    }

    #[test]
    fn pseudoconstant_dropped_from_both_lists_in_both_phases() {
        let clauses = vec![clause(0, true, 0, false, LeafVerdict::ExactRowFilter)];
        let path = classify_path_stage(&clauses, baserel(0));
        let plan = classify_plan_stage(&clauses, baserel(0));
        assert!(path.pushed.is_empty());
        assert!(path.residual.is_empty());
        assert!(plan.pushed.is_empty());
        assert!(plan.residual.is_empty());
    }

    #[test]
    fn security_gated_clause_in_residual_only() {
        let clauses = vec![clause(0, false, 5, false, LeafVerdict::ExactRowFilter)];
        let path = classify_path_stage(&clauses, baserel(0));
        let plan = classify_plan_stage(&clauses, baserel(0));
        assert!(path.pushed.is_empty());
        assert_eq!(path.residual, vec![0]);
        assert!(plan.pushed.is_empty());
        assert_eq!(plan.residual, vec![0]);
    }

    #[test]
    fn leakproof_high_security_clause_still_pushable() {
        let clauses = vec![clause(0, false, 99, true, LeafVerdict::ExactRowFilter)];
        let path = classify_path_stage(&clauses, baserel(0));
        let plan = classify_plan_stage(&clauses, baserel(0));
        assert_eq!(path.pushed, vec![0]);
        assert!(path.residual.is_empty());
        assert_eq!(plan.pushed, vec![0]);
        assert!(plan.residual.is_empty());
    }

    #[test]
    fn conservative_pruning_safe_clause_appears_in_both_lists() {
        let clauses =
            vec![clause(0, false, 0, false, LeafVerdict::ConservativePruning)];
        let path = classify_path_stage(&clauses, baserel(0));
        let plan = classify_plan_stage(&clauses, baserel(0));
        assert_eq!(path.pushed, vec![0]);
        assert_eq!(path.residual, vec![0]);
        assert_eq!(plan.pushed, vec![0]);
        assert_eq!(plan.residual, vec![0]);
    }

    #[test]
    fn unsupported_safe_clause_residual_only() {
        let clauses = vec![clause(0, false, 0, false, LeafVerdict::Unsupported)];
        let path = classify_path_stage(&clauses, baserel(0));
        let plan = classify_plan_stage(&clauses, baserel(0));
        assert!(path.pushed.is_empty());
        assert_eq!(path.residual, vec![0]);
        assert!(plan.pushed.is_empty());
        assert_eq!(plan.residual, vec![0]);
    }

    #[test]
    fn mixed_list_each_clause_treated_independently() {
        let clauses = vec![
            clause(0, true, 0, false, LeafVerdict::ExactRowFilter),
            clause(1, false, 5, false, LeafVerdict::ExactRowFilter),
            clause(2, false, 0, false, LeafVerdict::ExactRowFilter),
        ];
        let path = classify_path_stage(&clauses, baserel(0));
        let plan = classify_plan_stage(&clauses, baserel(0));
        assert_eq!(path.pushed, vec![2]);
        assert_eq!(path.residual, vec![1]);
        assert_eq!(plan.pushed, vec![2]);
        assert_eq!(plan.residual, vec![1]);
    }

    #[test]
    fn mixed_final_scan_clauses_keep_unmovable_ppi_residual() {
        let clauses = vec![
            (
                clause(0, false, 0, false, LeafVerdict::ExactRowFilter),
                ModelClauseSource::BaseRestriction,
            ),
            (
                clause(1, false, 0, false, LeafVerdict::ExactRowFilter),
                ModelClauseSource::Movable {
                    movable_to_relation: false,
                },
            ),
        ];

        let split = classify_final_scan_clauses(&clauses, baserel(0));
        assert_eq!(split.pushed, vec![0]);
        assert_eq!(split.residual, vec![1]);

        let old_all_base = clauses
            .iter()
            .map(|(rinfo, _)| (*rinfo, ModelClauseSource::BaseRestriction))
            .collect::<Vec<_>>();
        let old_split = classify_final_scan_clauses(&old_all_base, baserel(0));
        assert_eq!(old_split.pushed, vec![0, 1]);
        assert!(old_split.residual.is_empty());
    }

    #[test]
    fn mixed_final_scan_clauses_still_push_movable_ppi() {
        let clauses = vec![
            (
                clause(0, false, 0, false, LeafVerdict::ExactRowFilter),
                ModelClauseSource::BaseRestriction,
            ),
            (
                clause(1, false, 0, false, LeafVerdict::ExactRowFilter),
                ModelClauseSource::Movable {
                    movable_to_relation: true,
                },
            ),
        ];

        let split = classify_final_scan_clauses(&clauses, baserel(0));
        assert_eq!(split.pushed, vec![0, 1]);
        assert!(split.residual.is_empty());
    }
}
