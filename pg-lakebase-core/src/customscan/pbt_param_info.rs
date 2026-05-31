//! Host-side proptest model of parameterized CustomPath emission
//! (`param_info` iff non-empty `required_outer`). Mirrors
//! [`emit_custom_path`](crate::customscan::builder::emit_custom_path) and
//! [`enumerate_param_path_groups`](crate::customscan::hook::enumerate_param_path_groups)
//! over `BTreeSet<u32>` instead of live `Bitmapset` FFI.

use std::collections::BTreeSet;

use proptest::prelude::*;

type Bitmapset = BTreeSet<u32>;

#[inline]
fn bms_is_empty(b: &Bitmapset) -> bool {
    b.is_empty()
}

#[inline]
fn bms_is_subset(a: &Bitmapset, b: &Bitmapset) -> bool {
    a.is_subset(b)
}

#[inline]
fn bms_overlap(a: &Bitmapset, b: &Bitmapset) -> bool {
    !a.is_disjoint(b)
}

#[inline]
fn bms_equal(a: &Bitmapset, b: &Bitmapset) -> bool {
    a == b
}

#[inline]
fn bms_union(a: &Bitmapset, b: &Bitmapset) -> Bitmapset {
    a.union(b).copied().collect()
}

#[inline]
fn bms_difference(a: &Bitmapset, b: &Bitmapset) -> Bitmapset {
    a.difference(b).copied().collect()
}

#[inline]
fn bms_copy(a: &Bitmapset) -> Bitmapset {
    a.clone()
}

#[derive(Debug, Clone)]
struct JoinClause {
    clause_relids: Bitmapset,
    pseudoconstant: bool,
    passes_security_gate: bool,
    passes_movability_gate: bool,
}

fn enumerate_groups_model(
    rel_relids: &Bitmapset,
    lateral_relids: &Bitmapset,
    joininfo: &[JoinClause],
) -> Vec<Bitmapset> {
    let mut accepted: Vec<Bitmapset> = Vec::new();

    for rinfo in joininfo {
        if rinfo.pseudoconstant {
            continue;
        }
        if !rinfo.passes_security_gate || !rinfo.passes_movability_gate {
            continue;
        }

        let lateral_copy = bms_copy(lateral_relids);
        let clause_outer = bms_difference(&rinfo.clause_relids, rel_relids);
        let candidate = bms_union(&lateral_copy, &clause_outer);

        if bms_equal(&candidate, lateral_relids) {
            continue;
        }

        if accepted.iter().any(|c| bms_equal(c, &candidate)) {
            continue;
        }

        accepted.push(candidate);
    }

    accepted
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VariantKind {
    Plain,
    JoinParameterized,
}

#[derive(Debug, Clone)]
struct EmittedPath {
    kind: VariantKind,
    required_outer: Bitmapset,
    param_info: Option<()>,
}

fn get_baserel_parampathinfo_model(required_outer: &Bitmapset) -> Option<()> {
    if bms_is_empty(required_outer) {
        None
    } else {
        Some(())
    }
}

fn emit_paths_model(
    rel_relids: &Bitmapset,
    lateral_relids: &Bitmapset,
    joininfo: &[JoinClause],
) -> Vec<EmittedPath> {
    let mut paths = Vec::new();

    let plain_outer = bms_copy(lateral_relids);
    paths.push(emit_one(
        rel_relids,
        lateral_relids,
        VariantKind::Plain,
        plain_outer,
    ));

    let groups = enumerate_groups_model(rel_relids, lateral_relids, joininfo);
    for required_outer in groups {
        paths.push(emit_one(
            rel_relids,
            lateral_relids,
            VariantKind::JoinParameterized,
            required_outer,
        ));
    }

    paths
}

fn emit_one(
    rel_relids: &Bitmapset,
    lateral_relids: &Bitmapset,
    kind: VariantKind,
    required_outer: Bitmapset,
) -> EmittedPath {
    assert!(
        bms_is_subset(lateral_relids, &required_outer),
        "emit_paths_model: bms_is_subset(lateral_relids, required_outer) \
         must hold; lateral_relids={:?} required_outer={:?}",
        lateral_relids,
        required_outer,
    );
    assert!(
        !bms_overlap(rel_relids, &required_outer),
        "emit_paths_model: !bms_overlap(rel.relids, required_outer) must \
         hold; rel_relids={:?} required_outer={:?}",
        rel_relids,
        required_outer,
    );

    let param_info = get_baserel_parampathinfo_model(&required_outer);

    EmittedPath {
        kind,
        required_outer,
        param_info,
    }
}

const RTI_RANGE: u32 = 6;

fn arb_rel_and_lateral() -> impl Strategy<Value = (Bitmapset, Bitmapset)> {
    proptest::collection::vec(0u8..3, RTI_RANGE as usize)
        .prop_map(|tags| {
            let mut rel = Bitmapset::new();
            let mut lateral = Bitmapset::new();
            for (i, t) in tags.into_iter().enumerate() {
                let id = i as u32;
                match t {
                    0 => {
                        rel.insert(id);
                    }
                    1 => {
                        lateral.insert(id);
                    }
                    _ => {}
                }
            }
            (rel, lateral)
        })
        .prop_filter("rel_relids must be non-empty", |(rel, _)| !rel.is_empty())
}

fn arb_join_clause() -> impl Strategy<Value = JoinClause> {
    (
        proptest::collection::btree_set(0u32..RTI_RANGE, 0..=RTI_RANGE as usize),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(|(clause_relids, ps_seed, sec_seed, mov_seed)| JoinClause {
            clause_relids,
            pseudoconstant: ps_seed && sec_seed,
            passes_security_gate: sec_seed || mov_seed,
            passes_movability_gate: mov_seed || ps_seed,
        })
}

fn arb_rel_shape() -> impl Strategy<Value = (Bitmapset, Bitmapset, Vec<JoinClause>)> {
    arb_rel_and_lateral().prop_flat_map(|(rel, lateral)| {
        proptest::collection::vec(arb_join_clause(), 0..=8)
            .prop_map(move |joininfo| (rel.clone(), lateral.clone(), joininfo))
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn pbt3_param_info_iff_required_outer(
        (rel_relids, lateral_relids, joininfo) in arb_rel_shape(),
    ) {
        prop_assert!(
            !bms_overlap(&rel_relids, &lateral_relids),
            "generator invariant: rel and lateral must be disjoint; \
             rel={:?} lateral={:?}",
            rel_relids,
            lateral_relids,
        );

        let paths = emit_paths_model(&rel_relids, &lateral_relids, &joininfo);

        let plain_count = paths
            .iter()
            .filter(|p| p.kind == VariantKind::Plain)
            .count();
        prop_assert_eq!(
            plain_count,
            1,
            "Requirement 2.1: exactly one Plain variant per relation; got {} \
             rel={:?} lateral={:?} joininfo={:?}",
            plain_count,
            rel_relids,
            lateral_relids,
            joininfo
        );

        for path in &paths {
            prop_assert_eq!(
                path.param_info.is_none(),
                bms_is_empty(&path.required_outer),
                "Requirement 2.2 violated: param_info.is_none()={} but \
                 bms_is_empty(required_outer)={} for path {:?}",
                path.param_info.is_none(),
                bms_is_empty(&path.required_outer),
                path
            );

            prop_assert!(
                bms_is_subset(&lateral_relids, &path.required_outer),
                "Requirement 2.3 violated: lateral_relids={:?} not subset \
                 of required_outer={:?} (path {:?})",
                lateral_relids,
                path.required_outer,
                path
            );

            prop_assert!(
                !bms_overlap(&rel_relids, &path.required_outer),
                "Requirement 2.3 violated: rel_relids={:?} overlaps \
                 required_outer={:?} (path {:?})",
                rel_relids,
                path.required_outer,
                path
            );

            if path.kind == VariantKind::JoinParameterized {
                prop_assert!(
                    !bms_is_empty(&path.required_outer),
                    "JoinParameterized variant must have non-empty \
                     required_outer (path {:?})",
                    path
                );
            }
        }

        for i in 0..paths.len() {
            for j in (i + 1)..paths.len() {
                prop_assert!(
                    !bms_equal(&paths[i].required_outer, &paths[j].required_outer),
                    "duplicate required_outer across emitted paths: \
                     paths[{}]={:?} paths[{}]={:?}",
                    i,
                    paths[i],
                    j,
                    paths[j]
                );
            }
        }
    }
}

#[cfg(test)]
mod model_smoke {
    use super::*;

    fn bms(items: &[u32]) -> Bitmapset {
        items.iter().copied().collect()
    }

    /// `bms_is_empty` matches `BTreeSet::is_empty` and treats both the
    /// "no items" and "default-constructed" forms identically.
    #[test]
    fn bitmapset_helpers_match_set_semantics() {
        let empty = Bitmapset::new();
        let a = bms(&[1, 2, 3]);
        let b = bms(&[2, 3, 4]);
        let sub = bms(&[2, 3]);

        assert!(bms_is_empty(&empty));
        assert!(!bms_is_empty(&a));
        assert!(bms_is_subset(&sub, &a));
        assert!(!bms_is_subset(&a, &sub));
        assert!(bms_overlap(&a, &b));
        assert!(!bms_overlap(&a, &bms(&[5, 6])));
        assert!(bms_equal(&a, &bms(&[3, 2, 1])));
        assert_eq!(bms_union(&a, &b), bms(&[1, 2, 3, 4]));
        assert_eq!(bms_difference(&a, &b), bms(&[1]));
    }

    /// Plain variant on a rel with no lateral_relids: `required_outer`
    /// is empty, so `param_info` is `None` and the Plain path is the
    /// only emitted path.
    #[test]
    fn plain_variant_no_lateral_no_joininfo() {
        let rel = bms(&[1]);
        let lateral = Bitmapset::new();
        let paths = emit_paths_model(&rel, &lateral, &[]);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].kind, VariantKind::Plain);
        assert!(paths[0].required_outer.is_empty());
        assert!(paths[0].param_info.is_none());
    }

    /// Plain variant on a rel with non-empty lateral_relids: the
    /// required_outer equals lateral_relids (non-empty), so
    /// `param_info` is `Some(_)` even though no join clause was
    /// pushed (the `indxpath.c:223` "lateral-only parameterized"
    /// case).
    #[test]
    fn plain_variant_lateral_only_has_param_info() {
        let rel = bms(&[1]);
        let lateral = bms(&[2]);
        let paths = emit_paths_model(&rel, &lateral, &[]);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].kind, VariantKind::Plain);
        assert_eq!(paths[0].required_outer, lateral);
        assert!(paths[0].param_info.is_some());
    }

    /// JoinParameterized variant: a clause referencing rel + outer rel
    /// produces a JoinParameterized path whose required_outer is
    /// `lateral ∪ (clause - rel)`.
    #[test]
    fn join_parameterized_variant_basic() {
        let rel = bms(&[1]);
        let lateral = Bitmapset::new();
        let clauses = vec![JoinClause {
            clause_relids: bms(&[1, 3]),
            pseudoconstant: false,
            passes_security_gate: true,
            passes_movability_gate: true,
        }];
        let paths = emit_paths_model(&rel, &lateral, &clauses);
        // Plain + 1 JoinParameterized.
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].kind, VariantKind::Plain);
        assert_eq!(paths[1].kind, VariantKind::JoinParameterized);
        assert_eq!(paths[1].required_outer, bms(&[3]));
        assert!(paths[1].param_info.is_some());
    }

    /// Pseudoconstant clauses are skipped during JoinParameterized
    /// enumeration, so they do not produce extra paths.
    #[test]
    fn pseudoconstant_clause_does_not_produce_path() {
        let rel = bms(&[1]);
        let lateral = Bitmapset::new();
        let clauses = vec![JoinClause {
            clause_relids: bms(&[1, 3]),
            pseudoconstant: true,
            passes_security_gate: true,
            passes_movability_gate: true,
        }];
        let paths = emit_paths_model(&rel, &lateral, &clauses);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].kind, VariantKind::Plain);
    }

    /// Security/movability gate failures force the clause to residual
    /// and exclude it from JoinParameterized enumeration (    /// 5.1, 5.2).
    #[test]
    fn gate_failures_skip_clause() {
        let rel = bms(&[1]);
        let lateral = Bitmapset::new();
        let clauses = vec![
            JoinClause {
                clause_relids: bms(&[1, 3]),
                pseudoconstant: false,
                passes_security_gate: false,
                passes_movability_gate: true,
            },
            JoinClause {
                clause_relids: bms(&[1, 4]),
                pseudoconstant: false,
                passes_security_gate: true,
                passes_movability_gate: false,
            },
        ];
        let paths = emit_paths_model(&rel, &lateral, &clauses);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].kind, VariantKind::Plain);
    }

    /// Equality dedup: two clauses producing the same required_outer
    /// collapse to one JoinParameterized variant.
    #[test]
    fn equality_dedup() {
        let rel = bms(&[1]);
        let lateral = Bitmapset::new();
        let clauses = vec![
            JoinClause {
                clause_relids: bms(&[1, 3]),
                pseudoconstant: false,
                passes_security_gate: true,
                passes_movability_gate: true,
            },
            JoinClause {
                clause_relids: bms(&[1, 3]),
                pseudoconstant: false,
                passes_security_gate: true,
                passes_movability_gate: true,
            },
        ];
        let paths = emit_paths_model(&rel, &lateral, &clauses);
        // Plain + 1 deduped JoinParameterized.
        assert_eq!(paths.len(), 2);
    }

    /// Strict superset is NOT deduped: each candidate `S` enables at
    /// least one clause its strict subsets cannot push (the clause
    /// `c_S` whose `clause_relids - rel_relids` requires the rels in
    /// `S - lateral`). The earlier conservative form silently
    /// dropped useful join-parameterized variants.
    #[test]
    fn strict_superset_is_not_dropped() {
        let rel = bms(&[1]);
        let lateral = Bitmapset::new();
        let clauses = vec![
            JoinClause {
                // Generates candidate {3}; pushes only this clause.
                clause_relids: bms(&[1, 3]),
                pseudoconstant: false,
                passes_security_gate: true,
                passes_movability_gate: true,
            },
            JoinClause {
                // Generates candidate {3, 4}; can push BOTH this
                // clause AND the {1,3} clause above (because
                // {3} ⊆ {3, 4}). The variant is therefore strictly
                // useful: it pushes more than the {3} variant does.
                clause_relids: bms(&[1, 3, 4]),
                pseudoconstant: false,
                passes_security_gate: true,
                passes_movability_gate: true,
            },
        ];
        let paths = emit_paths_model(&rel, &lateral, &clauses);
        // Plain + 2 surviving JoinParameterized: {3} and {3, 4}.
        assert_eq!(paths.len(), 3);
        assert_eq!(paths[1].required_outer, bms(&[3]));
        assert_eq!(paths[2].required_outer, bms(&[3, 4]));
    }

    /// Clause that resolves to lateral_relids alone is skipped — the
    /// Plain variant already covers it.
    #[test]
    fn clause_equal_to_lateral_is_skipped() {
        let rel = bms(&[1]);
        let lateral = bms(&[2]);
        let clauses = vec![JoinClause {
            // clause - rel = {2}, union with lateral = {2}, equal to
            // lateral.
            clause_relids: bms(&[1, 2]),
            pseudoconstant: false,
            passes_security_gate: true,
            passes_movability_gate: true,
        }];
        let paths = emit_paths_model(&rel, &lateral, &clauses);
        // Only the Plain variant.
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].kind, VariantKind::Plain);
    }
}
