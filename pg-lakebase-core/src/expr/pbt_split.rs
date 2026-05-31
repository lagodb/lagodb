//! Property tests: residual + pushed filter equivalence and no false negatives
//! over a Rust-only model of [`ClauseClassifier`](crate::expr::walker::ClauseClassifier) composition.

use std::collections::HashMap;

use proptest::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LeafKind {
    /// `column[id] = literal`-style comparison the AM evaluates exactly.
    ExactRowFilter { id: u32 },
    /// `column[id] = literal`-style comparison the AM only over-
    /// approximates (e.g. file-level statistics). Original stays in
    /// residual, pushed is the same expression with `ConservativePruning` semantics.
    ConservativePruning { id: u32 },
    /// Provider cannot evaluate this leaf at all (volatile function,
    /// foreign type, etc.).
    Unsupported { id: u32 },
    /// `column[id] IS NULL`. Modeled as exact.
    IsNull { id: u32 },
    /// `column[id] IS NOT NULL`. Modeled as exact.
    IsNotNull { id: u32 },
}

/// Random AND/OR/NOT tree over [`LeafKind`].
#[derive(Debug, Clone, PartialEq)]
enum Tree {
    Leaf(LeafKind),
    And(Vec<Tree>),
    Or(Vec<Tree>),
    Not(Box<Tree>),
}

/// Synthetic row: id → `Some(true) | Some(false) | None`. `None` represents
/// SQL NULL on that column.
type Row = HashMap<u32, Option<bool>>;

/// SQL three-valued logic: `Some(true)`, `Some(false)`, `None` (NULL /
/// unknown).
type Tvl = Option<bool>;

/// AND in SQL three-valued logic. Any FALSE → FALSE; else any NULL → NULL;
/// else TRUE.
fn and_tvl(values: &[Tvl]) -> Tvl {
    let mut any_unknown = false;
    for v in values {
        match v {
            Some(false) => return Some(false),
            Some(true) => continue,
            None => any_unknown = true,
        }
    }
    if any_unknown { None } else { Some(true) }
}

/// OR in SQL three-valued logic. Any TRUE → TRUE; else any NULL → NULL;
/// else FALSE.
fn or_tvl(values: &[Tvl]) -> Tvl {
    let mut any_unknown = false;
    for v in values {
        match v {
            Some(true) => return Some(true),
            Some(false) => continue,
            None => any_unknown = true,
        }
    }
    if any_unknown { None } else { Some(false) }
}

/// NOT in SQL three-valued logic. NOT NULL is NULL.
fn not_tvl(v: Tvl) -> Tvl {
    v.map(|b| !b)
}

/// Evaluate a leaf against a row.
///
/// `ExactRowFilter`, `ConservativePruning`, and `Unsupported` are all comparison-style leaves
/// whose truth is determined directly by the row's value at `id`:
/// `Some(true)` → TRUE, `Some(false)` → FALSE, `None` → UNKNOWN. The leaf
/// kinds differ only in their classification contract, not in their SQL
/// semantics — which is exactly what makes the split safe to study
/// algebraically.
///
/// `IsNull` / `IsNotNull` are total: they always return `Some(_)` and
/// never `None`, mirroring the SQL semantics that null tests cannot
/// themselves be UNKNOWN.
fn eval_leaf(leaf: &LeafKind, row: &Row) -> Tvl {
    match leaf {
        LeafKind::ExactRowFilter { id }
        | LeafKind::ConservativePruning { id }
        | LeafKind::Unsupported { id } => row.get(id).copied().unwrap_or(None),
        LeafKind::IsNull { id } => {
            Some(row.get(id).copied().unwrap_or(None).is_none())
        }
        LeafKind::IsNotNull { id } => {
            Some(row.get(id).copied().unwrap_or(None).is_some())
        }
    }
}

/// Evaluate a tree against a row in SQL three-valued logic.
fn eval(tree: &Tree, row: &Row) -> Tvl {
    match tree {
        Tree::Leaf(l) => eval_leaf(l, row),
        Tree::And(children) => {
            let vs: Vec<Tvl> = children.iter().map(|c| eval(c, row)).collect();
            and_tvl(&vs)
        }
        Tree::Or(children) => {
            let vs: Vec<Tvl> = children.iter().map(|c| eval(c, row)).collect();
            or_tvl(&vs)
        }
        Tree::Not(inner) => not_tvl(eval(inner, row)),
    }
}

/// Evaluate an `Option<Tree>`. Absent residual / pushed is treated as TRUE
/// (no filtering).
fn eval_opt(tree: &Option<Tree>, row: &Row) -> Tvl {
    match tree {
        Some(t) => eval(t, row),
        None => Some(true),
    }
}

/// SQL row passes a qual when it evaluates to `Some(true)`. NULL and FALSE
/// drop the row.
#[inline]
fn passes(v: Tvl) -> bool {
    matches!(v, Some(true))
}

// =============================================================================
// Composed classification for the model.
//
// Mirrors `ClauseClassification` in `expr::walker` but works over `Tree`
// rather than `*mut pg_sys::Expr`. The split-stage output is two trees:
// `residual` (PG-side `plan.qual`) and `pushed` (AM-side filter). Either
// may be absent.
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelPushdownContract {
    ExactRowFilter,
    ConservativePruning,
}

/// Per-clause classification result, mirroring
/// [`crate::expr::walker::ClauseClassification`].
///
/// Carries owned `Tree`s so the split machinery can return a fresh tree
/// for AND-of-pushable-parts / OR-widening cases without juggling lifetimes.
#[derive(Debug, Clone)]
enum ModelClass {
    /// The clause as a whole is pushable (possibly as multiple independent parts).
    Pushable {
        parts: Vec<ModelPushedPart>,
        residual: Option<Tree>,
    },
    /// OR-ConservativePruning widening. Composed pushed always carries
    /// `ConservativePruning` semantics.
    PartialPush { pushed: Tree, residual: Tree },
    /// Fully unsupported. Stays in residual unchanged.
    Unsupported { residual: Tree },
}

#[derive(Debug, Clone)]
struct ModelPushedPart {
    contract: ModelPushdownContract,
    tree: Tree,
}

fn model_pushable_one(
    contract: ModelPushdownContract,
    tree: Tree,
    residual: Option<Tree>,
) -> ModelClass {
    ModelClass::Pushable {
        parts: vec![ModelPushedPart { contract, tree }],
        residual,
    }
}

fn absorb_model_child(
    child: ModelClass,
    pushed_parts: &mut Vec<ModelPushedPart>,
    residual_parts: &mut Vec<Tree>,
    all_exact_row_filter_pushable: &mut bool,
    any_pushed: &mut bool,
) {
    match child {
        ModelClass::Pushable { parts, residual } => {
            *any_pushed = true;
            for part in parts {
                if part.contract != ModelPushdownContract::ExactRowFilter {
                    *all_exact_row_filter_pushable = false;
                }
                pushed_parts.push(part);
            }
            if let Some(r) = residual {
                residual_parts.push(r);
                *all_exact_row_filter_pushable = false;
            }
        }
        ModelClass::PartialPush { pushed, residual } => {
            pushed_parts.push(ModelPushedPart {
                contract: ModelPushdownContract::ConservativePruning,
                tree: pushed,
            });
            residual_parts.push(residual);
            *any_pushed = true;
            *all_exact_row_filter_pushable = false;
        }
        ModelClass::Unsupported { residual } => {
            residual_parts.push(residual);
            *all_exact_row_filter_pushable = false;
        }
    }
}

/// Classify a leaf kind (the model's analogue of the provider's
/// `classify_qual` callback). `IsNull` / `IsNotNull` are exact.
fn classify_leaf(leaf: LeafKind) -> ModelClass {
    let original = Tree::Leaf(leaf);
    match leaf {
        LeafKind::ExactRowFilter { .. }
        | LeafKind::IsNull { .. }
        | LeafKind::IsNotNull { .. } => model_pushable_one(
            ModelPushdownContract::ExactRowFilter,
            original.clone(),
            None,
        ),
        LeafKind::ConservativePruning { .. } => model_pushable_one(
            ModelPushdownContract::ConservativePruning,
            original.clone(),
            Some(original),
        ),
        LeafKind::Unsupported { .. } => {
            ModelClass::Unsupported { residual: original }
        }
    }
}

/// Apply the AND/OR/NOT composition rules verbatim from
/// [`crate::expr::walker::ClauseClassifier`] over a [`Tree`]. The output is
/// a [`ModelClass`] for the root.
fn classify(tree: &Tree) -> ModelClass {
    match tree {
        Tree::Leaf(l) => classify_leaf(*l),
        Tree::And(children) => classify_and(tree, children),
        Tree::Or(children) => classify_or(tree, children),
        Tree::Not(inner) => classify_not(tree, inner),
    }
}

fn classify_and(original: &Tree, children: &[Tree]) -> ModelClass {
    if children.is_empty() {
        return ModelClass::Unsupported {
            residual: original.clone(),
        };
    }

    let mut pushed_parts: Vec<ModelPushedPart> = Vec::with_capacity(children.len());
    let mut residual_parts: Vec<Tree> = Vec::with_capacity(children.len());
    let mut all_exact_row_filter_pushable = true;
    let mut any_pushed = false;

    for child in children {
        absorb_model_child(
            classify(child),
            &mut pushed_parts,
            &mut residual_parts,
            &mut all_exact_row_filter_pushable,
            &mut any_pushed,
        );
    }

    if !any_pushed {
        // Every child unsupported -> the whole AND is unsupported. Keep
        // the original as residual.
        return ModelClass::Unsupported {
            residual: original.clone(),
        };
    }

    if all_exact_row_filter_pushable && residual_parts.is_empty() {
        let trees: Vec<Tree> = pushed_parts.iter().map(|p| p.tree.clone()).collect();
        let pushed = make_and(trees);
        return model_pushable_one(
            ModelPushdownContract::ExactRowFilter,
            pushed,
            None,
        );
    }

    let residual = if residual_parts.is_empty() {
        None
    } else {
        Some(make_and(residual_parts))
    };

    ModelClass::Pushable {
        parts: pushed_parts,
        residual,
    }
}

fn classify_or(original: &Tree, children: &[Tree]) -> ModelClass {
    if children.is_empty() {
        return ModelClass::Unsupported {
            residual: original.clone(),
        };
    }

    let child_results: Vec<ModelClass> = children.iter().map(classify).collect();

    let all_exact_row_filter = child_results.iter().all(|c| {
        matches!(
            c,
            ModelClass::Pushable {
                parts,
                residual,
                ..
            } if residual.is_none()
                && !parts.is_empty()
                && parts
                    .iter()
                    .all(|p| p.contract == ModelPushdownContract::ExactRowFilter)
        )
    });
    if all_exact_row_filter {
        let mut branch_parts: Vec<Tree> = Vec::with_capacity(child_results.len());
        for c in child_results {
            if let ModelClass::Pushable { parts, .. } = c {
                let trees: Vec<Tree> = parts.iter().map(|p| p.tree.clone()).collect();
                branch_parts.push(make_and(trees));
            }
        }
        let pushed = make_or(branch_parts);
        return model_pushable_one(
            ModelPushdownContract::ExactRowFilter,
            pushed,
            None,
        );
    }

    let mut widenings: Vec<Tree> = Vec::with_capacity(child_results.len());
    for c in &child_results {
        match c {
            ModelClass::Pushable { parts, .. } => {
                let trees: Vec<Tree> = parts.iter().map(|p| p.tree.clone()).collect();
                widenings.push(make_and(trees));
            }
            ModelClass::PartialPush { pushed, .. } => widenings.push(pushed.clone()),
            ModelClass::Unsupported { .. } => {
                return ModelClass::Unsupported {
                    residual: original.clone(),
                };
            }
        }
    }

    let pushed = make_or(widenings);
    ModelClass::PartialPush {
        pushed,
        residual: original.clone(),
    }
}

fn classify_not(original: &Tree, inner: &Tree) -> ModelClass {
    match classify(inner) {
        ModelClass::Pushable { parts, residual }
            if residual.is_none()
                && parts.len() == 1
                && parts[0].contract == ModelPushdownContract::ExactRowFilter =>
        {
            model_pushable_one(
                ModelPushdownContract::ExactRowFilter,
                Tree::Not(Box::new(parts[0].tree.clone())),
                None,
            )
        }
        // ConservativePruning / PartialPush / Unsupported: residual only.
        _ => ModelClass::Unsupported {
            residual: original.clone(),
        },
    }
}

/// Build an AND of `parts`. Single-element lists collapse to the part
/// itself, mirroring how `classify_and` in `expr::walker` avoids wrapping
/// a single child in a redundant `BoolExpr`.
fn make_and(mut parts: Vec<Tree>) -> Tree {
    if parts.len() == 1 {
        parts.remove(0)
    } else {
        Tree::And(parts)
    }
}

fn make_or(mut parts: Vec<Tree>) -> Tree {
    if parts.len() == 1 {
        parts.remove(0)
    } else {
        Tree::Or(parts)
    }
}

/// Top-level split: classify the root and project to `(residual, pushed parts)`.
fn split_model(tree: &Tree) -> (Option<Tree>, Vec<Tree>) {
    match classify(tree) {
        ModelClass::Pushable { parts, residual } => {
            let pushed: Vec<Tree> = parts.into_iter().map(|p| p.tree).collect();
            (residual, pushed)
        }
        ModelClass::PartialPush { pushed, residual } => {
            (Some(residual), vec![pushed])
        }
        ModelClass::Unsupported { residual } => (Some(residual), Vec::new()),
    }
}

fn passes_all_pushed(parts: &[Tree], row: &Row) -> bool {
    parts
        .iter()
        .all(|p| passes(eval_opt(&Some(p.clone()), row)))
}

// =============================================================================
// Generators.
// =============================================================================

/// Bound on the number of distinct column ids so rows stay small enough to
/// hit interesting cases (matching pushed and residual leaves) within
/// proptest's default case budget.
const ID_RANGE: u32 = 5;

/// Generator for [`LeafKind`].
fn arb_leaf() -> impl Strategy<Value = LeafKind> {
    let id = 0u32..ID_RANGE;
    prop_oneof![
        id.clone().prop_map(|id| LeafKind::ExactRowFilter { id }),
        id.clone()
            .prop_map(|id| LeafKind::ConservativePruning { id }),
        id.clone().prop_map(|id| LeafKind::Unsupported { id }),
        id.clone().prop_map(|id| LeafKind::IsNull { id }),
        id.prop_map(|id| LeafKind::IsNotNull { id }),
    ]
}

/// Generator for [`Tree`] with bounded depth. `prop_recursive` caps the
/// total number of nodes so we don't blow up the case budget on deep trees.
fn arb_tree() -> impl Strategy<Value = Tree> {
    let leaf = arb_leaf().prop_map(Tree::Leaf);
    leaf.prop_recursive(
        4,  // max depth
        32, // total leaves budget per case
        4,  // max children per AND/OR
        |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 1..=4).prop_map(Tree::And),
                proptest::collection::vec(inner.clone(), 1..=4).prop_map(Tree::Or),
                inner.prop_map(|t| Tree::Not(Box::new(t))),
            ]
        },
    )
}

/// Generator for a [`Row`] over `0..ID_RANGE`, sampling each id from
/// `{Some(true), Some(false), None}` independently.
fn arb_row() -> impl Strategy<Value = Row> {
    let value = prop_oneof![Just(Some(true)), Just(Some(false)), Just(None::<bool>),];
    proptest::collection::hash_map(0u32..ID_RANGE, value, ID_RANGE as usize)
}

// =============================================================================
// Property-based tests.
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn pbt1_residual_pushed_equivalence(
        tree in arb_tree(),
        rows in proptest::collection::vec(arb_row(), 1..16),
    ) {
        let (residual, pushed_parts) = split_model(&tree);

        for row in &rows {
            let original = eval(&tree, row);
            let residual_v = eval_opt(&residual, row);
            let pushed_pass = passes_all_pushed(&pushed_parts, row);

            let combined_pass = passes(residual_v) && pushed_pass;
            prop_assert_eq!(
                passes(original),
                combined_pass,
                "filter equivalence broken: original={:?} residual={:?} pushed={:?} \
                 tree={:?} row={:?}",
                original,
                residual_v,
                pushed_parts,
                tree,
                row
            );

            if let Some(true) = original {
                prop_assert!(
                    pushed_pass,
                    "pushed produced false negative: original={:?} pushed={:?} \
                     tree={:?} row={:?}",
                    original,
                    pushed_parts,
                    tree,
                    row
                );
            }
        }
    }
}

#[cfg(test)]
mod model_smoke {
    use super::*;

    fn row(pairs: &[(u32, Option<bool>)]) -> Row {
        pairs.iter().copied().collect()
    }

    #[test]
    fn exact_row_filter_leaf_alone_filters_via_pushed() {
        let t = Tree::Leaf(LeafKind::ExactRowFilter { id: 0 });
        let (residual, pushed) = split_model(&t);
        // ExactRowFilter leaf: residual omits the leaf, pushed is the leaf.
        assert!(residual.is_none());
        assert_eq!(pushed.len(), 1);
        assert!(matches!(
            pushed[0],
            Tree::Leaf(LeafKind::ExactRowFilter { id: 0 })
        ));

        // Filter equivalence at runtime: pass(residual=TRUE) ∧ pass(all pushed)
        // must equal pass(original=leaf).
        for v in [Some(true), Some(false), None] {
            let r = row(&[(0, v)]);
            let original = eval(&t, &r);
            let combined =
                passes(eval_opt(&residual, &r)) && passes_all_pushed(&pushed, &r);
            assert_eq!(
                passes(original),
                combined,
                "ExactRowFilter leaf filter equivalence failed at v={:?}",
                v
            );
        }
    }

    #[test]
    fn unsupported_leaf_alone_stays_in_residual() {
        let t = Tree::Leaf(LeafKind::Unsupported { id: 1 });
        let (residual, pushed) = split_model(&t);
        assert!(residual.is_some());
        assert!(pushed.is_empty());
    }

    #[test]
    fn and_partial_pushdown_splits() {
        // a = 1 (ExactRowFilter) AND unsupported(b) -> pushed = a=1 ; residual = unsupported(b)
        let t = Tree::And(vec![
            Tree::Leaf(LeafKind::ExactRowFilter { id: 0 }),
            Tree::Leaf(LeafKind::Unsupported { id: 1 }),
        ]);
        let (residual, pushed) = split_model(&t);
        assert_eq!(pushed.len(), 1);
        assert!(matches!(
            pushed[0],
            Tree::Leaf(LeafKind::ExactRowFilter { id: 0 })
        ));
        assert!(matches!(
            residual,
            Some(Tree::Leaf(LeafKind::Unsupported { id: 1 }))
        ));
    }

    #[test]
    fn and_exact_and_conservative_keeps_separate_parts() {
        let t = Tree::And(vec![
            Tree::Leaf(LeafKind::ExactRowFilter { id: 0 }),
            Tree::Leaf(LeafKind::ConservativePruning { id: 1 }),
        ]);
        let (residual, pushed) = split_model(&t);
        assert_eq!(pushed.len(), 2);
        assert!(matches!(
            pushed[0],
            Tree::Leaf(LeafKind::ExactRowFilter { id: 0 })
        ));
        assert!(matches!(
            pushed[1],
            Tree::Leaf(LeafKind::ConservativePruning { id: 1 })
        ));
        assert!(matches!(
            residual,
            Some(Tree::Leaf(LeafKind::ConservativePruning { id: 1 }))
        ));
    }

    #[test]
    fn or_with_unsupported_branch_does_not_push() {
        // (ExactRowFilter a=1) OR (Unsupported b)  -> entire OR stays in residual.
        let t = Tree::Or(vec![
            Tree::Leaf(LeafKind::ExactRowFilter { id: 0 }),
            Tree::Leaf(LeafKind::Unsupported { id: 1 }),
        ]);
        let (residual, pushed) = split_model(&t);
        assert!(residual.is_some());
        assert!(pushed.is_empty());
    }

    #[test]
    fn or_all_exact_row_filter_pushes() {
        let t = Tree::Or(vec![
            Tree::Leaf(LeafKind::ExactRowFilter { id: 0 }),
            Tree::Leaf(LeafKind::ExactRowFilter { id: 1 }),
        ]);
        let (residual, pushed) = split_model(&t);
        assert!(residual.is_none());
        assert_eq!(pushed.len(), 1);
    }

    #[test]
    fn not_exact_row_filter_pushes() {
        let t = Tree::Not(Box::new(Tree::Leaf(LeafKind::ExactRowFilter { id: 0 })));
        let (residual, pushed) = split_model(&t);
        assert!(residual.is_none());
        assert_eq!(pushed.len(), 1);
        assert!(matches!(pushed[0], Tree::Not(_)));
    }

    #[test]
    fn not_conservative_pruning_does_not_push() {
        let t = Tree::Not(Box::new(Tree::Leaf(LeafKind::ConservativePruning {
            id: 0,
        })));
        let (residual, pushed) = split_model(&t);
        assert!(residual.is_some());
        assert!(pushed.is_empty());
    }

    #[test]
    fn three_valued_logic_and() {
        assert_eq!(and_tvl(&[Some(true), Some(true)]), Some(true));
        assert_eq!(and_tvl(&[Some(true), Some(false)]), Some(false));
        assert_eq!(and_tvl(&[Some(true), None]), None);
        assert_eq!(and_tvl(&[Some(false), None]), Some(false));
        assert_eq!(and_tvl(&[None, None]), None);
        assert_eq!(and_tvl(&[]), Some(true));
    }

    #[test]
    fn three_valued_logic_or() {
        assert_eq!(or_tvl(&[Some(false), Some(false)]), Some(false));
        assert_eq!(or_tvl(&[Some(true), Some(false)]), Some(true));
        assert_eq!(or_tvl(&[None, Some(false)]), None);
        assert_eq!(or_tvl(&[None, Some(true)]), Some(true));
        assert_eq!(or_tvl(&[None, None]), None);
        assert_eq!(or_tvl(&[]), Some(false));
    }
}
