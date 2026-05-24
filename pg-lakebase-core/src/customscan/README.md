This is the revised design. The key correction is: the planner phase must not freeze a native predicate into the plan. The plan tree stores only PG Expr nodes and copyObject-safe metadata. Begin/ReScan translates those PG Expr nodes into provider-native predicates at runtime.

**Status**

Design only. The module list below is the intended implementation
layout; `pg-lakebase-core/src/customscan/` currently contains this
README but no Rust implementation. Any implementation task must
verify the generated `pgrx::pg_sys` bindings for the PG17 functions
named here before using them directly.

**Goal**

Build filter pushdown as a generic CustomScan framework capability in `pg-lakebase-core`, without binding the design to Iceberg.

```text
SQL WHERE
  -> PostgreSQL planner
  -> set_rel_pathlist_hook
       path-stage classify into pushed / residual / recheck
       enumerate plain and parameterized CustomPath variants
  -> add_path(CustomPath)
  -> PG selects path
  -> PlanCustomPath
       plan-stage re-classify final scan_clauses
       unwrap RestrictInfo to bare Expr
  -> CustomScan plan
       plan.qual       = residual quals
       custom_exprs    = pushed/recheck PG Expr
       custom_private  = copyObject-safe metadata
  -> PG replace_nestloop_params and set_plan_references
  -> executor Begin/ReScan
       translate PG Expr -> AM native predicate
       snapshot/params/file pruning
       open scan cursor
  -> ExecScan
       accessMtd  = core next_slot wrapper -> provider.next_slot
       recheckMtd = core recheck exact pushed quals
  -> PG residual qual / projection
```

The provider is `pg-iceberg-am`, future `pg-hudi-am`, or future `pg-delta-am`. It is not the PostgreSQL TableAM scan callback.

**Confirmed Facts**

1. A normal PostgreSQL TableAM scan does not receive ordinary WHERE quals. The current core `AmScanSession::scan_begin` only receives `OwnedScanKeys`; ordinary filters are still evaluated by the upper executor layer.

2. `CustomScan` has `custom_exprs`, `custom_private`, and `custom_scan_tlist`. PostgreSQL comments state that these fields have semantics similar to FDW `fdw_exprs`, `fdw_private`, and `fdw_scan_tlist`.

3. PostgreSQL applies setrefs and nestloop param rewrite to `scan.plan.qual` and `custom_exprs`. Therefore, anything that is still a PG Expr must be stored in one of these two fields. It must not be hidden inside JSON or a private blob.

4. `ExecCustomScan` does not automatically execute `plan.qual`. It only calls the provider's `ExecCustomScan` callback. To reuse PostgreSQL residual qual, projection, and EPQ behavior, the provider or core should call `ExecScan`.

5. `ExecScan` executes `node->ps.qual`, which is the initialized form of `scan.plan.qual`.

6. `CustomScan` does not have the FDW `fdw_recheck_quals` field. If we want FDW-like recheck semantics, core must store recheck expressions in `custom_exprs` and initialize/evaluate them in executor state with `ExecInitQual` and `ExecQual`.

7. PG17 `create_customscan_plan` does not call `extract_actual_clauses`
   on the scan clauses before handing them to the provider. It only
   runs `order_qual_clauses`. The `clauses` argument received by
   `PlanCustomPath` is therefore still a `List<RestrictInfo>`. This
   mirrors the FDW plan callback behavior in
   `create_foreignscan_plan` (which also passes `RestrictInfo` to
   `GetForeignPlan`); both differ from the built-in scan plan
   builders, which all unwrap to bare `Expr` first. Core unwraps
   `RestrictInfo` to bare `Expr` before writing into `plan.qual` and
   `custom_exprs`. After the provider returns the `CustomScan` node,
   PG runs `replace_nestloop_params` over both `plan.qual` and
   `custom_exprs`, and later `set_plan_references` will rewrite
   `Var`s in those fields — so anything stored there must be
   PG-Expr-shaped and copyObject-safe.

8. PG17 `ExecCustomScan` calls `CHECK_FOR_INTERRUPTS()` once per
   tuple, and so does `ExecScanFetch`. Long-running IO inside
   `provider.next_slot` must call `CHECK_FOR_INTERRUPTS()` itself.
   `nodeForeignscan.c::ForeignNext` switches into
   `econtext->ecxt_per_tuple_memory` before invoking the iterate
   callback; `core.next_slot_wrapper`, the access callback core
   passes to `ExecScan`, does the same for `provider.next_slot`.
   PG17 `ExecScan` resets the expression context at the start of
   each scan cycle and again after a tuple fails the scan qual;
   `ExecQual` and `ExecProject` do not reset it themselves.
   Therefore per-row scratch can live in the per-tuple context, while
   cursor state, decoder buffers, and any value that must survive
   into the next provider call must live in provider state or another
   longer-lived context.

9. PG17 nested-loop execution (`nodeNestloop.c`) sets
   `innerPlan->chgParam` and unconditionally calls
   `ExecReScan(innerPlan)` for each new outer tuple that has
   nestParams. So `ReScanCustomScan` is always invoked, but core can
   inspect `node->ss.ps.chgParam` to decide whether re-translating
   the predicate and re-pruning files is actually needed.

**Core Modules**

```text
pg-lakebase-core/src/customscan/
  mod.rs
  hook.rs
  provider.rs
  builder.rs
  private.rs
  state.rs
  exec.rs
  explain.rs

pg-lakebase-core/src/expr/
  mod.rs
  nodes.rs
  walker.rs
  translator.rs
  split.rs
  runtime_params.rs
```

`customscan` connects to the PostgreSQL planner and executor.

`expr` owns PG Expr typed wrappers, walkers/folders, pushdown
splitting, and runtime-side parameter resolution.
`expr/runtime_params.rs` is named to make clear that it operates on
`ParamListInfo` / `ParamExecData` during `Begin/ReScan`, not on the
plan-time `Param` node which is exposed as `PgParamRef` in
`expr/nodes.rs`.

**ParadeDB References**

Borrow these ideas:

- A Rust trait describes each CustomScan provider.
- Method tables are cached with a long enough lifetime to satisfy PostgreSQL static callback requirements.
- The `#[repr(C)]` state wrapper stores `CustomScanState` as the first field.
- Builders encapsulate `CustomPath`, `CustomScan`, and `CustomScanState` construction.

Do not copy these parts:

- Do not copy ParadeDB's per-type hook chaining. Core should use a
  single registry/router, similar to the existing hook style, so
  multiple AM providers do not override each other. Relation matching
  is expected to be unique by relation AM OID. If more than one
  registered provider returns true from `supports_relation()` for the
  same relation in v1, treat that as a provider-registration bug and
  fail closed rather than picking whichever provider happened to be
  registered first.
- Do not copy ParadeDB's search-specific expression IR. v1 should not require a neutral IR.

**Provider Trait**

The corrected point is that the planner phase must not put the
provider's native predicate type into the plan. The native predicate
type is only used inside `Begin/ReScan/next_slot`, lives entirely in
runtime state, and never needs to appear on the trait of the plan-time
provider object. So the trait does not have a `type Predicate`
associated type at all — providers carry their predicate type inside
`Self::State` and through the runtime `PgPredicateTranslator` impl
they own.

```rust
pub trait LakebaseCustomScanProvider: 'static {
    const NAME: &'static CStr;

    type PrivateData: CustomScanPrivate;
    type State;

    fn supports_relation(ctx: &RelPathContext<'_>) -> bool;

    fn classify_qual(
        ctx: &PlanTranslateContext<'_>,
        expr: PgExprRef<'_>,
    ) -> QualPushdownDecision;

    /// Build one CustomPath variant, given the enumeration decisions
    /// already made by core (which `outer_relids` to use, which
    /// pushable subset of `baserestrictinfo` + `ppi_clauses` applies
    /// to this variant). Core calls this once per variant: once for
    /// the plain (no-join-qual) path and once per surviving
    /// `outer_relids` derived from `baserel->joininfo`.
    fn create_path(
        ctx: &RelPathContext<'_>,
        variant: &PathVariant<'_>,
        builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>>;

    fn create_state(ctx: CreateStateContext<Self>) -> Self::State;

    fn begin(ctx: BeginContext<Self>) -> Result<()>;
    fn next_slot(ctx: NextSlotContext<Self>) -> Result<bool>;
    fn rescan(ctx: ReScanContext<Self>) -> Result<()>;
    fn end(ctx: EndContext<Self>) -> Result<()>;
}

/// What core has already decided for this CustomPath variant.
/// The provider only fills in cost / `custom_private`; it does not
/// re-enumerate or re-classify.
pub struct PathVariant<'a> {
    /// Distinguishes the plain (no-join-qual) variant from each
    /// parameterized variant derived from `baserel->joininfo`. Use
    /// this field — not `param_info.is_some()` — to decide which
    /// variant is being built. The plain variant is *also*
    /// parameterized whenever `baserel->lateral_relids` is non-empty
    /// (see `param_info` below), so `param_info.is_some()` does NOT
    /// imply `Kind::JoinParameterized`.
    pub kind: PathVariantKind,

    /// `Some(_)` whenever `required_outer` is non-empty by
    /// `!bms_is_empty(required_outer)`, regardless of whether the
    /// source is `lateral_relids` (plain variant) or `lateral_relids +
    /// joininfo-derived rels` (join-parameterized variant). The
    /// contained `ParamPathInfo` has been obtained via
    /// `get_baserel_parampathinfo` so that `ppi_rows` and
    /// `ppi_clauses` are already canonicalized. `None` only when
    /// `bms_is_empty(required_outer)` is true (i.e. the plain variant
    /// on a rel with no `lateral_relids`). Do not test pointer
    /// non-nullness of the bitmap to decide this.
    pub param_info: Option<&'a ParamPathInfo>,

    /// A PG `Relids` value: a nullable `Bitmapset *` where `NULL` and
    /// empty both mean "no required outer rels" for `bms_is_empty`.
    /// It is `bms_copy(baserel->lateral_relids)` plus, for
    /// join-parameterized variants, the additional outer rels that
    /// this variant pushes.
    pub required_outer: Relids,

    /// Pre-classified split of `baserestrictinfo` plus any
    /// planner-supplied `ppi_clauses` that belong to this variant
    /// into pushed / residual / recheck. Use `kind`, not
    /// `param_info.is_some()`, to decide whether this is one of the
    /// join-parameterized variants that core intentionally enumerated
    /// for AM-side evaluation of outer-driven predicates. A plain
    /// lateral-only path may still
    /// have `param_info`, and PG may still attach `ppi_clauses`; those
    /// clauses are part of the final scan-clause split but do not make
    /// the variant `JoinParameterized`.
    /// All safety gates (`pseudoconstant`,
    /// `restriction_is_securely_promotable`,
    /// `join_clause_is_movable_to`) have already been applied by
    /// core; the provider sees only clauses that survived.
    pub split: &'a PlanPushdownSplit,
}

pub enum PathVariantKind {
    /// The "no join qual" path. `required_outer` is exactly
    /// `bms_copy(baserel->lateral_relids)` (possibly NULL/empty).
    Plain,
    /// One variant per surviving `outer_relids` derived from
    /// `baserel->joininfo`. `required_outer` is
    /// `lateral_relids ∪ (clause_relids - baserel->relids)` for
    /// the safe outer-driven clauses in this group.
    JoinParameterized,
}
```

`classify_qual` only answers whether a PG Expr can be pushed down
safely and what semantic guarantee it has. The real native predicate
is built by the runtime translator in `Begin/ReScan`.

**Required outer / param_info convention**

Use PostgreSQL's bitmapset convention consistently: `Relids` is a
nullable `Bitmapset *`, and emptiness is tested with
`bms_is_empty(required_outer)`, not with raw pointer non-nullness.
PG17 `get_baserel_parampathinfo(root, baserel, required_outer)`
returns `NULL` when `bms_is_empty(required_outer)` is true, and
returns/caches a `ParamPathInfo` otherwise. The CustomScan builder may
therefore call it directly, but it must preserve the invariant:

```text
path.param_info == NULL iff bms_is_empty(required_outer)
```

This is the single source of truth for the plain/no-lateral case, the
plain/lateral-only case, and join-parameterized variants.

Path enumeration is owned by core, not by the provider. Concretely,
the `set_rel_pathlist_hook` router (see "Planner Phase" below) calls
`create_path` once per variant it has already decided to emit:

1. The `PathVariantKind::Plain` variant. `required_outer =
   bms_copy(baserel->lateral_relids)`. `param_info` is `None` when
   `lateral_relids` is empty (truly unparameterized) and
   `Some(get_baserel_parampathinfo(root, baserel, lateral_relids))`
   when it is non-empty (parameterized purely by lateral rels, no
   outer-driven predicate added; matches the PG comment at
   `indxpath.c:223`).
2. One `PathVariantKind::JoinParameterized` variant per surviving
   `outer_relids` derived from `baserel->joininfo` (after
   `join_clause_is_movable_to` and
   `restriction_is_securely_promotable` filtering, plus
   deduplication). `param_info` is always `Some(_)` for these.

Decide which variant a `create_path` call refers to by reading
`variant.kind`. Do not branch on `variant.param_info.is_some()` —
that flag answers a different question ("does this path have
non-empty `required_outer`?") and conflates the two plain-variant
sub-cases.

This split keeps gating logic in one place (core) while leaving
provider-specific cost shaping and `custom_private` layout to the
provider. A provider that returns `None` from `create_path` for a
particular variant declines that variant — core simply skips
`add_path()` for it; other variants are unaffected.

**CustomPath construction contract**

`create_path` returns a `CustomPathPlan` that the core builder turns
into a `CustomPath`. Path is a PG node and the planner compares paths
on its standard fields, so v1 fixes the following contract:

- `path.parent`: the `RelOptInfo` for the scan relation.
- `path.pathtarget`: `parent->reltarget`. v1 does not synthesize a
  custom target list, so the default rel target is correct and lets
  PG skip a projection node when possible. This does not mean v1
  disables column pruning: PG still builds `scan.plan.targetlist`
  from the selected path target, and that list is typically already
  pruned by upper plan nodes even though `custom_scan_tlist` remains
  `NIL`.
- `path.param_info`: the result of
  `get_baserel_parampathinfo(root, baserel, required_outer)`. In
  PG17 that function returns `NULL` when
  `bms_is_empty(required_outer)` is true, so the rule is not
  "bitmap pointer is non-null"; the rule is
  `param_info == NULL iff bms_is_empty(required_outer)`.
  v1 generates two kinds of paths:
  - one plain (no-join-qual) CustomPath. `required_outer =
    bms_copy(baserel->lateral_relids)`. When `lateral_relids` is
    empty this collapses to the truly unparameterized case
    (`required_outer = NULL`, `param_info = NULL`); when
    `lateral_relids` is non-empty the path is still parameterized
    (`param_info != NULL`) even though no outer-driven predicate is
    added, exactly as `indxpath.c` notes at line 223
    ("unparameterized so far as the indexquals are concerned").
  - one parameterized CustomPath for each distinct `required_outer`
    relid set that lets at least one additional safe outer-driven
    clause from `baserel->joininfo` become an AM-side scan predicate.
    See "Parameterized paths" below for how `required_outer` is
    enumerated.
- `path.parallel_aware = false`, `path.parallel_safe = false`,
  `path.parallel_workers = 0`. v1 does not implement the parallel
  CustomScan callbacks (`EstimateDSMCustomScan`,
  `InitializeDSMCustomScan`, `InitializeWorkerCustomScan`,
  `ReInitializeDSMCustomScan`, `ShutdownCustomScan`), so the path
  must opt out of parallelism. Declaring `parallel_safe = true`
  without those callbacks is a correctness bug — the scan would
  otherwise be eligible to appear under a `Gather`. Parameterized
  variants are likewise non-parallel; this matches what
  `indxpath.c` does (only the `outer_relids == NULL` branch is
  considered for parallel index paths).
- `path.rows`:
  - Unparameterized: `parent->rows`. PG already reduced this by the
    selectivity of the entire `baserestrictinfo` in
    `set_baserel_size_estimates` (`costsize.c:5247`), so the path
    must not multiply by pushed-quals selectivity again.
  - Parameterized: `param_info->ppi_rows`. PG already reduced this
    by both `baserestrictinfo` and the movable join clauses captured
    in `ppi_clauses` via `get_parameterized_baserel_size`
    (`costsize.c:5286`), so again no further selectivity adjustment.
  In short: `path.rows = param_info ? ppi_rows : parent->rows`.
  Pruning savings are not a change in output cardinality — every
  row that survives PG's qual evaluation must still be produced —
  so they belong in `startup_cost` / `total_cost`, never in
  `path.rows`. Adjusting `path.rows` here would double-count the
  pushed quals against PG's own estimate and mislead join ordering
  upstream.
- `path.startup_cost` / `path.total_cost`: model after PG17
  `cost_seqscan` (`costsize.c:284`). The key separation:
  - `path.rows` is *output* cardinality only; it must equal
    `param_info ? ppi_rows : parent->rows` and is not adjusted by
    pruning.
  - Disk cost scales with the number of *scanned* pages, not output
    rows: baseline `seq_page_cost * baserel->pages`.
  - Per-tuple qual-evaluation CPU cost scales with *scanned* tuples,
    not output rows: baseline `(cpu_tuple_cost + qpqual_cost.per_tuple)
    * baserel->tuples`.
  - Per-output-row cost (the projection / pathtarget evaluation)
    scales with `path->rows`: `pathtarget->cost.per_tuple *
    path->rows`, plus `pathtarget->cost.startup` once into
    `startup_cost`.
  Pruning savings show up as a smaller estimated *scanned* page and
  *scanned* tuple count — not as fewer output rows. Concretely the
  provider should expose its pruned estimates (`scanned_pages`,
  `scanned_tuples`) and substitute them for `baserel->pages` /
  `baserel->tuples` in the disk and per-tuple-CPU terms above; the
  projection term keeps using `path->rows` unchanged. For
  parameterized paths the row count input to the projection term is
  `ppi_rows`, so the cost naturally reflects the join-clause
  selectivity PG attributed to this parameterization while disk and
  per-tuple CPU continue to track the physical scan footprint. v1
  keeps the model simple: providers may add a small startup cost
  for opening Iceberg metadata. The numbers do not have to be
  precise; they only have to be lower than the TableAM seqscan path
  when pushdown actually wins, so PG picks the CustomPath under the
  right plan shape (in particular, lower than `seqscan` for the
  plain variant, and lower than the rescan-amortized seqscan for
  the parameterized variant). Conflating "rows skipped by pruning"
  with `path.rows` would discount qual-evaluation CPU at
  `path.rows` instead of `baserel->tuples`, dramatically
  underpricing the CustomPath against SeqScan on selective queries
  and producing plans that pick CustomScan even when SeqScan would
  be cheaper.
- `path.pathkeys`: `NIL` in v1. Providers do not promise any output
  ordering. If a provider can guarantee a sort order in the future,
  it can set this; until then, callers must not assume order.

`CustomPath`-specific fields:

- `flags = CUSTOMPATH_SUPPORT_PROJECTION` only (see
  "CustomPath / CustomScan Flags" below).
- `custom_paths = NIL` (no child paths in v1).
- `custom_private`: path-level metadata used to carry classification
  results into `PlanCustomPath` (e.g. the pre-classified
  `(RestrictInfo, decision)` map). This is path-private — it is
  separate from the `CustomScan.custom_private` that ends up in the
  plan tree, which has stricter copyObject-safe constraints.
- `custom_restrictinfo = NIL` for base relation scans. v1 does not
  replace joins, so this field — used by `create_scan_plan` only
  when `IS_JOIN_REL(rel)` — is unused.

Parameterized paths:

v1 generates parameterized CustomPaths so that the planner can pick
this scan as the inner side of a nestloop and evaluate safe
outer-driven clauses as AM-side scan predicates. This is still a
base-relation scan path: core does not create joinrel or upperrel
paths here. Without parameterized paths, PG has no reason to give us
`ppi_clauses` — `create_scan_plan` only appends them when
`best_path->param_info != NULL` (`createplan.c:597`). The
chgParam-gated `ReScanCustomScan` and `PARAM_EXEC` resolution
described later in this document are what turn each rescan into a
re-translation of the native predicate using the new outer-tuple
values.

Enumeration (modeled on `indxpath.c::create_index_paths`):

1. Always emit the plain (no-join-qual) variant first. This is the
   variant whose `outer_relids` does not include any rel reached
   through `baserel->joininfo`. `required_outer` is computed as
   `bms_copy(baserel->lateral_relids)`:
   - When `baserel->lateral_relids` is empty, `required_outer = NULL`
     and `param_info = NULL` — a true unparameterized path.
   - When `baserel->lateral_relids` is non-empty, the path is *not*
     unparameterized: `required_outer` is non-empty and
     `get_baserel_parampathinfo` produces a non-null `ParamPathInfo`
     even though no outer-driven predicate is added. This matches
     the PG comment at `indxpath.c:223` ("unparameterized so far as
     the indexquals are concerned") and the code at `indxpath.c:856`
     (`outer_relids = bms_copy(rel->lateral_relids)`).
2. For each `RestrictInfo rinfo` in `baserel->joininfo`:
   - **Pre-filter with PG's safety gates before considering AM-side
     evaluation.** A clause that fails any gate cannot be used in a
     parameterized scan predicate — early evaluation would change
     result semantics. PG's index path
     enforces both gates on join clauses: `match_join_clauses_to_index`
     filters by `join_clause_is_movable_to` (`indxpath.c:1995`) and
     then dispatches to the shared `match_clause_to_index`, which
     itself rejects via `restriction_is_securely_promotable`
     (`indxpath.c:2100`). CustomScan has to do the same:
     - `join_clause_is_movable_to(rinfo, baserel)`
       (`restrictinfo.c:553`, mirrored at `indxpath.c:1995`). This
       rejects outer-join clauses that would be moved into the
       non-nullable side, clauses where `baserel`'s Vars would be
       nulled by an intervening outer join, clauses that reference
       rels with LATERAL references back to `baserel`, and
       `is_clone` outer-join variants.
     - `restriction_is_securely_promotable(rinfo, baserel)`
       (`restrictinfo.c:425`, mirrored at `indxpath.c:2100`).
       Evaluating such a clause inside the AM is functionally
       equivalent to evaluating it early at the inner-rel level, so
       a clause whose `security_level` exceeds
       `baserel->baserestrict_min_security` and which is not
       leakproof cannot be used even if it is movable.
     Skip clauses that fail either gate outright — no path is
     generated for them.
   - For surviving clauses, build a candidate `outer_relids` set:
     start from `bms_copy(baserel->lateral_relids)` (always
     required), then add `rinfo->clause_relids - baserel->relids`
     for every join clause the provider can classify as pushable
     (`Pushable { Exact | InexactNoFalseNegative }`). Skip clauses
     that classify as `Unsupported` — including them in
     `outer_relids` would just produce a parameterized path without
     any extra AM-side predicate benefit.
   - Group clauses by the resulting `outer_relids`; each distinct
     group becomes one parameterized CustomPath.
   - Drop a candidate set if it is a strict superset of another
     candidate's set without enabling any additional pushable
     clause (matches `indxpath`'s deduplication intent).
3. For each surviving `outer_relids`, call
   `get_baserel_parampathinfo(root, baserel, outer_relids)` to get
   the canonical `ParamPathInfo`. PG itself walks `joininfo` and
   `generate_join_implied_equalities` to decide which clauses end
   up in `ppi_clauses`; the provider does not curate `ppi_clauses`
   directly. v1 then re-classifies the clauses that PG returned
   (`ppi_clauses`) against the same Exact / Inexact /
   Unsupported scheme used for `baserestrictinfo`, applying the
   same `join_clause_is_movable_to` and
   `restriction_is_securely_promotable` gates before pushdown
   classification (an implied-equality clause synthesized by
   `generate_join_implied_equalities` can carry a `security_level`
   inherited from a higher-security source clause, so the gate
   still matters even though the original `joininfo` was already
   filtered). Clauses that fail either gate are forced to residual.
   The classification is recorded on the path.
4. Cost the parameterized path against `ppi->ppi_rows`; produce the
   `CustomPath` and `add_path()` it.

Rules and caveats:

- Required asserts (per `relnode.c::get_baserel_parampathinfo`):
  `bms_is_subset(baserel->lateral_relids, required_outer)` and
  `!bms_overlap(baserel->relids, required_outer)`. Core's
  `CustomPathBuilder` enforces both before calling
  `get_baserel_parampathinfo`.
- `consider_parallel`: v1 does not produce a parallel variant of any
  parameterized path. Index paths only consider parallel when
  `outer_relids == NULL`; we adopt the same restriction.
- pseudoconstant clauses are skipped here too (path-stage rule),
  see "Pseudoconstant handling" in the Expr Walker section.
- `PlanCustomPath` will receive `scan_clauses` containing both
  `baserestrictinfo` and `ppi_clauses` (concatenated by
  `createplan.c:597` in PG17), still in `RestrictInfo` form. The
  same plan-stage classification, unwrap, and pseudoconstant skip
  applies uniformly to the whole list.
- `replace_nestloop_params` runs over `plan.qual` and `custom_exprs`
  after `PlanCustomPath` returns, rewriting outer-relation `Var`s
  into `PARAM_EXEC` `Param`s. The `column_refs` ordinal walker
  ignores those because it counts only scan-relation Vars (see Expr
  Walker section).

**Pushdown Semantics**

```rust
pub enum PredicateGuarantee {
    Exact,
    InexactNoFalseNegative,
}

pub enum QualPushdownDecision {
    Pushable {
        guarantee: PredicateGuarantee,
    },
    Unsupported,
}
```

Semantics:

`Exact`:

- The backend guarantees row-level SQL-equivalent semantics.
- The original expression is not placed in residual `plan.qual`.
- The original PG Expr is placed in the recheck section of
  `custom_exprs`. v1 lake tables do not implement
  `tuple_lock`/`tuple_fetch_row_version`, so they are not real
  participants in `EvalPlanQual` themselves. The recheck path is
  also not reachable in v1 via "sibling UPDATE/DELETE/MERGE drives
  EPQ and feeds substitution slots into this scan": PG17
  `preprocess_rowmarks` (`planner.c:2295`) adds a `PlanRowMark`
  for *every non-target base rel* whenever `commandType` is
  `CMD_UPDATE` / `CMD_DELETE` / `CMD_MERGE`, and the v1 path-stage
  rowmark gate refuses CustomPath whenever
  `get_plan_rowmark(root->rowMarks, rel->relid) != NULL`. So a lake
  rel that is joined into a sibling DML plan never gets a
  CustomScan in the first place, and the substitution-slot path
  through `execScan.c::ExecScanFetch` cannot enter our
  `recheckMtd`. The recheck `ExprState` is still initialized and
  wired into `recheckMtd` for two reasons: (1) defense in depth, so
  a future relaxation of the rowmark gate cannot silently regress
  to "exact pushed quals are skipped on recheck", and (2) future
  row-identity support, where a CustomScan may legitimately
  participate in EPQ. It is not run on the normal `next_slot` path;
  it is invoked only when `ExecScanFetch` enters the EPQ/recheck
  path. When invoked, core's `recheckMtd` always evaluates the
  recheck `ExprState` over the candidate slot.
- The expression is translated into a native predicate at runtime.

`InexactNoFalseNegative`:

- The backend only guarantees that no matching row is lost. This is safe for file, row-group, or page pruning.
- The original PG Expr must remain in residual `plan.qual`.
- The expression may also be placed in pushed expressions for pruning.
- The expression is not placed in exact recheck expressions.

`Unsupported`:

- The expression is not pushed down.
- The expression remains in residual `plan.qual`.

Composition rules:

```text
AND:
  Partial pushdown is allowed.
  Unsupported or inexact parts still remain in residual quals.

OR (Exact):
  The whole OR expression must be pushable as Exact, otherwise it cannot
  enter the Exact pushed list. Pushing only one side of an OR with Exact
  semantics is not allowed.

OR (InexactNoFalseNegative, pruning-only):
  Widening an unsupported branch to TRUE preserves no-false-negative,
  but a TRUE branch makes the whole OR a no-op for pruning. So widening
  is only useful when both sides still produce a useful pruning predicate
  after widening, e.g. (A AND unsupported) OR B widens to A OR B.
  When the entire side is unsupported, do not push the OR at all.
  In every case the original OR expression remains in residual plan.qual.

NOT:
  Core walker first applies a negate_clause-style rewrite (mirroring
  PG's prepqual.c::negate_clause): NOT (a op b) becomes (a negator-op b)
  using the operator's negator if available, NOT IS NULL becomes
  IS NOT NULL and vice versa, and NOT (A AND B) / NOT (A OR B) are
  pushed down to leaves via DeMorgan. Classification then runs on the
  rewritten leaves.

  After rewrite, only literal NOT nodes that could not be eliminated
  (e.g. NOT over a function call without negator) remain. v1 only
  pushes such NOT when the child is classified Exact and the backend
  supports exact NOT. Core must not automatically wrap an
  InexactNoFalseNegative child with NOT, because no-false-negative
  is not preserved under NOT.
```

This is an important correction to the previous version.

**Expr Walker**

v1 does not require a neutral IR. PG Expr is the source IR. Core provides typed wrappers and a folder.

Use two phases.

Planner classification:

```rust
pub trait PgPredicateClassifier {
    fn classify_comparison(
        &mut self,
        op: PgComparisonOp,
        left: PgScalarRef<'_>,
        right: PgScalarRef<'_>,
    ) -> QualPushdownDecision;

    fn classify_is_null(&mut self, value: PgScalarRef<'_>) -> QualPushdownDecision;
    fn classify_is_not_null(&mut self, value: PgScalarRef<'_>) -> QualPushdownDecision;
}

/// Operator identity passed to the provider. The provider must match on
/// (opno, opcollid, inputcollid) — never on operator name alone — so
/// that text equality under a non-default collation, or a numeric/int
/// cross-type comparison, is not silently classified as Exact.
/// `opfuncid` and `opresulttype` are exposed for diagnostics,
/// EXPLAIN, and runtime translation context; they are not part of
/// the classification key and must not override the
/// `(opno, opcollid, inputcollid)` decision.
pub struct PgComparisonOp {
    pub opno: pg_sys::Oid,
    pub opfuncid: pg_sys::Oid,
    pub opresulttype: pg_sys::Oid,
    pub opcollid: pg_sys::Oid,
    pub inputcollid: pg_sys::Oid,
}
```

Runtime native predicate construction:

```rust
pub trait PgPredicateTranslator {
    type Scalar;
    type Predicate;
    type Error;

    fn column(&mut self, col: PgColumnRef<'_>) -> Result<Self::Scalar, Self::Error>;
    fn literal(&mut self, lit: PgLiteral<'_>) -> Result<Self::Scalar, Self::Error>;
    fn param_value(&mut self, param: PgParamValue<'_>) -> Result<Self::Scalar, Self::Error>;

    fn comparison(
        &mut self,
        op: PgComparisonOp,
        left: Self::Scalar,
        right: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error>;

    fn is_null(&mut self, value: Self::Scalar) -> Result<Self::Predicate, Self::Error>;
    fn is_not_null(&mut self, value: Self::Scalar) -> Result<Self::Predicate, Self::Error>;

    fn and(&mut self, items: Vec<Self::Predicate>) -> Result<Self::Predicate, Self::Error>;
    fn or(&mut self, items: Vec<Self::Predicate>) -> Result<Self::Predicate, Self::Error>;
    fn not(&mut self, item: Self::Predicate) -> Result<Self::Predicate, Self::Error>;
}
```

Core owns:

- `RestrictInfo` unwrap. Note: in PG17 `create_customscan_plan` does NOT
  call `extract_actual_clauses` before invoking `PlanCustomPath`, so the
  `scan_clauses` argument is still a `List<RestrictInfo>`. This is the
  same shape FDW callbacks see (`GetForeignPlan` is also handed a
  `RestrictInfo` list); only the built-in non-FDW scan plan builders
  unwrap to bare `Expr` first. Core unwraps `RestrictInfo` to bare
  `Expr` before writing into `scan.plan.qual` / `custom_exprs`. Only
  bare `Expr` (and copyObject-safe metadata) ever lands in the plan
  tree.

- Pseudoconstant handling. Built-in scan plan builders call
  `extract_actual_clauses(scan_clauses, false)`, which drops
  pseudoconstant quals (those without `Var`s, e.g. `WHERE current_user
  = 'alice'`); PG handles them separately by adding a gating `Result`
  node above the scan via `create_gating_plan` /
  `get_gating_quals(scan_clauses)`. Core mirrors this at both stages:
  - Path stage (`set_rel_pathlist_hook`): when iterating
    `baserestrictinfo` to classify and to estimate selectivity /
    cost, skip any `RestrictInfo` with `pseudoconstant == true`.
    Counting them as pushable would inflate selectivity savings and
    misprice the path against the seqscan baseline.
  - Plan stage (`PlanCustomPath`): when unwrapping `RestrictInfo`s
    for `plan.qual` / `custom_exprs`, again skip
    `pseudoconstant == true`.

  Otherwise the same pseudoconstant gets evaluated twice — once in
  the gating `Result` above the CustomScan and again inside the
  scan's qual — and the path-stage cost will not match the chosen
  plan.

- Security-level gating for pushdown. PG only allows a clause to be
  evaluated "early" (before other restrictions on the same rel) when
  `restriction_is_securely_promotable(rinfo, baserel)` returns true
  (`restrictinfo.c:425`). The check passes when either
  `rinfo->security_level <= rel->baserestrict_min_security` or
  `rinfo->leakproof` is true. PG's index path applies this gate to
  every clause it considers pushing into the AM, both for
  `baserestrictinfo` (`match_clause_to_index` is called from
  `match_restriction_clauses_to_index`) and for `joininfo`
  (`match_join_clauses_to_index` filters by
  `join_clause_is_movable_to` first, then dispatches to the same
  `match_clause_to_index` at `indxpath.c:2100`). Pushing a clause
  into the AM is functionally equivalent to evaluating it early, so
  the path-stage classifier MUST treat
  `restriction_is_securely_promotable` as a hard pre-filter on
  every clause the provider would otherwise push, regardless of
  whether the source is `baserestrictinfo` or `ppi_clauses`. Any
  `RestrictInfo` that fails it is forced to the residual list
  regardless of how the provider would otherwise classify the bare
  `Expr`. Skipping this gate would let a higher-security clause run
  before a lower-security one, breaking RLS / security-barrier
  ordering.

  For `joininfo` and `ppi_clauses`, this gate composes with
  `join_clause_is_movable_to` (covered above in the path
  enumeration). The two gates are orthogonal and both must pass:
  movability handles outer-join / lateral safety; secure
  promotability handles RLS / leakproofness ordering.

- `RelabelType` unwrap.
- Typed views for `Var`, `Const`, and `Param`.
- Traversal for `OpExpr`, `BoolExpr`, and `NullTest`.
- A `negate_clause`-style rewrite pass before classification (operator
  negator, IS NULL flip, DeMorgan). The rewritten form is the form
  stored in `custom_exprs[pushed]`; column metadata below is computed
  by walking that same post-rewrite expression, not the original
  pre-rewrite qual.
- Rejection of volatile functions.
- Rejection of `SubPlan`.
- Safe AND/OR/NOT rules.
- Splitting into residual, pushed, and recheck quals.
- Pre-resolving column references at path/plan time. For each `Var`
  reference appearing in a pushed expression, core records resolved
  column metadata into `custom_private` so that `Begin/ReScan` does
  not have to interpret setrefs-rewritten `Var` shapes (e.g.
  `INDEX_VAR`) or post-`replace_nestloop_params` `Param` substitutions.

  Scope of the column ordinal:

  - `var_ordinal` only counts `Var` nodes that belong to the current
    scan relation (i.e. `varno == rel->relid`). These are the only
    ones that survive into runtime as `Var`.
  - Outer-relation `Var` nodes (those coming from `ppi_clauses` of a
    parameterized path) are NOT given a column ordinal. They are
    rewritten into `PARAM_EXEC` `Param` nodes by
    `replace_nestloop_params` after `PlanCustomPath` returns, and at
    runtime they are resolved through `PgParamValue` instead. If the
    plan-time ordinal counted outer Vars, the runtime walker would
    see fewer `Var` nodes (because they have become `Param`s) and
    every ordinal after the substitution point would be off-by-N.
  - The same rule applies to expressions that came in through join
    clauses: anything that does not reference the scan relation's
    `varno` is treated as a future `Param`, not a column.

  Layout of the resolved-column section in `custom_private`:

  ```text
  custom_private.column_refs:
    [
      {
        expr_index:   Integer,    // index into custom_exprs[pushed]
        var_ordinal:  Integer,    // 0-based count of *scan-relation*
                                  // Var nodes seen by a deterministic
                                  // walker over that pushed expression
        rel_oid:      Oid,        // pg_class OID of the scan relation,
                                  // resolved from RTE.relid at plan
                                  // time. Pinned by lock acquired in
                                  // the planner; safe to use at runtime
                                  // for catalog / pruning calls.
        attno:        AttrNumber,
        atttypid:     Oid,
        attcollation: Oid,
      },
      ...
    ]
  ```

  Note: do NOT carry `Var.varno` / `RelOptInfo.relid` (the 1-based
  range table index, an `Index`) inside `custom_private`.
  `set_plan_references` runs `set_customscan_references`
  (`setrefs.c:1671`) at the end of planning, which adjusts
  `cscan->scan.scanrelid`, every `Var.varno` inside `custom_exprs`,
  the `Var.varno`s inside `scan.plan.qual` / `scan.plan.targetlist`,
  and `cscan->custom_relids` — but it does NOT walk
  `custom_private`. Any RTI cached there is therefore pre-rtoffset
  and will diverge from the post-rtoffset `scan.scanrelid` whenever
  the CustomScan ends up nested below a subquery / subplan that
  introduces a non-zero `rtoffset`. Concrete consequences if you
  cache an RTI in `custom_private`:
  - "Is this `Var` a scan-relation column?" checks against
    `custom_private.scan_rti` will misidentify outer / sibling
    `Var`s as scan columns (or vice versa).
  - Cross-checks against `Scan.scanrelid` will trip falsely.

  Runtime rule: identify the scan relation only via
  `cscan->scan.scanrelid` (already rtoffset-adjusted). Resolve
  columns purely from `(rel_oid, attno, atttypid, attcollation)`
  recorded above plus the `(expr_index, var_ordinal)` matched by the
  walker. A debug-only `pre_setrefs_scan_rti` field MAY live in
  `custom_private` for EXPLAIN traceability, but it must not be
  used in any correctness-bearing comparison.

  The same expression-traversal walker is used in two places:
  - Inside `PlanCustomPath`, before returning the `CustomScan` node to
    PG, when emitting `column_refs[]`. This is before PG17
    `create_customscan_plan` runs `replace_nestloop_params` over
    `custom_exprs`.
  - At runtime, when `BeginCustomScan` walks `custom_exprs[pushed]`
    again and matches each scan-relation `Var` it encounters by
    `(expr_index, var_ordinal)` to the precomputed metadata.
    "Scan-relation `Var`" here means `Var.varno ==
    cscan->scan.scanrelid` (the post-setrefs value), never an RTI
    cached in `custom_private`.

  Both walkers must use the same traversal over the post-rewrite form
  of each pushed expression: pre-order, left-to-right children, no
  recursion into already-resolved `Param` values, and *do not
  increment* `var_ordinal` for `Var` nodes whose `varno` is not the
  scan relation. This skip-outer-Var rule is what makes the ordinal
  stable: the plan-time walker sees outer-relation `Var` nodes before
  `replace_nestloop_params`, while the runtime walker sees the
  corresponding `PARAM_EXEC` nodes after the rewrite, and neither
  side increments `var_ordinal` for them.

  For v1 base relation scans without `custom_scan_tlist`, PG still
  rewrites scan-relation `Var`s via the standard scan-refs path so
  `varattno` continues to map to the base relation; the layout above
  keeps the runtime translator independent of that detail and is
  also what we would need once join / upper CustomScan paths (which
  force `INDEX_VAR`) are added.

The AM provider owns:

```text
column op literal/param value -> native predicate
```

**Param Model**

Do not mix planner-phase `Param` nodes with executor-phase parameter values.

```rust
PgParamRef
  Visible during planning. It only represents a Param node reference.

PgParamValue
  The runtime value resolved from EState during Begin/ReScan.
```

`PARAM_EXTERN` resolution (`expr/runtime_params.rs`) must mirror
`ExecEvalParamExtern` (`execExprInterp.c:2531`). It is NOT enough to
read `econtext->ecxt_param_list_info->params[paramId - 1]` directly:
- If `paramInfo->paramFetch != NULL`, call it
  (`paramFetch(paramInfo, paramId, /*speculative=*/false, &prmdata)`)
  and use the returned `ParamExternData` rather than the entry in
  the `params[]` array. Some callers (notably PL/pgSQL `EXECUTE`
  with dynamic params, and prepared-statement repacking) only fill
  the value through `paramFetch`; reading the raw slot returns a
  stale or null value.
- After fetch, validate `prm->ptype == op->d.param.paramtype` (the
  type recorded on the plan-time `Param` node) and raise the same
  `ERRCODE_DATATYPE_MISMATCH` PG raises when they disagree. Failing
  to validate means a re-prepared or replanned statement could feed
  a Datum of the wrong physical layout into the predicate
  translator.
- `OidIsValid(prm->ptype)` must be checked before reading the
  value; an invalid `ptype` is PG's signal that no value has been
  supplied for this paramId, and the correct response is the
  `"no value found for parameter %d"` error.

`PARAM_EXEC` resolution must mirror `ExecEvalParamExec`
(`execExprInterp.c:2510`). Reading
`econtext->ecxt_param_exec_vals[paramId]` straight is also not
enough:
- `ParamExecData.execPlan != NULL` means the parameter is the output
  of a not-yet-run InitPlan / SubPlan (PG materializes InitPlan
  results lazily). Before reading `prm->value`, call
  `ExecSetParamPlan(prm->execPlan, econtext)`. After it returns,
  `prm->execPlan` is reset to `NULL` and `prm->value` / `prm->isnull`
  hold the materialized result. Without this step, queries like
  `WHERE a = (SELECT MAX(x) FROM t)` would see a zero / garbage
  Datum on the first call.
- When resolving a *set* of `PARAM_EXEC` ids in one shot (e.g. all
  the params referenced by `custom_exprs[pushed]` at the start of
  `BeginCustomScan` or in a `chgParam`-driven `ReScanCustomScan`),
  use `ExecSetParamPlanMulti(params_bitmap, econtext)` if the
  generated pgrx bindings expose it. PG17 declares it in
  `include/executor/nodeSubplan.h` and implements it in
  `nodeSubplan.c`, but the current generated `pg_sys` bindings may
  not expose cold executor symbols. If `ExecSetParamPlanMulti` is not
  available, loop over the referenced param ids and call
  `ExecSetParamPlan` for entries whose `ParamExecData.execPlan` is
  still pending, if that symbol is available. If neither symbol is in
  `pg_sys`, add a small version-gated FFI binding for the PG17
  prototype rather than reading `ParamExecData.value` blindly. The
  important contract is: pending InitPlan outputs must be materialized
  before the translator reads the param value.

In nested-loop plans, `ExecNestLoop` calls `ExecReScan(innerPlan)`
on every new outer tuple that has nestParams; the executor sets
`innerPlan->chgParam` with the changed param ids before doing so.
`ReScanCustomScan` is therefore guaranteed to run, but core does not
have to translate the predicate again unconditionally:

```text
ReScanCustomScan:
  if node->ss.ps.chgParam intersects param ids referenced by the
  pushed expressions:
    re-resolve PARAM_EXEC values
    rebuild AM native predicate
    redo file/row-group pruning
    reopen scan cursor
  else:
    reopen scan cursor without re-translating the predicate
```

This avoids re-translating predicates and re-pruning manifests on
every inner-side rescan when no referenced `Param` is marked changed
by `chgParam`. Note: PG nestloop sets `chgParam` for every nestParam
on every outer tuple — it does not compare new vs old `Datum` values
for equality. So "param marked changed by `chgParam`" is the only
trigger criterion; we never compare `Datum`s ourselves.

v1 does not expose a provider rewind capability flag, so the
non-translation branch still reopens the cursor. A future version can
add an explicit provider capability if reopening becomes too expensive.

**Planner Phase**

Use a core-owned `set_rel_pathlist_hook` router:

```text
set_rel_pathlist_hook
  -> previous hook
  -> reject if rel is UPDATE/DELETE/MERGE target
       (commandType != CMD_SELECT && rel->relid in all_result_relids)
  -> reject if get_plan_rowmark(root->rowMarks, rel->relid) != NULL
  -> reject if reltarget / baserestrictinfo / joininfo references
       any unsupported system column (ctid, xmin/xmax/cmin/cmax,
       and whole-row Var pre-condition checks)
  -> iterate registered providers
  -> supports_relation()
  -> classify baserestrictinfo
       (skip pseudoconstant; require restriction_is_securely_promotable)
  -> classify baserel->joininfo
       (skip pseudoconstant; require join_clause_is_movable_to AND
        restriction_is_securely_promotable)
  -> add_path(plain CustomPath: required_outer = lateral_relids)
  -> for each useful required_outer derived from joininfo:
       -> get_baserel_parampathinfo(root, baserel, required_outer)
       -> classify ppi_clauses + baserestrictinfo
            (same gates as above, applied per source)
       -> add_path(parameterized CustomPath)
```

v1 limits:

- Only handle base relation scans.
- This framework emits base-relation `CustomPath`s only. It does not
  install `set_join_pathlist_hook` or `create_upper_paths_hook`;
  joinrel and upperrel path generation are separate extension points
  and would belong in separate modules if ever needed.
- The relation must be a concrete storage relation. `supports_relation`
  must reject:
  - partitioned parents (`relkind = 'p'`): they have no `rd_tableam` of
    their own and are scanned via leaf relations.
  - foreign tables (`relkind = 'f'`).
  - other non-storage relkinds (views, sequences, composite types).
- Skip joinrels and upperrels.
- Do not create a provider path directly for a partitioned parent in v1. Let leaf relations handle it first.

DML / rowmark / system-column gating (v1).

CustomScan does not have FDW-style row-identity plumbing
(`fsSystemCol`, `IsForeignRelUpdatable`, `PlanDirectModify`, the
`junkfilter`-driven `tid` / `wholerow` injection). A lake AM has no
mutable `ctid` semantics and cannot serve as an EPQ re-fetch target.
Until v1 defines an explicit row-identity contract, the path-stage
hook MUST decline to emit any CustomPath when the lake rel would
need any of those facilities. Concretely, before any provider is
consulted for `baserel`:

- Refuse if the lake rel is the target of an UPDATE / DELETE /
  MERGE. Detect via `root->parse->commandType != CMD_SELECT` AND
  `bms_is_member(rel->relid, root->all_result_relids)`. CustomScan
  has no path that produces the row-identity tuple
  `ModifyTable` requires; without one, executor would reach
  `ExecGetUpdateNewTuple` / `table_tuple_lock` with a slot that has
  no valid `tts_tid` and crash or corrupt rows.
- Refuse if `get_plan_rowmark(root->rowMarks, rel->relid) != NULL`.
  This covers `SELECT ... FOR UPDATE / FOR SHARE / FOR NO KEY
  UPDATE / FOR KEY SHARE` on the lake rel and any rowmark added
  for EPQ re-fetch on behalf of a target rel that joins the lake
  rel. v1 has no `RefetchForeignRow`-equivalent and no native
  row-version concept; participating in `ExecLockRows` with an
  unfetchable `tts_tid` would either raise spurious serialization
  failures or silently lock nothing.
- Refuse if any `Var` reachable from `rel->reltarget->exprs`,
  `baserestrictinfo`, or `joininfo` references a system column
  outside the explicitly supported surfaces. v1 supports normal
  user attributes (`varattno > 0`) by default; the supported
  system-column surfaces are listed below (`tableoid` and, with
  preconditions, whole-row Var). Unsupported system attnos —
  `ctid` (`SelfItemPointerAttributeNumber`), `xmin`, `xmax`,
  `cmin`, `cmax` — are always rejected. Whole-row references
  (`varattno == 0`) are accepted when the slot contract preconditions
  documented below hold; otherwise they are rejected.
  Walk these expression lists with a small `Var`-collecting
  walker, OR'ing the seen `varattno`s into a bitset and rejecting
  on the first attno outside the supported set.
- `tableoid` (`TableOidAttributeNumber`) is supported by default.
  Core's `ExecScan` access method is not the provider function
  directly: it is `core.next_slot_wrapper`, which calls
  `provider.next_slot`, then sets
  `slot->tts_tableOid = RelationGetRelid(scan_rel)` on any non-empty
  returned slot before handing it back to `ExecScan`. This mirrors
  `nodeForeignscan.c::ForeignNext`. A provider that bypasses the core
  wrapper through a different return path must either set
  `tts_tableOid` itself or declare `tableoid` unsupported, causing
  the path-stage gate above to reject paths that read it.
- Whole-row `Var` (`varattno == 0`) is supported only when
  `slot->tts_tupleDescriptor` matches the base relation's
  `RelationGetDescr(rel)` exactly (column count, types, order)
  AND every user attribute holds its real value with `tts_isnull`
  reflecting only true SQL NULL. PG17 `ExecEvalWholeRowVar`
  (`execExprInterp.c:4946`) builds the composite via
  `toast_build_flattened_tuple(slot->tts_tupleDescriptor,
  slot->tts_values, slot->tts_isnull)`; any column-pruning-induced
  NULL in `tts_isnull` becomes an *observable* SQL NULL on column
  *i* of the composite and is wrong by construction. Whole-row Var
  therefore disables the "unreferenced attrs may be left NULL"
  shortcut from the slot contract for the lifetime of the scan:
  every user attribute must be materialized as its real value when
  whole-row Var is reachable. The path-stage gate above defaults
  whole-row Var to supported because the slot contract requires
  the matching `TupleDesc`, but if the runtime cannot guarantee
  full user-attribute materialization it must downgrade whole-row
  Var to unsupported (and the gate will then reject paths that
  read it).

These gates are enforced in core (in the `set_rel_pathlist_hook`
router, before calling any provider's `supports_relation`), so a
provider cannot accidentally relax them. Failing any gate means: do
nothing — leave PG's default seqscan/indexscan paths in place. This
matches what the FDW path does when `IsForeignRelUpdatable` returns
zero or `fsSystemCol` is unset for an attribute the plan needs.

Relation matching is based on the relation AM OID. For example, the Iceberg provider matches the Iceberg table AM OID.

The plan phase must split again from the final `scan_clauses` received by
`PlanCustomPath`. PG17 only calls `order_qual_clauses` on those clauses
before handing them to the provider; it does NOT call
`extract_actual_clauses`. CustomScan follows the FDW-style plan
callback behavior here (`create_foreignscan_plan` does the same for
`GetForeignPlan`); only the built-in non-FDW scan plan builders unwrap
to bare `Expr` first. The clauses are still `RestrictInfo` nodes,
possibly in a different order than during path creation, and may now
include `ppi_clauses` from a parameterized path (which can introduce
outer-relation `Var`s that PG will later rewrite into `PARAM_EXEC`
`Param`s). The final plan must use these `scan_clauses` as the source
of truth, drop pseudoconstant clauses (PG handles those via a gating
`Result`), unwrap the rest of the `RestrictInfo` list to bare `Expr`,
and re-run classification before writing `plan.qual` / `custom_exprs`.

**Plan Field Layout**

```text
scan.plan.qual:
  residual exprs
```

These are executed automatically by `ExecScan`.

```text
custom_exprs:
  [pushed_exprs..., recheck_exprs...]
```

These are not executed automatically by PostgreSQL, but PostgreSQL does apply setrefs and nestloop param processing to them.

```text
custom_private:
  provider id/name
  relation oid           // pg_class OID; resolved at plan time from
                         // the RTE. Do NOT cache scan.scanrelid /
                         // any RTI here — set_customscan_references
                         // does not adjust custom_private, so a
                         // cached RTI goes stale whenever rtoffset
                         // != 0. Runtime reads the (post-rtoffset)
                         // RTI from CustomScan.scan.scanrelid.
  private_version
  pushed_count
  recheck_count
  pushed_guarantees[]
  provider private metadata
```

`custom_private` may contain only copyObject-safe metadata:

- It may contain `Oid`, `Integer`, `String`, `List`, and simple flags.
- It must not contain a Rust pointer.
- It must not contain a native predicate.
- It must not contain the final file list.
- It must not contain unprocessed PG Expr encoded as JSON.

`private_version` is framework-wide, not provider-specific. For this
v1 layout it is fixed to integer `0`. Runtime decode must reject an
unknown framework `private_version` rather than attempting to execute
with a mismatched layout. Provider-specific private metadata can carry
its own provider version inside the provider section if needed; that
version is owned and interpreted by the provider.

For v1, it is acceptable for exact expressions to appear twice in
`custom_exprs`: once in the pushed section and once in the recheck
section. This keeps the layout simple and avoids index mapping. The
cost is real but bounded: PG runs `set_plan_references::fix_scan_expr`
and `replace_nestloop_params` over the entire `custom_exprs` list, so
two copies double that work for the affected expressions. PG itself
does not auto-print `custom_exprs` in EXPLAIN — only the provider's
`ExplainCustomScan` does — so duplicate output only happens if the
provider walks `custom_exprs` naively. Core's EXPLAIN helper must
print pushed and recheck sections under distinct labels (see EXPLAIN
section below). v2 may switch to a single deduplicated list with
`recheck_indices: List<Integer>` mapping recheck slots back into the
pushed list.

**Scan slot contract (v1: no `custom_scan_tlist`)**

v1 leaves `custom_scan_tlist = NIL`. PG's `ExecInitCustomScan` then
takes the branch that uses `RelationGetDescr(scan_rel)` for the scan
slot's `TupleDesc` and sets `tlistvarno = scanrelid`. This pins the
following provider contract:

- `provider.next_slot` must produce a slot whose `TupleDesc` matches
  the base relation's rowtype (`RelationGetDescr(rel)`), exactly as
  if a heap `SeqScan` had returned it. Column count, column types,
  and column order all have to match.
- The slot must hold values for every attribute that any of the
  following needs to read: residual `plan.qual` (executed by
  `ExecScan`), the result tlist projection (built by
  `ExecAssignScanProjectionInfoWithVarno` against `scanrelid`), and
  the recheck `ExprState` compiled from the `custom_exprs[recheck]`
  section. Concretely, that is at least the union of:
  - `Var`s referenced by `scan.plan.qual`,
  - `Var`s referenced by `cscan->scan.plan.targetlist`. PG17
    builds this via `build_path_tlist(root, best_path)` from
    `path->pathtarget` for every CustomPath:
    `use_physical_tlist` (`createplan.c:866`) explicitly returns
    false on `IsA(path, CustomPath)`, so the "physical /
    base-rel-full-tlist" branch that real-relation seqscans take
    does not apply. The provider therefore sees only the columns
    required by upstream nodes plus whatever the path's
    `pathtarget` contributes — typically a strict subset of the
    base rel's columns — and must materialize values for at least
    that subset.
  - `Var`s referenced by recheck expressions.
- Internally the provider can prune columns at the Iceberg / Arrow
  reader level for performance, but unreferenced attrs may be left
  NULL in the scan slot only when no executor expression
  (`plan.qual`, the tlist projection, or the recheck `ExprState`)
  can read them; any attr that is observable through one of those
  paths must hold its real value. A shorter `TupleDesc` is never
  acceptable — `ExecQual` and projection read the slot via
  `slot_getattr` against the slot's own descriptor, and a mismatch
  crashes or returns wrong values.
- System columns. The path-stage gates above guarantee that the
  only system columns the executor can request from a v1 lake scan
  are `tableoid` and (transitively, via whole-row Var) the user
  attributes. The slot contract for system columns is therefore:
  - `slot->tts_tableOid` must equal `RelationGetRelid(scan_rel)`
    on every returned slot. Core's `next_slot_wrapper` sets this
    after `provider.next_slot` returns a non-empty slot and before
    handing the slot to `ExecScan`; providers that bypass the wrapper
    must set it themselves.
    The slot supplied to `recheckMtd` is owned by EPQ machinery, not
    by `next_slot_wrapper`. v1 does not enter that path for lake
    tables; v2 must extend the slot contract to ensure
    `tts_tableOid` is set on the substitution slot before recheck
    expressions can read `tableoid`.
  - `slot->tts_tid` is left as `InvalidItemPointer`. v1 lake scans
    have no `ctid` identity, and the path-stage gates ensure no
    rowmark / DML / EPQ caller can reach a slot expecting a valid
    `tts_tid`. If a future provider gains a row-identity contract,
    `tts_tid` becomes part of the slot contract at that point.
  - `xmin` / `xmax` / `cmin` / `cmax` are not produced. v1 has no
    MVCC visibility surface; the path-stage gates reject any plan
    that reads them.
  - Whole-row `Var` (`varattno == 0`) is satisfied directly by
    the slot's user-attribute values. PG17 `ExecEvalWholeRowVar`
    (`execExprInterp.c:4946`) builds the composite via
    `toast_build_flattened_tuple(slot->tts_tupleDescriptor,
    slot->tts_values, slot->tts_isnull)` — `tts_tableOid` is *not*
    part of the row composite (it is a separate system attr served
    by `ExecEvalSysVar` / `slot_getsysattr`). So whole-row Var
    requires only that (a) `tts_tupleDescriptor` matches the base
    rel's user-attribute rowtype and (b) every user attribute holds
    its real value with `tts_isnull` reflecting only true SQL NULL.
    Pruning-induced absences must be materialized as the real value,
    not as NULL, whenever whole-row Var is reachable for the scan.
- `slotOps`: v1 returns virtual slots (the `TTSOpsVirtual` default
  produced by `ExecInitCustomScan` when `CustomScanState.slotOps`
  is left null). A future provider that wants to pass through Arrow
  buffers without copying can override `slotOps`, but that is out of
  scope for v1.

If a future version introduces `custom_scan_tlist` for projection
layouts visible to PG, `tlistvarno` becomes `INDEX_VAR` and the slot
contract changes accordingly. The column metadata layout already
accommodates that case (see `column_refs` in the Expr Walker
section).

**CustomPath / CustomScan Flags**

For v1, providers must declare flags conservatively:

```text
flags = CUSTOMPATH_SUPPORT_PROJECTION
```

- `CUSTOMPATH_SUPPORT_BACKWARD_SCAN`: not declared. v1 cursors are
  forward-only.
- `CUSTOMPATH_SUPPORT_MARK_RESTORE`: not declared. v1 does not support
  mark/restore (used by mergejoin inner side).
- `CUSTOMPATH_SUPPORT_PROJECTION`: declared. This makes PG's
  `is_projection_capable_path` / `is_projection_capable_plan` return
  `true` for the CustomScan, so the planner can skip an extra
  projection `Result` for ordinary tlist projection — core's
  `ExecCustomScan` wrapper handles tlist projection in place via
  `ExecAssignScanProjectionInfoWithVarno` + `ExecScan`. PG can still
  insert a `Result` node above the scan for unrelated reasons, most
  importantly when there are pseudoconstant gating quals
  (`create_gating_plan`, see Plan Field Layout below), so the flag
  reduces but does not eliminate `Result` insertion.

**Executor Begin/ReScan**

Do not freeze the final file list in the planner.

```text
CreateCustomScanState:
  create #[repr(C)] wrapper
  decode custom_private (resolved column metadata, provider private)
  record custom_exprs layout (pushed range / recheck range)
  initialize provider state

BeginCustomScan:
  read EState / Snapshot / params
  translate PG Expr nodes from custom_exprs[pushed]
  resolve PARAM_EXTERN / PARAM_EXEC into PgParamValue
  translate into AM native predicate (using metadata pre-resolved at
  plan time, not the possibly setrefs-rewritten Var shape)
  load current statement-visible lake metadata
  perform manifest/file/row-group pruning
  open scan cursor
  compile recheck exprs into ExprState (ExecInitQual)

ReScanCustomScan:
  inspect node->ss.ps.chgParam
  if it intersects the params referenced by pushed expressions:
    resolve params again
    translate native predicate again
    perform runtime scan planning again
    reopen cursor
  else:
    reopen cursor without re-translating
```

The part borrowed from pg_lake is that the Begin phase builds the scan
snapshot and performs pruning from the current snapshot and params,
instead of freezing the final file list during planning.

**Memory and interrupt discipline**

Core splits these responsibilities between the top-level
`ExecCustomScan` callback and the access callback passed to
`ExecScan`, so providers do not have to re-implement them and
semantics match what `nodeForeignscan.c` already does for FDWs:

- `ExecCustomScan` already runs `CHECK_FOR_INTERRUPTS()` once per call,
  and `ExecScanFetch` does the same. Long-running IO inside
  `provider.next_slot` (object-store reads, decompression of a large
  row group) must still call `CHECK_FOR_INTERRUPTS()` itself — the
  outer wrapper only fires once per returned tuple.
- `core.next_slot_wrapper` switches into
  `econtext->ecxt_per_tuple_memory` before calling
  `provider.next_slot`, mirroring `ForeignNext` (the FDW access
  callback passed to `ExecScan`). The provider can allocate scratch
  memory for the row it is currently returning in the current context.
  PG17 `ExecScan` resets this context before each scan cycle and
  after a tuple fails the scan qual; it does not depend on `ExecQual`
  or `ExecProject` resetting it. If `provider.next_slot` internally
  loops over many backend rows before returning one PG slot, that
  callback must manage any per-internal-row scratch itself. Cursor
  handles, decoder buffers, cached Arrow arrays, and other state
  needed by later calls must not be allocated in the per-tuple context.
- The recheck path mirrors `ForeignRecheck` exactly: set
  `econtext->ecxt_scantuple = slot`, then `ResetExprContext(econtext)`,
  then `ExecQual(recheck_state, econtext)`. `ExecQual` itself does
  NOT reset the context — the caller does. Core only needs to make
  sure the recheck `ExprState` was initialized once in
  `BeginCustomScan` against the right `PlanState`.

**Executor Next**

Core provides a common wrapper:

```text
provider ExecCustomScan
  -> core ExecCustomScan wrapper
       -> ExecScan(
            accessMtd  = core.next_slot_wrapper,
            recheckMtd = core.recheck_exact_pushed_quals
          )

core.next_slot_wrapper(node):
  slot = provider.next_slot(node)
  if slot is not empty:
    slot->tts_tableOid = RelationGetRelid(scan_rel)
  return slot
```

Normal path:

```text
ExecScan
  -> accessMtd gets the next row
  -> execute scan.plan.qual residual quals
  -> projection
  -> return slot
```

EPQ/recheck path:

```text
core.recheck_exact_pushed_quals(node, slot):
  econtext = node->ss.ps.ps_ExprContext
  econtext->ecxt_scantuple = slot
  ResetExprContext(econtext)
  return ExecQual(recheck_state, econtext)
```

Note: in v1 this path is normally only taken when this CustomScan
itself owns the locked row, but lake tables do not implement
`tuple_lock` so this case does not arise either. Reaching the
recheck path via "an UPDATE/DELETE/MERGE on a sibling relation
drives EPQ and feeds substitution slots into our scan" (see
`execScan.c::ExecScanFetch`) is also not possible in v1: the
path-stage rowmark gate refuses CustomPath whenever
`get_plan_rowmark(root->rowMarks, rel->relid) != NULL`, and PG17
`preprocess_rowmarks` adds rowmarks to *every non-target base rel*
in any UPDATE/DELETE/MERGE plan, so any lake rel joined into a
sibling-DML plan is rejected before a CustomScan is built. The
recheck `ExprState` is wired up anyway, but it is only invoked when
`ExecScanFetch` enters the recheck path; it is not an extra qual on
the normal `next_slot` path. When that path is reached, core
evaluates it as defense in depth and to keep the contract stable for
future row-identity support.

Important: `ExecInitCustomScan` does not initialize `custom_exprs` for us. Core must call `ExecInitQual` for recheck expressions itself.

**EXPLAIN Output**

PG's EXPLAIN does not auto-print `custom_exprs`; only `scan.plan.qual`
is printed (as `Filter`). The structured pushdown information has to
come from core's own `ExplainCustomScan` callback. v1 prints a compact
summary by default and prints expression text only when EXPLAIN VERBOSE
is enabled, so ordinary EXPLAIN output does not become a predicate dump.

```text
Lakebase Pushdown:
  Provider: pg-iceberg-am
  Pushed Exact: 1
  Pushed Inexact: 1
  Recheck: 1
  Residual: 1
```

With VERBOSE, each non-zero section also prints the expression text
under that section.

Rules:

- Pushed and Recheck are printed under separate labels even when v1
  stores them as duplicate copies in `custom_exprs`. The provider's
  EXPLAIN walker must respect the
  `pushed_count` / `recheck_count` boundaries from `custom_private`,
  not iterate `custom_exprs` blindly.
- Counts are always present, even when zero, so EXPLAIN diffs in
  regression tests are stable.
- Inexact pushed expressions also appear in Residual by design: they
  are pushed for pruning and re-evaluated by the executor for
  correctness. The two counts describe two roles, not two independent
  qual objects.
- Residual is also printed here (in addition to PG's own `Filter:`
  line) so the pushdown summary is complete in one place. Residual
  expression text follows the same VERBOSE-only rule.

**Iceberg Integration**

Add these pieces to `pg-iceberg-am`:

```text
IcebergCustomScanProvider
IcebergPredicateClassifier
IcebergPredicateTranslator
```

Execution path:

```text
PG Expr
  -> core walker
  -> IcebergPredicateTranslator
  -> iceberg_lite::expr::Predicate
  -> TableScanBuilder::with_filter
  -> Arrow reader / row filter / pruning
```

Initial policy:

- Start conservatively for simple `column op literal/param` expressions.
- Mark an expression as `Exact` only after tests prove that the Iceberg reader semantics match PostgreSQL SQL semantics.
- If equivalence is not proven, use `InexactNoFalseNegative` and keep residual filtering.
- Keep the existing TableAM scan as fallback.

`ScanSpec` and `ScanCursor` should be refactored into a scan backend shared by the provider and the TableAM fallback, so Iceberg scan logic does not split into two implementations.

**V1 Scope**

v1 includes:

1. Core CustomScan trait, builder, and state wrapper.
2. Core `set_rel_pathlist_hook` registry/router.
3. Core PG Expr typed wrapper plus classifier and translator/folder,
   including a `negate_clause`-style NOT rewrite pass and pre-resolved
   column metadata (`rel_oid`, `attno`, `atttypid`, `attcollation`;
   no RTI is cached because `set_customscan_references` does not
   adjust `custom_private`).
4. Path-stage and plan-stage classification into pushed / residual /
   recheck expressions, with operator identity (`opno`, `opcollid`,
   `inputcollid`) exposed to the classifier.
5. `plan.qual`, `custom_exprs`, and `custom_private` layout, with
   `RestrictInfo` unwrap done in core before writing into the plan
   tree, and pseudoconstant clauses dropped at both the path-stage
   classify/cost and the plan-stage unwrap.
6. CustomPath construction contract: `pathtarget = parent->reltarget`,
   `parallel_aware = parallel_safe = false`, `parallel_workers = 0`,
   `pathkeys = NIL`, `rows`/cost set so the path beats seqscan
   exactly when pushdown wins. Parameterized variants are emitted
   for every distinct `required_outer` derived from
   `baserel->joininfo` that lets at least one additional safe
   outer-driven clause become an AM-side scan predicate
   (always-respecting
   `baserel->lateral_relids`); each variant uses
   `get_baserel_parampathinfo` to obtain `param_info` /
   `ppi_clauses` / `ppi_rows`.
7. Scan slot contract: with `custom_scan_tlist = NIL`,
   `provider.next_slot` returns a slot whose `TupleDesc` matches
   the base relation rowtype and that holds every attribute needed
   by residual qual, projection, and recheck.
8. Begin/ReScan runtime predicate rebuild, gated on `chgParam`,
   resolving both `PARAM_EXTERN` (prepared statement params) and
   `PARAM_EXEC` (nestloop-driven params from parameterized paths,
   plus subplan / InitPlan params) into `PgParamValue`.
9. Per-tuple memory context switch in `core.next_slot_wrapper` and
   interrupt discipline in core's top-level `ExecCustomScan` callback.
10. CustomPath flags: `CUSTOMPATH_SUPPORT_PROJECTION` only;
    backward-scan and mark-restore explicitly not supported.
11. DML/rowmark closed-world fallback: in v1, lake relations use the
    TableAM fallback for all UPDATE / DELETE / MERGE statements,
    including when the lake relation is only a read-only FROM / USING
    / join source. CustomScan pushdown is entirely disabled for those
    statement types until a row-identity contract exists.
12. Iceberg provider for simple predicates.
13. Structured EXPLAIN output for pushed-exact / pushed-inexact /
    recheck / residual, printed by core's `ExplainCustomScan`.
14. Keep TableAM fallback.

The TableAM scan path is retained for v1 for two separate reasons:
DML / rowmark gates leave UPDATE / DELETE / MERGE on the TableAM path,
and some SELECT queries fall outside the CustomScan path-stage gates
(for example unsupported system-column surfaces, partitioned parents,
or non-storage relations) and still need a working scan. v2 may keep
or replace the TableAM scan after the row-identity contract makes the
DML / rowmark fallback unnecessary, but no removal is planned for v1.

v1 excludes:

- Mandatory neutral CoreExpr IR.
- Parameterized variants generated for parallel plans
  (matches `indxpath`'s rule: parallel index paths are only
  considered for `outer_relids == NULL`).
- Parallel CustomScan DSM and the parallel callbacks
  (`EstimateDSMCustomScan` etc.).
- `custom_scan_tlist` and `INDEX_VAR`-based projection.
- TableAM access to CustomScanState.
- Planner-phase final file list.
- Global or thread-local filter side channel.
- CustomScan pushdown in UPDATE / DELETE / MERGE, even for lake
  tables that appear only as non-target join sources. This is a known
  v1 feature gap caused by rowmark/EPQ identity requirements.

**Test Plan**

Core unit tests:

- AND partial pushdown.
- OR Exact all-or-nothing.
- OR Inexact widening rule: `(A AND unsupported) OR B` widens to
  `A OR B` for pruning; OR with one fully-unsupported side does not
  widen (would degenerate to TRUE).
- NOT walker rewrite: `NOT (a = 1)` becomes `a <> 1`,
  `NOT (a IS NULL)` becomes `a IS NOT NULL`, DeMorgan applied to
  `NOT (A AND B)` and `NOT (A OR B)`.
- Inexact under NOT must not be automatically pushed down.
- Exact does not enter residual and does enter recheck.
- Inexact remains in residual.
- Unsupported remains in residual.
- Operator identity: same operator name with different
  `(opno, opcollid, inputcollid)` is classified independently;
  non-default collation `text =` is not auto-classified Exact.
- `RestrictInfo` unwrap: `scan_clauses` from `PlanCustomPath` are
  passed in as `RestrictInfo`, but `plan.qual` and `custom_exprs`
  contain only bare `Expr`.
- Pseudoconstant clauses (`RestrictInfo.pseudoconstant == true`) are
  dropped from `plan.qual` / `custom_exprs`; they are evaluated by
  PG's gating `Result` instead. Test by including a clause like
  `current_user = 'alice'` and asserting it does not appear in
  EXPLAIN's Filter / Pushed lists.
- `var_ordinal` stability under parameterized scans: when
  `ppi_clauses` introduces an outer-relation `Var` that
  `replace_nestloop_params` rewrites to `PARAM_EXEC`, the runtime
  walker still resolves remaining scan-relation `Var`s correctly
  (i.e. plan-time and runtime ordinals match).
- `PgParamRef` is not fixed to a value in the planner.
- `chgParam` gating: ReScan without `chgParam` intersection does not
  re-translate the predicate.
- `custom_exprs/custom_private` layout roundtrip.
- `supports_relation` rejects partitioned parents
  (`relkind = 'p'`) and foreign tables (`relkind = 'f'`).

PG regression tests:

- `WHERE a = 1` generates CustomScan.
- `unsupported_func(a)` falls back to residual.
- `a = 1 AND unsupported_func(b)` performs partial pushdown.
- `a = 1 OR unsupported_func(b)` does not perform partial pushdown.
- Prepared statement `PARAM_EXTERN` rescan.
- Nested-loop `PARAM_EXEC` rescan: a query whose nestloop inner side
  is the lake table with a join clause like `lake.id = outer.id`
  picks the parameterized CustomPath (visible in EXPLAIN as a
  scan-level filter on `id` instead of an above-scan join filter),
  and re-translates the native predicate on each outer tuple.
- Parameterized vs unparameterized selection: when the join clause
  is non-pushable, the unparameterized CustomPath wins; when the
  join clause is pushable, the parameterized variant wins.
- Security-barrier / non-leakproof regression: a `baserestrictinfo`
  clause whose `security_level` exceeds
  `baserel->baserestrict_min_security` (e.g. RLS-derived qual
  `WHERE secret(a)` on a table with a security-barrier view) and is
  not leakproof must NOT appear in EXPLAIN's Pushed list. It must
  remain in the above-scan filter (or, for index-equivalent paths,
  be visible as a residual qual on the CustomScan). Same expectation
  for join clauses: a non-leakproof higher-security join qual must
  not feed a parameterized CustomPath even when
  `join_clause_is_movable_to` would otherwise allow it.
- Movability regression on outer joins / lateral: with `lake LEFT
  JOIN other ON other.k = lake.k` (where `lake` is on the
  non-nullable side), the `ON` clause is not movable to the outer
  rel — the planner must NOT emit a parameterized CustomPath
  parameterized on `other`. Likewise for a query that introduces
  LATERAL references back to the lake rel: any joinqual whose
  source side has a lateral reference into `lake` must fail
  `join_clause_is_movable_to` and be excluded from
  parameterization.
- `rtoffset` regression: place the lake rel inside a subquery /
  CTE / `LATERAL` derived table so that
  `set_customscan_references` is invoked with `rtoffset != 0`.
  Run a query whose `custom_exprs[pushed]` references at least
  one scan-relation `Var`. Expectations:
  - `cscan->scan.scanrelid` after planning equals
    `pre_rtoffset_rti + rtoffset`.
  - The runtime walker over `custom_exprs[pushed]` resolves every
    scan-relation `Var` correctly using the post-rtoffset
    `Var.varno == cscan->scan.scanrelid` rule, NOT a
    `custom_private`-cached RTI.
  - The native predicate produced at `BeginCustomScan` matches
    the predicate produced by the same query without the
    enclosing subquery (modulo subquery-introduced Vars). A
    cached-RTI implementation would mis-classify outer Vars or
    drop the scan-rel Var from the predicate; the regression
    must catch that.
- DML / rowmark / system-column boundary. Each of the following
  must NOT plan a CustomScan over `lake`; EXPLAIN must show a
  built-in seqscan / indexscan instead:
  - `UPDATE lake SET v = v + 1 WHERE k = 1`
  - `DELETE FROM lake WHERE k = 1`
  - `MERGE INTO lake USING dim ON lake.k = dim.k WHEN MATCHED ...`
  - `UPDATE other SET v = lake.v FROM lake WHERE other.k = lake.k`
    Here `lake` is a `RTE_RELATION` *source*, not the result rel,
    so `bms_is_member(rel->relid, root->all_result_relids)` does
    NOT fire on `lake`. The actual rejection trigger is the
    rowmark gate: `preprocess_rowmarks` (`planner.c:2295`) adds a
    `PlanRowMark` for every non-target base rel in any
    UPDATE/DELETE/MERGE plan, so
    `get_plan_rowmark(root->rowMarks, lake_rti) != NULL` and the
    rowmark gate refuses CustomPath. The
    `all_result_relids`-based DML-target gate is the one that
    handles the *target*-side scans (UPDATE/DELETE/MERGE result
    rel, plus wCTE / DO INSTEAD-introduced result rels); keep
    both gates so the two cases are covered independently.
  - `SELECT * FROM lake WHERE k = 1 FOR UPDATE`
  - `SELECT * FROM lake WHERE k = 1 FOR SHARE`
  - `SELECT ctid FROM lake WHERE k = 1`
  - `SELECT xmin, xmax FROM lake WHERE k = 1`
  And these MUST plan a CustomScan when the rest of the query is
  pushable:
  - `SELECT tableoid FROM lake WHERE k = 1` (tableoid is
    supported via `tts_tableOid`)
  - `SELECT lake FROM lake WHERE k = 1` (whole-row Var, supported
    when the slot's `TupleDesc` matches the base rel's rowtype)
- EXPLAIN shows pushed/residual/recheck.
- Results match SeqScan fallback.

Iceberg integration tests:

- File pruning does not lose rows.
- Inexact pushdown plus residual filtering returns correct results.
- Exact pushdown matches PostgreSQL filtering.
- Parameter changes after rescan take effect.

**Open Items**

1. Which Iceberg expressions can be marked `Exact` cannot be assumed upfront. This must be tested expression by expression across types, NULL semantics, coercion, and collation behavior.

2. Before implementing `customscan/state.rs`, rename the existing
   TableAM scan descriptor helper
   `pg-lakebase-core/src/access/scan.rs::CustomScanDesc<T>` to a
   TableAM-specific name such as `TableAmScanDesc<T>`. It currently
   wraps `TableScanDescData` and is unrelated to PostgreSQL
   `CustomScan`; keeping both names would make future maintenance and
   grep output unnecessarily ambiguous.

**One-Sentence Summary**

`pg-lakebase-core` owns the CustomScan framework, PG Expr walker, pushed/residual/recheck split, and executor glue. Each AM provider only decides whether an expression can be pushed down, translates runtime PG Expr nodes into its native predicate, and opens its own scan backend.
