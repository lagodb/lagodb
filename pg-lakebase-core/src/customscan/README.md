# Provider CustomScan framework

This module is the generic CustomScan framework inside `pg-lakebase-core`.
Its job is to let lake table providers (today `pg-iceberg-am`, later
`pg-hudi-am` / `pg-delta-am`) push SQL `WHERE` filters down into their own
scan backend instead of relying on PostgreSQL to evaluate every qual above
an ordinary TableAM scan. The same framework also supplies modification scans
that extend the raw tuple with storage row identity.

This README is for maintainers. It explains *why* the module is shaped the
way it is and *how* a query flows through it. It deliberately avoids
function signatures and field-level layouts — read the source for those.

## Module boundaries

The implementation is organized by lifecycle rather than by Rust item type:

```text
customscan/
  planning/   planner hook, gates, path variants, costing, plan construction
  plan_data/  PostgreSQL-copyable plan carriers and scan-tuple contract
  execution/  executor callbacks, parameter resolution, state, EXPLAIN
  provider/   typed provider SPI plus the type-erased process registry
```

Dependencies should follow the query lifecycle. Planning may produce
`plan_data`; execution may consume it; neither layer owns provider runtime
state. The provider registry is planner infrastructure and is deliberately
separate from the typed SPI implemented by storage backends. The facade in
`mod.rs` keeps provider-facing paths concise without making the implementation
flat again.

## Why this exists

A normal PostgreSQL TableAM scan never sees ordinary `WHERE` quals; the
executor evaluates them in the node above the scan. That is fine for a heap,
but a lake table can do much better: it can prune whole files / row groups /
pages using table metadata, and in some cases apply row-level filtering
inside its reader. To do that the provider needs the predicate at scan time.

PostgreSQL's `CustomScan` is the extension point that makes this possible.
It gives us three carriers in the plan tree (`custom_exprs`,
`custom_private`, `custom_scan_tlist`) with semantics similar to the FDW
`fdw_*` fields, plus planner and executor callbacks we control. The
framework uses those carriers to move classified predicates from the planner
into the executor, where the provider turns them into a native scan
predicate.

The provider here is the lake access method (Iceberg, etc.). It is **not**
the PostgreSQL TableAM scan callback. Query scans may keep the TableAM path as
a fallback; Modify scans may not, because that path cannot emit storage row
identity outside the relation TupleDesc.

## End-to-end flow

```text
SQL WHERE
  -> PostgreSQL planner
  -> set_rel_pathlist_hook (core router)
       classify ScanPurpose as Query or Modify
       gate the relation (see "Gating")
       classify quals into pushed / residual / recheck
       enumerate a plain CustomPath plus parameterized variants
  -> add_path(CustomPath)
  -> PG picks a path on cost
  -> PlanCustomPath
       re-classify the final scan_clauses
       unwrap RestrictInfo to bare Expr
       collect PathTarget + target + residual + pushed/recheck dependencies
       Query: build a narrow Var-only custom_scan_tlist and base-attno mapping
       Modify: retain a relation-shaped slot and prune only storage columns
  -> CustomScan plan node
       plan.qual      = residual quals (PG evaluates these)
       custom_exprs   = pushed + recheck PG Exprs (PG rewrites, never runs)
       custom_private = copyObject-safe metadata + tuple-layout contract
  -> PG runs setrefs + nestloop param rewrite over plan.qual / custom_exprs
       projected query Vars become INDEX_VAR references into the narrow scan tuple
       Modify system Vars use standard slot tts_tid/tts_tableOid metadata
  -> executor Begin / ReScan
       translate pushed predicates -> provider native predicate
       resolve params, load current metadata, prune files
       Query opens its cursor; Modify waits for outer relation-state binding
  -> ExecScan
       fetch rows from the provider
       evaluate residual plan.qual + projection
  -> rows out
```

The central design rule: **PG Expr is the durable source of truth.**
Anything that is still a PG Expr (residual, pushed, recheck) must live in
`plan.qual` or `custom_exprs`, because PG runs setrefs and nestloop param
rewriting only over those fields. The framework does not freeze a native
predicate or a final file list into the plan; both are rebuilt at execution
time from the current snapshot and parameter values.

## Pushdown classification

Every candidate qual is classified into one of three outcomes. This is the
core semantic contract of the module:

- **ExactRowFilter** — the provider applies row-level, SQL-equivalent
  filtering. Because the filter is exact, the original expression is removed
  from residual `plan.qual`. A copy is kept in the recheck section for
  EPQ / future row-identity support. Runtime translation failure is a hard
  error; there is no silent fallback.

- **ConservativePruning** — the provider may only prune candidates with no
  false negatives (false positives allowed). It is safe for file / row-group
  / page pruning. The expression *stays* in residual `plan.qual` so
  correctness is preserved even if pruning is loose, and translation failure
  can simply drop the pushed copy.

- **Unsupported** — not pushed; stays entirely in residual `plan.qual`.

Composition across `AND` / `OR` / `NOT` follows from these guarantees:

- `AND` allows partial pushdown — each leaf is handled by its own contract.
- `OR` with row-filter semantics is all-or-nothing; pushing one side of an
  exact `OR` would be wrong. For pruning, an `OR` is only useful if every
  branch still yields a useful predicate after widening unsupported parts.
- PostgreSQL planner preprocessing owns `NOT` normalization before
  `RestrictInfo` construction. The framework classifies that resulting tree
  without flipping operators or `NullTest` nodes, or applying De Morgan a
  second time. A remaining `NOT` is pushed only when its child is one exact
  predicate with no residual; a pruning child is never negated because
  no-false-negative is not preserved under negation.

The classifier keys decisions on operator *identity* (operator OID plus
collations), not operator name, so that e.g. text equality under a
non-default collation is not mistaken for an exact filter.

## Two-phase split

Classification happens twice, on purpose:

1. **Path stage** (`set_rel_pathlist_hook`): classify to estimate
   selectivity and cost, and to decide which path variants to emit. This
   stage only exposes counts and selectivity to the provider, never raw
   `Expr` pointers.

2. **Plan stage** (`PlanCustomPath`): PG hands back the final
   `scan_clauses` (still as `RestrictInfo`, possibly reordered, possibly
   including parameterized join clauses). The framework unwraps them to bare
   `Expr`, drops pseudoconstant clauses (PG evaluates those via a gating
   `Result` node), and re-runs classification before writing the plan fields.

Doing the split again at plan stage is necessary because the clause list the
provider finally receives is not identical to what it saw during path
enumeration.

## Gating and scan purpose

Before any provider is consulted, the router declines the relation in cases
where a lake scan cannot honor PostgreSQL's contract. These gates live in
core so a provider cannot accidentally relax them:

- **Modification targets** — UPDATE / DELETE / MERGE result relations use
  `ScanPurpose::Modify`. Providers choose `TableAm` when their ordinary scan
  can populate a complete `tts_tid`, or `CustomRequired` when storage metadata
  cannot be supplied by the ordinary scan. `TableAm` retains standard paths
  and may also emit an optional provider CustomScan; costing chooses between
  them. During executor initialization ModifyTable eagerly creates each
  selected target SeqScan's descriptor under a one-shot binding; it is removed
  immediately after `scan_begin` accepts the target. This pins every partition's
  read context before writes begin, trigger/SPI scans cannot inherit it, and
  the per-row scan hot path is unchanged. The AM can then read private metadata
  such as Iceberg `_file`/`_pos` without exposing extra PostgreSQL columns.
  Query and CustomScan-based Modify reuse the same provider, classifier, projection,
  costing, scan specification, cursor, and runtime state; purpose only selects
  tuple layout and binding lifecycle.
- **Rowmarks** — `SELECT ... FOR UPDATE/SHARE` and unsupported concurrent
  EPQ/recheck paths remain rejected. A Modify scan's synthetic `ctid` is an
  executor-local token and does not provide heap row-lock or re-fetch semantics.
- **Unsupported system columns** — `ctid`, `xmin`, `xmax`, `cmin`, `cmax`.
  `tableoid` is supported, and whole-row Var is supported only when the slot
  matches the base relation rowtype exactly.
- **Non-storage / non-base relations** — partitioned parents, foreign
  tables, views, sequences, joinrels, upperrels. v1 handles base relation
  scans only.
- **Security ordering** — a clause may only be pushed if it is safe to
  evaluate early (leakproof, or no higher than the relation's minimum
  security level). This preserves RLS / security-barrier ordering. For join
  clauses, movability gates compose with this.

When a Query gate fails, the framework leaves PG's default paths in place.
For a `CustomRequired` Modify, failure to create a provider path is an error;
falling back to SeqScan would silently lose storage row identity. A `TableAm`
Modify retains PostgreSQL's ordinary scan paths and costs provider CustomScan
paths as optional projection/pushdown alternatives.

## Parameterized paths

The framework emits two kinds of base-relation paths:

- A **plain** path with no join-driven parameters (it may still be
  parameterized purely by lateral relids).
- One **parameterized** path per distinct set of outer relations that lets
  an additional safe join clause become an AM-side predicate.

Parameterized paths are what let the lake scan sit on the inner side of a
nested loop and filter using the current outer-tuple values. PostgreSQL only
provides the join clauses (`ppi_clauses`) when a path is parameterized, so
without these variants there would be nothing extra to push. Path
enumeration, costing, and the safety gates are owned by core; the provider
only shapes cost and fills its private metadata, and may decline any variant.

Costing keeps output cardinality (`path.rows`) aligned with PostgreSQL's own
estimate and expresses pruning savings only through scan-volume cost terms.
Conflating "rows skipped by pruning" with output rows would mislead join
ordering and mis-price the path against seqscan.

## Plan field layout

The plan node carries three things, each with a strict role:

- `plan.qual` — residual expressions. PG's `ExecScan` evaluates these
  automatically. ConservativePruning expressions also live here.
- `custom_exprs` — the pushed expressions followed by the recheck
  expressions. PG never runs these, but it *does* apply setrefs and nestloop
  param rewriting to them, which is exactly why they must be PG-Expr-shaped.
- `custom_private` — only copyObject-safe metadata (OIDs, integers, strings,
  lists, flags): the provider id, the scan relation OID, section counts,
  per-expression contract tags, pre-resolved column metadata, and provider
  private data. It must never contain raw pointers, native predicates, a
  final file list, or Exprs smuggled in as JSON.
- `custom_scan_tlist` — a Var-only description of the raw provider tuple.
  Ordinary output Vars come first in targetlist order; residual, pushed, and
  recheck-only dependencies are appended as resjunk entries. Vars referenced
  from `SubPlan.testexpr` and `SubPlan.args` participate in the same mapping.
  Whole-row, supported system-column, or otherwise unprovable shapes fall back
  atomically to a NIL tlist and the relation rowtype.

A subtle but important rule: `set_plan_references` rewrites Var range-table
indexes inside `custom_exprs` but does **not** walk `custom_private`. So the
range-table index is never cached in `custom_private`. Relation-shaped scans
resolve `(scanrelid, attno)` directly; projected scans resolve
`(INDEX_VAR, resno)` through the encoded `resno -> base attno` layout, then
use stable column metadata (relation OID, attribute number, type, collation).

## Parameter model

Plan-time `Param` nodes and runtime parameter values are kept strictly
separate. At execution time the framework resolves both external
(prepared-statement) and exec (nestloop / subplan / InitPlan) parameters,
materializing any pending InitPlan output before reading a value.

In nested loops, PostgreSQL re-scans the inner side for every outer tuple and
marks which params changed. The framework only re-translates the predicate
and re-prunes when a *referenced* param actually changed; otherwise it just
reopens the cursor. This avoids re-pruning manifests on every inner rescan.

## Executor and slot contract

For an ordinary base scan, the planner emits a narrow `custom_scan_tlist` and
PostgreSQL builds `ss_ScanTupleSlot` from it. Core stores the matching
base-attno map in `custom_private`, validates the tlist/layout/slot widths at
Begin, and exposes the actual scan descriptor to the provider. The provider
may read storage fields in a different order, but it must map each base
attribute to its compact scan destination explicitly.

When planning cannot prove a projected layout safe, the tlist remains NIL and
the scan slot uses the base relation rowtype. Whole-row and tableoid queries
use this fallback; tableoid can still carry a referenced-user-column storage
subset, while whole-row requires all live user columns. In both modes
`SlotColumns` derives its slice length only from the live
`tts_tupleDescriptor`; a provider can never supply `natts`. Every cell
observable by residual quals or final projection must be written.

Memory and interrupt handling mirror the FDW model: the per-tuple memory
context is switched in before the provider's row producer runs, and
long-running IO inside the provider must check for interrupts itself. The
recheck path mirrors PostgreSQL's `ForeignRecheck`. In v1 the recheck path is
not normally reachable for lake tables (the rowmark gate prevents it), but
the recheck `ExprState` is still wired up as defense in depth and for future
row-identity support.

## EXPLAIN

PostgreSQL does not print `custom_exprs` automatically; only residual
`plan.qual` shows up as `Filter`. The framework's own EXPLAIN callback prints
the pushed and recheck information. Plain EXPLAIN stays compact (a single
combined pushed line); VERBOSE prints the provider name and per-class
predicate lines. Pushed and recheck are always labeled separately even though
they may be stored as duplicate copies. ConservativePruning expressions
intentionally appear both as a pushed line and in PG's residual `Filter`,
because the executor must still re-evaluate them for correctness.

## Division of responsibility

Core owns the framework: gating, path enumeration and costing, the
pushed / residual / recheck split, `RestrictInfo` unwrapping, the plan field
layout, parameter resolution, and the executor glue (including translating
the pushed predicates into the call sequence the provider implements).

A provider owns only the parts that are specific to its storage format:
whether it supports a relation, how it classifies a parsed predicate leaf,
how it turns a classified predicate into its own native predicate at runtime,
and how its scan backend prunes and reads data. For Iceberg, that means the
predicate is translated into an `iceberg_lite` predicate and fed to the table
scan builder; the existing TableAM scan stays as a fallback.
