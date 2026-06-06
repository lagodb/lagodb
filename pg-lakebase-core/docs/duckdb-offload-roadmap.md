# DuckDB Offload Roadmap (pg-lakebase-core CustomScan extension)

**Status**: Roadmap / design draft. No implementation exists yet. This
document proposes how `pg-lakebase-core` grows a **query-fragment offload**
capability on top of the existing single-relation **filter pushdown**
CustomScan framework (`customscan/README.md`), targeting a **standalone
DuckDB server process** as the execution engine. Providers such as
`pg-iceberg-am` opt in; the capability lives in core so future
`pg-hudi-am` / `pg-delta-am` reuse it.

This draft is faithful to the existing framework contracts in
`customscan/README.md` and `customscan/provider.rs`. Where it extends them,
it says so explicitly and marks the seam.

---

## 1. Background & Goals

### 1.1 Current State

`pg-lakebase-core`'s current CustomScan framework does **single-relation
filter pushdown**:

```text
SQL WHERE
  -> set_rel_pathlist_hook router
  -> classify pushable predicates into pushed / residual / recheck
  -> CustomPath (still a single base-rel scan)
  -> Begin/ReScan translates PG Expr into a provider-native predicate
  -> provider.next_slot emits Row by row (AM reads files + file-level pruning)
```

In other words, **scanning and predicate pruning** are pushed down to the
provider (the Iceberg AM reads locally cached Parquet), but **join /
aggregate / sort / limit still run inside the PostgreSQL executor**. This is
a correct first step, but it does not yet exploit the columnar vectorized
engine.

### 1.2 Goals

Introduce a **new, parallel pushdown level** — hand a "fully pushable query
fragment" (including join / agg / sort / limit) to a **standalone DuckDB
server process**, with PostgreSQL receiving only the final result rows. This
is exactly the route `pg_lake` takes (a standalone `pgduck_server` + libpq +
`read_parquet`), in contrast to `pg_duckdb`'s in-process model.

We deliberately choose the **standalone-process** model; rationale in §2.2.

### 1.3 Non-goals

- **No in-process DuckDB.** We do not link DuckDB into the PG backend, so we
  give up `pg_duckdb`'s ability to "have DuckDB call back into the PG
  executor to read heap tables" (see the limitation in §6).
- **No replacement of the existing filter-pushdown framework.** Query offload
  is the *upper layer* of it, not a replacement; when a whole query cannot be
  offloaded, we fall back to the existing single-relation pushdown, and then
  to the TableAM seqscan.
- **v1 does not push down writes** (INSERT/UPDATE/DELETE still go through the
  existing DML path).
- **v1 does not push a mixed lake⋈heap join down to DuckDB.** Note the precise
  wording: this means "pulling the heap side into DuckDB to do the join
  there." **The lake-table subtree itself (including lake⋈lake joins and
  aggregates over lake tables) can still be pushed down** — see the
  "partial pushdown" clarification in §6. v1 scope here matches `pg_lake`.
- **No DuckDB server lifecycle management inside this module.** It reuses the
  bgworker supervisor pattern of `worker/storage` as a separate subsystem
  (see §4.1).

---

## 2. Architecture Decisions

### 2.1 Three-level Pushdown Model

```text
┌─────────────────────────────────────────────────────────────┐
│ Level 2: Query-fragment offload  (new in this roadmap)        │
│   Pushable SELECT fragment -> DuckDB server                   │
│   2a whole query: all-lake tables -> offload join/agg/sort/lim│
│   2b subtree:     mixed with heap -> offload the largest pure  │
│                   lake subtree (lake⋈lake join, agg over lake),│
│                   return result to PG                          │
│   Entry: planner_hook (2a) + join/upper pathlist hook (2b)     │
├─────────────────────────────────────────────────────────────┤
│ Level 1: Single-relation filter pushdown  (exists)            │
│   WHERE predicate -> provider-native predicate, file pruning  │
│   Entry: set_rel_pathlist_hook + base-rel CustomScan          │
├─────────────────────────────────────────────────────────────┤
│ Level 0: TableAM seqscan fallback  (exists)                   │
│   provider.next_slot row by row, upper executor does the rest │
└─────────────────────────────────────────────────────────────┘
```

**Priority**: the planner first tries **Level 2a** (whole-query offload);
when the whole query mixes in a heap table / an unpushable operator, it falls
back to **Level 2b** (offload only the largest pure-lake subtree; the join/agg
involving the heap side stays in the PG executor); below that, **Level 1**
(single-relation pushdown); and finally **Level 0**. This is isomorphic to
`pg_lake`'s dual path: `EnableFullQueryPushdown` corresponds to 2a, and the
FDW's `GetForeignJoinPaths` / `GetForeignUpperPaths` correspond to 2b.

> **Clarification on "partial pushdown" (cross-ref §6)**: Level 2b is the
> key. "The query touches a heap table" does **not** mean "DuckDB cannot be
> used at all." The only thing that fails is "pulling the heap side into
> DuckDB to do the join together" (impossible across processes — see §6). The
> lake subtree — including lake⋈lake joins and aggregates over lake tables —
> **is still offloaded to DuckDB as one block**, and only its result is handed
> to PG to do the final join with the heap table. `pg_lake` does exactly this.

### 2.2 Why a Standalone DuckDB Server Process

| Dimension | Standalone process (our choice) | in-process (pg_duckdb) |
|---|---|---|
| Isolation | DuckDB OOM / crash does not take down the PG backend | can take down the backend |
| Resource governance | independent memory/thread budget for DuckDB | competes with PG for the same process resources |
| Sharing across backends | one server serves all backends; shared cache/connection pool | one DuckDB per backend |
| Heap join pushdown | **cannot** have DuckDB call back into the PG executor to read heap tables (see §6); lake subtree still pushed down | can pull heap tables into a DuckDB join |
| Deployment | one more process to manage | no extra process |

Our product positioning is a **lakehouse (querying Iceberg/Parquet files)**,
not "accelerating PG heap-table analytics," so the cost of "heap-table join
cannot be pushed down" is acceptable (the lake subtree is still pushed down,
see §2.1 Level 2b); the isolation and resource-governance benefits matter
more.

### 2.3 How Data Enters DuckDB: Let It Read Local Cache Files Directly, Don't Feed Arrow

Following the earlier discussion, under the three premises **standalone
process + standard Parquet + an existing local cache**:

- **Scan-input side**: let DuckDB use `read_parquet('/cache/.../f.parquet')`
  to directly read the files that `pg-lakebase-storage` has already cached
  locally. This preserves DuckDB's:
  - predicate/projection pushdown into Parquet (row-group / page-level
    min/max pruning),
  - morsel-driven parallel scan,
  - streaming execution + spill.
  Decoding the files into Arrow batches first and feeding them across the
  process boundary would **lose all of the above** plus add one serialization
  step. So the scan-input side **does not use Arrow**.
- **Result-return side**: this is where Arrow earns its keep (see §3.4).

Two ways to wire the cache in:
- **Simple path (v1)**: at execution time the provider splices the
  locally-cached file paths straight into `read_parquet([...])`. Zero DuckDB
  extension work.
- **Thorough path (later)**: install a custom FS extension into the DuckDB
  server that routes `lakebase://` URL I/O to `pg-lakebase-storage` (socket +
  pread / FD passing), making the cache fully transparent to DuckDB and
  supporting on-demand fetch on a miss. `pg_lake` follows the same idea with
  its `duckdb_pglake` caching file system
  (`duckdb_pglake/src/fs/caching_file_system.cpp`), though it wraps the
  object-store FS and controls caching via a `nocache` URL prefix rather than
  delegating to a separate storage process; our `lakebase://` routing to
  `pg-lakebase-storage` is an extension of that idea.

---

## 3. Query Lifecycle

### 3.1 Planning: Two Pushdown Entries and How They Divide Work

Level 2 has **two independent planner entries**, corresponding to 2a / 2b
in §2.1.

#### 3.1.1 Whole-query entry: `planner_hook` (Level 2a)

```text
planner_hook(parse, ...)
  1. If offload is off (GUC) or there is no lake table -> hand to downstream planner
  2. copyObject(parse) to keep a pristine copy
  3. Call the downstream planner to produce local_plan (always have a usable
     plan; this step internally fires set_join_pathlist_hook /
     create_upper_paths_hook, i.e. Level 2b and Level 1)
  4. FullQueryIsOffloadable(original_query)?
       - CMD_SELECT, no modifying CTE, no FOR UPDATE
       - every RTE_RELATION is a lake table of this provider (no heap / system table)
       - every expression is shippable (types/functions/operators DuckDB supports, see §5)
     yes -> build a whole-query offload plan and replace local_plan
     no  -> return local_plan (its pure-lake subtrees were already pushed down
            by Level 2b; the heap portion is handled by the PG executor)
```

This borrows `pg_lake`'s `LakeTablePlanner` / `FullQueryIsPushdownable`
structure.

#### 3.1.2 Subtree entry: join / upper pathlist hooks (Level 2b)

When the whole query is not pushable (it mixes in a heap table, etc.), the
**pure-lake subtree must still be pushed down**. This relies on two **new**
path-level hooks, which live in the planner's pathlist phase alongside the
existing `set_rel_pathlist_hook` but target different RelOptInfo types:

```text
set_join_pathlist_hook(joinrel, outerrel, innerrel, ...)   // Level 2b: join pushdown
  - generate an offload join CustomPath only when the "pushdown roots" of both
    outerrel and innerrel belong to lake tables of the same provider (both sides
    lake; corresponds to pg_lake's foreign_join_ok)
  - if either side contains a heap table -> do not generate; PG uses an ordinary
    join path (the heap join goes back to PG)

create_upper_paths_hook(stage=UPPERREL_GROUP_AGG, input_rel, ...)  // Level 2b: agg pushdown
  - generate an offload aggregate CustomPath only when input_rel is entirely lake
    (a base rel or an already-pushed-down lake join)
```

This is the CustomScan-world counterpart of `pg_lake` FDW's
`GetForeignJoinPaths` / `GetForeignUpperPaths`. **Core capability to add**:
the existing framework has only `set_rel_pathlist_hook` (base rel); Level 2b
requires core to also implement routers for the join and upper hooks (see the
conflict design in §5.4).

> **Key reuse point**: the three hooks (rel / join / upper), the whole-query
> decision, and Level 1's `classify_predicate` share the same "expression
> shippability" knowledge. Not sharing it would let the layers disagree about
> "what can be pushed down." `expr/shippable.rs` must be extracted (see §5.1).

### 3.2 Deparse: PG Query Tree -> DuckDB SQL (the placeholder trick)

Deparse the `Query` tree back into SQL text. Lake tables are **not written as
their real table names**, but as a placeholder
`__lakebase_read_table(relid, unique_id)`:

```sql
-- produced at deparse time (at plan time, files are unknown):
SELECT category, sum(amount)
FROM __lakebase_read_table('public.sales'::regclass, 1)
WHERE sale_date > $1
GROUP BY category
ORDER BY category
```

At execution time the placeholder is replaced by the real read call (the file
list is only known now):

```sql
-- produced at execution time (inject the snapshot-decided file list):
SELECT category, sum(amount)
FROM (SELECT * FROM read_parquet(['/cache/sales/f1.parquet',
                                  '/cache/sales/f2.parquet']))
WHERE sale_date > $1
GROUP BY category
ORDER BY category
```

The reason for the two-stage approach is the same as Level 1: **which files
to read depends on the runtime snapshot + file pruning**, and must not be
frozen at plan time. This is consistent with the existing framework's iron
rule that "the plan tree never freezes a native predicate / file list" — what
the offload plan freezes is only a **SQL template string + copyObject-safe
metadata**, and the file list is injected at Begin/ReScan.

> Deparse reuse: today there is **no vendored ruleutils** in
> `pg-lakebase-core`. Level 1 EXPLAIN deparses single expressions by calling
> PostgreSQL's own `pg_sys::deparse_expression` (in `customscan/explain.rs`),
> which only handles individual `Expr` nodes. Query offload needs
> **whole-query** deparse, so this proposal must port a whole-query deparser —
> either `pg_lake`'s `deparse_ruleutils.c` or a `pgduckdb_get_querydef`-style
> deparser (the latter from `pg_duckdb`) — most likely by vendoring a
> `ruleutils` copy into the crate. This is the **highest-effort,
> highest-risk** piece of this proposal; see §7 Risks.

### 3.3 Execution: the offload CustomScan

Whole-fragment pushdown produces a **new CustomScan provider variant**
(distinct from Level 1's base-rel scan). It is not a scan on a base relation
but a **"virtual" scan node** (similar to `pg_duckdb`'s `CreatePlan`: a single
RTE, with `custom_scan_tlist` describing the output columns of the DuckDB
prepared statement):

```text
BeginOffloadScan
  - CreateOffloadSnapshot(rte_list)         // take the PG transaction snapshot
  - for each lake-table RTE: prune files to get the path list (reuse Level 1 pruning)
  - ReplaceReadTablePlaceholders(sql_template, snapshot)  // inject the file list
  - conn = DuckDbServerPool::acquire()      // §4
  - conn.send_query_with_params(sql, params)

OffloadNext  (= ExecScan's accessMtd)
  - stream the next row from the DuckDB connection
  - convert to a PG TupleTableSlot (reuse core's Row / TupleSlotWriter)
  - return the slot; clear the slot at EOF

EndOffloadScan
  - return the connection to the pool
```

Note: above the offload scan there is **no** residual qual / locally-done
join — because the whole fragment is pushed down, what DuckDB returns is the
final result. This differs from Level 1 (where the upper layer still does
join/agg).

### 3.4 Result Return: wire protocol -> evolving to Arrow

- **v1**: read results from the DuckDB server over the PG wire protocol (like
  `pg_lake`'s libpq single-row / `TRANSMIT` CSV batching). Simple, runs end to
  end immediately.
- **later**: DuckDB can natively output Arrow; have the server stream results
  back in batches via **Arrow IPC**, and have the PG side write the Arrow array
  straight into a `TupleTableSlot`, skipping CSV text encoding. Core's
  `batch.rs` buffering abstraction can be wired in (it currently targets DML
  writes; the read side can add a symmetric Arrow→Slot path).

---

## 4. DuckDB Server Subsystem (new in core)

### 4.1 Process Lifecycle: reuse the storage bgworker pattern

DuckDB server process management **copies** the supervisor pattern of
`worker/storage` (`worker/storage/README.md`):

```text
pg-lakebase-core/src/worker/duckdb/
  mod.rs          // init_for_extension(library_name) + bgworker entry
  gucs.rs         // GUCs at Postmaster + Sighup scope
  config.rs       // GUC snapshot -> DuckDbServerConfig
  supervisor.rs   // bgworker main-thread lifecycle / signals / start-stop
  logging.rs      // reuse storage's tracing->PG log bridge (can be factored out)
```

Key points (aligned with the storage worker):
- the AM-layer `_PG_init` calls
  `worker::duckdb::init_for_extension("pg_iceberg_am")` to register a static
  bgworker;
- `set_restart_time(None)`; after a crash, queries error rather than silently
  reconnecting (v1);
- snapshot GUCs before handing work to the subprocess; FFI stays on the main
  thread;
- when `pg_lakebase.duckdb_server_enabled = false`, return immediately and do
  not register the worker.

> Whether the subprocess is a DuckDB server binary or a Rust process embedding
> DuckDB is an independent decision:
> - Option A: **vendor `pg_lake`'s `pgduck_server`** (C; already implements PG
>   wire protocol + DuckDB). Least effort, but introduces a C dependency.
> - Option B: **write our own Rust DuckDB server** (using the `duckdb` crate +
>   a self-implemented PG wire, or Arrow Flight directly). More control, more
>   work.
> For v1, do Option A first to get end-to-end working, then evaluate B.

### 4.2 Connection Management: reuse the client pattern

The DuckDB client on the PG backend side **copies** `pg_lake`'s `client.c`
pattern, but implemented in Rust:

```text
pg-lakebase-core/src/customscan/duckdb/
  pool.rs         // DuckDbServerPool: manage connections by xact/subxact, release via xact callback
  client.rs       // send_query_with_params / wait_for_result (with CHECK_FOR_INTERRUPTS)
  result.rs       // wire row / Arrow batch -> core::tuple::Row
  deparse.rs      // whole-query deparse + read_table placeholder replacement
  shippable.rs    // expression shippability (shared with Level 1 classify_predicate, see §5)
```

Connections use `WaitLatchOrSocket` to stay responsive to interrupts/cancel
while awaiting results, and are released uniformly at transaction end — these
battle-tested `pg_lake` patterns port directly. Transport uses a Unix domain
socket (same as storage).

---

## 5. Integration with the Existing Framework (Integration Seams)

This is the heart of the proposal — **maximize reuse, minimize what's new**.

### 5.1 Sharing Shippability Knowledge (mandatory reuse)

Level 1's `LakebaseCustomScanProvider::classify_predicate` already answers whether a
parsed leaf can be pushed down via `QualPushdownDecision::Pushable { contract, costing }`,
with `PushdownContract::ExactRowFilter` (row-level SQL-equivalent filter on the
normal scan path) or `PushdownContract::ConservativePruning` (conservative
pruning: no false negatives, false positives allowed; residual `plan.qual`
keeps correctness) — plus a `PushdownCosting` tier
(`CostedPruning` / `UncostedBestEffort`) that controls whether the pushed
expression is allowed to discount scan-volume cost. Level 2's whole-query shippability
decision **must be built on the same knowledge**, or the two layers will disagree
about "what can be pushed down."

Extract a shared layer:

```rust
// pg-lakebase-core/src/expr/shippable.rs (new, shared by both layers)
pub trait Shippability {
    /// Whether a single operator/function/type can be evaluated on the DuckDB side.
    fn is_shippable_operator(opno: pg_sys::Oid, ...) -> bool;
    fn is_shippable_function(funcid: pg_sys::Oid) -> bool;
    fn is_shippable_type(typid: pg_sys::Oid) -> bool;
}
```

- Level 1 `classify_predicate` uses it to judge a single predicate;
- Level 2 `FullQueryIsOffloadable` uses it to walk the whole query tree
  (targetlist, join qual, group/order, having).

The provider (Iceberg AM) implements `Shippability` once, and both layers
consume it. This corresponds to `pg_lake`'s `shippable_builtin_functions.c` /
`shippable_builtin_operators.c` (plus `shippable_spatial_*.c` and the FDW-side
`fdw/shippable.c` with `is_shippable`).

**For the concrete list of which query shapes / operators / types / subtrees
can be pushed down, see the full inventory in §5.6.**

### 5.2 Sharing File Pruning (mandatory reuse)

Level 1 already has "snapshot + predicate -> file path list" pruning
(corresponding to the Iceberg AM's catalog scan; see the
`CreatePgLakeScanSnapshot` comparison in `pg-iceberg-am/src/catalog/README.md`).
When Level 2 injects the file list at Begin time it **calls the same pruning
entry**; only the consumer changes — from "provider native predicate" to
"splice into `read_parquet([...])` SQL."

Lift pruning into a provider-trait method that both Level 1 and Level 2 call:

```rust
// extend the provider trait (new method; Level 1 internals also switch to it)
fn prune_files(
    ctx: &PruneContext<'_>,     // snapshot + already-classified pushed predicates
) -> PrunedFileSet;             // { data_files: Vec<Path>, delete_files: Vec<Path>, stats }
```

### 5.3 Provider Trait Extension (additive, backward-compatible)

The existing `LakebaseCustomScanProvider` (`provider.rs`) focuses on base-rel
scans. The capabilities query offload needs are layered on **as an optional
extension trait**, without breaking existing providers:

```rust
/// Optional: a provider declares that it supports query-fragment offload.
/// Providers that do not implement this trait only participate in Level 1.
pub trait DuckDbOffloadProvider: LakebaseCustomScanProvider {
    type Shippability: crate::expr::shippable::Shippability;

    /// The DuckDB-side function-call template for reading this relation
    /// (read_parquet / read_csv / ...).
    fn read_call_template(ctx: &ReadCallContext<'_>) -> ReadCall;

    /// Reuse prune_files from 5.2; no new method needed.
}
```

The router attempts Level 2 (both whole-query 2a and subtree 2b) only for
relations whose provider implements `DuckDbOffloadProvider`. Providers that do
not implement this trait participate only in Level 1.

### 5.4 Relationship with Existing Hooks + Conflict Design

This is the part of the proposal most prone to conflicts; spell it out item by
item.

#### 5.4.1 The Full Hook Picture

| Hook | Status | Level | Responsibility | RelOptInfo type |
|---|---|---|---|---|
| `planner_hook` | **new** | 2a | replace the whole plan if the whole query is pushable | the whole `PlannedStmt` |
| `set_join_pathlist_hook` | **new** | 2b | lake⋈lake join pushdown | join rel |
| `create_upper_paths_hook` | **new** | 2b | agg/group pushdown over lake | upper rel |
| `set_rel_pathlist_hook` | exists, **untouched** | 1 | single-relation filter pushdown | base rel |
| `utility_hook` / `object_access_hook` | exists | — | DDL / permissions | not involved |

All new hooks follow the existing `install_set_rel_pathlist_hook`
`OnceLock<prev_hook>`-captures-prev chaining convention (already established in
`hook.rs`), ensuring coexistence with other extensions.

#### 5.4.2 Why These Hooks Do Not "Take Over the Same Relation Twice" — layered by RelOptInfo type

Key fact: these PostgreSQL pathlist hooks **act on different RelOptInfo types
and naturally do not overlap**:

- `set_rel_pathlist_hook` fires only on **base rels** (`RELOPT_BASEREL`).
- `set_join_pathlist_hook` fires only on **join rels**.
- `create_upper_paths_hook` fires only on **upper rels** (grouping/agg,
  window, distinct, ordered, etc. stages).

So Level 1 (base), Level 2b-join, and Level 2b-agg each look only at their own
kind of RelOptInfo and **never generate paths for the same RelOptInfo
simultaneously**. What they generate are **candidate paths** in PG's
`pathlist`, and **PG's cost comparison picks one** in the end. This is the
fundamental conflict-resolution mechanism: not "whoever takes over first
wins," but "everyone calls add_path, and the planner picks the cheapest."

Example `SELECT category, sum(amount) FROM lake_a JOIN lake_b USING(category) GROUP BY category`:

```text
base rel  lake_a -> set_rel_pathlist_hook:   Level1 scan path + seqscan path
base rel  lake_b -> set_rel_pathlist_hook:   Level1 scan path + seqscan path
join rel  a⋈b    -> set_join_pathlist_hook:  Level2b offload-join path
                                              + PG hashjoin/mergejoin path
upper rel group  -> create_upper_paths_hook: Level2b offload-agg path
                                              + PG HashAggregate path
outermost planner_hook: whole query is all-lake -> hits 2a,
                  replaces the entire tree above with one whole-query offload plan
```

Note the last line: **when 2a hits, it "absorbs" the work of 2b/Level1.** This
is by design, not a conflict — `planner_hook` runs **after**
`standard_planner` returns, and it picks one of {the whole-query offload plan,
`local_plan` (which already contains the 2b/Level1 paths)} to substitute. If
2a wins, the whole fragment goes to DuckDB as a single SQL, which is the most
efficient; if 2a does not apply (heap mixed in), `local_plan` is kept and its
2b/Level1 paths remain in effect.

#### 5.4.3 Deduplicating Nested Pushdown: avoid "pushing a subtree down twice"

The conflict that truly needs care is **nesting**: after `lake_a ⋈ lake_b` is
pushed down by 2b into one offload CustomPath, if an agg is wrapped on top, the
2b-agg must not treat the "already-pushed-down join" as a base input and
re-splice the SQL. The solution (borrowing the FDW `fdw_private` passing
trick):

- The offload join / agg CustomPath carries an **`OffloadFragment`** in
  `custom_private` (the SQL template of the already-pushed-down subtree + the
  lake tables involved + the file-pruning handle).
- When an upper hook (2b-agg) finds that the optimal input of an input rel is
  already an offload CustomPath, it **reuses and wraps** that
  `OffloadFragment` (wrapping `SELECT ... GROUP BY` around it) instead of
  re-enumerating the underlying tables.
- This forms a chain of "the offload fragment growing layer by layer,"
  ultimately materialized into one DuckDB SQL in `CreateOffloadPlan`. This is
  consistent with `pg_lake`'s use of `read_table` placeholders + layered
  deparse.

To decide "is the input already an offload path of this framework," compare
the node's `CustomScanMethods` pointer (this provider's method table in the
registry) — unique and safe.

#### 5.4.4 Coordination with Level 1 base-rel Paths

For 2b-join to push down `lake_a ⋈ lake_b`, both base rels must be "pushable
as lake." Here we directly reuse the information Level 1 has already computed
on the base rels:

- During the `set_rel_pathlist_hook` phase, Level 1 has already done
  provider matching + predicate classification for each lake base rel. 2b
  reads this result (attached to the rel's private data, similar to the FDW's
  `fpinfo->pushdown_safe`) as the criterion for "is this side pure lake, does
  it have a local cond" — corresponding to `pg_lake`'s `foreign_join_ok`
  checking `fpinfo_o->pushdown_safe && fpinfo_i->pushdown_safe` with no
  `local_conds`.
- If either side has a "residual condition that must be evaluated on the PG
  side" (an unshippable WHERE), the join is not pushed down.

> Suggested v1 implementation order: do only 2a (whole query) + Level 1 first,
> and **defer 2b to Phase 3**. That way the first version adds only one hook,
> `planner_hook`, with the smallest conflict surface; the 2b join/upper hooks
> come after the whole-query path is stable (see the phased plan in §8).

#### 5.4.5 Others

- **utility_hook / object_access_hook (exist)**: unaffected.
- **registry (exists)**: offload provider registration follows the single
  registry / router style (the README explicitly argues against ParadeDB's
  per-type hook chaining). If a relation is claimed by more than one provider,
  fail closed (consistent with the existing framework).

### 5.5 EXPLAIN

The offload CustomScan's `ExplainCustomScan` reuses Level 1's explain
infrastructure (`explain.rs`), adding: the SQL template pushed to DuckDB, the
number of files / bytes matched, and (optionally, by querying the server) the
DuckDB-side EXPLAIN. At `EXPLAIN` time the `read_table` placeholder is replaced
with the relation name rather than the file list (mirroring `pg_lake`'s
`EXPLAIN_REQUESTED` flag). For Level 2b partial pushdown, EXPLAIN should
clearly mark "which subtree went into DuckDB, and which part stayed in PG."

### 5.6 The Pushdown Surface: which queries / subtrees can be offloaded to DuckDB (detailed inventory)

§5.1 stated that "shippability knowledge must be shared"; this section gives
the **concrete inventory** — compiled strictly against `pg_lake`'s
`ProcessNotShippableExpressionWalker` + `is_shippable` +
`GetDuckDBTypeForPGType`. The decision follows a **fail-closed** principle:
**only what is listed is pushed down; everything else falls back** (the whole
query falls back to subtree/Level 1; the subtree falls back to PG). When in
doubt, do not push down — falling back is always semantically safe.

#### 5.6.1 Query (statement) level: admission gate for whole-query 2a

| Dimension | Pushable | Not pushable (-> fall back) |
|---|---|---|
| Command type | `SELECT` | `INSERT/UPDATE/DELETE/MERGE` (v1 goes through the existing DML path) |
| CTE | non-modifying `WITH` (ordinary subquery-style CTE) | `hasModifyingCTE` (a write inside WITH) |
| Row locks | none | `FOR UPDATE/SHARE` (`hasForUpdate`) |
| target list | at least one non-resjunk column | empty target / resjunk-only (DuckDB rejects an empty SELECT list) |
| `LIMIT` | plain `LIMIT/OFFSET` | `LIMIT ... WITH TIES` (DuckDB lacks this feature) |
| Cursor | ordinary execution | `CURSOR_OPT_SCROLL` (scrollable cursor, incompatible with streaming) |
| Set operations | `UNION [ALL]` / `INTERSECT` / `EXCEPT` (when every branch is pushable) | any branch not pushable |
| Subquery / SubLink | `IN` / `EXISTS` / scalar subquery, all internally pushable | the inside touches a heap table or an unpushable operator |
| Window functions | ordinary window functions | `unnest()` appearing in GROUP BY or in a window simultaneously (DuckDB limitation) |

> These correspond to `pg_lake`'s `FullQueryIsPushdownable` and the Query-node
> checks in the walker (`limitOption == LIMIT_OPTION_WITH_TIES`,
> `TargetListHasOnlyResjunk`, `hasModifyingCTE`, `hasForUpdate`,
> `HasGroupByWithUnnest`, etc.).

#### 5.6.2 RTE (FROM item) level: which sources can enter the pushdown subtree

| RTE kind | Pushable | Not pushable (-> fall back) |
|---|---|---|
| `RTE_RELATION` | a lake table of this provider | **heap table / system table / other AM** (2a fails; triggers 2b to offload only the lake subtree) |
| Inheritance / partitioning | all children are lake tables | any inheritance child is not lake (`AllInheritorsAreLakeTable` is false) |
| `RTE_SUBQUERY` | the subquery is pushable as a whole | the inside of the subquery is not pushable |
| `RTE_JOIN` | ordinary join | `joinmergedcols > 0` with an alias (DuckDB alias-handling bug, see pg_lake comments) |
| `RTE_FUNCTION` | a single set-returning function and the function is shippable | multi-function `ROWS FROM(...)`; `WITH ORDINALITY`; function not shippable |
| `RTE_VALUES` | `VALUES` list (after column-name alignment) | — |
| `RTE_CTE` | references a pushable CTE | references a non-pushable CTE |
| `RTE_NAMEDTUPLESTORE` | — | always not pushable (PG internal, e.g. trigger transition tables) |
| `RTE_TABLEFUNC` | — | always not pushable (e.g. `XMLTABLE`) |

> Note: column-name alignment is what `pg_lake`'s `AddMissingRTEAliasaes`
> handles — DuckDB and PG auto-generate different column names for
> VALUES/subqueries (`column1` vs `col0`), so an explicit alias must be added
> before pushdown.

#### 5.6.3 Expression / node level: pushability of each node inside the subtree

**Pushable node types** (the provider's `Shippability` judges each internal
OID):

- Leaves: `Var` (user columns only, `varattno > 0`), `Const`, `Param` (`$n` /
  PARAM_EXEC)
- Operators: `OpExpr` / `DistinctExpr` / `NullIfExpr` / `ScalarArrayOpExpr`
  (`x = ANY(...)`) / `RowCompareExpr` — their `opno` must be shippable
- Functions: `FuncExpr` — `funcid` must be shippable; set-returning functions
  are allowed only in FROM (`unnest` is the exception, see 5.6.1)
- Aggregates: `Aggref` — `aggfnoid` shippable, and a non-default sort operator
  inside `ORDER BY` must also be shippable
- Window: `WindowFunc` — `winfnoid` shippable
- Type casts: `CoerceViaIO` — **both** input and output types shippable
  (because the underlying I/O functions such as `textin()` are not listed as
  shippable individually; we rely on type shippability), `RelabelType`,
  `ArrayCoerceExpr`, `CoerceToDomain`
- Conditional / constructor: `BoolExpr` (AND/OR/NOT), `CaseExpr`,
  `CoalesceExpr`, `MinMaxExpr`, `NullTest`, `BooleanTest`, `ArrayExpr`,
  `RowExpr`, `SubscriptingRef`, `FieldSelect`, JSON constructors

**Always not pushable nodes / situations**:

- System columns: `Var.varattno <= 0` (`ctid`, `xmin`, `tableoid`, etc.;
  DuckDB has no counterpart)
- Non-shippable `opno` / `funcid` / `aggfnoid` / `winfnoid`
- Non-default collation: the node's `exprCollation` is not the default/`C`
  collation (DuckDB's collation model differs; see all collatable node types
  covered by `ExpressionHasCollation`)
- Expressions whose return type is not shippable (see 5.6.4)
- volatile functions (results depend on evaluation count/order, unsafe across
  engines)

> `now()` is a special case: at execution time `pg_lake` replaces `now()` with
> the **transaction-start-time constant** before sending it to DuckDB,
> guaranteeing consistency within a transaction. This proposal does the same at
> the deparse/injection stage (corresponding to the `PG_LAKE_NOW_TEMPLATE`
> replacement).

#### 5.6.4 Type level: column types DuckDB supports (from `GetDuckDBTypeForPGType`)

Pushable types (and their arrays): `bool`, `int2/int4/int8`,
`float4/float8`, `numeric`, `text/varchar/bpchar/name`, `bytea`, `date`,
`time`, `timetz`, `timestamp`, `timestamptz`, `interval`, `uuid`, `bit`,
`json/jsonb`, `record` (-> STRUCT), arrays (-> LIST), map types (`pg_map`, ->
MAP), geometry (PostGIS, optional).

Types that are explicitly **not pushable / have caveats**: `oid`, `tid`,
`pg_lsn`, `money`, `inet`, `xid8`, etc. — these types also make their
`min()/max()` aggregates not pushable (see the shippability notes); `jsonb[]`
(nested mapping not done yet); user-defined types are not pushable by default.

#### 5.6.5 Semantically "conditionally pushable" (pushed down, but results may differ slightly from PG)

These are **still pushable by default**, but the provider's `Shippability`
should be able to mark them so they can be tightened later via a GUC
(corresponding to `engineering-notes/pgduck_shippability.md`):

- floating-point `sum`/`avg`, `numeric` averages: DuckDB and PG results may
  differ slightly
- integer division: `SELECT 4/10` yields `0` in PG, `0.4` in DuckDB
- `date_trunc`: DuckDB counts millennium/century from 2000 (pg_lake corrects
  this for constant arguments); the 3-argument time-zone version does not
  exist in DuckDB
- statistical aggregates `stddev*` / `variance` / `var_*`: results may differ
  slightly (more pronounced on `real` columns)
- `regexp_replace`: only the 3/4-argument forms are pushable
- `sum` over `interval` / ordered-set aggregates (`percentile_*`) /
  `avg(interval)`: not pushable

#### 5.6.6 Where These Rules Live

- **Enumeration / lookup** (OID allowlists for operators/functions/types/
  collations): the provider implements `Shippability` in `expr/shippable.rs`
  (corresponding to `shippable_builtin_*.c` / `shippable_spatial_*.c` +
  `GetDuckDBTypeForPGType`).
- **Node traversal + Query/RTE-level gates**: core's shared walker
  (corresponding to `ProcessNotShippableExpressionWalker`); both Level 1
  `classify_predicate` (single predicate) and Level 2 `FullQueryIsOffloadable` /
  2b subtree decisions (whole tree / subtree) call it.
- **SQL shim** (functions with a different name/arguments, e.g.
  `regexp_matches` -> the DuckDB side): in the deparse + duckdb_pglake module
  (corresponding to `rewrite_query.c`'s `RewriteFuncExpr` and the
  `PG_LAKE_INTERNAL_NSP` trampoline); see §10.

---

## 6. Known Limitation: heap-table joins cannot be pushed down, but the lake subtree still is

**Correcting a common misunderstanding**: "the query touches a heap table"
does **not** equal "DuckDB cannot be used at all." The precise limitation is:

> **The heap side cannot be pulled into DuckDB to do the join together.** But
> the **pure-lake subtree (including lake⋈lake joins and aggregates over lake
> tables) is still offloaded to DuckDB**, and only its result is handed back to
> PG to do the final join with the heap table.

The reason is fundamental and tied to the standalone-process model:

- `pg_duckdb` can let heap tables participate in a DuckDB join **entirely
  because it is in-process** — the DuckDB executor can call back into
  `ExecutorStart` / `ExecProcNode`, share the PG snapshot, share parallel
  workers, and convert PG tuples in place into DuckDB DataChunks.
- **A standalone process cannot do any of this**: across processes there is no
  way to have a remote DuckDB call back into this backend's executor. This
  matches `pg_lake`'s trade-off — `pg_lake`'s `foreign_join_ok` also requires
  both join sides to be lake tables (`pushdown_safe`), and any join involving a
  heap table goes back to PG.

So for `SELECT ... FROM lake_a JOIN lake_b ON ... JOIN heap_t ON ...`:
- whole-query pushdown (Level 2a) fails (heap_t is present);
- but `lake_a ⋈ lake_b` (and the filtering/aggregation above it) goes through
  **Level 2b** and is **offloaded to DuckDB as one offload CustomScan block**;
- the PG executor only does the final join of that block with `heap_t`.
- What is truly lost is just "the single outermost lake⋈heap join did not run
  inside DuckDB."

In the future, if heap-table joins are also to be pushed down, the only
cross-process path is to **feed the heap-table data to the DuckDB server via
Arrow IPC** (serialization cost + loss of PG parallel scan), and the decision
of which side to do the join must be cost-estimate driven. This is long-term
(§8 Phase 5), outside the v1/v2 scope.

---

## 7. Risks

| Risk | Description | Mitigation |
|---|---|---|
| Whole-query deparse correctness | PG query tree -> DuckDB SQL covers a broad surface (subqueries, CTEs, windows, type casts, collation), with many corner cases | port `pg_lake`'s battle-tested deparser; strict shippability gate, do not push down when unsure (falling back to Level 1 is always safe) |
| Semantic differences | DuckDB and PG differ subtly on numeric/float sums, `date_trunc`, integer division, etc. (see `pg_lake`'s shippability notes) | the shippability layer explicitly marks "conditionally pushable"; conservative by default |
| DuckDB server operations | one more process; crash/OOM handling | reuse the storage worker's supervisor; in v1 a crash means the query errors, with no silent reconnect |
| Cache consistency | the local files DuckDB reads must match the files the PG snapshot selected | the file list is decided by PG-side snapshot+pruning and then injected; DuckDB does not discover files on its own; immutable-file assumption (storage design §2.1) |
| Multiple AMs contending for the server | the same "single owner" problem as the storage worker | the same long-term plan: converge ownership onto a dedicated `pg-lakebase` extension |

---

## 8. Phased Roadmap

### Phase 0 — Shared foundation (paving the way for reuse; can proceed independently of DuckDB)
- Extract the `expr/shippable.rs` shared trait; switch Level 1
  `classify_predicate` to use it.
- Lift file pruning into a provider-trait method; switch Level 1 to call it.
- Outcome: no functional change, but both layers share the knowledge entry.
  **Reduces later risk.**

### Phase 1 — DuckDB server subsystem skeleton
- `worker/duckdb/` supervisor (copy the storage worker).
- Option A: vendor `pg_lake`'s `pgduck_server` and get start-stop working.
- `customscan/duckdb/{pool,client,result}.rs`: connect to the server and get
  a `SELECT 1` wire round-trip + interrupt/cancel + transaction release
  working.
- Outcome: able to send one raw SQL from a PG backend to the DuckDB server and
  retrieve the result.

### Phase 2 — Single-relation offload (minimal end to end)
- planner_hook + `FullQueryIsOffloadable` (initially allow only a **single
  lake table, no join** SELECT; the pushdown surface starts with the safe
  subset of §5.6: basic operators/types + simple aggregates).
- Whole-query deparse + `read_table` placeholder + execution-time injection of
  cached file paths (§2.3 simple path).
- offload CustomScan: Begin injects files -> send SQL -> next_slot streams to
  Slot.
- Basic EXPLAIN output.
- Outcome: `SELECT category, sum(amount) FROM sales WHERE ... GROUP BY
  category` runs entirely in DuckDB, with the result returned to PG.

### Phase 3 — Multi lake-table whole query + subtree pushdown (Level 2b)
- Open up `FullQueryIsOffloadable` (2a) to multi lake-table joins, subqueries,
  CTEs, set operations.
- **Add Level 2b's two hooks**: `set_join_pathlist_hook` (lake⋈lake join
  pushdown), `create_upper_paths_hook` (agg pushdown over lake), implementing
  the `OffloadFragment` layered wrapping and dedup from §5.4.3. From here,
  queries that contain heap tables can also push down the pure-lake subtree.
- Extend deparse coverage + fill out the pushdown surface of §5.6 (windows,
  conditionally-pushable aggregates, SQL shims, collation/type boundaries).
- Parameterization (`$1` / PARAM_EXEC) and prepared-statement support.

### Phase 4 — Performance evolution
- Switch result return to **Arrow IPC** (§3.4), wiring into `batch.rs`.
- Wire the cache via the "thorough path": a custom FS extension in the DuckDB
  server -> `lakebase://` -> storage socket (§2.3), supporting on-demand fetch
  on a miss.
- DuckDB-side parallelism / resource-budget tuning.

### Phase 5 (long-term, may not happen) — heap-table join pushdown
- Only when there is a clear benefit: Arrow IPC feeding of heap tables +
  cost-driven join-placement decision (§6).

---

## 9. Module Layout Overview (where code lives)

```text
pg-lakebase-core/src/
  expr/
    shippable.rs            # [Phase 0] shared shippability trait (used by both layers)
  customscan/
    duckdb-offload-roadmap.md   # this document
    duckdb/
      mod.rs                # offload CustomScan provider variant + install the three hooks
      pool.rs               # [Phase 1] connection pool / xact binding
      client.rs             # [Phase 1] wire round-trip + interrupts
      result.rs             # [Phase 1/4] wire row / Arrow -> core::tuple::Row
      deparse.rs            # [Phase 2/3] whole-query/subtree deparse + read_table placeholder
      offloadable.rs        # [Phase 2] FullQueryIsOffloadable (2a, planner_hook)
      subtree.rs            # [Phase 3] join/upper pathlist hook (2b) + OffloadFragment dedup
      explain.rs            # [Phase 2] offload EXPLAIN (reuses customscan/explain.rs)
  worker/
    duckdb/                 # [Phase 1] DuckDB server bgworker (copy worker/storage/)
      mod.rs gucs.rs config.rs supervisor.rs logging.rs
```

The provider side (`pg-iceberg-am`) only needs to:
1. implement `DuckDbOffloadProvider` (declare `Shippability` +
   `read_call_template`);
2. in `_PG_init`, call `worker::duckdb::init_for_extension(...)` and
   `customscan::duckdb::init()` (install `planner_hook`; from Phase 3 also
   install the join/upper pathlist hooks; all idempotent and chained).

---

## 10. Reference Mapping (against the investigated projects)

| Capability | pg_lake counterpart | This proposal's home |
|---|---|---|
| Whole-query pushdown decision (2a) | `LakeTablePlanner` / `FullQueryIsPushdownable` | `customscan/duckdb/offloadable.rs` |
| Subtree pushdown: lake⋈lake join (2b) | `postgresGetForeignJoinPaths` (FdwRoutine `GetForeignJoinPaths`) / `foreign_join_ok` | `customscan/duckdb/subtree.rs` (`set_join_pathlist_hook`) |
| Subtree pushdown: aggregate over lake (2b) | `postgresGetForeignUpperPaths` (FdwRoutine `GetForeignUpperPaths`) / `add_foreign_grouping_paths` | `customscan/duckdb/subtree.rs` (`create_upper_paths_hook`) |
| read_table placeholder + execution-time replacement | `deparse_ruleutils.c` / `ReplaceReadTableFunctionCalls` (pg_lake's placeholder literal is `__lake_read_table` / `PG_LAKE_READ_TABLE`; our `__lakebase_read_table` is the analogous name) | `customscan/duckdb/deparse.rs` |
| PG↔DuckDB client (libpq + interrupt/cancel) | `pgduck/client.c` | `customscan/duckdb/{pool,client}.rs` |
| Standalone DuckDB server process | `pgduck_server/` | `worker/duckdb/` + (vendored) pgduck_server |
| Result return (wire / TRANSMIT) | `pgsession.c` / `duckdb.c` | `customscan/duckdb/result.rs` (v1 wire, later Arrow) |
| Shippability rules | `shippable_builtin_*.c` / `shippable_spatial_*.c` (+ `fdw/shippable.c`) | `expr/shippable.rs` (shared by both layers; inventory in §5.6) |
| Pushdown surface (query/RTE/expr/type-level decisions) | `ProcessNotShippableExpressionWalker` + `GetDuckDBTypeForPGType` | core shared walker + `expr/shippable.rs` (§5.6) |
| SQL shim (renamed functions, e.g. `regexp_matches`) | `rewrite_query.c` `RewriteFuncExpr` + `PG_LAKE_INTERNAL_NSP` | `customscan/duckdb/deparse.rs` + duckdb_pglake module |
| `now()` -> transaction-time constant | `PG_LAKE_NOW_TEMPLATE` replacement | `customscan/duckdb/deparse.rs` (injection stage) |
| Cache-transparent FS | `duckdb_pglake/src/fs/caching_file_system.cpp` | (Phase 4) DuckDB FS extension -> storage socket |
| Heap-table join pushdown | (pg_lake does not support; heap join falls back to PG) | (not supported, see §6; the lake subtree is still pushed down; pg_duckdb's in-process approach does not apply) |
