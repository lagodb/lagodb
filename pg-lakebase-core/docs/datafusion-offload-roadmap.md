# DataFusion Offload Roadmap (pg-lakebase-core CustomScan extension)

**Status**: Roadmap / design draft. No implementation exists yet.

The preferred design is to embed Apache DataFusion as a Rust query engine
inside the PostgreSQL backend and use it for larger lake-table query fragments:
joins, aggregates, sort, limit, and eventually window-capable fragments. The
design is based on the current `pg-lakebase-core` CustomScan filter-pushdown
framework and on ParadeDB's DataFusion integration.

## Conclusion

DataFusion can provide the desired class of benefit: a columnar, vectorized
engine for operators that PostgreSQL otherwise executes row-by-row above the
table access method. The fit is strong for this workspace because
`pg-lakebase` already reads Parquet into Arrow batches, already has
`pg-arrow-conv` for Arrow to PostgreSQL conversion, and is written in Rust.

The first implementation should **not** deparse PostgreSQL queries into SQL and
send them to a standalone process. It should follow the ParadeDB shape:

- PostgreSQL planner hooks identify a pushable relation, join, or upper
  aggregate/sort/limit subtree.
- The plan stores a copyObject-safe custom IR plus PG `Expr` nodes that
  PostgreSQL can still run through setrefs and parameter rewriting.
- Execution builds a DataFusion logical plan directly using DataFusion APIs,
  registers lake-table `TableProvider`s, creates a physical plan, and consumes
  `RecordBatch` streams.
- `pg-arrow-conv` projects the DataFusion result batches into PostgreSQL tuple
  slots.

This avoids a SQL dialect bridge, a separate server lifecycle, and an IPC result
path. The tradeoff is that DataFusion is in-process, so memory limits, panic/OOM
handling, cancellation, and PostgreSQL thread-safety boundaries must be designed
explicitly.

## External facts used

Apache DataFusion describes itself as a fast extensible Rust query engine that
uses Apache Arrow as its in-memory format. Its documented features include a
vectorized, multithreaded, streaming execution engine; native Parquet support;
custom `TableProvider`s; custom plan/execution nodes; optimizer passes; filter
and projection pushdown; join reordering; and sort/distribution-aware
optimizations.

Relevant official documentation:

- <https://datafusion.apache.org/user-guide/introduction.html>
- <https://datafusion.apache.org/library-user-guide/custom-table-providers.html>
- <https://datafusion.apache.org/library-user-guide/building-logical-plans.html>
- <https://datafusion.apache.org/library-user-guide/query-optimizer.html>
- <https://datafusion.apache.org/user-guide/sql/select.html>
- <https://datafusion.apache.org/user-guide/cli/datasources.html>

Important dependency note: DataFusion's public APIs use Arrow types. The
workspace already depends on Arrow 57, so the implementation must choose a
DataFusion version whose Arrow dependency can be unified with the workspace or
must first upgrade the workspace Arrow stack.

## Current pg-lakebase baseline

`pg-lakebase-core` currently has one pushdown layer:

```text
Level 1: base-relation filter pushdown
  SQL WHERE
    -> set_rel_pathlist_hook
    -> classify pushed / residual / recheck predicates
    -> CustomPath for a single base relation
    -> Begin/ReScan translates PG Expr into provider-native predicate
    -> provider reads files and emits rows
```

This lets `lagodb-iceberg` prune Iceberg files and row groups and apply exact
row filters. It does not move joins, aggregates, global sort, or limit into a
columnar engine. PostgreSQL still receives scan rows and executes those upper
operators itself.

The DataFusion roadmap adds a second layer:

```text
Level 2: DataFusion fragment offload
  pushable relation/join/upper subtree
    -> DataFusion logical plan
    -> DataFusion physical plan
    -> Arrow RecordBatch stream
    -> pg-arrow-conv writes final PostgreSQL slots
```

Level 2 is optional. If a query fragment fails any gate, the planner falls back
to Level 1. If Level 1 also cannot help, PostgreSQL uses the TableAM path.

## Reference: embedded DataFusion

ParadeDB provides a useful reference because it embeds DataFusion in a pgrx
extension and drives it from PostgreSQL CustomScan nodes.

The important patterns are:

- **Planner hooks build CustomPaths**. JoinScan uses join path hooks for join
  subtrees; AggregateScan uses upper path hooks for aggregate queries. This is
  closer to PostgreSQL's natural planner shape than a whole-query SQL deparser.
- **Plan state is a serializable IR**. ParadeDB stores a `JoinCSClause` /
  aggregate clause in `custom_private`, then reconstructs DataFusion plans at
  execution. It does not store live Rust pointers in the plan.
- **PG expressions stay PG-shaped until execution**. Expressions that need
  setrefs, parameter rewrite, or runtime resolution stay in `custom_exprs` and
  are translated after PostgreSQL has rewritten them.
- **DataFusion is driven through `SessionContext`**. ParadeDB registers custom
  table providers, builds DataFrames/logical plans, configures optimizer rules,
  creates a physical plan, and executes it into a `SendableRecordBatchStream`.
- **Custom optimizer rules matter**. ParadeDB installs rules for visibility,
  late materialization, sort-merge join enforcement, filter pushdown, dynamic
  filter propagation, and segmented TopK.
- **Execution consumes Arrow batches**. DataFusion emits `RecordBatch` values;
  ParadeDB converts those rows back into PostgreSQL tuple slots.
- **Runtime is explicit**. ParadeDB uses a Tokio runtime from the CustomScan
  state to poll DataFusion streams from PostgreSQL callbacks.
- **The design is conservative**. ParadeDB gates aggressively on supported join
  types, fast-field availability, expression support, and LIMIT/materialization
  economics.

For `pg-lakebase`, the equivalent of ParadeDB's Tantivy fast fields is not an
index. It is the statement-selected Iceberg scan: committed metadata plus the
transaction-local `SnapshotDelta` overlay, projected into Arrow batches.

## Proposed architecture

### Pushdown levels

```text
Level 2b: subtree offload
  lake-lake join, aggregate over lake input, sort/limit over lake input
  -> DataFusion CustomScan

Level 1: base-relation filter pushdown
  WHERE predicate -> Iceberg predicate / file pruning / row filtering

Level 0: TableAM fallback
  ordinary PostgreSQL executor above the scan
```

The first milestone should target Level 2b, not a whole-query planner
replacement. Whole-query replacement can be considered later if the subtree
infrastructure proves out.

### Data path

The v1 scan input should come from a custom DataFusion table provider backed by
`lagodb-iceberg` scan planning:

```text
DataFusion TableProvider::scan()
  -> create a lightweight ExecutionPlan

ExecutionPlan::execute(partition, task_ctx)
  -> use a statement snapshot captured by the CustomScan Begin path
  -> load Iceberg metadata through TxMetadata
  -> apply SnapshotDelta overlay
  -> plan files, projection, and pushed filters
  -> stream Arrow RecordBatches
```

Do not make DataFusion discover files directly from the table location in v1.
The Iceberg AM already owns metadata visibility, delete-file handling, projected
schema mapping, object-storage cache interaction, and transaction-local overlay
semantics. Bypassing that layer would duplicate correctness-sensitive logic.

Later, a deeper DataFusion file source can read selected Parquet files directly
from `pg-lakebase-storage` cache paths or from an object-store implementation,
but only after Iceberg delete semantics and overlay visibility are preserved.

### Planner integration

`pg-lakebase-core` needs a second CustomScan framework beside the current
base-relation filter pushdown:

- Register `set_join_pathlist_hook` and `create_upper_paths_hook` routers.
- Let providers declare whether a base relation belongs to the same offload
  domain.
- Build a copyObject-safe `OffloadFragment` IR for joins and upper operators.
- Keep original PG expressions in `custom_exprs`; store only indexes, counts,
  relation OIDs, type OIDs, sort metadata, and provider-private scalar data in
  `custom_private`.
- Cost the offload path against ordinary PostgreSQL paths, with a high fallback
  bias until benchmarks exist.

The framework should not vendor a full SQL deparser for v1. Build DataFusion
logical plans directly from the pushed fragment IR and translated PG Expr nodes.

### Runtime integration

The v1 execution model should be embedded and single-backend:

- Create a current-thread Tokio runtime per active offload scan, or reuse one
  per backend if that proves safe.
- Build a DataFusion `SessionContext` with a controlled `target_partitions`.
- Size DataFusion memory through a custom memory pool tied to PostgreSQL
  `work_mem` and `hash_mem_multiplier`.
- Poll `SendableRecordBatchStream` in `ExecCustomScan`.
- Convert DataFusion batches to slots with `pg-arrow-conv`.
- Check PostgreSQL interrupts between batch polls and during long scan work.

Do not call PostgreSQL APIs from DataFusion worker threads. PostgreSQL backend
state is not thread-safe. In v1, keep DataFusion execution on a current-thread
runtime unless every code path reachable from a table provider is pure Rust /
Arrow / object-store code that does not touch pgrx or `pg_sys`.

## Provider API sketch

The exact API should be designed in code, but the shape should be:

```rust
trait LakebaseDataFusionProvider {
    fn supports_offload_relation(ctx: &RelationContext<'_>) -> bool;

    fn classify_offload_expr(
        expr: &pg_sys::Expr,
        scope: OffloadScope,
    ) -> OffloadExprDecision;

    fn build_table_provider(
        rel_oid: pg_sys::Oid,
        snapshot: SnapshotHandle,
        projection: OffloadProjection,
        planned_filters: &[PlannedOffloadFilter],
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>>;

    fn translate_expr(
        fragment: &FilterFragment,
        mapper: &ColumnMapper,
    ) -> Result<datafusion::logical_expr::Expr>;
}
```

The provider-specific v1 implementation for `lagodb-iceberg` should reuse
`ScanSpec` and the existing Arrow batch cursor where possible. If the existing
cursor API is too tied to PostgreSQL tuple slots, split out a pure Arrow
`RecordBatch` stream first.

## Pushdown surface

Start narrow and fail closed:

- Relations: Iceberg tables from the same provider and compatible snapshot
  rules.
- Joins: inner equi-joins first. Add semi/anti and outer joins only after
  null-extension and parallel partitioning correctness are specified.
- Aggregates: `COUNT`, `SUM`, `MIN`, `MAX`, `AVG` over Arrow-compatible scalar
  types; grouped and ungrouped.
- Sort/limit: simple `ORDER BY` and static `LIMIT/OFFSET`; dynamic parameters
  can be resolved at execution but may disable DataFusion TopK planning.
- Expressions: column refs, constants, boolean connectives, comparisons, casts
  with matching semantics, arithmetic where PostgreSQL/DataFusion behavior is
  known to match, and selected stable functions.
- Types: types already supported by `pg-arrow-conv`; reject unsupported
  collations, domains needing special coercion, lossy numeric behavior, and
  functions with PostgreSQL-specific semantics.

Mixed heap/lake queries should initially push only the pure lake subtree. The
final join with a heap table remains in PostgreSQL. Pulling PostgreSQL heap
tuples into DataFusion is a separate feature and should not block lake-table
operator pushdown.

## Why this can accelerate queries

The acceleration target is not single-row point lookup. It is analytical work
where PostgreSQL's row executor becomes the bottleneck after the lake scan:

- Aggregation over large Arrow batches can stay columnar until the final result.
- Joins over lake inputs can run in DataFusion with hash/sort-merge algorithms
  over Arrow arrays.
- Sort/limit can use DataFusion physical optimizations such as bounded TopK.
- Projection and filter pushdown reduce both scan I/O and intermediate column
  width.
- Returning only final aggregate/join results reduces PostgreSQL tuple
  formation and executor overhead.

This is not a guaranteed win for every query. Offload must lose to ordinary
plans for small row counts, unsupported expressions, highly selective index-like
lookups, and mixed heap/lake plans where materializing the lake subtree result
is more expensive than letting PostgreSQL drive the join.

## Main risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| PostgreSQL thread safety | DataFusion may execute streams on runtime threads | v1 current-thread runtime; forbid PG API use outside backend thread |
| Memory governance | DataFusion hash join/aggregate/sort can exceed backend budget | custom memory pool sized from `work_mem`; fail query on limit |
| Panic/OOM boundary | In-process engine failures can affect backend | catch unwind at callback boundaries where possible; keep unsafe/PG calls out of DataFusion tasks |
| Semantics mismatch | PG and DataFusion differ for some casts, functions, collations, numeric edge cases | strict expression/type shippability; fallback to PG when uncertain |
| Snapshot correctness | DataFusion must scan the same Iceberg snapshot PG selected | build providers from execution-time `TxMetadata`/`SnapshotDelta`, not from plan-time file lists |
| Arrow dependency drift | DataFusion and workspace Arrow crates must match | choose/upgrade versions together before implementation |
| Planner instability | Bad costs can replace good PG plans | conservative costing; GUC kill switch; EXPLAIN-visible reason and plan |

## Phases

### Phase 0 - Version and API foundation

- Pick a DataFusion version compatible with the workspace Arrow version, or
  upgrade Arrow across `iceberg-lite`, `pg-arrow-conv`, and `lagodb-iceberg`.
- Add a feature-gated DataFusion dependency in `pg-lakebase-core`.
- Extract shared expression shippability helpers from the existing CustomScan
  filter framework.
- Add a GUC such as `pg_lakebase.enable_datafusion_offload`.

### Phase 1 - Single-relation DataFusion scan

- Implement an Iceberg-backed DataFusion `TableProvider`.
- Execute a simple projected scan through DataFusion and return slots through
  `pg-arrow-conv`.
- Preserve existing Iceberg metadata tracker overlay behavior.
- Keep target partitions at one until thread-safety constraints are proven.

### Phase 2 - Aggregate offload

- Add `create_upper_paths_hook` routing in core.
- Push `GROUP BY`, aggregate functions, `HAVING`, and simple `ORDER BY/LIMIT`
  over one Iceberg relation.
- Validate results against PostgreSQL executor plans for supported types.

### Phase 3 - Lake-lake join offload

- Add `set_join_pathlist_hook` routing in core.
- Push inner equi-joins where both sides are Iceberg tables owned by the same
  provider.
- Add expression translation for join keys and join filters.
- Add EXPLAIN output showing DataFusion logical and physical plans.

### Phase 4 - Broaden operators and parallelism

- Add sort/limit over joined inputs, semi/anti joins, selected outer joins, and
  window operators where semantics match.
- Evaluate multi-partition DataFusion execution only after all provider code
  used from worker threads is proven PG-free.
- Consider a cache-aware native DataFusion file source that reads selected
  Parquet files directly while preserving Iceberg delete and overlay semantics.

## Expected file layout

```text
pg-lakebase-core/
  docs/
    datafusion-offload-roadmap.md
  src/customscan/
    datafusion/
      mod.rs
      hooks.rs
      fragment.rs
      expr.rs
      path.rs
      plan.rs
      exec.rs
      explain.rs
      memory.rs

lagodb-iceberg/src/
  datafusion/
    table_provider.rs
    execution_plan.rs
    stream.rs
    expr.rs
```

## Open questions

- Should the first table provider wrap the existing `IcebergBatchCursor`, or
  should `lagodb-iceberg` expose a new pure Arrow stream API first?
- Can DataFusion memory accounting be made strict enough for backend-local
  execution, or do we eventually need a worker process for resource isolation?
- How much of PostgreSQL expression translation should live in core versus the
  provider?
- Should whole-query offload exist, or is join/upper subtree offload enough?
- Can a future MPP shape reuse PostgreSQL parallel workers, or should it stay
  separate from PostgreSQL's parallel executor?
