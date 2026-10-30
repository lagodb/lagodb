# Query preparation and path routing

The runtime's `QueryTreePreparation` owns one query-tree traversal, including
nested queries, sublinks, and CTEs. For each Query it dispatches the Modify
descriptor's `prepare_query` callbacks. Provider-local `ModifyQueryPreparation`
injects whole-row inputs and applies mutation restrictions without recursing.
System-column checks retain their Query-local expression scan; providers do not
repeat the full tree walk.

Preparation captures the Modify descriptor snapshot once at the planner entry.
Without one, the runtime skips preparation without inspecting the Query or
walking VALUES. Capability admission reads registration metadata without invoking
provider callbacks or resolving database-local AM OIDs. Utility admission retains
its separate database-local provider check.

The runtime keeps one descriptor snapshot for preparation and plan fixup.
Provider callbacks keep the existing `CallbackErrorReport` transport, and
walker errors return to the planner's FFI report boundary without early
reporting. No Rust-owned provider registry crosses the DSO boundary.

Provider tables are ordinary relations, so PostgreSQL's standard sizing,
inheritance expansion and constraint exclusion apply to them. Constraint exclusion
also applies to UPDATE and DELETE. ModifyScanBinder binds every remaining target
scan but accepts plans with no target scan. The mutation state still requires a
binding before any row-level update or delete; an empty plan creates no AM
ModifyState and retains PG's statement executor lifecycle.
