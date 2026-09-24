# Provider-owned partitioned table planning

The runtime's `QueryTreePreparation` owns one query-tree traversal, including
nested queries, sublinks, and CTEs. For each Query it first calls
`ProviderPartitionedTablePlanner::prepare_query()` to disable leaf expansion for
provider-owned partitioned tables through the existing runtime table-provider
descriptor. This is shared
read/write planning, independent of the Custom ModifyTable registry. It then
dispatches the Modify descriptor's `prepare_query` callbacks for that Query.
Provider-local `ModifyQueryPreparation` injects whole-row inputs and applies
mutation restrictions without recursing. System-column checks retain their
Query-local expression scan; providers no longer repeat the full tree walk.

Preparation captures the committed table-provider partitioned table capability
and the Modify descriptor snapshot once at the planner entry. With neither facet,
the runtime skips preparation without inspecting the Query or walking VALUES.
Either facet independently enables the one shared traversal. Preparing partitioned
tables does not require a Modify descriptor, and Modify-only preparation
does not scan RTEs for provider-owned partitioned tables. Capability admission reads registration
metadata without invoking provider callbacks or resolving database-local AM
OIDs. Actual table ownership remains an AM lookup for each partitioned RTE.
Utility admission retains its separate database-local provider check.

The runtime keeps one descriptor snapshot for preparation and plan fixup.
Provider callbacks keep the existing `CallbackErrorReport` transport, and
walker errors return to the planner's FFI report boundary without early
reporting. No Rust-owned provider registry crosses the DSO boundary.

Query RTEs retain their catalog `relkind`; only provider-owned partitioned tables
have `inh` cleared.
The rewrite is applied before PostgreSQL's `expand_inherited_rtentry()`, as in
the former Modify pre-hook; sizing and the planner-local RTE view stay at
`get_relation_info` so native path costs and constraint exclusion retain their
original ordering.
PostgreSQL native partition and inheritance trees remain unchanged.

PostgreSQL 17.10 `plancat.c:get_relation_info()` exposes its catalog-information
hook before `allpaths.c` performs base-relation sizing. `make_one_rel()` requires
every base relation to be sized before computing `total_table_pages` and before
building paths. `indxpath.c:get_loop_count()` explicitly depends on that ordering.

The runtime owns `get_relation_info_hook` and forwards its catalog stage through
the same exact-build descriptor as relation path planning. Core classifies the
Query RTE and sizes only provider-owned partitioned tables through their existing
TableAM callback. PostgreSQL then derives rows, widths and restriction costs.
No estimates or ParamPathInfo caches are repaired in the pathlist hook.

A provider-owned partitioned table has storage but no PostgreSQL leaves.
A plain, non-inherited RTE
view in `PlannerInfo.simple_rte_array` lets PostgreSQL use its normal baserel
sizing instead of making a storage-less dummy. This planner-view pattern adapts
the relation's storage semantics for standard planning while preserving its
catalog identity in the Query RTE.

The view is allocated in the planner context and borrows the Query RTE's fields.
It changes only `relkind` and `inh`. The Query, catalog, and shared relcache kinds
are unchanged, including on ERROR. `setrefs.c` obtains the executor's flattened
range table from `root->parse->rtable`. Runtime provider callbacks and query
offload admission also use these Query RTEs, retaining their existing ownership,
partitioned table restrictions, and ModifyTable contracts. Other planning hooks
observe the same storage view used by PostgreSQL's size/path calculations.

The partitioned table path router still requires a provider CustomPath and removes
the native paths generated for the storage view. It preserves a dummy path
produced by actual constraint exclusion. Native relations and unowned partition
trees retain their catalog-stage RTEs and estimates.

Constraint exclusion also applies to UPDATE and DELETE. ModifyScanBinder binds
every remaining target scan but accepts plans with no target scan. The mutation
state still requires a binding before any row-level update or delete; an empty
plan creates no AM ModifyState and retains PG's statement executor lifecycle.

Re-audit this adapter against `relnode.c:build_simple_rel()`, `plancat.c`,
`allpaths.c`, `indxpath.c`, `inherit.c`, and `setrefs.c` when updating PostgreSQL.
