# Provider CustomScan framework

This module connects PostgreSQL `CustomScan` planning and execution to typed
lake-storage providers. It owns PostgreSQL semantics and lifecycle; provider
code owns storage-specific planned predicates and scan state.

## Module boundaries

```text
customscan/
  planning/   planner hook, gates, path costing, final plan construction
  plan_data/  copyObject-safe envelopes, expression sections, tuple layout
  execution/  Begin/ReScan/NextSlot/End, EPQ recheck, EXPLAIN
  provider/   typed provider SPI and type-erased planning registry

expr/pushdown/
  stable IR, normalization, negotiation, contracts, binding, shared codec
```

Planning produces plan data; execution consumes it. The registry performs only
planner routing. It never erases or downcasts provider planned predicates after
a concrete provider has been selected.

## End-to-end flow

```text
RestrictInfo clauses
  -> core PostgreSQL gates
       security ordering, movability, volatile/SubPlan, pseudoconstants
  -> FilterNormalizer
       complete PG expression -> provider-neutral FilterFragment + value slots
  -> provider FilterPushdownPlanner::try_plan_filter
       Unsupported | Exact(planned predicate) | Conservative(planned predicate)
  -> FilterNegotiator
       residual/recheck decisions, AND fallback, OR widening + final confirmation
  -> final CustomScan plan
       plan.qual      = unsupported + conservative original expressions
       custom_exprs   = value bindings + pushed candidate-expression provenance
       custom_private = planned predicates + contracts + slot metadata
  -> PostgreSQL replace_nestloop_params + setrefs
  -> BeginCustomScan
       decode planned predicates once, evaluate value bindings, bind predicates
       derive Exact EPQ recheck from pushed provenance + decoded contracts
  -> ReScanCustomScan
       reevaluate bindings when referenced parameters changed
       atomically replace the complete bound predicate set
  -> NextSlot
       consume the provider cursor; no expression planning or predicate building
```

The provider's successful complete-tree conversion is the only structural
capability decision. Core does not maintain leaf capability flags and execution
does not translate PostgreSQL filter trees again.

## Pushdown contracts

- **ExactRowFilter**: the provider predicate is row-level equivalent to the
  original PostgreSQL expression. The original is removed from ordinary
  `plan.qual` and is retained as pushed provenance so the framework can execute
  it during EPQ recheck.
- **ConservativePruning**: the provider predicate can discard only rows/files
  that cannot satisfy the original expression. The original remains in
  `plan.qual`; PostgreSQL therefore preserves correctness when pruning is loose
  or a runtime value cannot be represented.
- **Unsupported**: no provider artifact is persisted and the complete original
  expression remains in `plan.qual`.

`AND` may fall back to independently negotiated children. `OR` is accepted only
as a complete provider-confirmed tree; a widened candidate is submitted to the
provider again and is always persisted as Conservative/Uncosted. `NOT` never
uses partial fallback.

## Two planning stages

Path planning and final plan construction deliberately negotiate separately:

1. The path stage creates an owned relation-scoped provider planner and uses an
   artifact-free summary for eligibility and costing. No path-stage planned
   predicate is persisted.
2. `PlanCustomPath` receives PostgreSQL's final ordered clauses, creates a new
   provider planner, negotiates authoritatively, and writes the resulting plan.

This prevents a temporary costing result from becoming executor state after
PostgreSQL changes or reorders the final clause set.

## PostgreSQL gates

Core applies gates before a provider sees a fragment:

- pseudoconstants remain owned by PostgreSQL's gating plan nodes;
- unsafe security-barrier/RLS ordering is rejected;
- parameterized join clauses must be movable to the scan relation;
- volatile expressions and SubPlans remain residual;
- the current implementation handles relation-backed base scans only.

Scan-purpose and tuple-layout gates additionally protect modification scans,
row identity, whole-row output, system columns, and projected scan tuples.

## Plan field layout

- `plan.qual`: unsupported originals and Conservative originals. `ExecScan`
  evaluates these normally.
- `custom_exprs`: first the value-binding expressions, then one original
  expression for each planned predicate in planned-record order. PostgreSQL
  performs nestloop parameter replacement and setrefs on the complete list.
- `custom_private`: a copyObject-safe framework envelope containing provider
  identity, relation identity, section counts, tuple layout, encoded planned
  predicates, binding-slot metadata, and provider private data.
- `custom_scan_tlist`: the raw provider tuple shape and base-attribute mapping.

Exact expressions are not duplicated in a separate `custom_exprs` recheck
section. Begin pairs the pushed-provenance section with decoded contracts and
builds the `ExecInitQual` input for Exact entries only. The planned record is
therefore the single source of truth for the contract.

Raw PostgreSQL pointers never enter `custom_private`. Provider planned
predicates must be owned, encodable, and free of executor-lifetime handles.

## Parameters and ReScan

Const, `PARAM_EXTERN`, `PARAM_EXEC`, and outer values become value slots during
normalization. Their PostgreSQL expressions stay in `custom_exprs`, so
PostgreSQL owns setrefs and runtime evaluation.

Begin evaluates every binding slot and builds every provider filter once.
ReScan uses the filter-specific `PARAM_EXEC` bitmap to skip unrelated changes;
when a relevant parameter changes, it reevaluates only dynamic slots and
rebuilds only planned records that contain dynamic bindings. Stable records
retain their Begin-time predicates. Dynamic replacement is atomic: failure
cannot leave predicates from different outer tuples installed. Conservative
filters may disappear for one binding when the runtime value is not
representable because their original expression remains residual. Exact
binders must be total for every value of the accepted PostgreSQL type.

The cache grain is one planned-filter record. ReScan does not introduce a
slot-to-predicate dependency graph, boolean-subtree cache, or per-parameter
incremental state machine.

## Executor and hot path

Begin validates the tuple layout once and installs provider state and bound
filters. ReScan performs value rebinding and cursor replacement. End drops the
cursor before the scan specification and preserves provider-owned teardown
ordering.

NextSlot performs no PostgreSQL expression walk, catalog capability lookup,
planned-payload decode, string conversion, predicate-tree allocation, or filter
trait dispatch. It only asks the monomorphized provider state for the next row
and enforces the provider/slot produced-row protocol.

## EXPLAIN

PostgreSQL prints residual `plan.qual` as `Filter` but does not display
`custom_exprs`. The framework deparses the pushed-provenance section:

- ordinary EXPLAIN emits one combined `Pushed Filter` property;
- VERBOSE emits `Pushed Filter Exact` and `Pushed Filter Conservative`;
- Exact entries are also emitted as `Recheck`.

The callback uses PostgreSQL's post-setrefs expressions. It does not decode
values, invoke the provider planner, or reconstruct a provider predicate.
Conservative expressions intentionally appear both under pushed information
and PostgreSQL's ordinary `Filter`, because they remain residual.

## Responsibility split

Core owns PostgreSQL gates, normalization, negotiation, residual/recheck,
binding-expression lifecycle, plan envelopes, tuple layout, and callback error
reporting. Provider code owns complete-fragment conversion, storage field
binding, planned-predicate codec, runtime value conversion, and scan execution.

Errors are returned through these layers and reported only by the PostgreSQL
FFI trampoline. Unsupported structure is a normal planning result, never an
execution-time fallback or error.
