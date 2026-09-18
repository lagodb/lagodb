# lagodb-query

[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

## DataFusion-backed Query Offload Framework

`lagodb-query` is the vectorized query offload and execution engine for LagoDB. It is an internal library crate (`rlib`) linked into the LagoDB runtime host (`lagodb-base`). Pluggable storage providers (such as [`lagodb-iceberg`](../lagodb-iceberg)) do not link `lagodb-query` directly; instead, they supply scan streams through C-compatible callback descriptors and the Arrow C Stream ABI.

It bridges PostgreSQL's row-at-a-time execution engine and Apache DataFusion's columnar Arrow-native execution pipeline. For supported query shapes, it validates a provider-neutral plan, estimates its cost, compiles the selected plan for DataFusion, and streams the result back into PostgreSQL slots.

## Core Architecture and Principles

`lagodb-query` decouples semantic query representation from physical execution engines and storage formats:

```text
               PostgreSQL CustomScan Planner / Executor (lagodb-base)
                                        │
                                        ▼
+─────────────────────────────────────────────────────────────────────────────+
│                                lagodb-query                                 │
│                                                                             │
│  [ Semantic Query Plan IR ]                  [ Cost & Feasibility Model ]   │
│  - Provider-neutral relational operators      - Operator-tree cost estimates │
│  - Typed expressions and output contracts     - Provider scan cost facts      │
│  - Storage-format-independent plan data       - PostgreSQL-scaled path costs  │
│                                                                             │
│  [ Dual-Path Semantic Engine ]               [ DataFusion Plan Compiler ]   │
│  - Native vectorized expression evaluation   - Logical to physical lowering │
│  - PostgreSQL expression evaluation bridge   - Relational operator planning │
│  - Explicit typed aggregate handling         - Bound table scan streams     │
│                                                                             │
│  [ Vectorized Execution Runtime ]            [ Metrics & Governance ]       │
│  - Backend-local current-thread runtime       - Execution resource ownership │
│  - Lazy Arrow batch streaming                 - PostgreSQL EXPLAIN bridge    │
│  - Direct TupleTableSlot projection           - Operator-level metrics       │
+───────────────────────────────────────┬─────────────────────────────────────+
                                        │
                                        ▼ (Arrow C Stream ABI / Callbacks)
+─────────────────────────────────────────────────────────────────────────────+
│                      Provider Arrow C Stream Sources                        │
│               (Supplied via runtime TableScanDescriptor ABI)                │
+─────────────────────────────────────────────────────────────────────────────+
```

### Key Architectural Principles

1. **Provider-Neutral Intermediate Representation (IR)**:
   - Plans are represented as a format-agnostic relational operator tree covering the supported scan, filter, join, aggregate, distinct, projection, ordering, and limit shapes.
   - Storage-format planning and batch production remain provider responsibilities. The query engine consumes provider-neutral schemas, cost facts, plan payloads, and stream callbacks.

2. **Explicit Semantic Capability Gating**:
   - Planning admits only query shapes whose expression, type, operator, and lifecycle requirements are covered by an implemented execution path.
   - Supported expressions are either evaluated by the vectorized engine or, when the host policy permits it, through a controlled PostgreSQL expression-evaluation bridge.

3. **PostgreSQL-Owned Path Selection**:
   - Produces PostgreSQL-scaled cost estimates from the relational operator tree and provider scan facts.
   - `lagodb-base` adds a legal offload candidate to the pathlist, and PostgreSQL remains responsible for choosing between it and native alternatives.

4. **Batch-Oriented Execution Hot Path**:
   - Physical execution pulls pre-projected Arrow `RecordBatch` streams directly from storage providers via the Arrow C Stream ABI.
   - Output conversion rules are prepared before row iteration; each non-empty batch is bound once and its rows are projected directly into PostgreSQL `TupleTableSlot` structures.

5. **Memory & Lifecycle Discipline**:
   - Each selected query owns one backend-local execution runtime, its compiled plan, provider streams, output state, and execution metrics.
   - Normal completion, rescan, query cancellation, and abort cleanup follow the lifecycle established by the PostgreSQL host.

## Main Processing Flows

### 1. Plan Compilation and Lowering Flow

```text
Query Candidate (from lagodb-base Planner Hook)
  │
  ▼
Semantic Plan Validation & Costing
  ├─ Inspect operator tree and check expression compatibility
  ├─ Combine operator estimates with provider scan facts
  └─ Return a PostgreSQL-scaled cost for the legal offload candidate
  │
  ▼
PostgreSQL Path Selection
  ├─ Compare the offload candidate with native alternatives
  └─ Materialize the winning plan
  │
  ▼
Selected-Plan Preparation
  ├─ Bind provider table-scan sources and runtime values
  ├─ Lower the semantic operator tree into a DataFusion physical plan
  └─ Install the lazy execution stream for the PostgreSQL executor
```

### 2. Vectorized Execution and Slot Projection (The Hot Path)

```text
PostgreSQL Executor (ExecCustomScan)
  │
  ▼
Current-Thread Execution Pipeline (SerialQueryExecution)
  │
  ├─ Execute compiled physical DataFusion plan on backend-local runtime
  │
  ▼
Pull RecordBatch from Physical Stream
  │
  ├─ If stream ends: Return an empty slot to signal scan completion
  ├─ If a batch has zero rows: Continue pulling the stream
  ├─ If a non-empty batch arrives: Bind it once for output conversion
  │
  ▼
Per-Row Slot Projection (Hot Path)
  ├─ Project Arrow array values directly into target TupleTableSlot columns
  ├─ Use the output conversion rules prepared for the selected plan
  └─ Hand populated slot back to outer PostgreSQL executor pipeline
```

### 3. Dual-Path Expression Evaluation

```text
Expression Evaluation
  │
  ▼
Evaluate Expression Kind & Volatility
  │
  ├─ [Native Vectorized Path]:
  │    └─ Execute vectorized Arrow compute kernel across entire column array
  │
  └─ [PostgreSQL Evaluation Path, when permitted]:
       ├─ Extract batch inputs into PostgreSQL Datum array
       ├─ Use PostgreSQL's expression evaluator
       └─ Pack evaluated results back into Arrow arrays for downstream operators
```

### 4. Metrics and EXPLAIN Integration

```text
EXPLAIN [ANALYZE] Query
  │
  ▼
Execution Tree Inspection
  ├─ Present the logical offload tree and provider scan information
  ├─ Record operator-specific properties (join types, keys, filter predicates)
  │
  ▼
Metrics Instrumentation (When ANALYZE is active)
  ├─ Collect processed row counts and batch sizes
  ├─ Record available operator timing and engine memory metrics
  └─ Format and attach metrics to PostgreSQL's native EXPLAIN output stream
```

## Requirements

- **Rust**: 1.97.1 or later
- **PostgreSQL**: 17
- **Apache DataFusion**: 55.0.0
- **Apache Arrow**: 59.1.0

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE) for details.
