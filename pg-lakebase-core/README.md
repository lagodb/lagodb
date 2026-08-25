# pg-lakebase-core

[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

## PostgreSQL-Lakehouse Integration Framework

`pg-lakebase-core` is the reusable framework layer between PostgreSQL's C engine interfaces and concrete lakehouse table format implementations.

It concentrates unsafe C FFI, planner hooks, executor lifecycle management, memory contexts, error recovery, and transaction boundaries in one place. Concrete providers (such as [`lagodb-iceberg`](../lagodb-iceberg) and [`lagodb-connectors`](../lagodb-connectors)) implement storage and format logic behind safe Rust traits instead of re-implementing PostgreSQL internals in each extension.

## Core Architecture and Seams

`pg-lakebase-core` connects PostgreSQL's database engine with pluggable lakehouse storage providers across four distinct seams:

```text
               PostgreSQL Planner / Executor / TableAM / FDW
                                     │
                                     ▼
+─────────────────────────────────────────────────────────────────────────────+
│                              pg-lakebase-core                               │
│                                                                             │
│  [ TableAM Seam ]               [ CustomScan & Pushdown Seam ]              │
│  - Stateless AM traits          - Expression normalization                  │
│  - Stateful operation sessions  - Exact vs Conservative capability contract │
│  - Slot & batch value adapters  - Runtime param binding & residual quals    │
│                                                                             │
│  [ FDW Seam ]                   [ Lifecycle & Durability Seam ]             │
│  - Scan, modify, schema import  - ResourceOwner cleanup on ERROR / abort    │
│  - Foreign option parsing       - Transaction commit & rollback callbacks   │
│  - Server / mapping routing     - Custom WAL resource-manager integration   │
+────────────────────────────────────┬────────────────────────────────────────+
                                     │ (Safe Rust Traits)
                                     ▼
+─────────────────────────────────────────────────────────────────────────────+
│                         Pluggable Format Providers                          │
│                                                                             │
│     lagodb-iceberg          lagodb-connectors          Future Providers     │
│   (Apache Iceberg TAM)      (Object Store FDW)       (Delta Lake, Hudi)     │
+─────────────────────────────────────────────────────────────────────────────+
```

### The Four Integration Seams

1. **TableAM Seam**:
   - Bridges PostgreSQL `TableAmRoutine` callbacks to Rust.
   - Separates stateless relation operations (sizing, DDL, index metadata) from stateful operation sessions (scans, index fetches, DML mutations, COPY).
   - Provides zero-copy tuple slot and datum views alongside owned `Row` and `Cell` abstractions.
2. **CustomScan & Pushdown Seam**:
   - Hooks into the planner (`set_rel_pathlist_hook`) and executor to push safe SQL `WHERE` predicates into lakehouse format scans.
   - Implements two-phase predicate planning: Core normalizes PostgreSQL AST expressions and handles residual qual rechecks, while the provider classifies format capabilities.
   - Supports both plain and parameterized scans (e.g. inner side of nested-loop joins).
3. **FDW Seam**:
   - Encapsulates PostgreSQL Foreign Data Wrapper callbacks for external table scans, DML mutations, and schema import (`IMPORT FOREIGN SCHEMA`).
   - Normalizes storage provider options, region endpoints, and credential resolution.
4. **Lifecycle & Durability Seam**:
   - **`ResourceOwner` Integration**: Guarantees cleanup of memory contexts, open file handles, and staging states when PostgreSQL throws `ERROR` (via `longjmp`/C-unwind).
   - **Transaction Callbacks**: Connects post-commit publication and abort rollbacks to PostgreSQL transaction boundaries.
   - **WAL Resource Manager**: Provides custom WAL record registration for crash recovery and replication.

## Key Interaction Flows

### 1. DML Lifecycle Flow

```text
ModifyTable Executor Starts
  │
  ├─ Core initializes query-scoped `ModifyQueryState`
  ├─ Associates target relations with stable state descriptors
  └─ Relations lazily construct format-specific `ModifyState`
  │
  ▼
Per-Tuple / Batch Dispatch
  ├─ Core converts tuple slots to slot views or owned row batches
  └─ Provider writes data/delete files and tracks uncommitted actions
  │
  ▼
Execution Completion
  ├─ Success: Finalizes `ModifyState` objects and prepares transaction commit
  └─ Abort / ERROR: ResourceOwner hook safely discards uncommitted state
```

### 2. Filter Pushdown Flow

```text
SQL WHERE Clause
  │
  ▼
Planner Path Generation (set_rel_pathlist_hook)
  ├─ Core normalizes SQL expressions into complete predicate fragments
  ├─ Provider classifies each fragment:
  │    • Exact: Full row filtering handled by storage engine (no residual check)
  │    • Conservative: File/row-group pruning only (PostgreSQL re-evaluates residual)
  │    • Unsupported: Filter evaluated entirely by PostgreSQL
  ├─ Core constructs plain and parameterized CustomScan paths
  └─ Planner chooses optimal path based on cost model
  │
  ▼
Executor Execution
  ├─ Core binds runtime execution values into the planned predicate
  ├─ Provider scans pruned records from storage
  └─ PostgreSQL applies any remaining residual quals
```

### 3. Error and Abort Safety Flow

```text
PostgreSQL ereport(ERROR) / Abort
  │
  ▼
C Exception Unwind / Longjmp Boundary
  │
  ▼
pgrx / Core ResourceOwner Cleanup Hook
  ├─ Releases active scans and locks
  ├─ Discards uncommitted staged files and memory contexts
  └─ Resets provider session state without memory or resource leaks
```

## Provider Model

A storage provider implements safe Rust traits defined by `pg-lakebase-core`:
- **Stateless AM Traits**: Relation properties, sizing, and DDL checks.
- **Session Types**: Scans, mutations, and batch writers instantiated per operation.
- **CustomScan Traits**: Predicate capabilities and execution builders.
- **FDW Traits**: Foreign table access, credentials, and schema reflection.

The reference implementation is [`lagodb-iceberg`](../lagodb-iceberg), which implements both TableAM and FDW providers on top of this framework.

## Requirements

- **Rust**: 1.97.1 or later
- **PostgreSQL**: 16 or 17
- **pgrx**: 0.19.2

## Testing

`pg-lakebase-core` is tested at two levels:
1. **Pure Rust Unit Tests**: Fast, host-only tests for expression normalization, batch buffers, option parsing, and state machines:
   ```bash
   cargo test -p pg-lakebase-core
   ```
2. **PostgreSQL Backend Tests**: Tests requiring a live PostgreSQL backend (Datum round-trips, syscache lookups, TableAM C callbacks) are aggregated in the shared [`pg-backend-tests`](../pg-backend-tests) crate:
   ```bash
   cargo pgrx test pg17 --package pg-backend-tests
   ```

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE) for details.
