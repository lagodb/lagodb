# lagodb-base

[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

## PostgreSQL Foundation Runtime and System Host

`lagodb-base` is the foundational PostgreSQL extension (`cdylib`, `lagodb_base`) and cluster-wide runtime host for the LagoDB ecosystem.

It acts as the primary runtime coordinator loaded into PostgreSQL via `shared_preload_libraries`. It centralizes engine-level hook interception, postmaster-time provider discovery and registration, background worker orchestration, cloud storage volume and credentials management, and vectorized query offload hosting.

Storage format providers (such as [`lagodb-iceberg`](../lagodb-iceberg)) and data connectors (such as [`lagodb-connectors`](../lagodb-connectors)) register into `lagodb-base` at startup, relying on it to mediate interaction with PostgreSQL core internals.

## Architecture and Core Subsystems

`lagodb-base` serves as the single integration anchor between the PostgreSQL server process and the rest of the LagoDB lakehouse platform:

```text
                        PostgreSQL Postmaster & Backends
     (Planner / Executor / ProcessUtility / ObjectAccess / Shared Memory)
                                     │
                                     ▼
+─────────────────────────────────────────────────────────────────────────────+
│                                 lagodb-base                                 │
│                                                                             │
│  [ Provider Bootstrap & Registry ]        [ Storage & Volume Subsystem ]    │
│  - Postmaster library loading             - Cluster singleton storage server│
│  - ABI verification & registration        - Volumes & tablespace bindings   │
│  - Provider lifecycle management          - Staging directory governance    │
│                                                                             │
│  [ PostgreSQL Hook Routers ]              [ Multi-Process Worker Host ]     │
│  - Planner & Upper path hooks             - Cluster supervisor bgworker     │
│  - ProcessUtility (DDL & COPY) hook       - Database-local coordinators     │
│  - ObjectAccess lifecycle hook            - Maintenance task schedulers     │
│                                                                             │
│  [ Query Offload Host ]                   [ Maintenance Coordination ]      │
│  - CustomScan method table registration   - Table compaction & snapshot GC  │
│  - Query subtree extraction & costing     - Periodic & on-demand execution  │
│  - Execution lifecycle & EXPLAIN bridge   - Provider maintenance hooks      │
+────────────────────────────────────┬────────────────────────────────────────+
                                     │
          ┌──────────────────────────┴──────────────────────────┐
          ▼                                                     ▼
+───────────────────────────────────+ +───────────────────────────────────────+
│           lagodb-query            │ │      Pluggable Storage Providers      │
│   (Vectorized Query Offload)      │ │   (lagodb-iceberg, lagodb-connectors) │
+───────────────────────────────────+ +───────────────────────────────────────+
```

### Core Subsystems

1. **Provider Bootstrap & Host Registry**:
   - Dynamically loads and validates configured provider libraries during PostgreSQL postmaster startup.
   - Enforces ABI checks and bounded startup registration before provider capabilities become visible.
   - Publishes provider capabilities for planning, table scans, utility handling, object lifecycle, and maintenance through a single runtime registration boundary.

2. **Hook Routing & Multiplexing**:
   - Serves as the sole owner and coordinator of PostgreSQL's global engine hook pointers.
   - **Planner Hooks**: Intercepts relation path generation, join path generation, and upper paths (grouping, aggregation, distinct) to identify query subtrees eligible for lakehouse acceleration.
   - **ProcessUtility Hook**: Intercepts and routes utility commands, including storage volume DDL and object-URI `COPY` commands (`COPY ... TO/FROM 's3://...'`), preventing them from falling through to local file handlers.
   - **Object Access Hooks**: Monitors database, tablespace, and relation drop events to coordinate cascading resource cleanup and metadata synchronization.

3. **Storage & Volume Management**:
   - Manages the cluster-wide storage server background worker singleton responsible for local staging cache management and asynchronous cloud object operations.
   - Maintains the catalog of storage volumes (S3, GCS, Azure), secret resolution, credential lifetimes, and tablespace-to-volume bindings.

4. **Multi-Process Background Worker Framework**:
   - Implements a resilient three-tier worker architecture: a cluster-level supervisor, database-local coordinators, and ephemeral task execution workers.
   - Manages inter-process communication, worker registration tables, database-level lifecycle locks, and crash recovery via PostgreSQL shared memory.

5. **Query Offload Host**:
   - Connects PostgreSQL's cost-based optimizer and executor to `lagodb-query`.
   - Registers process-lifetime `CustomScan` method tables.
   - Manages execution state lifecycles and safely bridges query execution cleanup with PostgreSQL `ResourceOwner` and `MemoryContext` semantics.

6. **Maintenance Orchestration**:
   - Coordinates table maintenance tasks (compaction, snapshot expiration, orphan file removal) across lakehouse formats.
   - Provides administrative SQL functions and background worker scheduling for automated storage maintenance.

## Main Processing Flows

### 1. Postmaster Bootstrap and Provider Loading

```text
PostgreSQL Postmaster Startup (shared_preload_libraries)
  │
  ├─ 1. Initialize LagoDB GUC configurations
  ├─ 2. Initialize shared state for worker and storage coordination
  ├─ 3. Install global engine hook routers (Planner, ProcessUtility, ObjectAccess)
  ├─ 4. Register cluster-level static background workers (Storage, Supervisor)
  ├─ 5. Parse configured provider libraries (e.g. lagodb_iceberg, lagodb_connectors)
  │      │
  │      ▼
  │   Iterate & Load Each Provider Library
  │      ├─ Execute provider initialization routines
  │      ├─ Validate ABI compatibility and identity contracts
  │      └─ Publish the provider's validated capabilities as one registration
  │
  └─ 6. Finish provider bootstrap and enter normal backend operation
```

### 2. Query Offload Planning and Execution

```text
SQL Query Ingestion (SELECT ...)
  │
  ▼
PostgreSQL Planner Hook (Pathlist / Join / Upper Paths)
  │
  ├─ Inspect relation tree and query shape for lakehouse tables
  ├─ Construct provider-neutral query plan candidate
  ├─ Evaluate offload feasibility and cost via lagodb-query
  ├─ Keep PostgreSQL's native paths available
  │      │
  │      ├─ [Candidate Unsupported]: Do not add an offload path
  │      └─ [Candidate Supported]: Add a costed LagoDB CustomScan path
  │
  └─ PostgreSQL selects the winning path using its normal cost comparison
  │
  ▼
PostgreSQL Executor (ExecutorRun / ExecCustomScan)
  │
  ├─ Instantiate QueryHost execution state and memory context boundaries
  ├─ Link provider scan streams to the vectorized execution runtime
  ├─ Stream computed batches from lagodb-query and project into PostgreSQL slots
  └─ Complete execution or handle ResourceOwner cleanup on abort/error
```

### 3. Utility Command and Object-URI Routing

```text
SQL Utility Command (COPY ... TO/FROM 's3://...' or DDL)
  │
  ▼
ProcessUtility Hook Router
  │
  ├─ Examine statement type and parameters
  │      │
  │      ├─ Case 1: Object-URI COPY (s3://, gs://, az://)
  │      │    ├─ Resolve longest matching storage server or active volume
  │      │    ├─ Validate credentials and format options
  │      │    └─ Dispatch execution directly to the designated connector provider
  │      │
  │      ├─ Case 2: Storage Volume DDL (CREATE/DROP TABLESPACE ... WITH storage_volume)
  │      │    ├─ Validate volume existence, permissions, and tablespace constraints
  │      │    ├─ Record binding metadata in the storage catalog
  │      │    └─ Delegate physical tablespace management to PostgreSQL core
  │      │
  │      └─ Case 3: Standard PostgreSQL Utility Command
  │           └─ Forward untouched to the previous ProcessUtility hook
```

### 4. Background Worker and Maintenance Lifecycle

```text
Cluster Supervisor BGWorker
  │
  ├─ Monitors active databases and shared memory worker tables
  ├─ Spawns database-local Coordinator workers as needed
  │
  ▼
Database Coordinator BGWorker
  │
  ├─ Reconciles database-local extension-worker registrations
  ├─ Starts ready workers and applies restart/backoff policy
  │
  ▼
Provider or Runtime Worker
  ├─ Executes its registered maintenance or service entry point
  ├─ Applies provider-owned table-format policy and operations
  └─ Reports completion and the next requested schedule to the runtime
```

## Requirements

- **Rust**: 1.97.1 or later
- **PostgreSQL**: 17
- **pgrx**: 0.19.2

## Configuration

`lagodb-base` requires preloading in `postgresql.conf`:

```conf
shared_preload_libraries = 'lagodb_base'
lagodb.provider_libraries = 'lagodb_iceberg,lagodb_connectors'
```

Query offload is disabled by default. To let supported provider scans participate
in PostgreSQL's normal cost-based path selection, set:

```conf
lagodb.query_offload_mode = 'auto'
```

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE) for details.
