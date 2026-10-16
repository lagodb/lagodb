# Testing Architecture and Methodology

This document outlines the testing architecture, testing tiers, and execution guidelines for `lagodb-iceberg`.

## Testing Philosophy and Layered Architecture

`pg_regress` is the primary home for SQL-visible feature coverage. Each new feature needs a basic positive case; a small number of negative cases should protect significant failure boundaries. NULLs, empty results, and supported fallback paths are ordinary functional cases, not invalid inputs.

The four testing layers below describe existing execution boundaries, not coverage priorities. Add new feature coverage in `pg_regress`; do not add Rust unit tests or `#[pg_test]` cases. Maintain existing tests without repeating complete operator matrices across layers or scan routes.

Follow PostgreSQL's regression style: prefer ordinary SQL and `EXPLAIN (COSTS OFF)` followed by the query when both plan selection and results matter. Extract stable JSON fields only when runtime metrics are needed or a representation matrix needs compact, named route assertions. Report each case separately instead of reducing unrelated cases to one boolean. Keep GUC changes scoped to the behavior under test; avoid unnecessary DO blocks, exception subtransactions, and psql controls.

External Java/Spark fixtures only prepare source data that SQL tests consume. Keep them in `scripts/pg_regress/iceberg_fixtures`; do not introduce a second assertion framework.

```text
+─────────────────────────────────────────────────────────────────────────────+
| Tier 1: Host-Side Pure Unit Tests (`cargo test`)                            |
| - Fast, deterministic, sub-second execution without PostgreSQL backend      |
| - Expression algebra, capability policies, schema converters, statistics    |
+─────────────────────────────────────────────────────────────────────────────+
                                       ▲
                                       │
+─────────────────────────────────────────────────────────────────────────────+
| Tier 2: PostgreSQL Backend Tests (`#[pgrx::pg_test]`)                       |
| - Live PostgreSQL backend process context                                   |
| - MemoryContext lifecycles, Datum extraction, syscache lookups, SPI queries |
+─────────────────────────────────────────────────────────────────────────────+
                                       ▲
                                       │
+─────────────────────────────────────────────────────────────────────────────+
| Tier 3: SQL Regression & Isolation Suites (`pg_regress`, Isolation Specs)   |
| - End-to-end SQL verification, DDL/DML coverage, transaction isolation      |
| - Concurrency conflicts, savepoint rollbacks, schema evolution validation   |
+─────────────────────────────────────────────────────────────────────────────+
                                       ▲
                                       │
+─────────────────────────────────────────────────────────────────────────────+
| Tier 4: Object Storage & Ecosystem E2E Tests                                |
| - Real object stores (S3/MinIO, GCS, Azure) via storage volumes             |
| - External REST Catalog synchronization and multi-table validation          |
+─────────────────────────────────────────────────────────────────────────────+
```

## The Execution Boundary Principle

The fundamental architectural test rule is:

> **Can this logic run safely in an ordinary host process, or does it require PostgreSQL backend process semantics?**

### 1. Host-Safe Logic (Tier 1)

Existing pure Rust tests use standard `#[test]` modules gated by `#[cfg(test)]`. Their subjects include:
- Pure Iceberg expression translation and simplification algebra.
- Pushdown capability matrices and classification policies.
- Type mapping tables and schema compatibility checks.
- Static option parsing and configuration validators.

**Rule**: Pure logic must not import or call PostgreSQL backend symbols (`palloc`, `CurrentMemoryContext`, `PG_exception_stack`, `pg_sys::*`). Keeping pure logic decoupled from PostgreSQL C ABI guarantees instant compilation and test execution.

### 2. Backend-Dependent Logic (Tier 2)

Existing tests that directly touch PostgreSQL backend internals use `#[pgrx::pg_test]` modules gated by `#[cfg(feature = "pg_test")]`. This includes:
- PostgreSQL `Datum` extraction, tuple slot unpacking, and varlena operations.
- Syscache lookups and catalog metadata verification.
- MemoryContext allocations and `ResourceOwner` cleanup handlers.
- Direct SPI execution.

### 3. Concurrency and Transaction Isolation (Tier 3)

PostgreSQL transactional semantics (ACID, MVCC, read-your-own-writes, conflict detection) cannot be fully verified with single-threaded unit tests. We use:
- **`pg_regress`**: SQL-level regression tests asserting output correctness across DDL, DML, partitioning, and maintenance commands.
- **Isolation Tester Specs**: Multi-session concurrency specifications validating lock acquisition, concurrent commit serialization, and rollback safety.

### 4. Fault Injection and Crash Testing

To verify resilience against crashes, aborts, and network interruptions, tests leverage PostgreSQL 17's native **Injection Points** (`--enable-injection-points`):
- Injecting errors at transactional boundaries to verify that uncommitted data/delete files are discarded without metadata corruption.
- Testing background worker crash recovery and WAL replay safety.

## Test Execution Commands

### Run Host-Side Unit Tests
Runs Tier 1 pure unit tests instantly without starting PostgreSQL:

- **Run `lagodb-iceberg` unit tests**:
  ```bash
  cargo test -p lagodb-iceberg --lib
  ```
- **Run `lagodb-core` unit tests**:
  ```bash
  cargo test -p lagodb-core --lib
  ```
- **Run all workspace pure unit tests** (matches `xtask` Phase 1):
  ```bash
  cargo test --workspace --exclude pg-backend-tests --exclude lagodb-base --exclude lagodb-iceberg --exclude pg-delta-am --exclude lagodb-connectors --no-default-features --features pg17
  ```


### Run PostgreSQL Backend Tests
Runs Tier 2 `#[pg_test]` tests inside an ephemeral PostgreSQL test cluster:
```bash
# Ensure the shared runtime is installed in the target pgrx installation
cargo pgrx install --package lagodb-base --pg-config "$(cargo pgrx info pg-config pg17)"

# Run backend tests
cargo pgrx test pg17 --package lagodb-iceberg
```

### Run SQL Regression Tests
Runs the complete SQL regression suite:
```bash
cargo xtask regress pg17
```

Credential coverage uses the existing MinIO and REST fixtures without changing
the PostgreSQL launch environment. `storage_volume.sql` exercises the AWS
access-key configuration, registry replacement and object I/O. `iceberg_fdw_reads.sql`
covers REST without catalog user mappings, vended credentials overriding a local
identity, lazy fallback-route selection, and missing OAuth credentials. Default-chain
and named-profile authentication, profile refresh and STS/AssumeRole exchanges
are not covered by these fixtures.

### Run Full Test Suite (Unit, Backend, Isolation, E2E)
Runs the complete end-to-end verification suite:
```bash
cargo xtask test-all pg17
```

## Summary of Best Practices

1. **Prefer SQL Feature Coverage**: Add basic positive coverage in `pg_regress`, with a few critical failure cases. Avoid repeating the same behavior through different SQL syntax.
2. **Maintain Existing Test Boundaries**: Do not add Rust unit tests or `#[pg_test]` cases. Preserve existing tests for internal and C FFI contracts without duplicating SQL coverage.
3. **No Test-Only Production Pollutions**: Production code should not contain test-only flags or artificial branches; use native PostgreSQL injection points for fault simulation.
