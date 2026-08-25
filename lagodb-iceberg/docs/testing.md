# Testing Architecture and Methodology

This document outlines the testing architecture, testing tiers, and execution guidelines for `lagodb-iceberg`.

## Testing Philosophy and Layered Architecture

Testing a PostgreSQL-native lakehouse extension requires a strict separation of concerns. `lagodb-iceberg` uses a **4-tier layered testing matrix** to balance fast test feedback, deterministic execution, and end-to-end correctness.

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

Logic that is pure Rust belongs in standard `#[test]` modules gated by `#[cfg(test)]`. This includes:
- Pure Iceberg expression translation and simplification algebra.
- Pushdown capability matrices and classification policies.
- Type mapping tables and schema compatibility checks.
- Static option parsing and configuration validators.

**Rule**: Pure logic must not import or call PostgreSQL backend symbols (`palloc`, `CurrentMemoryContext`, `PG_exception_stack`, `pg_sys::*`). Keeping pure logic decoupled from PostgreSQL C ABI guarantees instant compilation and test execution.

### 2. Backend-Dependent Logic (Tier 2)

Logic that directly touches PostgreSQL backend internals belongs in `#[pgrx::pg_test]` modules gated by `#[cfg(feature = "pg_test")]`. This includes:
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
Runs all Tier 1 pure unit tests instantly:
```bash
cargo test -p lagodb-iceberg --lib
```

### Run PostgreSQL Backend Tests
Runs Tier 2 `#[pg_test]` tests inside an ephemeral PostgreSQL test cluster:
```bash
# Ensure the shared runtime is installed in the target pgrx installation
cargo pgrx install --package pg-lakebase-runtime --pg-config "$(cargo pgrx info pg-config pg17)"

# Run backend tests
cargo pgrx test pg17 --package lagodb-iceberg
```

### Run SQL Regression Tests
Runs the complete SQL regression suite:
```bash
cargo xtask regress pg17
```

### Run Full Test Suite (Unit, Backend, Isolation, E2E)
Runs the complete end-to-end verification suite:
```bash
cargo xtask test-all pg17
```

## Summary of Best Practices

1. **Maximize Tier 1 Coverage**: Keep core algorithms (planning, translation, algebra) in pure Rust to keep testing fast and deterministic.
2. **Boundary-Proportional Backend Tests**: Use `#[pg_test]` specifically to verify the C FFI boundary, not to re-test pure algorithm matrices.
3. **No Test-Only Production Pollutions**: Production code should not contain test-only flags or artificial branches; use native PostgreSQL injection points for fault simulation.
