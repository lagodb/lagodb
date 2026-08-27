# Contributing

The primary SQL-facing implementation is `lagodb-iceberg` on PostgreSQL 17.
Changes should preserve PostgreSQL lifecycle semantics, keep the Iceberg
storage path as the product focus, and avoid presenting the Delta skeleton as
a second implementation.

## Repository map

| Path | Responsibility |
|---|---|
| [`lagodb-iceberg`](lagodb-iceberg) | Managed and REST-catalog foreign Apache Iceberg tables |
| [`lagodb-base`](lagodb-base) | Shared workers, runtime coordination, and storage-volume control plane |
| [`lagodb-core`](lagodb-core) | PostgreSQL TableAM, CustomScan, and FDW frameworks |
| [`pg-arrow-conv`](pg-arrow-conv) | Arrow/PostgreSQL value conversion |
| [`iceberg-lite`](iceberg-lite) | Synchronous PostgreSQL-oriented Iceberg library |
| [`lagodb-storage`](https://github.com/lagodb/lagodb-storage) | Local cache and object-storage service maintained in a separate repository |
| [`pg-delta-am`](pg-delta-am) | Experimental access-method skeleton; not Delta storage |
| [`xtask`](xtask) | Workspace test orchestration |

See [Build from source](docs/build-from-source.md) for local installation.

## Test the workspace

The full suite is:

```bash
cargo xtask test-all pg17
```

The full command runs workspace unit tests, pgrx backend tests, extension
tests, SQL regression, and isolation tests. It requires:

- a pgrx-managed PostgreSQL 17 configured with
  `--enable-injection-points`; and
- Docker for the object-storage, Iceberg REST catalog, and Spark fixtures used
  by the SQL regression suites.

Initialize the injection-enabled server with:

```bash
cargo pgrx init --pg17=download \
  --configure-flag=--enable-injection-points
```

Useful focused commands include:

The Iceberg pgrx test requires the runtime extension to be installed in the target
pgrx PostgreSQL installation. Install it once before running the focused Iceberg
test if it is not already present:

```bash
cargo pgrx install \
  --package lagodb-base \
  --pg-config "$(cargo pgrx info pg-config pg17)"
```

```bash
cargo pgrx test pg17 --package lagodb-iceberg
cargo xtask isolation pg17
```

The storage service has its own test suite and CI in the separate
[`lagodb-storage`](https://github.com/lagodb/lagodb-storage) repository. From a
sibling checkout, run its E2E suite as an independent project:

```bash
cd ../lagodb-storage
cargo test --features integration --test e2e
```

The detailed pgrx testing model, including the distinction between ordinary
Rust tests and `#[pgrx::pg_test]`, is documented in
[`lagodb-iceberg/docs/testing.md`](lagodb-iceberg/docs/testing.md).

## Test locations

- SQL regression inputs: [`lagodb-iceberg/tests/pg_regress/sql`](lagodb-iceberg/tests/pg_regress/sql)
- SQL expected output: [`lagodb-iceberg/tests/pg_regress/expected`](lagodb-iceberg/tests/pg_regress/expected)
- Isolation specifications: [`lagodb-iceberg/tests/isolation/specs`](lagodb-iceberg/tests/isolation/specs)
- Framework backend tests: [`pg-backend-tests`](pg-backend-tests)
- Storage-service E2E tests: [`lagodb-storage/tests/e2e`](https://github.com/lagodb/lagodb-storage/tree/main/tests/e2e)

The full test command prepares PostgreSQL's upstream `injection_points` test
extension from the pgrx-managed source tree. Product extensions do not expose
a production injection-point attach/detach API.

## Iceberg library changes

`iceberg-lite` is derived from
[`apache/iceberg-rust`](https://github.com/apache/iceberg-rust) and adapts its
execution and IO model for PostgreSQL. Changes to `iceberg-lite` must preserve
the synchronous PostgreSQL integration and remain straightforward to compare
and merge with upstream changes.

## Documentation changes

Keep the root README focused on the current Iceberg product, runnable usage,
verified capability boundaries, and the roadmap. Put build internals, test
orchestration, fault-injection details, and crate-level implementation notes in
the linked development documents instead.
