# Contributing

The primary SQL-facing implementation is `lagodb-iceberg` on supported
PostgreSQL 16, 17 and 18 installations.
Changes should preserve PostgreSQL lifecycle semantics, keep the Iceberg
storage path as the product focus, and avoid presenting the Delta skeleton as
a second implementation.

## Repository map

| Path | Responsibility |
|---|---|
| [`lagodb-iceberg`](lagodb-iceberg) | Managed and REST-catalog foreign Apache Iceberg tables |
| [`lagodb-base`](lagodb-base) | Shared workers, runtime coordination, and storage-volume control plane |
| [`lagodb-core`](lagodb-core) | PostgreSQL TableAM, CustomScan, and FDW frameworks |
| [`lagodb-arrow`](lagodb-arrow) | PostgreSQL/Arrow conversion and typed table-scan bridge |
| [`iceberg-lite`](https://github.com/lagodb/iceberg-lite) | Synchronous Iceberg library |
| [`lagodb-storage`](https://github.com/lagodb/lagodb-storage) | Local cache and object-storage service maintained in a separate repository |
| [`pg-delta-am`](pg-delta-am) | Experimental access-method skeleton; not Delta storage |
| [`xtask`](xtask) | Workspace test orchestration |

See [Build from source](docs/build-from-source.md) for local installation.

## Test the workspace

Run the full suite for each target major:

```bash
cargo xtask test-all pg16
cargo xtask test-all pg17
cargo xtask test-all pg18
```

The full command runs workspace unit tests, pgrx backend tests, extension
tests, SQL regression, and isolation tests. It requires:

- a pgrx-managed PostgreSQL 16, 17 or 18, with 17 and 18 configured with
  `--enable-injection-points`; and
- Docker for the object-storage, Iceberg REST catalog, and Spark fixtures used
  by the SQL regression suites.

PostgreSQL 16 has no injection points or COPY `ON_ERROR`, so its run skips those
regression cases and the worker lifecycle backend tests. `expected/<test>_1.out`
holds the alternate output of a test whose output varies by major, normally
PostgreSQL 16; `query_offload_joins_1.out` holds PostgreSQL 16 and 17, because
only PostgreSQL 18 has `JOIN_RIGHT_SEMI`. Update it together with
`<test>.out`. Initialize the servers with:

```bash
cargo pgrx init --pg16=download
cargo pgrx init --pg17=download \
  --configure-flag=--enable-injection-points
cargo pgrx init --pg18=download \
  --configure-flag=--enable-injection-points
```

The Iceberg pgrx test requires the runtime extension to be installed in each
target pgrx PostgreSQL installation. The following runs the focused tests and
isolation suite for each target major:

```bash
for pg in pg16 pg17 pg18; do
  cargo pgrx install \
    --package lagodb-base \
    --pg-config "$(cargo pgrx info pg-config "${pg}")"
  cargo pgrx test "${pg}" --package lagodb-iceberg
  cargo xtask isolation "${pg}"
done
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

[`iceberg-lite`](https://github.com/lagodb/iceberg-lite) is maintained as a
standalone repository and is derived from
[`apache/iceberg-rust`](https://github.com/apache/iceberg-rust) and adapts its
execution and IO model for synchronous systems. Changes to the library must
preserve its standalone dependency boundary and remain straightforward to
compare and merge with upstream changes; changes to PostgreSQL integration
belong in `lagodb-iceberg`.

## Documentation changes

Keep the root README focused on the current Iceberg product, runnable usage,
verified capability boundaries, and the roadmap. Put build internals, test
orchestration, fault-injection details, and crate-level implementation notes in
the linked development documents instead.
