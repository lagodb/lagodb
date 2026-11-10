# Build from source

LagoDB supports PostgreSQL 15, 16, 17 and 18. PostgreSQL 18 is the default Cargo
feature for convenient local development; all four majors are first-class
build, test, and CI targets.

## Prerequisites

- Rust 1.97.1, as pinned by `rust-toolchain.toml`
- `cargo-pgrx` 0.19.2
- PostgreSQL 15, 16, 17 or 18 development files and `pg_config`
- a C build toolchain, Clang/libclang, `pkg-config`, OpenSSL, ICU, Readline,
  zlib, Bison, and Flex

Install the matching cargo-pgrx release:

```bash
cargo install --locked cargo-pgrx --version 0.19.2
```

## Select PostgreSQL

Set the target major and the absolute path to its `pg_config`:

```bash
PG_MAJOR=18 # or 15, 16, 17
PG_CONFIG=/path/to/pg_config

cargo pgrx init "--pg${PG_MAJOR}=${PG_CONFIG}"
```

For a pgrx-managed source build, use `download` instead of a local path.
PostgreSQL 17 and 18 need the injection-point flag for the repository's full
regression and isolation suites. PostgreSQL 15 and 16 have no injection points,
so omit the flag there:

```bash
# PostgreSQL 17 or 18
cargo pgrx init "--pg${PG_MAJOR}=download" \
  --configure-flag=--enable-injection-points

# PostgreSQL 15 or 16
cargo pgrx init "--pg${PG_MAJOR}=download"

PG_CONFIG="$(cargo pgrx info pg-config "pg${PG_MAJOR}")"
```

## Build and install

Install the runtime before its providers:

```bash
cargo pgrx install --package lagodb-base --pg-config "${PG_CONFIG}"
cargo pgrx install --package lagodb-iceberg --pg-config "${PG_CONFIG}"
cargo pgrx install --package lagodb-connectors --pg-config "${PG_CONFIG}"
```

`cargo-pgrx` reads the PostgreSQL major from `PG_CONFIG`, disables any other
PostgreSQL default feature, and enables the matching `pg15`, `pg16`, `pg17` or `pg18`
feature. Do not add more than one PostgreSQL feature to one build.

Add the runtime and providers to `postgresql.conf`, then restart PostgreSQL:

```conf
shared_preload_libraries = 'lagodb_base'
lagodb.provider_libraries = 'lagodb_iceberg,lagodb_connectors'
```

Create the extensions in each database that will use LagoDB:

```sql
CREATE EXTENSION lagodb_base;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION lagodb_connectors;
```

## Ordinary Cargo commands

Commands that run Cargo directly use PG18 by default, so PG18 must already be
registered with `cargo pgrx init`:

```bash
cargo check -p lagodb-iceberg
```

Select PG15, PG16 or PG17 explicitly by disabling the PG18 default first. This form
is also required when PG18 is not registered:

```bash
cargo check -p lagodb-iceberg \
  --no-default-features \
  --features pg17 # or pg15, pg16
```

`cargo xtask` commands follow the same default: `cargo xtask test-all` targets
PG18, while `cargo xtask test-all pg15`, `cargo xtask test-all pg16` and
`cargo xtask test-all pg17` select
those majors. They install into the selected pgrx PostgreSQL installation.

Never use `--all-features` for PostgreSQL-aware crates: the `pg15`, `pg16`,
`pg17` and `pg18` features are mutually exclusive. See [CONTRIBUTING.md](../CONTRIBUTING.md) for the complete
test workflow.
