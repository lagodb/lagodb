# PostgreSQL C compatibility boundary

`lakebase_pg_compat.h` is the single compile-time version policy for every
Lakebase C fork. Each functional fork stays in one version-neutral,
Lakebase-prefixed source file across supported PostgreSQL versions, with
version differences behind predicates in this header and local `#if` blocks at
the semantic change, following the model used by TimescaleDB.

The C sources support the PostgreSQL 17 major line. PostgreSQL 17.10 remains
the provenance baseline, not the only accepted minor. Confirmed minor API and
private-layout changes stay next to the affected code as `PG_VERSION_NUM`
branches. In particular, ANALYZE has PG17.0-17.4 and PG17.5+ ReadStream layout
epochs, while ModifyTable has PG17.1, PG17.6 and PG17.7 API boundaries.

The `pg16` Cargo feature currently keeps its pre-existing Rust-only path and
does not compile these forks. Its TableAM ANALYZE callback ABI needs a separate
port. PostgreSQL 18 support likewise belongs in a separate audited change; the
predicates here reserve its branch structure but do not claim support.

Adding a PostgreSQL version requires:

1. adding local compatibility branches to ANALYZE, ModifyTable and VACUUM FULL;
2. recording provenance and source hashes in each module's `README.md`, plus
   pristine sources in `upstream/` when a complete upstream file is forked;
3. running that version's complete regression matrix;
4. only then extending `LAKEBASE_SUPPORTED_PG_MAJOR` and marking the Cargo
   feature's `c_forks_supported` entry in `build.rs` as true.

Release binaries should be built against the target installation's headers.
Source compatibility is maintained across supported minors, but Lakebase does
not add a stricter runtime minor-version check than PostgreSQL module magic.
