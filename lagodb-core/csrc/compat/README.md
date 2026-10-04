# PostgreSQL C compatibility boundary

`lagodb_pg_compat.h` is the shared compile-time vocabulary and support gate
for lagodb-core C forks and adapters, including the partition-key bridge.
Version predicates describe known PostgreSQL major
lines so API differences can remain local to the affected adapter or fork.
`LAGODB_SUPPORTED_PG_MAJOR`, independently, decides which complete major-line
port is allowed to compile. It currently rejects every major except PostgreSQL
17, even when a local adapter already records a later major's API shape.

lagodb-base owns its runtime C support gate in
`csrc/compat/lagodb_base_pg_compat.h` within that crate. Its build script tracks
PostgreSQL configuration inputs independently, following core's tracking rules.
Neither crate relies on a workspace-level build module or compatibility header,
so their build and C support boundaries remain intact when the crates are split
into separate workspaces.

The PostgreSQL source forks currently support the PostgreSQL 17 major line. PostgreSQL
17.10 remains the provenance baseline, not the only accepted minor. Confirmed
minor API and private-layout changes stay next to the affected code as
`PG_VERSION_NUM` branches. In particular, ANALYZE has PG17.0-17.4 and PG17.5+
ReadStream layout epochs, while ModifyTable has PG17.1, PG17.6 and PG17.7 API
boundaries.

The repository keeps compiled implementations and headers under `csrc`, without
vendored PostgreSQL source copies. Each module's `README.md` records the audited
release tag, source paths, adapted scope, and confirmed version differences.
Use the matching PostgreSQL release source for comparisons. Shared version
predicates belong in `lagodb_pg_compat.h`; compatibility branches stay next to
the affected implementation.

`lagodb_injection_point.h` is a capability adapter rather than a source
fork. It records the known API difference between PostgreSQL 17's
`INJECTION_POINT(name)` and PostgreSQL 18's `INJECTION_POINT(name, arg)`, and
maps older versions to a no-op. This PG18 branch is compatibility scaffolding,
not a declaration of framework support: the workspace has no complete `pg18`
Cargo feature path and the shared PG17-only support gate rejects a PG18 build.
The Rust facade also checks the target installation's `pg_config.h`: standard
PostgreSQL builds that do not define `USE_INJECTION_POINTS` compile Rust call
sites to an inline no-op and do not retain an FFI call.

The `pg16` Cargo feature remains a build-selection entry and does not compile
these forks. Its TableAM and ANALYZE callback ABIs need a separate port; this
layer does not add Rust fallbacks for an unsupported major. A PG16 build may
therefore fail during Rust compilation, linking, or runtime initialization.
PostgreSQL 18 predicates and the injection-point ABI branch are retained as
compatibility scaffolding, but the complete PG18 port still belongs in a
separate audited change.

Adding a PostgreSQL version requires:

1. auditing the COPY internal contract and adding local compatibility branches
   to ANALYZE, ModifyTable and VACUUM FULL;
2. adding its Cargo feature and `build.rs` version entry with C forks disabled;
3. recording the audited release tag, source paths, adapted scope, source
   hashes, and confirmed version differences in each module's `README.md`;
4. running that version's complete regression matrix;
5. only then extending `LAGODB_SUPPORTED_PG_MAJOR` and marking the Cargo
   feature's `c_forks_supported` and any bridge-specific support entries in
   `build.rs` as true.

Release binaries should be built against the target installation's headers.
Source compatibility is maintained across supported minors, but LagoDB does
not add a stricter runtime minor-version check than PostgreSQL module magic.
