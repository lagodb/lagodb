# PostgreSQL C compatibility boundary

`lagodb_pg_compat.h` is the shared compile-time vocabulary and support gate
for lagodb-core C forks and adapters.
Version predicates describe known PostgreSQL major lines so API differences
can remain local to the affected adapter or fork.
`LAGODB_SUPPORTED_PG_MAJOR()`, independently, decides which target major-line
ports are allowed to compile. It admits the PostgreSQL 15, 16, 17 and 18 major
lines.

The PostgreSQL source forks currently target the PostgreSQL 15, 16, 17 and 18
major lines. PostgreSQL 15.19, 16.15, 17.11 and 18.6 are the provenance
baselines, not the only accepted minors. Confirmed minor API and private-layout changes stay next
to the affected code as `PG_VERSION_NUM` branches. A change PostgreSQL
back-patched into several major lines gets one named `LAGODB_HAS_*` capability
in `lagodb_pg_compat.h`, and the owning module's `README.md` records its first
minors. In particular, ANALYZE has PG17.0-17.4, PG17.5+, and PG18.6 ReadStream
layout epochs; PostgreSQL 15 and 16 have no ReadStream and do not build that
adapter.
ModifyTable and COPY retain their minor API boundaries.

The repository keeps compiled implementations and headers under `csrc`, without
vendored PostgreSQL source copies. Each module's `README.md` records the audited
release tag, source paths, adapted scope, and confirmed version differences.
Use the matching PostgreSQL release source for comparisons. Shared version
predicates belong in `lagodb_pg_compat.h`; compatibility branches stay next to
the affected implementation.

C sources under `csrc` follow PostgreSQL 18 pgindent layout, so fork audits
compare against upstream without whitespace noise. PostgreSQL 16, 17 and 18
use the same `pg_bsd_indent` options; only their typedef lists differ.
`csrc/typedefs.list` holds LagoDB's own typedef names, one per line without
comments, and must be extended whenever a new typedef is added. Build
`src/tools/pg_bsd_indent` in a configured PostgreSQL 18 source tree, then run
from a scratch directory, because pgindent writes temporary files to its
working directory:

```sh
PG18_SRC=~/.pgrx/18.6
CSRC=/path/to/lagodb/lagodb-core/csrc
perl "$PG18_SRC/src/tools/pgindent/pgindent" \
  --indent="$PG18_SRC/src/tools/pg_bsd_indent/pg_bsd_indent" \
  --typedefs="$PG18_SRC/src/tools/pgindent/typedefs.list" \
  --list-of-typedefs="$(cat "$CSRC/typedefs.list")" \
  $(find "$CSRC" -name '*.[ch]')
```

Add `--diff` to preview changes, or `--check` to only report whether any
file needs formatting.

`lagodb_injection_point.h` is a capability adapter rather than a source
fork. It records the known API difference between PostgreSQL 17's
`INJECTION_POINT(name)` and PostgreSQL 18's `INJECTION_POINT(name, arg)`, and
maps PostgreSQL 15 and 16 and builds without injection-point support to a no-op. The Rust facade also
checks the target installation's `pg_config.h`: standard
PostgreSQL builds that do not define `USE_INJECTION_POINTS` compile Rust call
sites to an inline no-op and do not retain an FFI call.

`lagodb_collation.c` exposes PostgreSQL's own C-equivalent collation fact.
PostgreSQL 15, 16 and 17 provide it through `lc_collate_is_c()`, while PostgreSQL 18
stores it in the locale object returned by `pg_newlocale_from_collation()`.
The adapter preserves that `InvalidOid` behavior before entering the
PostgreSQL 18 lookup.

`lagodb_json_category.c` exposes PostgreSQL's JSON output classification under
LagoDB's own enum, so no PostgreSQL enum order crosses into Rust. PostgreSQL 17
and 18 export `json_categorize_type()`. PostgreSQL 15 and 16 keep their
identical variant private to `src/backend/utils/adt/json.c`, so the adapter carries the PostgreSQL 17.11
semantics from `src/backend/utils/adt/jsonfuncs.c` with `is_jsonb = false`
(`REL_17_11`, SHA-256
`eb3081406e737463c36962e7d62f54db22f3395c6250d32bc2517225373c4cb5`). The source
comment in `lagodb_json_category.c` is authoritative for this derivation.

Cargo features and C gates intentionally expose only PG15, PG16, PG17 and PG18. A later
major gets a predicate only after a concrete compatibility branch needs it and
every C fork and Rust owner has been audited.

Adding a PostgreSQL version requires:

1. auditing the COPY internal contract and adding local compatibility branches
   to ANALYZE, ModifyTable and VACUUM FULL;
2. adding its Cargo feature, `build.rs` version entry, and compile gate so the
   port can be built and tested;
3. recording the audited release tag, source paths, adapted scope, source
   hashes, and confirmed version differences in each module's `README.md`;
4. running that version's complete regression matrix;
5. only then declaring the major supported in product documentation.

Release binaries should be built against the target installation's headers.
Source compatibility is maintained across supported minors, but LagoDB does
not add a stricter runtime minor-version check than PostgreSQL module magic.
