# COPY C bridge

## PostgreSQL 17 provenance

- Baseline release: PostgreSQL 17.10
- Upstream tag: `REL_17_10`
- Internal contract: `src/include/commands/copy.h`
- COPY preparation owner: `src/backend/commands/copy.c`
- COPY FROM owner: `src/backend/commands/copyfrom.c`
- COPY TO owner: `src/backend/commands/copyto.c`

Audited baseline hashes:

- `commands/copy.h`:
  `cb4402e503464fd2988f0506fc1dd6e24dec3d8e2f0d2e5b9ecccc81f775d55b`
- `copyfrom.c`:
  `c78adfdcc4acfc678a798837b7b7f17663f27d485f6f3eb03b7b8c0ce618c10a`
- `copyto.c`:
  `21d2a34fd16fcea4c41f55f650729333fe300dc4242739522f82ab32ab0dfaa1`
- `commands/copy.c`:
  `6336c0d7049f4e883a22c4be6ccc430539584857b79367f7ae8ba07245495d64`

`lakebase_copy.c` is a narrow adapter, not a copy of PostgreSQL's COPY
implementation. It mirrors the relation, permission, and `COPY FROM WHERE`
preparation that PostgreSQL performs before calling the internal COPY entry
points, then exposes the PG17 `BeginCopy*`, `CopyFrom`/`DoCopyTo`, and
`EndCopy*` calls through a small FFI surface.

The relation-bound Text/CSV Foreign Table row encoder additionally mirrors the
private `CopyToStateData` layout and the text/CSV part of `CopyOneRowTo` from
`copyto.c`. It creates the COPY output state with a typed, zero-row query and
uses the same output functions and escaping as PostgreSQL, while returning the
completed row buffer to Rust without invoking a callback per row. This private
layout and serializer contract was compared across `REL_17_0` through
`REL_17_10`. It is unchanged throughout that epoch, so the row encoder has no
minor-version branch.

The shared `lakebase_pg_compat.h` gate rejects every major except PostgreSQL
17. The public COPY preparation has local semantic epochs: PG17.0-17.6 has no
generated-column validation, PG17.7-17.9 adds that validation, and PG17.10
adds the system-attribute guard that the bridge also applies to the earlier
validation epoch. The private row-encoder contract restricts the current C
build to the PG17 major line. A future PG17 minor that changes its mirrored
layout or serializer must add a local `PG_VERSION_NUM` branch here; a new major
remains rejected by the shared compatibility gate.

Before adopting a new PostgreSQL baseline or PG17 minor release:

1. Compare the internal declarations in `commands/copy.h`.
2. Compare `DoCopy` preparation ordering and relation/permission handling in
   `commands/copy.c`, including each supported PG17 minor epoch.
3. Compare the state lifecycle and callback contracts in `copyfrom.c` and
   `copyto.c`.
4. Add a local `PG_VERSION_NUM` branch only when a COPY bridge contract differs.
5. Reconcile the Rust FFI declarations and run the complete COPY regression
   matrix before enabling that PostgreSQL version.
