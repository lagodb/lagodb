# Runtime VACUUM C bridge

## PostgreSQL 17 provenance

- Baseline release: PostgreSQL 17.10
- Upstream tag: `REL_17_10`
- Primary source: `src/backend/commands/vacuum.c`
- Primary source SHA-256:
  `8dab2237d1d25b5870520ca4694a93bbd96a76882e179a436c35bcaacaea09bd`
- Public contract: `src/include/commands/vacuum.h`
- Public contract SHA-256:
  `273a972adfd62e0579ec22ff35c5f38cbf69ac5c2140a8e682883c6a14675449`

`lagodb_vacuum.c` is a narrow reconstruction of the private option parsing,
relation expansion and per-relation transaction boundaries needed to route a
table-maintenance provider in place of PostgreSQL's `cluster_rel()`. Native
relations remain delegated to PostgreSQL.

PostgreSQL 17.10 is the provenance baseline, not the only accepted PG17 minor.
Before adopting a new baseline or extending another major:

1. Compare option parsing and validation in `ExecVacuum()`.
2. Compare `expand_vacuum_rel()` including permissions and partitions.
3. Compare `vacuum_rel()` transaction, snapshot, security-context and search
   path boundaries.
4. Reconcile every initialized `VacuumParams` field.
5. Refresh both hashes and run the mixed provider/native FULL regression matrix.
