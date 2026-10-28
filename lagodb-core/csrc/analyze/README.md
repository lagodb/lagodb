# ANALYZE sampler adapter

## PostgreSQL source provenance

- Baseline releases: PostgreSQL 16.15, 17.11 and 18.6
- Upstream tags: `REL_16_15`, `REL_17_11` and `REL_18_6`
- Private layout source (PostgreSQL 17 and 18): `src/backend/storage/aio/read_stream.c`
- Private layout SHA-256 epochs:
  - PG17.0-17.4:
    `8d9bc88420e3979af108e787e243cb5792c96f7eac3ad9d159b444a223074e62`
  - PG17.5-17.11:
    `5b4638b6f9f101f9de5a4378025ed25bd28521ada4855f1aec4a2d07718e4000`
  - PG18.6:
    `14d2016e5c51e8705b357450b3d561edf1af6c44907e77edb7a902e1ef91693f`
- ANALYZE owner: `src/backend/commands/analyze.c`
- ANALYZE SHA-256:
  - PG16.15:
    `41ff28e75d8bc04a678349f9905ba56c4175e166eadff536fdf0d6f5ef745698`
  - PG17.11:
    `88bd83b0cefa3ac9cc164982bca119d63b709181061c1cfea59247869c037694`
  - PG18.6:
    `9458dc18af4ad14e76935aaa3b58b45695679d0b1c4273f7f27db1c1fc6d5310`
- Public sampler layout: `src/include/utils/sampling.h`
- Sampler-layout SHA-256:
  - PG17.11:
    `12808e5c50e949771afe6e495de4d78cfc867ca3be7104d002fc31a45877b083`
  - PG18.6:
    `459be28db8d1dfc85367f5b3644047e949f32d0751e7007128068823edb56168`

PostgreSQL's own `analyze_rel()` runs ANALYZE for every provider table, through
the TableAM sampling callbacks, so autovacuum's analyze scheduling and statistics
publication apply unchanged. LagoDB adds no ANALYZE executor.

`lagodb_analyze_sampler.c/.h` owns the extension-local private-layout adapter.
Its private `ReadStream` definition and layout epochs stay in that C file;
the header exposes only the sampler snapshot and its accessor. The adapter
does not replace PostgreSQL's `ReadStream` or TableAM ABI.
In PostgreSQL 17 and 18, `acquire_sample_rows()` initializes a stack-owned
`BlockSamplerData` with its actual `targrows` argument and passes `&bs` as the
ReadStream callback-private pointer. For inherited ANALYZE the caller has
already replaced that argument with the relation's proportional
`childtargrows`, so every physical scan exposes its own exact target.

PG17.5 inserted `io_combine_limit` immediately after `max_ios`, before the
callback fields used by the bridge. PG18 changed multiple fields before the
callback and callback-private pointer. `lagodb_analyze_sampler.c` selects these
known layout epochs locally. Before updating the PostgreSQL minor release or
adding another major:

1. Compare the three upstream files and refresh the hashes above.
2. Reconcile every field of the copied private `struct ReadStream`.
3. Confirm `block_sampling_read_stream_next()` still casts
   `callback_private_data` to `BlockSamplerData *`.
4. Confirm `acquire_sample_rows()` still passes `&bs` to
   `read_stream_begin_relation()` for TableAM ANALYZE scans.
5. Run the ANALYZE regression matrix, including ordinary, column-list,
   inherited, repeated-relation, empty, and high-target cases.

Rust validates both snapshots of the sampler against the tickets consumed
from that same stream. Those checks protect sampler semantics. The recorded
source hashes document the audited baseline; the private layout must be
reviewed explicitly whenever the supported PostgreSQL version changes.

PG18 creates the ANALYZE stream with `READ_STREAM_USE_BATCHING`, but a
provider consumes sampling tickets through the exported
`read_stream_next_block()` function. That function calls the stream callback
directly; it does not enter `read_stream_look_ahead()` or submit relation I/O.
The batching flag therefore does not change provider ticket order, count, or
lifetime. Native TableAM implementations continue to use PostgreSQL's normal
buffer-reading path, where the flag controls batched asynchronous I/O.

## PostgreSQL 16

PostgreSQL 16 has no `ReadStream`, so `build.rs` omits this adapter and the
sampler has no private layout to read. `acquire_sample_rows()` drives
`scan_analyze_next_block(scan, blockno, bstrategy)` and scales
`liverows / bs.m` by `RelationGetNumberOfBlocks()`. Core exposes that callback
through a PG16-only `TableAccessMethod` signature; it does not emulate the
ticket protocol. A provider without PostgreSQL blocks reports its row-weight
correction through `AnalyzeTupleOutcome::end_of_block_with_live_delta`.

On `USE_PREFETCH` builds, such as PG16 on Linux, `PrefetchBuffer()` reaches
`mdprefetch()`, which requires the block's segment file to exist. The first
main-fork segment that holds a provider's synthetic blocks must therefore exist,
although it can be empty, and `relation_size` must stay within `RELSEG_SIZE`.
`RelFileLocator::create_storage` WAL-logs that fork for permanent and UNLOGGED
relations, so a relation without an init fork keeps it through crash recovery
and promotion.

PostgreSQL also stores these blocks as `pg_class.relpages`. With Iceberg's 8 MiB
ANALYZE blocks, that value is about 1/1024 of its `BLCKSZ` page count, short of
rounding and the `RELSEG_SIZE` cap. The planner does not read it, because
`relation_estimate_size` reports `BLCKSZ` pages from storage bytes.

PostgreSQL 16 keeps `targrows` private, so `RelationHandle::analyze_sample_budget`
offers `max(100, 300 * largest resolved statistics target)` rows. That covers
PostgreSQL's built-in analyzers. A custom `typanalyze` that sets a larger
`minrows` silently gets a smaller sample, as `VacAttrStats` permits; LagoDB
neither rejects it nor logs per-ANALYZE warnings. Managed Iceberg reaches this
only after a superuser replaces an analyzer with
`ALTER TYPE ... SET (ANALYZE = ...)`, because its type admission is a closed set.

### Why PG16 has no LagoDB ANALYZE executor/fork

LagoDB deliberately leaves PG16 ANALYZE under PostgreSQL's native
`analyze_rel()` and `do_analyze_rel()` control flow. The PG16 TableAM boundary
does not expose enough invocation context to reproduce that control flow in a
provider callback:

- `table_beginscan_analyze()` starts a scan without the selected column list or
  `targrows`.
- `scan_analyze_next_block()` receives only the selected `blockno` and buffer
  strategy; it does not receive the sample target.
- The actual `targrows` is a local parameter of PostgreSQL's static
  `acquire_sample_rows()`.
- `acquire_inherited_sample_rows()` computes each proportional
  `childtargrows`, then passes it to `acquire_sample_rows()` without exposing it
  through TableAM.
- PG16 calls `PrefetchBuffer()` directly from `acquire_sample_rows()`, outside
  a TableAM callback that a non-heap provider could intercept or veto.

Consequently, neither a Rust TableAM adapter nor a provider-local state object
can recover the exact target, selected columns, inherited allocation, or
prefetch policy. Catalog inspection can only derive a relation-wide
approximation; it cannot reconstruct the current invocation because a
column-list ANALYZE and an inherited parent use context that is absent at the
TableAM boundary.

Removing these limitations would require core to own a PG16-specific copy of
the ANALYZE executor around `do_analyze_rel()`, `acquire_sample_rows()`, and
`acquire_inherited_sample_rows()`, or require a PostgreSQL kernel change. That
fork would have to track PostgreSQL's type and index analyzers, memory contexts,
progress reporting, cost delay, statistics publication, error cleanup, and
minor-version control-flow changes. A ProcessUtility-only route would also not
cover autovacuum, which invokes PostgreSQL's ANALYZE machinery internally.

The project therefore keeps one PostgreSQL-owned execution path for explicit
ANALYZE and autovacuum. This preserves PostgreSQL's scheduling, lifecycle, and
statistics-publication semantics and avoids a large version-specific source
fork. The tradeoff is intentional: built-in non-inherited ANALYZE remains
statistically valid, while the inherited sample-composition limitations below
are documented PG16 capability restrictions.

### Consequences of not owning the PG16 ANALYZE executor

The PG16 TableAM callbacks expose neither the invocation's selected columns nor
its actual `targrows`. For inherited ANALYZE they also hide the `childtargrows`
that PostgreSQL computed from the parent's statistics requirements and child
sizes. The relation-local budget is therefore exact enough for built-in,
non-inherited ANALYZE, but has two known consequences:

- A column-list ANALYZE can generate candidates for a higher target on an
  unselected column. PostgreSQL's reservoir still produces correct statistics,
  but the provider has already paid the excess scan and tuple-output cost.
- During inherited ANALYZE, a child whose local budget is below its allocated
  `childtargrows` contributes too few rows to the parent's combined sample.
  That changes the parent's MCV, histogram, and selectivity statistics.

PG16 also uses the same `relation_size` both as the block-sampling population
and as the inherited child-size weight, while prefetching those block numbers
through heap storage before the TableAM callback. Iceberg maps 8 MiB to one
synthetic block and caps the result at `RELSEG_SIZE` to keep prefetch inside the
only physical segment. Children below 8 MiB can consequently collapse to one
block, and sufficiently large children can collapse to the same cap, changing
their inherited sample proportions. Per-child live-row correction preserves
row-count estimates but cannot reweight the rows already combined for parent
column statistics.

These limitations do not bias a non-inherited built-in ANALYZE. Removing them
requires core to own the PG16 `acquire_sample_rows()` and inherited allocation
control flow so it can pass the exact target and separate logical child weight
from physical prefetch. Adjusting the relation-local budget or synthetic block
constant alone cannot recover that hidden invocation context.
