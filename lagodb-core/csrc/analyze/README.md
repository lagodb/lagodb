# ANALYZE C bridge

## PostgreSQL source provenance

- Baseline release: PostgreSQL 17.10
- Upstream tag: `REL_17_10`
- Private layout source: `src/backend/storage/aio/read_stream.c`
- Private layout SHA-256 epochs:
  - PG17.0-17.4:
    `8d9bc88420e3979af108e787e243cb5792c96f7eac3ad9d159b444a223074e62`
  - PG17.5-17.10:
    `5b4638b6f9f101f9de5a4378025ed25bd28521ada4855f1aec4a2d07718e4000`
- ANALYZE owner: `src/backend/commands/analyze.c`
- ANALYZE SHA-256:
  `88bd83b0cefa3ac9cc164982bca119d63b709181061c1cfea59247869c037694`
- Public sampler layout: `src/include/utils/sampling.h`
- Sampler-layout SHA-256:
  `12808e5c50e949771afe6e495de4d78cfc867ca3be7104d002fc31a45877b083`

`lagodb_analyze.c/.h` owns the PostgreSQL-derived ANALYZE executor for native
relations and provider-owned partitioned tables. It follows the `analyze.c` source
baseline, including PostgreSQL's statistics computation and publication flow.

`lagodb_analyze_sampler.c/.h` owns the extension-local private-layout adapter.
Its private `ReadStream` definition and layout epochs stay in that C file;
the header exposes only the sampler snapshot and its accessor. The adapter
does not replace PostgreSQL's `ReadStream` or TableAM ABI.
In PostgreSQL 17, `acquire_sample_rows()` initializes a stack-owned
`BlockSamplerData` with its actual `targrows` argument and passes `&bs` as the
ReadStream callback-private pointer. For inherited ANALYZE the caller has
already replaced that argument with the relation's proportional
`childtargrows`, so every physical scan exposes its own exact target.

PG17.5 inserted `io_combine_limit` immediately after `max_ios`, before the
callback fields used by the bridge. `lagodb_analyze_sampler.c` selects these two
known layout epochs locally. Before updating the PostgreSQL minor release or
adding another major:

1. Compare the three upstream files and refresh the hashes above.
2. Reconcile every field of the copied private `struct ReadStream`.
3. Confirm `block_sampling_read_stream_next()` still casts
   `callback_private_data` to `BlockSamplerData *`.
4. Confirm `acquire_sample_rows()` still passes `&bs` to
   `read_stream_begin_relation()` for TableAM ANALYZE scans.
5. Run the ANALYZE regression matrix, including ordinary, column-list,
   inherited, partitioned, repeated-relation, empty, and high-target cases.

Rust validates both snapshots of the sampler against the tickets consumed
from that same stream. Those checks protect sampler semantics. The recorded
source hashes document the audited baseline; the private layout must be
reviewed explicitly whenever the supported PostgreSQL version changes.

The provider-root path in `lagodb_analyze.c` runs the normal
non-inherited `do_analyze_rel()` path for a provider-owned
`RELKIND_PARTITIONED_TABLE`. A callback-scoped shallow Relation view supplies
the catalog-selected AM and an OID-based synthetic smgr identity needed only
to construct PostgreSQL's sampling `ReadStream`; the Iceberg callback consumes
sampler tickets and performs no buffer I/O. The synthetic smgr handle is
closed in `PG_FINALLY`, while the relcache object and `pg_class.relkind` are
never modified. For native relations the same fork follows the unmodified
PostgreSQL `analyze_rel()` branch. Ownership is resolved from the live Relation only
after `ShareUpdateExclusiveLock` is acquired, so the preceding `NoLock`
statement probe and expansion-time catalog state are never treated as an
execution plan. Native-only explicit statements do not enter this fork: they
are passed unchanged to the captured ProcessUtility parent.

Provider-root sampling does not request PostgreSQL's heap XID visibility
horizon: `GetOldestNonRemovableTransactionId()` accepts only ordinary tables,
materialized views, and TOAST relations. The provider callback resolves row
visibility from its Iceberg snapshot and ignores the `OldestXmin` argument,
which this path sets to `InvalidTransactionId`. Native sampling retains the
normal heap horizon calculation.

The fork carries an explicit provider-backed-root semantic class through
`do_analyze_rel()`. It retains native partitioned table behavior for PostgreSQL
indexes and visibility-map access, but applies the storage-backed relation
correction for transaction-local INSERT/UPDATE/DELETE counters before
publishing cumulative ANALYZE statistics. The stack Relation view borrows
statistics state only after that state is associated with the real relcache
Relation, so no backend-local statistics entry can retain the stack address.

The root entry receives the same `BufferAccessStrategy` selected from
`BUFFER_USAGE_LIMIT` (or `vacuum_buffer_usage_limit`) as PostgreSQL's native
ANALYZE path. It also brackets the fork with PostgreSQL's vacuum cost globals
and `PG_FINALLY` cleanup. Recursion state belongs to the runtime's maintenance
command bridge, not this relation executor. The command scope covers both
provider execution and native parent delegation, so native type/index code
cannot enter this executor through a nested provider-root ANALYZE. Relation
and VACUUM/ANALYZE phase transitions stay within the same command scope.

The fork ends after PostgreSQL's `ind_fetch_func()` and calls the backend's
exported `std_typanalyze()` instead of duplicating the type-specific statistics
algorithms. Compare the fork with `src/backend/commands/analyze.c` from the
recorded PostgreSQL release tag; the partitioned table changes and source hash
must be re-audited together on every PostgreSQL update.
