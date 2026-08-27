# ANALYZE C bridge

## PostgreSQL 17 provenance

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

`lagodb_analyze.c` is an extension-local, narrow private-layout adapter. It
does not replace PostgreSQL's `ReadStream`, `analyze_rel()`, or TableAM ABI.
PostgreSQL 17.10 `acquire_sample_rows()` initializes a stack-owned
`BlockSamplerData` with its actual `targrows` argument and passes `&bs` as the
ReadStream callback-private pointer. For inherited ANALYZE the caller has
already replaced that argument with the relation's proportional
`childtargrows`, so every physical scan exposes its own exact target.

PG17.5 inserted `io_combine_limit` immediately after `max_ios`, before the
callback fields used by the bridge. `lagodb_analyze.c` selects these two
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
