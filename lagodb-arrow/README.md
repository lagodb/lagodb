# lagodb-arrow

[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-15%20%7C%2016%20%7C%2017%20%7C%2018-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

**PostgreSQL⇆Arrow interoperability for LagoDB runtimes and providers.**

`lagodb-arrow` owns the shared Arrow boundary: format-neutral conversion between
Arrow arrays and PostgreSQL datums/slots, plus the typed provider adapter that
exports serial and parallel-worker scans through the Arrow C Stream ABI. It
contains no query-engine execution logic and never names a table or file
format.

The crate is a library (`rlib`) linked into the extension crates that need it:

- [lagodb-iceberg](../lagodb-iceberg) uses the conversion APIs and implements
  the typed table-scan provider SPI.
- [lagodb-connectors](../lagodb-connectors) uses the conversion APIs for its
  file-format FDW and COPY paths.
- [lagodb-query](../lagodb-query) uses the explicit physical `DatumCodec`s for
  query results and the array reader/builder for PostgreSQL expression
  evaluation.

It is not a PostgreSQL extension itself and produces no `cdylib`.

## Why this crate exists

Every consumer in this workspace meets PostgreSQL through Arrow. Once the data
is Arrow, the rules for converting each value to and from a PostgreSQL datum are
identical regardless of which format produced it: integer widening, decimal
scale, the PostgreSQL↔Unix epoch offset for temporal types, varlena detoast,
UUID byte order, and so on.

What differs between consumers is the **format schema → Arrow schema** mapping
and the **column model** (slot positions for an access method, foreign-table
columns for an FDW, expression arguments for the query engine). Each consumer
keeps those; `lagodb-arrow` takes over the moment an Arrow schema exists.

The public `scan` module is the provider-facing half of the same boundary. It
converts typed provider plans, prepared handles, schemas, and streams into
`lagodb-core`'s Arrow-independent exact-build descriptor ABI. The host registry
remains in `lagodb-base`, and the query-engine consumer in `lagodb-query`.

## Principle: bind once, convert per row

The leaf conversion is determined by a pair: the Arrow `DataType` of the source
column and the target PostgreSQL type. The Arrow `DataType` already carries
decimal precision/scale, timestamp unit and timezone, and binary width, so the
PostgreSQL type is load-bearing for one distinction Arrow cannot make on its
own — separating a `uuid` from a fixed-width `bytea` when both arrive as
`FixedSizeBinary(16)`.

`CellRule::resolve` decides that pair, and `BoundDatumPlan::bind` extends it to
a declared PostgreSQL type, recursing through arrays, composites, intervals and
nested JSONB. All fallible setup — rule resolution, decimal codecs, array
element layouts, UTF-8 server-encoding checks — happens while a column is
bound. Binding a batch only selects concrete arrays, and each row reuses the
bound reader or encoder with no further type dispatch. Incompatible pairs are
rejected up front rather than mid-scan.

A provider or query engine that uses a private physical representation (for
example, complete PostgreSQL numeric varlena bytes in Arrow `Binary`) binds the
corresponding explicit `DatumCodec` at its own planning boundary; the
format-neutral resolver never infers that representation from a PostgreSQL OID.

## Read and write paths

- **Read:** `ArrowColumnDecoder` binds each `RecordBatch` once with
  `bind_batch_unchecked`, trusting the producer to emit the scan schema the
  decoder was built from, and writes one batch row straight into a slot. The
  producing crate owns its batch iterator and error conversion.
- **Write:** `BoundWriteBuffer` appends relation slot rows and
  `BoundDatumBuffer` appends slot-free datum streams such as COPY into one Arrow
  builder per column; both implement `BatchBuffer` and produce a `RecordBatch`
  on flush.
- **Expressions:** `PgDatumArrayReader` / `PgDatumArrayBuilder` bind one Arrow
  array to a PostgreSQL expression argument or result type.

Reads and writes of the same declared type share one `BoundDatumPlan`, so a
value written by one path reads back bit-identically through the other.

## Memory and error discipline

The conversion functions never own buffers that must outlive a callback. The
batch source forwards a single live batch held by core's cursor; the read
decoder palloc's varlena payloads into the slot's target memory context, which
the calling shim has already switched to, so per-row resets reclaim them
correctly.

Errors use the same domain-error machinery as the rest of the workspace.
`ArrowConversionError` is a `thiserror` enum that implements `lagodb-core`'s
`SqlStateError`, so each variant maps to a `PgSqlErrorCode` (datatype mismatch,
data exception, or internal error). Consumers embed it with a `#[from]` variant
in their own error type and delegate the SQLSTATE, so a conversion failure
surfaces the right error at the callback boundary. `lagodb-arrow` itself only
ever returns this plain domain error; turning it into a PostgreSQL report
happens at the consumer's callback boundary, matching how the rest of the
workspace is layered.

## Requirements

- Rust 1.97.1 or later
- PostgreSQL 15, 16, 17 or 18
- pgrx 0.19.2

Dependencies are limited to `pgrx`, `lagodb-core`, the `arrow-*` crates, and
small support crates (`uuid`, `thiserror`, and `libc`). The crate deliberately
does **not** depend on a query engine or any table-format crate.

## Testing

Pure-Rust logic (rule resolution, codec math, SQLSTATE mapping) lives in this
crate and runs with ordinary `cargo test`:

```bash
cargo test -p lagodb-arrow
```

Paths that require a live PostgreSQL backend — Arrow⇆PG encode/decode into real
slots, write/read round trips, buffer/flush behavior, and toast detoast — are
covered by the `lagodb-iceberg` and `lagodb-connectors` SQL regression suites.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE)
for details.
