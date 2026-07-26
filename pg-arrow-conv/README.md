# pg-arrow-conv

[![Rust](https://img.shields.io/badge/rust-1.96.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

**Format-neutral Arrow⇆PostgreSQL value conversion.**

`pg-arrow-conv` is the shared conversion layer between Apache Arrow data and
PostgreSQL datums. It turns a column of an Arrow `RecordBatch` into the values a
PostgreSQL tuple slot expects, and turns buffered PostgreSQL values back into an
Arrow array. It knows about Arrow and PostgreSQL only — it never names a table
format such as Iceberg, Delta, or Hudi.

The crate is a library (`rlib`) linked into the extension crates that need it
(today [pg-iceberg-am](../pg-iceberg-am); planned Hudi/Delta access methods and
an Iceberg FDW). It is not a PostgreSQL extension itself and produces no
`cdylib`.

## Why this crate exists

Every lakehouse format in this workspace materializes its data as Arrow (via
Parquet) before it reaches PostgreSQL. Once the data is Arrow, the rules for
converting each value to and from a PostgreSQL datum are identical regardless of
which format produced it: integer widening, decimal scale, the PostgreSQL↔Unix
epoch offset for temporal types, varlena detoast, UUID byte order, and so on.

What actually differs between consumers is *not* the value conversion. It is:

- the **format schema → Arrow schema** mapping (Iceberg `timestamp_ns` →
  `Timestamp(Nanosecond)`, `Uuid` → `FixedSizeBinary(16)`, and so on), and
- the **column model** — an access method maps Arrow columns to `attno`/slot
  positions, while an FDW maps them to foreign-table columns.

Pulling the value rules into one crate means each consumer implements the
[pg-lakebase-core](../pg-lakebase-core) framework interfaces once and never
rewrites the conversion logic. Each consumer keeps its own schema mapping and
column model; `pg-arrow-conv` takes over the moment an Arrow schema exists and
hands back datums (or arrays) with no knowledge of where the Arrow came from.

## Principle: dispatch on `(Arrow DataType, PgColumnType)`

The crate's central idea is that **standard semantic** conversion for a column is
determined by a pair: the Arrow `DataType` of the source column and the target
PostgreSQL column type. The Arrow `DataType` already carries decimal
precision/scale, timestamp unit and timezone, and binary width, so the target
PostgreSQL type is load-bearing for exactly one distinction Arrow cannot make on
its own — separating a `uuid` from a fixed-width `bytea` when both arrive as
`FixedSizeBinary(16)`.

This pair is resolved **once per column** into a `ColumnRule`, an enum that
captures everything the conversion needs (and nothing about column position or
format type). After resolution, every row of the batch reuses the same rule with
no further type dispatch or allocation. Schemas that pair an Arrow type with an
incompatible PostgreSQL type, or that use an Arrow type the layer cannot
materialize, are rejected up front rather than failing mid-scan. A provider that
uses a private physical representation (for example, complete PostgreSQL
JSONB varlena bytes in Arrow `Binary`) must bind the corresponding explicit
`ColumnRule` and `DatumCodec` at its own column-planning boundary; the
format-neutral resolver never infers that representation from `JSONBOID`.

## Two worlds, one set of rules

`pg-arrow-conv` mirrors the row/column split that `pg-lakebase-core` defines, and
keeps both sides driven by the same per-column rule so a value converted one way
is bit-identical to the same value converted the other.

**Column world (the hot path).** The columnar scan and DML paths work directly
in terms of Arrow batches and tuple slots, skipping any owned intermediate
representation:

- On read, a batch source yields one Arrow `RecordBatch` at a time and a row
  decoder writes one row of that batch straight into a slot, column by column.
- On write, a relation-bound slot-fed buffer appends validated PostgreSQL
  datums into one Arrow column builder per column and produces a `RecordBatch`
  on flush.

The read half implements the `AmScanBatchSource` / `BatchRowDecoder` traits from
`pg-lakebase-core`; the relation-bound write buffer implements `BatchBuffer`
while keeping its provider-specific source binding in `pg-arrow-conv`.

**Row world (FDW and row-mode).** A row-at-a-time consumer (an FDW, a row-mode
access method, or buffering and `EXPLAIN` rendering) works through `pg-lakebase-core`'s
owned `Cell`/`Row` types instead. After the caller establishes the row bound,
the bound `ColumnReader::read_cell_unchecked` extracts a standard semantic
Arrow value into a `Cell`, and the same `ColumnRule` builds an Arrow array from
buffered `Cell`s (`ColumnRule::build`). A provider-owned physical codec is
intentionally not exposed as a semantic `Cell` through
`read_cell_unchecked`; its slot path binds the `DatumCodec` once and copies
directly into the destination slot. Because the build path drives the very same
column encoder the columnar hot path uses, the two write sources stay in
lockstep.

## Memory and error discipline

The conversion functions never own buffers that must outlive a callback. The
batch source forwards a single live batch held by core's cursor; the read
decoder palloc's varlena payloads into the slot's target memory context, which
the calling shim has already switched to, so per-row resets reclaim them
correctly.

Errors use the same domain-error machinery as the rest of the workspace.
`ArrowConversionError` is a `thiserror` enum that implements `pg-lakebase-core`'s
`SqlStateError`, so each variant maps to a `PgSqlErrorCode` (datatype mismatch,
data exception, or internal error). Consumers embed it with a `#[from]` variant
in their own error type and delegate the SQLSTATE, so a conversion failure
surfaces the right error at the callback boundary. `pg-arrow-conv` itself only
ever returns this plain domain error; turning it into a PostgreSQL report
happens at the consumer's callback boundary, matching how the rest of the
workspace is layered.

## Requirements

- Rust 1.96.0 or later
- PostgreSQL 16 or 17
- pgrx 0.18.1

Dependencies are limited to `pgrx`, `pg-lakebase-core`, the `arrow-*` crates, and
`uuid`. The crate deliberately does **not** depend on any table-format crate.

## Testing

Pure-Rust logic (rule resolution, codec math, validator parity) lives in this
crate and runs with ordinary `cargo test`:

```bash
cargo test -p pg-arrow-conv
```

Paths that require a live PostgreSQL backend — Arrow⇆PG encode/decode into real
slots, encoder/decoder equivalence, buffer/flush behavior, and toast detoast —
are `#[pg_test]` tests hosted in the aggregator crate
[pg-backend-tests](../pg-backend-tests):

The aggregator preloads `pg_lakebase_runtime`. Install that runtime into the
target pgrx PostgreSQL installation before running the test:

```bash
cargo pgrx install \
  --package pg-lakebase-runtime \
  --pg-config "$(cargo pgrx info pg-config pg17)"

cargo pgrx test pg17 --package pg-backend-tests
```

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE)
for details.
