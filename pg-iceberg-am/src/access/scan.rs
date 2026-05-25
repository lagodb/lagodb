//! Iceberg Table Scan Implementation.
//!
//! # Architecture
//!
//! A scan's state is split into two parts with very different lifetimes, and
//! the [`AmScanSession`] callbacks orchestrate them:
//!
//! - [`ScanSpec`] is the immutable description of the scan: the
//!   `Arc<Table>`, a [`RecordBatchRowReader`] bound to the snapshot's
//!   schema, the optional column projection, and the optional Iceberg
//!   [`Predicate`] translated from PostgreSQL's `OwnedScanKeys`. It is
//!   built once in [`AmScanSession::scan_begin`] and is preserved across
//!   `scan_rescan`. The metadata (and therefore the visible snapshot) is
//!   therefore frozen for the duration of the scan.
//!
//!   This is a *deliberate* behavior change from the previous implementation,
//!   which re-resolved `TxMetadata::current_table_metadata(...)`
//!   inside every `scan_rescan` call. The PostgreSQL Read Committed contract
//!   wants one statement to see one consistent snapshot; because every
//!   `scan_rescan` is issued by the same statement that issued the original
//!   `scan_begin` (e.g. as the inner side of a nested-loop join), holding
//!   the metadata is the more correct behavior, not the less. New work
//!   committed by other backends becomes visible at the next
//!   *statement*'s `scan_begin`. TODO: cover this with a regression test
//!   once the pgrx test harness for `pg-iceberg-am` is stood up.
//!
//! - [`ScanCursor`] is the per-cursor mutable state: the Arrow iterator and
//!   the position within the current `RecordBatch`. `scan_rescan` rebuilds
//!   only the cursor from the spec.
//!
//! `scan_rescan` honors the keys handed in by the dispatcher:
//! [`ScanSpec::set_filter`] is replaced from the dispatcher-owned key set
//! every rescan (the dispatcher has already applied PostgreSQL's
//! "non-null replaces, null keeps" rule before calling us). The same
//! `&OwnedScanKeys` is also surfaced to [`AmScanSession::scan_begin`] so
//! that the *initial* scan and every subsequent rescan go through exactly
//! the same translation path. The borrow is only valid for the duration of
//! each callback; the AM consumes it (translates into a `Predicate` stored
//! on the spec) before returning, rather than retaining the reference.
//!
//! Today the [`scan_keys_to_predicate`] translator is a stub that always
//! returns `None`, which means non-pushed-down predicates remain handled by
//! the executor (`ExecQual`) above the scan. The path is wired so that
//! adding real predicate pushdown is a localized change in
//! [`scan_keys_to_predicate`] only — the AM-level scan lifecycle no longer
//! needs to be touched.

use std::sync::Arc;

use arrow_array::RecordBatch;
use iceberg_lite::expr::Predicate;
use iceberg_lite::scan::ArrowRecordBatchIterator;
use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::prelude::*;
use pg_lakebase_core::tuple::Row;
use pgrx::pg_sys;

use crate::IcebergTableAm;
use crate::access::conversion::RecordBatchRowReader;
use crate::catalog::bridge::IcebergTableId;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::StorageContext;

// ---------------------------------------------------------------------------
// ScanSpec: immutable description of the scan
// ---------------------------------------------------------------------------

/// Immutable parameters for a scan: which table, which snapshot's schema,
/// which columns, and which predicate.
///
/// Construction (`ScanSpec::build`) is the only place metadata is read from
/// storage during a scan's life; `scan_rescan` reuses an existing `ScanSpec`
/// and rebuilds only the [`ScanCursor`].
struct ScanSpec {
    /// Ready-to-scan Iceberg table. Cheap to clone (`Arc`-backed internally).
    table: Arc<Table>,
    /// Schema-bound row reader for the snapshot captured in `table`. Owns
    /// the per-column dispatch plan used by [`ScanCursor::next_row`] and
    /// also exposes the underlying `IcebergSchema` for predicate
    /// translation.
    row_reader: RecordBatchRowReader,
    /// Column projection. `None` means "select all".
    ///
    /// Reserved for projection pushdown; today the AM has no path to populate
    /// it, but keeping the field here means turning it on later is a
    /// non-structural change.
    projection: Option<Vec<String>>,
    /// Predicate translated from PostgreSQL `ScanKey`, if any. Replaced (not
    /// merged) by `scan_rescan` whenever the dispatcher updates the
    /// [`OwnedScanKeys`] buffer.
    filter: Option<Predicate>,
}

impl ScanSpec {
    /// Build a `ScanSpec` by resolving the current visible metadata location
    /// for `rel_oid` and constructing an Iceberg [`Table`].
    fn build(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        keys: &OwnedScanKeys,
    ) -> IcebergResult<Self> {
        let ctx = StorageContext::for_tablespace(spc_oid)?;

        // Single point of metadata IO for the entire scan. Read here, then
        // reuse across every `scan_rescan` until `scan_end`.
        let loaded =
            TxMetadata::current().current_table_metadata(rel_oid, ctx.file_io())?;
        let schema = loaded.metadata.current_schema().clone();

        // Translate scan keys *after* the schema is in hand: the translator
        // needs Iceberg type and field-id information that only exists once
        // metadata has been parsed.
        let filter = scan_keys_to_predicate(keys, &schema)?;

        let table = Table::builder()
            .file_io(ctx.file_io().clone())
            .metadata_location(loaded.location)
            .metadata(loaded.metadata)
            .identifier(IcebergTableId::for_relation(rel_oid).into_table_ident())
            .build()?;

        Ok(Self {
            table: Arc::new(table),
            row_reader: RecordBatchRowReader::new(schema)?,
            projection: None,
            filter,
        })
    }

    /// Construct a fresh [`ScanCursor`] from this spec.
    ///
    /// Called once in `scan_begin` and again per `scan_rescan`. Does no
    /// catalog or metadata IO: `self.table` already has metadata in memory,
    /// and `to_arrow()` only resolves manifests/data files for this snapshot.
    fn open_cursor(&self) -> IcebergResult<ScanCursor> {
        let mut builder = self.table.scan();
        if let Some(columns) = self.projection.as_deref() {
            builder = builder.select(columns.iter().map(String::as_str));
        } else {
            builder = builder.select_all();
        }
        if let Some(predicate) = self.filter.as_ref() {
            builder = builder.with_filter(predicate.clone());
        }
        let table_scan = builder.build()?;
        Ok(ScanCursor {
            iterator: table_scan.to_arrow()?,
            current_batch: None,
            current_row_idx: 0,
        })
    }

    /// Re-translate the current effective [`OwnedScanKeys`] into a filter,
    /// using this spec's already-resolved Iceberg schema.
    fn refresh_filter(&mut self, keys: &OwnedScanKeys) -> IcebergResult<()> {
        self.filter = scan_keys_to_predicate(keys, self.row_reader.schema())?;
        Ok(())
    }

    fn row_reader(&self) -> &RecordBatchRowReader {
        &self.row_reader
    }
}

// ---------------------------------------------------------------------------
// ScanCursor: mutable per-cursor state
// ---------------------------------------------------------------------------

/// Mutable iteration state for one cursor over a [`ScanSpec`].
///
/// `scan_rescan` drops this and asks the spec for a fresh one.
struct ScanCursor {
    iterator: ArrowRecordBatchIterator,
    current_batch: Option<RecordBatch>,
    current_row_idx: usize,
}

impl ScanCursor {
    /// Pull the next row into `row`, advancing through `RecordBatch`es as
    /// needed. Returns `Ok(false)` at end-of-scan.
    fn next_row(
        &mut self,
        reader: &RecordBatchRowReader,
        row: &mut Row,
    ) -> IcebergResult<bool> {
        loop {
            if let Some(ref batch) = self.current_batch
                && self.current_row_idx < batch.num_rows()
            {
                reader.read_row(batch, self.current_row_idx, row)?;
                self.current_row_idx += 1;
                return Ok(true);
            }

            match self
                .iterator
                .next()
                .transpose()
                .map_err(IcebergError::from)?
            {
                Some(batch) => {
                    self.current_batch = Some(batch);
                    self.current_row_idx = 0;
                }
                None => return Ok(false),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// IcebergScan: AmScanSession orchestrator
// ---------------------------------------------------------------------------

/// PostgreSQL-facing scan session for the Iceberg table AM.
///
/// Holds bookkeeping (`rel_oid` / `spc_oid`) plus the lazily-built [`ScanSpec`]
/// and current [`ScanCursor`]. The struct itself is intentionally thin: the
/// real scan logic lives on `ScanSpec` and `ScanCursor`.
pub struct IcebergScan {
    rel_oid: pg_sys::Oid,
    spc_oid: pg_sys::Oid,
    spec: Option<ScanSpec>,
    cursor: Option<ScanCursor>,
}

impl AmScan for IcebergTableAm {}

impl AmScanSession for IcebergScan {
    fn new(
        rel: &RelationHandle,
        _snapshot: &SnapshotHandle,
        _pscan: Option<&ParallelTableScanDescHandle>,
        _flags: u32,
    ) -> AmResult<Self> {
        // No metadata IO yet: defer all schema-dependent work to
        // `scan_begin`, where the dispatcher also surfaces the initial
        // effective scan keys.
        Ok(IcebergScan {
            rel_oid: rel.oid(),
            spc_oid: rel.tablespace_oid(),
            spec: None,
            cursor: None,
        })
    }

    fn scan_begin(&mut self, keys: &OwnedScanKeys) -> AmResult<()> {
        let spec = ScanSpec::build(self.rel_oid, self.spc_oid, keys)?;
        let cursor = spec.open_cursor()?;
        self.spec = Some(spec);
        self.cursor = Some(cursor);
        Ok(())
    }

    fn scan_getnextslot(
        &mut self,
        _direction: ScanDirection,
        row: &mut Row,
    ) -> AmResult<bool> {
        // TODO(rowless-scan): once core exposes a slot-first scan callback,
        // move this path from `RecordBatchRowReader -> Row -> TupleSlotWriter`
        // to an Iceberg scan-slot writer that consumes the current Arrow
        // `RecordBatch` column values directly. The writer should own the
        // schema-bound per-column encoders and write Datum/null values into the
        // caller's `TupleTableSlot` in the callback memory context. That keeps
        // primitive reads cheap and prevents list/string columns from
        // allocating `Cell::*Array`, `Vec<Option<_>>`, or element `String`
        // values just to immediately convert them back into PostgreSQL Datums.
        let (Some(spec), Some(cursor)) = (self.spec.as_ref(), self.cursor.as_mut())
        else {
            return Ok(false);
        };
        Ok(cursor.next_row(spec.row_reader(), row)?)
    }

    /// Restart the scan, re-translating the current effective scan keys.
    ///
    /// PostgreSQL's "non-null replaces, null keeps" rule has already been
    /// applied by the core dispatcher to the [`OwnedScanKeys`] buffer
    /// (see `pg-lakebase-core/src/access/scan.rs::scan_rescan`), so `keys`
    /// here is always the effective key set for the restarted scan; we
    /// simply re-translate it on top of the already-resolved schema.
    ///
    /// `set_params` and the `allow_*` flags only affect heap-AM scan
    /// strategy choices (sync scan, page mode, BufferAccessStrategy) which
    /// the Iceberg reader does not use, so they are explicitly ignored.
    ///
    /// Note: metadata is *not* re-read here. The whole point of holding a
    /// `ScanSpec` across rescans is that a single statement (which is what
    /// drives any `scan_rescan` call, including the inner side of a
    /// nested-loop) sees a consistent snapshot.
    fn scan_rescan(
        &mut self,
        keys: &OwnedScanKeys,
        _set_params: bool,
        _allow_strat: bool,
        _allow_sync: bool,
        _allow_pagemode: bool,
    ) -> AmResult<()> {
        let Some(spec) = self.spec.as_mut() else {
            // Rescan before the first scan_begin shouldn't happen via the
            // dispatcher, but be defensive: just clear the cursor and let a
            // subsequent scan_begin populate state.
            self.cursor = None;
            return Ok(());
        };

        spec.refresh_filter(keys)?;
        self.cursor = Some(spec.open_cursor()?);
        Ok(())
    }

    fn scan_end(&mut self) -> AmResult<()> {
        self.cursor = None;
        self.spec = None;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ScanKey -> Predicate translation
// ---------------------------------------------------------------------------

/// Translate PostgreSQL [`OwnedScanKeys`] into an Iceberg [`Predicate`],
/// using `schema` to resolve column names and types.
///
/// This is the *only* place the AM converts PostgreSQL filter
/// representations into Iceberg expressions. Keeping the translation in one
/// schema-aware function means predicate-pushdown work can land here
/// without touching the scan lifecycle.
///
/// Current status: stub.
///
/// Returning `Ok(None)` is safe today because PostgreSQL only supplies
/// `ScanKey`s through `IndexScan` / `BitmapIndexScan` / `SampleScan` /
/// `TidRangeScan` / analyze paths, none of which the Iceberg AM advertises
/// support for. Plain `SeqScan` calls `table_beginscan(rel, snap, 0, NULL)`
/// and lets the executor (`ExecQual`) apply `WHERE` above the scan, so no
/// rows are missed by ignoring the keys here.
///
/// This *will* become load-bearing as soon as predicate pushdown is wired
/// up: at that point the trait contract that we now honor (translate, and
/// re-translate on rescan against the current effective key set) makes
/// parameterized nested-loop joins return correct results.
fn scan_keys_to_predicate(
    _keys: &OwnedScanKeys,
    _schema: &IcebergSchema,
) -> IcebergResult<Option<Predicate>> {
    // TODO(predicate-pushdown): for each ScanKeyEntry,
    //   * map sk_attno -> Iceberg field via `_schema.field_by_id` (after
    //     converting the PostgreSQL attno to the corresponding field id;
    //     the AM stores the column-order mapping when the table is
    //     created).
    //   * map sk_strategy + sk_subtype + sk_argument into a `Predicate`
    //     (Eq / LessThan / etc.), interpreting the `Datum` according to
    //     the Iceberg field type.
    //   * combine across keys with `Predicate::and`.
    //   * If any single key cannot be translated, return `Ok(None)` for the
    //     whole set rather than a partial predicate, so the executor
    //     remains responsible for correctness.
    Ok(None)
}
