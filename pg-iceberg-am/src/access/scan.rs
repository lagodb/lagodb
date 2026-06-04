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
use crate::access::conversion::{LiveColumn, RecordBatchRowReader};
use crate::access::projection::Projection;
use crate::catalog::bridge::IcebergTableId;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::StorageContext;

// ---------------------------------------------------------------------------
// Relation shape: live attnos + full tuple width
// ---------------------------------------------------------------------------

/// The relation-shape inputs the converter needs to build a position-correct
/// [`ColumnPlan`](crate::access::conversion): the live (non-dropped) columns
/// (`(attno, name)`) in ascending attno order, and the full tuple width
/// (`natts`).
///
/// This is derived from the relation's `TupleDesc` once per scan and threaded
/// into [`RecordBatchRowReader`]. It contains **no** Arrow-column-to-slot
/// position arithmetic — it only records which columns are live (and their
/// names, used to resolve Iceberg fields by name) and how wide the tuple is;
/// the converter turns `attno` into `dest`.
#[derive(Debug, Clone)]
pub(crate) struct RelationShape {
    /// Live (non-dropped) columns in ascending attno order. Each carries its
    /// 1-based attno and its column name (which is also the Iceberg field
    /// name). The converter resolves the Iceberg field **by name**, so this
    /// stays correct even when the stored Iceberg schema is wider than the
    /// live PG columns (e.g. after `ALTER TABLE ... DROP COLUMN`, which does
    /// not rewrite the Iceberg metadata schema).
    live_columns: Vec<LiveColumn>,
    /// Full PG tuple width (`natts`), counting dropped-column positions.
    slot_width: usize,
}

impl RelationShape {
    /// Derive the relation shape from a live [`RelationHandle`].
    ///
    /// Uses [`RelationHandle::live_columns`] to collect each live
    /// (non-dropped) column's attno and name in ascending attno order. The
    /// full `natts` (including dropped positions) is the slot width.
    pub(crate) fn from_relation(rel: &RelationHandle) -> Self {
        let slot_width = rel.natts();
        let live_columns = rel
            .live_columns()
            .into_iter()
            .map(|(attno, name)| LiveColumn::new(attno, name))
            .collect();

        Self {
            live_columns,
            slot_width,
        }
    }

    fn live_columns(&self) -> &[LiveColumn] {
        &self.live_columns
    }

    fn slot_width(&self) -> usize {
        self.slot_width
    }
}

// ---------------------------------------------------------------------------
// ScanSpec: immutable description of the scan
// ---------------------------------------------------------------------------

/// Immutable parameters for a scan: which table, which snapshot's schema,
/// which columns, and which predicate.
///
/// Construction (`ScanSpec::build`) is the only place metadata is read from
/// storage during a scan's life; `scan_rescan` reuses an existing `ScanSpec`
/// and rebuilds only the [`ScanCursor`].
///
/// `pub(crate)` so the CustomScan provider in
/// [`crate::customscan`] can build a `ScanSpec` from a runtime-built
/// [`iceberg_lite::expr::Predicate`] (Requirement 18.4 — reuse the same
/// scan core in both the TableAM seqscan path and the CustomScan path).
pub(crate) struct ScanSpec {
    /// Ready-to-scan Iceberg table. Cheap to clone (`Arc`-backed internally).
    table: Arc<Table>,
    /// Schema-bound row reader for the snapshot captured in `table`. Owns
    /// the per-column dispatch plan used by [`ScanCursor::next_row`] and
    /// also exposes the underlying `IcebergSchema` for predicate
    /// translation.
    row_reader: RecordBatchRowReader,
    /// Column projection. `None` means "select all".
    ///
    /// `Some(Projection)` carries the `(attno, name)` pairs (in scan order)
    /// used both to `select(names)` against the Iceberg scan builder and to
    /// build the projected [`ColumnPlan`](crate::access::conversion). A
    /// `Some(Projection)` always has ≥ 1 column — select-all is `None`, never
    /// an empty `Projection`. Populated only on the CustomScan
    /// `build_with_projection` path; the seqscan and select-all paths leave
    /// it `None`.
    projection: Option<Projection>,
    /// Predicate to push into the Iceberg scan layer for manifest /
    /// file / row-group pruning. The TableAM seqscan path translates
    /// from `OwnedScanKeys` (currently a stub returning `None`); the
    /// CustomScan path supplies a runtime-built
    /// [`iceberg_lite::expr::Predicate`] built from the pushed PG
    /// `Expr`s by [`crate::customscan::IcebergPredicateTranslator`].
    /// Replaced (not merged) by `refresh_filter` /
    /// [`Self::set_filter`].
    filter: Option<Predicate>,
}

impl ScanSpec {
    /// Build a `ScanSpec` from a `RestrictInfo`-derived
    /// [`OwnedScanKeys`] (the TableAM seqscan path).
    ///
    /// `shape` is the scan relation's live-attno list + full tuple width,
    /// derived once from its `TupleDesc` (see [`RelationShape::from_relation`]).
    /// A plain SeqScan never tells the AM which columns are needed, so the
    /// projection stays `None` (all live columns) — but the full-schema
    /// [`ColumnPlan`](crate::access::conversion) built from `shape` still
    /// fixes dropped-column alignment (Requirement 6.2, 6.3, 6.5).
    pub(crate) fn build(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        keys: &OwnedScanKeys,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        // Single point of metadata IO for the entire scan. Read here, then
        // reuse across every `scan_rescan` until `scan_end`.
        let mut spec = Self::build_with_predicate(rel_oid, spc_oid, None, shape)?;

        // Translate scan keys *after* the schema is in hand: the translator
        // needs Iceberg type and field-id information that only exists once
        // metadata has been parsed.
        spec.filter = scan_keys_to_predicate(keys, spec.row_reader.schema())?;
        Ok(spec)
    }

    /// Build a `ScanSpec` with a runtime-built Iceberg [`Predicate`]
    /// already translated from the pushed PG `Expr`s (the CustomScan
    /// select-all path).
    ///
    /// This is the entry point [`crate::customscan`] uses inside
    /// `provider.begin` / `provider.rescan` when no projection is requested
    /// (`NeededColumns::All`): the framework's runtime translator
    /// (`IcebergPredicateTranslator`) has already produced the Iceberg-side
    /// predicate from the post-rewrite pushed expressions, so we skip the
    /// `OwnedScanKeys`-based translation path entirely. Sharing the Iceberg
    /// scan core (table load + [`Self::open_cursor`]) with
    /// [`Self::build_with_projection`] is what satisfies Requirement 9.6 (one
    /// Iceberg scan core, two PG-side entry points).
    ///
    /// `shape` drives a full-schema [`ColumnPlan`](crate::access::conversion)
    /// so dropped-column alignment is correct even when selecting all columns.
    pub(crate) fn build_with_predicate(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        predicate: Option<Predicate>,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let (table, schema) = Self::load_table(rel_oid, spc_oid)?;
        let row_reader = RecordBatchRowReader::new(
            schema,
            shape.live_columns(),
            shape.slot_width(),
        )?;
        Ok(Self {
            table: Arc::new(table),
            row_reader,
            projection: None,
            filter: predicate,
        })
    }

    /// Build a `ScanSpec` for the CustomScan path with a column projection
    /// (the new entry point).
    ///
    /// `projection` carries the resolved `(attno, name)` pairs in scan order;
    /// it drives both `select(names)` (read fewer columns) and a projected
    /// [`ColumnPlan`](crate::access::conversion) (write each selected column
    /// to its `attno - 1` slot). `shape.slot_width` sizes the output `Row` to
    /// the full tuple width so projected-away positions stay SQL NULL.
    pub(crate) fn build_with_projection(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        projection: Projection,
        predicate: Option<Predicate>,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let (table, schema) = Self::load_table(rel_oid, spc_oid)?;
        let row_reader = RecordBatchRowReader::with_projection(
            schema,
            projection.columns(),
            shape.slot_width(),
        )?;
        Ok(Self {
            table: Arc::new(table),
            row_reader,
            projection: Some(projection),
            filter: predicate,
        })
    }

    /// Shared Iceberg scan core: resolve the relation's metadata location
    /// through PG's transactional metadata cache and build the `Arc<Table>`
    /// bound to the current snapshot's schema. Used by both PG-side entry
    /// points (Requirement 9.6).
    fn load_table(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
    ) -> IcebergResult<(Table, Arc<IcebergSchema>)> {
        let ctx = StorageContext::for_tablespace(spc_oid)?;

        let loaded =
            TxMetadata::current().current_table_metadata(rel_oid, ctx.file_io())?;
        let schema = loaded.metadata.current_schema().clone();

        let table = Table::builder()
            .file_io(ctx.file_io().clone())
            .metadata_location(loaded.location)
            .metadata(loaded.metadata)
            .identifier(IcebergTableId::for_relation(rel_oid).into_table_ident())
            .build()?;

        Ok((table, schema))
    }

    /// Replace the active predicate with a new one. Used by the
    /// CustomScan provider's `rescan` impl when `chgParam` overlaps
    /// the cached pushed param ids and a fresh predicate is built
    /// (Requirement 11.2).
    pub(crate) fn set_filter(&mut self, predicate: Option<Predicate>) {
        self.filter = predicate;
    }

    /// Construct a fresh [`ScanCursor`] from this spec.
    ///
    /// Called once in `scan_begin` and again per `scan_rescan`. Does no
    /// catalog or metadata IO: `self.table` already has metadata in memory,
    /// and `to_arrow()` only resolves manifests/data files for this snapshot.
    pub(crate) fn open_cursor(&self) -> IcebergResult<ScanCursor> {
        let mut builder = self.table.scan();
        match self.projection.as_ref() {
            Some(proj) => builder = builder.select(proj.names()),
            None => builder = builder.select_all(),
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

    pub(crate) fn row_reader(&self) -> &RecordBatchRowReader {
        &self.row_reader
    }
}

// ---------------------------------------------------------------------------
// ScanCursor: mutable per-cursor state
// ---------------------------------------------------------------------------

/// Mutable iteration state for one cursor over a [`ScanSpec`].
///
/// `scan_rescan` drops this and asks the spec for a fresh one.
///
/// `pub(crate)` so the CustomScan provider can drive the same cursor
/// implementation (Requirement 18.4).
pub(crate) struct ScanCursor {
    iterator: ArrowRecordBatchIterator,
    current_batch: Option<RecordBatch>,
    current_row_idx: usize,
}

impl ScanCursor {
    /// Pull the next row into `row`, advancing through `RecordBatch`es as
    /// needed. Returns `Ok(false)` at end-of-scan.
    pub(crate) fn next_row(
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

            // Cooperate with query cancellation at IO-sub-phase granularity
            // (Requirement 13.2): a single `next_row` can loop across several
            // exhausted/empty batches, and each `iterator.next()` may open a
            // new data file / row group. Checking here — rather than only once
            // per returned tuple at the `next_slot` boundary — keeps a long
            // file-walk responsive to cancel/terminate. On a pending interrupt
            // this does not return (it `longjmp`s via pgrx's guarded
            // `ProcessInterrupts`, which unwinds to the framework trampoline).
            pg_sys::check_for_interrupts!();

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
    /// Relation shape (live attnos + full tuple width) captured from the
    /// relation's `TupleDesc` in [`AmScanSession::new`], where the
    /// `RelationHandle` is in scope. Threaded into `ScanSpec::build` so the
    /// constructor builds a dropped-column-correct full-schema `ColumnPlan`
    /// without re-opening the relation.
    shape: RelationShape,
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
        // effective scan keys. The relation shape (live attnos + natts) is
        // captured here, the one place the `RelationHandle` is in scope, so
        // `scan_begin` does not need to re-open the relation.
        Ok(IcebergScan {
            rel_oid: rel.oid(),
            spc_oid: rel.tablespace_oid(),
            shape: RelationShape::from_relation(rel),
            spec: None,
            cursor: None,
        })
    }

    fn scan_begin(&mut self, keys: &OwnedScanKeys) -> AmResult<()> {
        let spec = ScanSpec::build(self.rel_oid, self.spc_oid, keys, &self.shape)?;
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
