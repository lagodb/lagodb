//! Iceberg Table Scan Implementation.
//!
//! A scan's state is split in two:
//!
//! - [`ScanSpec`] is the immutable scan description (table, [`ScanColumns`],
//!   optional projection, optional [`Predicate`]). Built once in
//!   [`AmScanSession::scan_begin`] and preserved across `scan_rescan`, so the
//!   visible snapshot is frozen for the scan's duration. This matches the Read
//!   Committed contract: every `scan_rescan` comes from the same statement that
//!   issued `scan_begin`, so holding the metadata is correct. Newly committed
//!   work becomes visible at the next statement's `scan_begin`.
//! - [`IcebergBatchCursor`] is the per-cursor mutable state; `scan_rescan`
//!   rebuilds only the cursor from the spec.
//!
//! `scan_rescan` re-translates the dispatcher-supplied keys (PostgreSQL's
//! "non-null replaces, null keeps" rule is applied by the dispatcher first).
//! The borrow is consumed within each callback, never retained.
//!
//! [`scan_keys_to_predicate`] is currently a stub returning `None`, so
//! predicates remain handled by the executor (`ExecQual`). Adding real pushdown
//! is a localized change there.

use std::sync::Arc;

use arrow_array::RecordBatch;
use iceberg_lite::expr::Predicate;
use iceberg_lite::scan::{ArrowRecordBatchIterator, TableScan};
use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use pg_arrow_conv::{ArrowBatchSource, ArrowColumnDecoder};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use crate::IcebergTableAm;
use crate::access::column_mapping::{LiveColumn, ScanColumns};
use crate::access::projection::Projection;
use crate::catalog::bridge::IcebergTableId;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::StorageContext;

/// Relation-shape inputs the converter needs to build a position-correct
/// [`ColumnMapping`](crate::access::column_mapping): the live (non-dropped)
/// columns in ascending attno order plus the full tuple width.
///
/// Derived from the relation's `TupleDesc` once per scan. Records only which
/// columns are live (and their names) and the tuple width; the converter turns
/// `attno` into `dest`.
#[derive(Debug, Clone)]
pub(crate) struct RelationShape {
    /// Live (non-dropped) columns in ascending attno order, each with its
    /// 1-based attno and column name (also the Iceberg field name). Fields are
    /// resolved by name, so this stays correct even when the Iceberg schema is
    /// wider than the live PG columns (e.g. after `DROP COLUMN`).
    live_columns: Vec<LiveColumn>,
    /// Full PG tuple width (`natts`), counting dropped-column positions.
    slot_width: usize,
    /// Per-attribute `(type oid, typmod)` indexed by `attno - 1`, used to
    /// disambiguate PG types that share one `ColumnRule`.
    attr_types: Vec<(pg_sys::Oid, i32)>,
}

impl RelationShape {
    /// Derive the relation shape from a live [`RelationHandle`].
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
            attr_types: rel.attr_types(),
        }
    }

    fn live_columns(&self) -> &[LiveColumn] {
        &self.live_columns
    }

    fn slot_width(&self) -> usize {
        self.slot_width
    }

    pub(crate) fn attr_types(&self) -> &[(pg_sys::Oid, i32)] {
        &self.attr_types
    }
}

/// Immutable parameters for a scan: which table, snapshot schema, columns, and
/// predicate.
///
/// `ScanSpec::build*` is the only place metadata is read from storage during a
/// scan; `scan_rescan` reuses the spec and rebuilds only the cursor.
///
/// `pub(crate)` so the CustomScan provider in [`crate::customscan`] can build a
/// `ScanSpec` from a runtime-built [`Predicate`], reusing the same scan core in
/// both the TableAM seqscan and CustomScan paths.
pub(crate) struct ScanSpec {
    /// Ready-to-scan Iceberg table. Cheap to clone (`Arc`-backed).
    table: Arc<Table>,
    /// Schema-bound column plan for the captured snapshot. Drives the cursor
    /// decoder and exposes the `IcebergSchema` for predicate translation.
    plan: ScanColumns,
    /// Column projection. `None` means select-all; a `Some` always has ≥ 1
    /// column. Populated only on the CustomScan `build_with_projection` path.
    projection: Option<Projection>,
    /// Predicate pushed into the Iceberg scan layer for pruning. Replaced (not
    /// merged) by [`Self::set_filter`] / `refresh_filter`.
    filter: Option<Predicate>,
}

impl ScanSpec {
    /// Build a `ScanSpec` from an [`OwnedScanKeys`] (the TableAM seqscan path).
    ///
    /// A plain SeqScan never specifies needed columns, so the projection stays
    /// `None`; the full-schema [`ColumnMapping`](crate::access::column_mapping)
    /// built from `shape` still fixes dropped-column alignment.
    pub(crate) fn build(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        keys: &OwnedScanKeys,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let mut spec = Self::build_with_predicate(rel_oid, spc_oid, None, shape)?;

        // Translate keys after the schema is in hand (the translator needs
        // Iceberg type / field-id info from parsed metadata).
        spec.filter = scan_keys_to_predicate(keys, spec.plan.schema())?;
        Ok(spec)
    }

    /// Build a `ScanSpec` with an already-translated [`Predicate`] (the
    /// CustomScan select-all path), skipping the `OwnedScanKeys` translation.
    pub(crate) fn build_with_predicate(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        predicate: Option<Predicate>,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let (table, schema) = Self::load_table(rel_oid, spc_oid)?;
        let plan = ScanColumns::new(
            schema,
            shape.live_columns(),
            shape.slot_width(),
            shape.attr_types(),
        )?;
        Ok(Self {
            table: Arc::new(table),
            plan,
            projection: None,
            filter: predicate,
        })
    }

    /// Build a `ScanSpec` for the CustomScan path with a column projection.
    ///
    /// `projection` drives both `select(names)` (read fewer columns) and a
    /// projected [`ColumnMapping`](crate::access::column_mapping). The decoder
    /// writes only the projected `dest` slots; projected-away positions are
    /// left untouched, which is safe because they are never read (see
    /// [`ColumnMapping`](crate::access::column_mapping)) — not because the
    /// cleared slot reads them back as NULL.
    pub(crate) fn build_with_projection(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        projection: Projection,
        predicate: Option<Predicate>,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let (table, schema) = Self::load_table(rel_oid, spc_oid)?;
        let plan = ScanColumns::with_projection(
            schema,
            projection.columns(),
            shape.slot_width(),
            shape.attr_types(),
        )?;
        Ok(Self {
            table: Arc::new(table),
            plan,
            projection: Some(projection),
            filter: predicate,
        })
    }

    /// Shared Iceberg scan core: resolve the relation's metadata through PG's
    /// transactional cache and build the `Arc<Table>` bound to the current
    /// snapshot's schema. Used by both PG-side entry points.
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

    /// Replace the active predicate. Used by the CustomScan provider's `rescan`
    /// when `chgParam` overlaps the cached pushed param ids.
    pub(crate) fn set_filter(&mut self, predicate: Option<Predicate>) {
        self.filter = predicate;
    }

    /// Construct a fresh slot-first [`IcebergBatchCursor`] for the TableAM scan.
    pub(crate) fn open_batch_cursor(
        &self,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<IcebergBatchCursor> {
        let source = ArrowBatchSource::new(IcebergArrowBatches(
            self.build_scan()?.to_arrow()?,
        ));
        let decoder = ArrowColumnDecoder::new(self.plan.decoded_columns(attr_types));
        Ok(BatchRowCursor::new(source, decoder))
    }

    /// Build the Iceberg [`TableScan`] for this spec's projection and filter.
    fn build_scan(&self) -> IcebergResult<TableScan> {
        let mut builder = self.table.scan();
        match self.projection.as_ref() {
            Some(proj) => builder = builder.select(proj.names()),
            None => builder = builder.select_all(),
        }
        if let Some(predicate) = self.filter.as_ref() {
            builder = builder.with_filter(predicate.clone());
        }
        Ok(builder.build()?)
    }

    /// Re-translate the current effective [`OwnedScanKeys`] into a filter.
    fn refresh_filter(&mut self, keys: &OwnedScanKeys) -> IcebergResult<()> {
        self.filter = scan_keys_to_predicate(keys, self.plan.schema())?;
        Ok(())
    }
}

/// Adapts the Iceberg Arrow batch iterator into the conversion crate's batch
/// source. The producer error (`iceberg_lite::Error`: IO, Parquet, metadata,
/// schema) is preserved as an [`IcebergError`] so it reaches the callback
/// boundary with its own SQLSTATE (IO/internal/feature) rather than being
/// reclassified as a `ConvError::DatumConversionError` (`DATA_EXCEPTION`).
/// `pg-arrow-conv` stays format-neutral: it only requires the error to map into
/// the boundary error, which `IcebergError` already does.
pub struct IcebergArrowBatches(ArrowRecordBatchIterator);

impl Iterator for IcebergArrowBatches {
    type Item = Result<RecordBatch, IcebergError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Cooperative cancellation at the batch boundary. PG's `ExecScanFetch`
        // already fires `CHECK_FOR_INTERRUPTS` once per *returned* tuple for both
        // the TableAM seqscan and the CustomScan, but a single `getnextslot` /
        // `next_slot` call can pull many batches here (skipping batches fully
        // eliminated by pushed filters, or reading the next Parquet row group),
        // so a query cancel issued mid-IO would otherwise wait until the next
        // tuple surfaces. Checking per batch — the unit of Iceberg scan IO —
        // closes that gap for both scan paths, which share this iterator.
        pgrx::pg_sys::check_for_interrupts!();
        self.0.next().map(|batch| batch.map_err(IcebergError::from))
    }
}

/// The TableAM scan driver: Arrow batches decoded straight into the slot.
/// Driven by both the TableAM seqscan and the CustomScan provider.
pub type IcebergBatchCursor = BatchRowCursor<
    ArrowBatchSource<IcebergArrowBatches, IcebergError>,
    ArrowColumnDecoder,
>;

/// PostgreSQL-facing scan session for the Iceberg table AM. Thin bookkeeping
/// (`rel_oid` / `spc_oid` / `shape`) over the lazily-built [`ScanSpec`] and
/// current [`IcebergBatchCursor`].
pub struct IcebergScan {
    rel_oid: pg_sys::Oid,
    spc_oid: pg_sys::Oid,
    /// Relation shape captured in [`AmScanSession::new`] (the one place the
    /// `RelationHandle` is in scope), threaded into `ScanSpec::build`.
    shape: RelationShape,
    spec: Option<ScanSpec>,
    cursor: Option<IcebergBatchCursor>,
}

impl AmScan for IcebergTableAm {}

impl AmScanSession for IcebergScan {
    type BatchDriver = IcebergBatchCursor;

    fn new(
        rel: &RelationHandle,
        _snapshot: &SnapshotHandle,
        _pscan: Option<&ParallelTableScanDescHandle>,
        _flags: u32,
    ) -> AmResult<Self> {
        // No metadata IO yet: defer schema-dependent work to `scan_begin`. The
        // relation shape is captured here, where the `RelationHandle` is in scope.
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
        let cursor = spec.open_batch_cursor(self.shape.attr_types())?;
        self.spec = Some(spec);
        self.cursor = Some(cursor);
        Ok(())
    }

    /// Slot-first scan driver: the Arrow batch cursor that decodes the current
    /// batch straight into the slot. The framework drives every scan through
    /// this one path; there is no row variant for a columnar AM.
    fn scan_driver(&mut self) -> &mut Self::BatchDriver {
        // `scan_begin` builds the cursor before the executor fetches any row,
        // so it is always present by the time the framework calls this.
        self.cursor
            .as_mut()
            .expect("scan_driver called after scan_begin")
    }

    /// Restart the scan, re-translating the current effective scan keys.
    ///
    /// The dispatcher has already applied the "non-null replaces, null keeps"
    /// rule, so `keys` is the effective set. `set_params` and the `allow_*`
    /// flags only affect heap-AM strategy choices and are ignored. Metadata is
    /// not re-read: a single statement drives every `scan_rescan` and must see
    /// a consistent snapshot.
    fn scan_rescan(
        &mut self,
        keys: &OwnedScanKeys,
        _set_params: bool,
        _allow_strat: bool,
        _allow_sync: bool,
        _allow_pagemode: bool,
    ) -> AmResult<()> {
        let Some(spec) = self.spec.as_mut() else {
            // Defensive: rescan before the first scan_begin shouldn't happen.
            self.cursor = None;
            return Ok(());
        };

        spec.refresh_filter(keys)?;
        self.cursor = Some(spec.open_batch_cursor(self.shape.attr_types())?);
        Ok(())
    }

    fn scan_end(&mut self) -> AmResult<()> {
        self.cursor = None;
        self.spec = None;
        Ok(())
    }
}

/// Translate PostgreSQL [`OwnedScanKeys`] into an Iceberg [`Predicate`].
///
/// The only place the AM converts PG filter representations into Iceberg
/// expressions, keeping predicate-pushdown work out of the scan lifecycle.
///
/// Current status: stub. `Ok(None)` is safe today because PostgreSQL only
/// supplies `ScanKey`s through paths the Iceberg AM does not advertise support
/// for; plain `SeqScan` lets the executor apply `WHERE` above the scan.
fn scan_keys_to_predicate(
    _keys: &OwnedScanKeys,
    _schema: &IcebergSchema,
) -> IcebergResult<Option<Predicate>> {
    // TODO(predicate-pushdown): map each ScanKeyEntry's attno -> Iceberg field,
    // strategy/subtype/argument -> Predicate, combine with `Predicate::and`,
    // and return `Ok(None)` for the whole set if any key cannot be translated.
    Ok(None)
}
