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

use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, RunArray, StringArray};
use iceberg_lite::expr::Predicate;
use iceberg_lite::metadata_columns::{
    RESERVED_COL_NAME_FILE, RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_FILE,
    RESERVED_FIELD_ID_POS,
};
use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::scan::{ArrowRecordBatchIterator, TableScan};
use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pg_arrow_conv::{ArrowBatchSource, ArrowColumnDecoder, BoundBatch};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use crate::IcebergTableAm;
use crate::access::column_mapping::{RelationShape, ScanColumns};
use crate::access::isolation::PgTransactionIsolation;
use crate::access::projection::Projection;
use crate::access::row_location::{RowLocationMapHandle, begin_dml_scan};
use crate::catalog::bridge::IcebergTableId;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::StorageContext;

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
    /// Transaction-local Iceberg file delta captured for this statement.
    delta: Option<Arc<SnapshotDelta>>,
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
        let (table, schema, delta) = Self::load_table(rel_oid, spc_oid)?;
        let plan = ScanColumns::new(schema, shape)?;
        Ok(Self {
            table: Arc::new(table),
            plan,
            projection: None,
            filter: predicate,
            delta,
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
        scan_attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let (table, schema, delta) = Self::load_table(rel_oid, spc_oid)?;
        let plan = ScanColumns::with_projection(
            schema,
            projection.columns(),
            scan_attr_types.len(),
            scan_attr_types,
        )?;
        Ok(Self {
            table: Arc::new(table),
            plan,
            projection: Some(projection),
            filter: predicate,
            delta,
        })
    }

    /// Shared Iceberg scan core: resolve the relation's metadata through PG's
    /// transactional cache and build the `Arc<Table>` bound to the current
    /// snapshot's schema. Used by both PG-side entry points.
    fn load_table(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
    ) -> IcebergResult<(Table, Arc<IcebergSchema>, Option<Arc<SnapshotDelta>>)> {
        // Validate the transaction mode on every execution-side scan entry.
        // Serializable currently retains statement-level metadata visibility
        // and strengthens Iceberg row-level write validation; see
        // `access::isolation` for the intentionally incomplete PG SSI scope.
        PgTransactionIsolation::current()?;
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

        Ok((table, schema, loaded.delta))
    }

    /// Replace the active predicate. Used by the CustomScan provider's `rescan`
    /// when `chgParam` overlaps the cached pushed param ids.
    pub(crate) fn set_filter(&mut self, predicate: Option<Predicate>) {
        self.filter = predicate;
    }

    /// Construct a fresh slot-first [`IcebergBatchCursor`] for the TableAM scan.
    pub(crate) fn open_batch_cursor(
        &self,
        row_locations: Option<RowLocationMapHandle>,
    ) -> IcebergResult<IcebergBatchCursor> {
        let include_row_locations = row_locations.is_some();
        let source = ArrowBatchSource::new(IcebergArrowBatches(
            self.build_scan(include_row_locations)?.to_arrow()?,
        ));
        let decoder = ArrowColumnDecoder::new(self.plan.decoded_columns());
        Ok(IcebergBatchCursor::new(source, decoder, row_locations))
    }

    /// Build the Iceberg [`TableScan`] for this spec's projection and filter.
    //
    // TODO(scan-plan-decoupling): split `to_arrow` into an explicit `plan_files`
    // step plus a task-driven Arrow reader, and cache the planned
    // `Vec<FileScanTask>` on the `ScanSpec`. Today `open_batch_cursor` builds a
    // fresh `TableScan` and calls `to_arrow`, which re-plans on every
    // `scan_begin`/`scan_rescan`: the table's shared `ObjectCache` keeps the
    // metadata file in memory and caches manifest-list/manifest objects, but the
    // per-entry partition/metrics evaluation, `FileScanTask` construction, and
    // delete-index building still repeat each time. Decoupling would let us:
    //   1. plan once and reuse the cached task list across rescans (a
    //      nested-loop-driven UPDATE/DELETE rescans the target per outer row),
    //      and
    //   2. feed the planned data-file count into `begin_dml_scan` so the
    //      row-location ctid codec can size its file/row bit split dynamically
    //      (à la pg_lake) instead of the fixed 17/30 split in `row_location.rs`,
    //      without paying for a second planning pass.
    fn build_scan(&self, include_row_locations: bool) -> IcebergResult<TableScan> {
        let mut builder = self.table.scan();
        match self.scan_column_names(include_row_locations) {
            Some(names) => builder = builder.select(names),
            None => builder = builder.select_all(),
        }
        if let Some(predicate) = self.filter.as_ref() {
            builder = builder.with_filter(predicate.clone());
        }
        if let Some(delta) = self.delta.as_ref() {
            builder = builder.with_delta(Arc::clone(delta));
        }
        Ok(builder.build()?)
    }

    fn scan_column_names(&self, include_row_locations: bool) -> Option<Vec<String>> {
        let mut names = match self.projection.as_ref() {
            Some(proj) => Some(proj.names().map(ToOwned::to_owned).collect()),
            None if include_row_locations => Some(
                self.plan
                    .schema()
                    .as_struct()
                    .fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
            ),
            None => None,
        };

        if include_row_locations {
            let names = names.get_or_insert_with(Vec::new);
            names.push(RESERVED_COL_NAME_FILE.to_owned());
            names.push(RESERVED_COL_NAME_POS.to_owned());
        }

        names
    }

    fn starting_snapshot_id(&self) -> Option<i64> {
        self.table.metadata().current_snapshot_id()
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

type IcebergArrowBatchSource = ArrowBatchSource<IcebergArrowBatches, IcebergError>;

struct IcebergBoundBatch {
    decoded: BoundBatch,
    pos_column: Option<Int64Array>,
    file_indices: Option<Vec<u32>>,
}

#[derive(Clone)]
struct MetadataStringColumn {
    array: ArrayRef,
}

impl MetadataStringColumn {
    fn try_new(array: ArrayRef, name: &'static str) -> AmResult<Self> {
        if array.as_any().is::<StringArray>() {
            return Ok(Self { array });
        }

        if let Some(run_array) = array.as_any().downcast_ref::<RunArray<Int32Type>>()
            && run_array.values().as_any().is::<StringArray>()
        {
            return Ok(Self { array });
        }

        Err(IcebergError::ArrowTypeMismatch(format!(
            "metadata column {name} has unexpected Arrow type {:?}",
            array.data_type()
        ))
        .into())
    }

    fn is_null(&self, row_idx: usize) -> bool {
        if let Some(string_array) = self.array.as_any().downcast_ref::<StringArray>()
        {
            return string_array.is_null(row_idx);
        }

        let run_array = self
            .array
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .expect("metadata string column type checked at construction");
        let value_idx = run_array.get_physical_index(row_idx);
        run_array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("metadata string column values type checked at construction")
            .is_null(value_idx)
    }

    fn value(&self, row_idx: usize) -> &str {
        if let Some(string_array) = self.array.as_any().downcast_ref::<StringArray>()
        {
            return string_array.value(row_idx);
        }

        let run_array = self
            .array
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .expect("metadata string column type checked at construction");
        let value_idx = run_array.get_physical_index(row_idx);
        run_array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("metadata string column values type checked at construction")
            .value(value_idx)
    }
}

/// The TableAM scan driver: Arrow batches decoded straight into the slot, with
/// optional DML row-location synthesis from `_file`/`_pos`.
pub struct IcebergBatchCursor {
    source: IcebergArrowBatchSource,
    decoder: ArrowColumnDecoder,
    current: Option<IcebergBoundBatch>,
    row_idx: usize,
    row_locations: Option<RowLocationMapHandle>,
}

impl IcebergBatchCursor {
    fn new(
        source: IcebergArrowBatchSource,
        decoder: ArrowColumnDecoder,
        row_locations: Option<RowLocationMapHandle>,
    ) -> Self {
        Self {
            source,
            decoder,
            current: None,
            row_idx: 0,
            row_locations,
        }
    }

    fn bind_batch(&self, batch: RecordBatch) -> AmResult<IcebergBoundBatch> {
        let (pos_column, file_indices) =
            if let Some(row_locations) = self.row_locations {
                let file_column = MetadataStringColumn::try_new(
                    Self::metadata_column_ref(
                        &batch,
                        RESERVED_FIELD_ID_FILE,
                        RESERVED_COL_NAME_FILE,
                    )?,
                    RESERVED_COL_NAME_FILE,
                )?;
                let file_indices = Self::build_file_indices(
                    &file_column,
                    batch.num_rows(),
                    row_locations,
                )?;
                let pos_column = Self::typed_metadata_column::<Int64Array>(
                    &batch,
                    RESERVED_FIELD_ID_POS,
                    RESERVED_COL_NAME_POS,
                )?;

                (Some(pos_column), Some(file_indices))
            } else {
                (None, None)
            };

        let decoded = self.decoder.bind(batch)?;
        Ok(IcebergBoundBatch {
            decoded,
            pos_column,
            file_indices,
        })
    }

    fn build_file_indices(
        file_column: &MetadataStringColumn,
        num_rows: usize,
        row_locations: RowLocationMapHandle,
    ) -> AmResult<Vec<u32>> {
        let mut file_indices = Vec::with_capacity(num_rows);
        let mut cached_path: Option<&str> = None;
        let mut cached_index: Option<u32> = None;

        for row_idx in 0..num_rows {
            if file_column.is_null(row_idx) {
                return Err(IcebergError::InvariantViolated(
                    "DML row-location metadata cannot be NULL",
                )
                .into());
            }

            let file_path = file_column.value(row_idx);
            let file_index = if cached_path == Some(file_path) {
                cached_index.ok_or(IcebergError::InvariantViolated(
                    "cached DML file index is missing",
                ))?
            } else {
                let file_index = row_locations.file_index_for(file_path)?;
                cached_path = Some(file_path);
                cached_index = Some(file_index);
                file_index
            };

            file_indices.push(file_index);
        }

        Ok(file_indices)
    }

    fn typed_metadata_column<T: Array + Clone + 'static>(
        batch: &RecordBatch,
        field_id: i32,
        name: &'static str,
    ) -> AmResult<T> {
        let index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| {
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|raw| raw.parse::<i32>().ok())
                    == Some(field_id)
            })
            .ok_or(IcebergError::InvariantViolated(
                "row-location metadata column is missing from DML scan",
            ))?;
        let array = batch.column(index);
        array.as_any().downcast_ref::<T>().cloned().ok_or_else(|| {
            IcebergError::ArrowTypeMismatch(format!(
                "metadata column {name} has unexpected Arrow type {:?}",
                array.data_type()
            ))
            .into()
        })
    }

    fn metadata_column_ref(
        batch: &RecordBatch,
        field_id: i32,
        _name: &'static str,
    ) -> AmResult<ArrayRef> {
        let index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| {
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|raw| raw.parse::<i32>().ok())
                    == Some(field_id)
            })
            .ok_or(IcebergError::InvariantViolated(
                "row-location metadata column is missing from DML scan",
            ))?;
        Ok(Arc::clone(batch.column(index)))
    }

    fn set_row_location_tid(
        &self,
        bound: &IcebergBoundBatch,
        row_idx: usize,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<()> {
        let Some(row_locations) = self.row_locations else {
            return Ok(());
        };

        let file_indices =
            bound
                .file_indices
                .as_ref()
                .ok_or(IcebergError::InvariantViolated(
                    "DML scan is missing file index cache",
                ))?;
        let pos_column =
            bound
                .pos_column
                .as_ref()
                .ok_or(IcebergError::InvariantViolated(
                    "DML scan is missing _pos column",
                ))?;

        if pos_column.is_null(row_idx) {
            return Err(IcebergError::InvariantViolated(
                "DML row-location metadata cannot be NULL",
            )
            .into());
        }

        let position = pos_column.value(row_idx);
        if position < 0 {
            return Err(IcebergError::InvariantViolated(
                "DML row position cannot be negative",
            )
            .into());
        }

        let file_index =
            *file_indices
                .get(row_idx)
                .ok_or(IcebergError::InvariantViolated(
                    "DML file index cache is too short",
                ))?;
        let tid = row_locations.tid_for_file_index(file_index, position as u64)?;
        out.set_tid(&tid);
        Ok(())
    }
}

impl ScanBatchDriver for IcebergBatchCursor {
    fn next_into_slot(&mut self, out: &mut SlotColumns<'_>) -> AmResult<bool> {
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_idx < self.decoder.num_rows(&bound.decoded)
            {
                self.decoder.write_row(&bound.decoded, self.row_idx, out)?;
                self.set_row_location_tid(bound, self.row_idx, out)?;
                self.row_idx += 1;
                return Ok(true);
            }

            self.current = None;
            match self.source.next_batch()? {
                Some(batch) => {
                    self.current = Some(self.bind_batch(batch)?);
                    self.row_idx = 0;
                }
                None => return Ok(false),
            }
        }
    }
}

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
        let row_locations =
            begin_dml_scan(self.rel_oid, spec.starting_snapshot_id())?;
        let cursor = spec.open_batch_cursor(row_locations)?;
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
        let row_locations =
            begin_dml_scan(self.rel_oid, spec.starting_snapshot_id())?;
        self.cursor = Some(spec.open_batch_cursor(row_locations)?);
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
