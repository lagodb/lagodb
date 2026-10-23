//! Delete-aware PostgreSQL ANALYZE for REST-backed Iceberg foreign tables.

use std::sync::Arc;

use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::scan::FileScanTask;
use iceberg_lite::table::Table;
use lagodb_core::fdw::{
    FdwAnalyze, ForeignAnalyzeContext, ForeignAnalyzeSupport, ForeignSampleContext,
    ForeignSampleStatistics, ForeignTableMaintenanceError,
};
use lagodb_core::handles::HeapTupleGuard;
use lagodb_core::tuple::SlotDatumWriter;
use pgrx::pg_sys;
use rand::Rng;

use super::error::IcebergFdwError;
use super::options::ForeignTableIdentity;
use super::provider::LagodbIceberg;
use super::relation::ResolvedForeignTableBinding;
use super::schema::ForeignSchemaBinding;
use super::transaction::ForeignTransaction;
use crate::scan::{IcebergReadSnapshot, PreparedRowScan, ScanPredicates};

const MAX_ANALYZE_PAGES: u64 = u32::MAX as u64 - 1;

impl FdwAnalyze for LagodbIceberg {
    fn analyze(
        context: &ForeignAnalyzeContext<'_>,
    ) -> Result<Option<ForeignAnalyzeSupport>, ForeignTableMaintenanceError> {
        let source = AnalyzeSource::load(context.relation().oid())?;
        let tasks = source.plan_files()?;
        let bytes = tasks.iter().try_fold(0_u64, |total, task| {
            total.checked_add(task.file_size_in_bytes).ok_or_else(|| {
                ForeignTableMaintenanceError::unsupported(
                    "Iceberg foreign-table size exceeds unsigned long range",
                )
            })
        })?;
        let pages = bytes.div_ceil(pg_sys::BLCKSZ as u64).min(MAX_ANALYZE_PAGES)
            as pg_sys::BlockNumber;
        Ok(Some(ForeignAnalyzeSupport::new(pages)))
    }

    fn acquire_sample_rows(
        context: &mut ForeignSampleContext<'_>,
    ) -> Result<ForeignSampleStatistics, ForeignTableMaintenanceError> {
        let source = AnalyzeSource::load(context.relation().oid())?;
        let binding = ForeignSchemaBinding::new(context.relation())?;
        binding.validate_read(
            source.table.metadata(),
            &source.identity,
            binding
                .layout()
                .live_columns()
                .iter()
                .map(|column| column.attno),
        )?;
        let layout = binding.into_relation_layout();
        let mut prepared = PreparedRowScan::full(
            IcebergReadSnapshot::new(source.table, source.delta),
            ScanPredicates::unfiltered(),
            &layout,
        )?;
        let mut cursor = prepared.open_analyze_row_cursor()?;
        let tuple_desc = context.relation().tuple_desc();
        let slot = AnalyzeSlot::new(tuple_desc);
        let row_context = AnalyzeRowContext::new();
        let mut seen = 0_u64;
        let mut rng = rand::rng();

        loop {
            row_context.reset();
            unsafe { pg_sys::ExecClearTuple(slot.raw()) };
            let produced = row_context.with(|| {
                let mut columns =
                    unsafe { SlotDatumWriter::new(slot.raw(), row_context.raw()) };
                cursor.next_into_slot(&mut columns)
            });
            if !produced? {
                break;
            }
            unsafe { pg_sys::ExecStoreVirtualTuple(slot.raw()) };
            seen = seen.checked_add(1).ok_or_else(|| {
                ForeignTableMaintenanceError::unsupported(
                    "Iceberg foreign-table live row count exceeds unsigned long range",
                )
            })?;
            let selected = if context.len() < context.target_rows() {
                Some(context.len())
            } else if context.target_rows() == 0 {
                None
            } else {
                let candidate = rng.random_range(0..seen);
                (candidate < context.target_rows() as u64)
                    .then_some(candidate as usize)
            };
            if let Some(index) = selected {
                let tuple = unsafe { pg_sys::ExecCopySlotHeapTuple(slot.raw()) };
                let tuple = unsafe { HeapTupleGuard::new(tuple) };
                if index == context.len() {
                    context.push(tuple)?;
                } else {
                    context.replace(index, tuple)?;
                }
            }
        }

        Ok(ForeignSampleStatistics::new(seen as f64, 0.0))
    }
}

struct AnalyzeSource {
    table: Table,
    delta: Option<Arc<SnapshotDelta>>,
    identity: ForeignTableIdentity,
}

impl AnalyzeSource {
    fn load(relation_oid: pg_sys::Oid) -> Result<Self, ForeignTableMaintenanceError> {
        let effective_user = unsafe { pg_sys::GetUserId() };
        let resolved =
            ResolvedForeignTableBinding::resolve(relation_oid, effective_user)?;
        let identity = resolved.identity().clone();
        let view =
            ForeignTransaction::scan_view(resolved.into_catalog_table(), None)?;
        let (_key, table, delta) = view.into_parts();
        Ok(Self {
            table,
            delta,
            identity,
        })
    }

    fn plan_files(&self) -> Result<Vec<FileScanTask>, ForeignTableMaintenanceError> {
        let mut builder = self.table.scan();
        if let Some(delta) = self.delta.as_ref() {
            builder = builder.with_delta(Arc::clone(delta));
        }
        Ok(builder
            .build()
            .map_err(IcebergFdwError::from)?
            .plan_files()
            .map_err(IcebergFdwError::from)?)
    }
}

struct AnalyzeSlot(*mut pg_sys::TupleTableSlot);

impl AnalyzeSlot {
    fn new(tuple_desc: pg_sys::TupleDesc) -> Self {
        let slot = unsafe {
            pg_sys::MakeSingleTupleTableSlot(tuple_desc, &pg_sys::TTSOpsVirtual)
        };
        Self(slot)
    }

    fn raw(&self) -> *mut pg_sys::TupleTableSlot {
        self.0
    }
}

impl Drop for AnalyzeSlot {
    fn drop(&mut self) {
        unsafe { pg_sys::ExecDropSingleTupleTableSlot(self.0) };
    }
}

struct AnalyzeRowContext(pg_sys::MemoryContext);

impl AnalyzeRowContext {
    fn new() -> Self {
        let context = unsafe {
            pg_sys::AllocSetContextCreateExtended(
                pg_sys::CurrentMemoryContext,
                c"Iceberg FDW ANALYZE row".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            )
        };
        Self(context)
    }

    fn raw(&self) -> pg_sys::MemoryContext {
        self.0
    }

    fn reset(&self) {
        unsafe { pg_sys::MemoryContextReset(self.0) };
    }

    fn with<R>(&self, allocate: impl FnOnce() -> R) -> R {
        // SAFETY: ANALYZE runs on the backend thread. Both the caller's
        // context and this owned child outlive the closure and its guard.
        let _restore = unsafe {
            let restore = RestoreAnalyzeContext(pg_sys::CurrentMemoryContext);
            pg_sys::CurrentMemoryContext = self.0;
            restore
        };
        allocate()
    }
}

// Restore on PG ERROR unwinding as well as normal return, before the owned
// row context is deleted. Sample tuples are copied after this guard is gone.
struct RestoreAnalyzeContext(pg_sys::MemoryContext);

impl Drop for RestoreAnalyzeContext {
    fn drop(&mut self) {
        // SAFETY: the caller's context outlives the scoped row allocation.
        unsafe { pg_sys::CurrentMemoryContext = self.0 };
    }
}

impl Drop for AnalyzeRowContext {
    fn drop(&mut self) {
        unsafe { pg_sys::MemoryContextDelete(self.0) };
    }
}
