//! Delete-aware PostgreSQL ANALYZE for REST-backed Iceberg foreign tables.

use std::sync::Arc;

use iceberg_lite::scan::FileScanTask;
use pg_lakebase_core::fdw::{
    FdwAnalyze, ForeignAnalyzeContext, ForeignAnalyzeSupport, ForeignSampleContext,
    ForeignSampleStatistics, ForeignTableMaintenanceError,
};
use pg_lakebase_core::handles::HeapTupleGuard;
use pg_lakebase_core::tuple::SlotColumns;
use pgrx::pg_sys;
use rand::Rng;

use super::provider::LagodbIceberg;
use super::relation::RestForeignTable;
use super::schema::ForeignSchemaBinding;
use super::transaction::{ForeignTableView, ForeignTransaction};
use crate::engine::scan::{ScanSource, ScanSpec};

const MAX_ANALYZE_PAGES: u64 = u32::MAX as u64 - 1;

impl FdwAnalyze for LagodbIceberg {
    fn analyze(
        context: &ForeignAnalyzeContext<'_>,
    ) -> Result<Option<ForeignAnalyzeSupport>, ForeignTableMaintenanceError> {
        let view = analyze_view(context.relation().oid())?;
        let tasks = plan_files(&view)?;
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
        let view = analyze_view(context.relation().oid())?;
        let shape = ForeignSchemaBinding::bind(
            context.relation(),
            view.table.metadata().current_schema(),
        )?
        .into_relation_shape();
        let mut spec = ScanSpec::full(
            ScanSource::transaction_view(view.table, view.delta, None),
            None,
            None,
            &shape,
        )?;
        let mut cursor = spec.open_query_cursor()?;
        let tuple_desc = context.relation().tuple_desc();
        let slot = AnalyzeSlot::new(tuple_desc);
        let row_context = AnalyzeRowContext::new();
        let mut seen = 0_u64;
        let mut rng = rand::rng();

        loop {
            row_context.reset();
            unsafe { pg_sys::ExecClearTuple(slot.raw()) };
            let prior = unsafe { pg_sys::MemoryContextSwitchTo(row_context.raw()) };
            let produced = {
                let mut columns =
                    unsafe { SlotColumns::new(slot.raw(), row_context.raw()) };
                cursor.next_into_slot(&mut columns)
            };
            unsafe { pg_sys::MemoryContextSwitchTo(prior) };
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

fn analyze_view(
    relation_oid: pg_sys::Oid,
) -> Result<ForeignTableView, ForeignTableMaintenanceError> {
    let effective_user = unsafe { pg_sys::GetUserId() };
    let resolved = RestForeignTable::resolve(relation_oid, effective_user)?;
    Ok(ForeignTransaction::scan_view(resolved)?)
}

fn plan_files(
    view: &ForeignTableView,
) -> Result<Vec<FileScanTask>, ForeignTableMaintenanceError> {
    let mut builder = view.table.scan();
    if let Some(delta) = view.delta.as_ref() {
        builder = builder.with_delta(Arc::clone(delta));
    }
    Ok(builder
        .build()
        .map_err(super::error::IcebergFdwError::from)?
        .plan_files()
        .map_err(super::error::IcebergFdwError::from)?)
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
}

impl Drop for AnalyzeRowContext {
    fn drop(&mut self) {
        unsafe { pg_sys::MemoryContextDelete(self.0) };
    }
}
