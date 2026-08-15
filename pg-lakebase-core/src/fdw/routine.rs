//! Static `FdwRoutine` construction and capability registration.

use pgrx::{AllocatedByPostgres, AllocatedByRust, PgBox, pg_sys};

use super::maintenance::{self, FdwAnalyze, FdwTruncate};
use super::modify::{self, FdwModify};
use super::scan::{self, FdwScan};

/// PostgreSQL-owned FDW routine returned by a handler function.
pub type FdwRoutine = PgBox<pg_sys::FdwRoutine, AllocatedByPostgres>;

/// Allocate an empty routine. PostgreSQL's node allocator initializes all
/// optional callbacks to NULL; capability registration fills complete callback
/// groups below.
#[doc(hidden)]
pub fn new_routine() -> FdwRoutine {
    // SAFETY: PostgreSQL permits FdwRoutine allocation through its node
    // allocator; the returned PgBox transfers ownership to PostgreSQL after
    // the selected callback groups have been initialized.
    unsafe {
        PgBox::<pg_sys::FdwRoutine, AllocatedByRust>::alloc_node(
            pg_sys::NodeTag::T_FdwRoutine,
        )
        .into_pg_boxed()
    }
}

/// Install the complete base-relation scan callback group for `P`.
pub fn register_scan<P: FdwScan>(routine: &mut FdwRoutine) {
    routine.GetForeignRelSize = Some(scan::get_foreign_rel_size::<P>);
    routine.GetForeignPaths = Some(scan::get_foreign_paths::<P>);
    routine.GetForeignPlan = Some(scan::get_foreign_plan::<P>);
    routine.BeginForeignScan = Some(scan::begin_foreign_scan::<P>);
    routine.IterateForeignScan = Some(scan::iterate_foreign_scan::<P>);
    routine.ReScanForeignScan = Some(scan::rescan_foreign_scan::<P>);
    routine.EndForeignScan = Some(scan::end_foreign_scan::<P>);
}

/// Install the complete INSERT/UPDATE/DELETE callback group for `P`.
pub fn register_modify<P: FdwModify>(routine: &mut FdwRoutine) {
    routine.AddForeignUpdateTargets = Some(modify::add_foreign_update_targets::<P>);
    routine.IsForeignRelUpdatable = Some(modify::is_foreign_rel_updatable::<P>);
    routine.PlanForeignModify = Some(modify::plan_foreign_modify::<P>);
    routine.BeginForeignModify = Some(modify::begin_foreign_modify::<P>);
    routine.ExecForeignInsert = Some(modify::exec_foreign_insert::<P>);
    routine.ExecForeignBatchInsert = Some(modify::exec_foreign_batch_insert::<P>);
    routine.GetForeignModifyBatchSize =
        Some(modify::get_foreign_modify_batch_size::<P>);
    routine.ExecForeignUpdate = Some(modify::exec_foreign_update::<P>);
    routine.ExecForeignDelete = Some(modify::exec_foreign_delete::<P>);
    routine.EndForeignModify = Some(modify::end_foreign_modify::<P>);
    routine.BeginForeignInsert = Some(modify::begin_foreign_insert::<P>);
    routine.EndForeignInsert = Some(modify::end_foreign_insert::<P>);
}

/// Install the foreign-table ANALYZE negotiation and sampling callbacks for
/// `P`.
pub fn register_analyze<P: FdwAnalyze>(routine: &mut FdwRoutine) {
    routine.AnalyzeForeignTable = Some(maintenance::analyze_foreign_table::<P>);
}

/// Install the batched foreign-table TRUNCATE callback for `P`.
pub fn register_truncate<P: FdwTruncate>(routine: &mut FdwRoutine) {
    routine.ExecForeignTruncate = Some(maintenance::exec_foreign_truncate::<P>);
}
