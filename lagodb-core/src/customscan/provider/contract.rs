//! Scan-provider trait and its stable public contract.

use core::ffi::CStr;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::expr::pushdown::FilterPushdown;
use crate::runtime_api::TableScanTaskMetrics;

use super::execution::{NextSlotContext, NextSlotResult};
use super::lifecycle::{
    BeginContext, CreateStateContext, EndContext, ReScanContext, StartContext,
};
use super::planning::{
    CustomPathBuilder, CustomPathPlan, PathContext, PathVariant, RelationContext,
};
use super::private_data::CustomScanPrivate;

/// Lake backend provider trait: relation routing, CustomPath emission, and
/// scan lifecycle.
pub trait LagodbCustomScanProvider: FilterPushdown {
    /// Unique provider name (EXPLAIN + registry).
    const NAME: &'static CStr;

    /// Provider tail of `custom_private`; framework owns the envelope.
    type PrivateData: CustomScanPrivate;

    /// Per-scan runtime state inside `CustomScanStateWrapper`.
    type State;

    /// Whether read scans provide PostgreSQL native-parallel DSM lifecycle
    /// support. The framework can install an unparameterized parallel-aware
    /// partial path for this capability. A complete path is marked
    /// parallel-safe separately by [`CustomPathBuilder`],
    /// because it receives no provider DSM coordinate in a worker.
    const NATIVE_PARALLEL: bool = false;

    /// Whether the underlying table AM lacks its own parallel task-distribution
    /// contract and standard partial SeqScan paths must therefore be removed.
    /// This is independent of CustomScan parallel capability: retaining such a
    /// path would call unsupported table-AM callbacks or duplicate the scan.
    const SUPPRESS_TABLE_AM_PARALLEL_SCAN: bool = false;

    /// Whether this provider stores a PostgreSQL partitioned table as one
    /// provider-owned logical table instead of PostgreSQL leaf relations.
    ///
    /// The planner consults this once while building relation paths so it can
    /// replace PostgreSQL's dummy/Append paths for that table. It adds no
    /// executor or per-row dispatch.
    const OWNS_PARTITIONED_TABLE: bool = false;

    /// Whether this provider claims the relation after framework path gates.
    fn supports_relation(ctx: &RelationContext<'_>) -> bool;

    /// Build one CustomPath for a framework-emitted variant; `None` declines.
    fn create_path(
        ctx: &PathContext<'_>,
        variant: &PathVariant<'_>,
        builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>>
    where
        Self: Sized;

    /// Construct per-scan state before [`Self::begin`].
    fn create_state(ctx: CreateStateContext<Self>) -> Self::State;

    /// Prepare scan resources and statement-stable predicates at Begin.
    /// Dynamic inputs and the first row cursor belong to Start.
    fn begin(ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError>;

    /// Open the first cursor with the framework's bound dynamic predicates
    /// after executor initialization, before the first row. This runs once,
    /// including when PostgreSQL rescans the node before its first execution.
    fn start(ctx: StartContext<'_, Self>) -> Result<(), CustomScanError>;

    /// Produce the next row through the framework-owned slot publication path.
    fn next_slot<'a>(
        ctx: NextSlotContext<'a, Self>,
    ) -> Result<NextSlotResult<'a>, CustomScanError>;

    /// Rewind the scan, replacing predicates when `filters_changed`.
    fn rescan(ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError>;

    /// Close the cursor and release provider-owned runtime resources.
    fn end(ctx: EndContext<'_, Self>) -> Result<(), CustomScanError>;

    /// Physical inventory selected by the active scan after storage pruning.
    ///
    /// Called only by EXPLAIN ANALYZE VERBOSE, before scan teardown. Return
    /// `None` when this execution mode has no retained inventory. Reading this
    /// state must not plan another scan or perform storage I/O. These are
    /// selected tasks, not per-row counters or the number of opened files.
    /// Rescan-dependent filters may narrow reads within this retained inventory
    /// without changing its counts.
    fn scan_task_metrics(_state: &Self::State) -> Option<TableScanTaskMetrics> {
        None
    }

    fn estimate_dsm(
        _state: &mut Self::State,
    ) -> Result<pg_sys::Size, CustomScanError> {
        Err(CustomScanError::internal(std::io::Error::other(
            "provider enabled native parallel scan without EstimateDSM support",
        )))
    }

    /// # Safety
    /// `coordinate` is the provider coordinate allocated by PostgreSQL using
    /// the size returned from [`Self::estimate_dsm`].
    unsafe fn initialize_dsm(
        _state: &mut Self::State,
        _coordinate: *mut core::ffi::c_void,
    ) -> Result<(), CustomScanError> {
        Err(CustomScanError::internal(std::io::Error::other(
            "provider enabled native parallel scan without InitializeDSM support",
        )))
    }

    /// # Safety
    /// `coordinate` is the live provider coordinate for this scan.
    unsafe fn reinitialize_dsm(
        _state: &mut Self::State,
        _coordinate: *mut core::ffi::c_void,
    ) -> Result<(), CustomScanError> {
        Err(CustomScanError::internal(std::io::Error::other(
            "provider enabled native parallel scan without ReInitializeDSM support",
        )))
    }

    /// # Safety
    /// `toc` and `coordinate` are the live DSM objects supplied by PostgreSQL.
    unsafe fn initialize_worker(
        _state: &mut Self::State,
        _toc: *mut pg_sys::shm_toc,
        _coordinate: *mut core::ffi::c_void,
    ) -> Result<(), CustomScanError> {
        Err(CustomScanError::internal(std::io::Error::other(
            "provider enabled native parallel scan without worker initialization support",
        )))
    }

    fn shutdown_parallel(_state: &mut Self::State) -> Result<(), CustomScanError> {
        Ok(())
    }

    /// Reparameterize `PrivateData` for an appendrel child; default no-op.
    ///
    /// # Safety
    ///
    /// All pointers must be live planner-owned nodes for the same appendrel
    /// planning operation. Implementations must return a `List` allocated in a
    /// PostgreSQL memory context that outlives the planned path.
    #[allow(unused_variables)]
    unsafe fn reparameterize_private_data(
        root: *mut pg_sys::PlannerInfo,
        private: *mut pg_sys::List,
        child_rel: *mut pg_sys::RelOptInfo,
    ) -> *mut pg_sys::List {
        private
    }
}
