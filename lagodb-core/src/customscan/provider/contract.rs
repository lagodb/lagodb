//! Scan-provider trait and its stable public contract.

use core::ffi::CStr;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::expr::pushdown::FilterPushdown;
use crate::runtime_api::TableScanTaskMetrics;

use super::execution::{NextSlotContext, NextSlotResult};
use super::lifecycle::{BeginContext, EndContext, ReScanContext, StartContext};
use super::planning::{
    CustomPathBuilder, CustomPathPlan, PathContext, PathVariant, RelationContext,
};
use super::private_data::CustomScanPrivate;

/// Treatment of PostgreSQL's SeqScan paths over a provider's table AM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableAmSeqScan {
    /// Keep every SeqScan path.
    Keep,
    /// Replace the complete SeqScan of a read scan with the provider's path
    /// while CustomScan paths are enabled. The table AM scans the same
    /// storage without projection or pushed filters, yet PostgreSQL prices it
    /// as a heap scan (`cost_seqscan`; a table AM has no costing callback), so
    /// cost cannot rank the two paths. The SeqScan returns when the provider
    /// declines the Plain variant, which shares its `lateral_relids`.
    ///
    /// Partial SeqScans of a read scan are removed even while CustomScan
    /// paths are disabled: the table AM lacks its own parallel
    /// task-distribution contract, so such a path would call unsupported
    /// table-AM callbacks or duplicate the scan. This is independent of
    /// CustomScan parallel capability.
    ReplaceWhenProvided,
}

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

    /// What becomes of the table AM's own SeqScan paths on a relation this
    /// provider claims.
    const TABLE_AM_SEQ_SCAN: TableAmSeqScan = TableAmSeqScan::Keep;

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

    /// Construct the per-scan state at Begin from statement-stable inputs.
    /// Dynamic inputs and the first row cursor belong to Start. The framework
    /// installs the state only when this succeeds; EXPLAIN-only execution never
    /// calls Begin and therefore has no provider state.
    fn begin(ctx: BeginContext<'_, Self>) -> Result<Self::State, CustomScanError>;

    /// Consume the begun state and return the state that opened the first
    /// cursor with the framework's bound dynamic predicates. This runs once,
    /// after executor initialization and before the first row, including when
    /// PostgreSQL rescans the node before its first execution. On failure the
    /// consumed state is dropped before the framework reports the error.
    fn start(
        state: Self::State,
        ctx: StartContext<'_, Self>,
    ) -> Result<Self::State, CustomScanError>;

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
}
