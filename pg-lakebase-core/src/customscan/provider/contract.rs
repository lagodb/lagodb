//! Scan-provider trait and its stable public contract.

use core::ffi::CStr;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::expr::pushdown::FilterPushdown;

use super::execution::{
    BeginContext, CreateStateContext, EndContext, NextSlotContext, ReScanContext,
};
use super::planning::{
    CustomPathBuilder, CustomPathPlan, PathContext, PathVariant, RelationContext,
};
use super::private_data::CustomScanPrivate;

/// Lake backend provider trait: relation routing, CustomPath emission, and
/// scan lifecycle.
pub trait LakebaseCustomScanProvider: FilterPushdown {
    /// Unique provider name (EXPLAIN + registry).
    const NAME: &'static CStr;

    /// Provider tail of `custom_private`; framework owns the envelope.
    type PrivateData: CustomScanPrivate;

    /// Per-scan runtime state inside `CustomScanStateWrapper`.
    type State;

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

    /// Open scan cursor; framework calls from BeginCustomScan.
    fn begin(ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError>;

    /// Produce the next row; `Ok(false)` means end of scan.
    fn next_slot(ctx: NextSlotContext<'_, Self>) -> Result<bool, CustomScanError>;

    /// Rewind the scan, replacing predicates when `filters_changed`.
    fn rescan(ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError>;

    /// Close the cursor and release provider-owned runtime resources.
    fn end(ctx: EndContext<'_, Self>) -> Result<(), CustomScanError>;

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
