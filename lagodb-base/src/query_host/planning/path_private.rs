//! Path-stage `CustomPath.custom_private` envelope for query offload.
//!
//! The engine-owned [`SelectedQueryPlan`] payload is one nested list that
//! `PlanCustomPath` moves unchanged to `CustomScan.custom_private`. Runtime
//! value expressions and scan-target expressions remain PostgreSQL nodes until
//! that callback moves them to `CustomScan.custom_exprs` and
//! `CustomScan.custom_scan_tlist`, so they belong to this host envelope.

use std::ffi::c_void;
use std::ptr;

use lagodb_core::expr::pg::expr_list;
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_query::ExecutionProfile;
use lagodb_query::plan::{PlannedTableScan, QueryPlanData, SelectedQueryPlan};
use pgrx::pg_sys;

use crate::query_host::error::QueryHostError;

/// Decoded wrapper carried between path creation and `PlanCustomPath`.
pub(super) struct QueryPathPrivate<'plan> {
    selected_plan: &'plan pg_sys::List,
    runtime_exprs: Box<[*mut pg_sys::Expr]>,
    scan_target_exprs: Box<[*mut pg_sys::Expr]>,
}

impl<'plan> QueryPathPrivate<'plan> {
    /// Encode a path whose runtime expressions follow the query runtime layout
    /// and whose scan-target expressions follow its tuple layout.
    ///
    /// # Safety
    ///
    /// Every provider plan must satisfy [`PlannedTableScan::new`], and every
    /// expression must remain live in the current planner memory context.
    pub(super) unsafe fn encode(
        query: &QueryPlanData,
        execution: ExecutionProfile,
        parallel_safe: bool,
        scans: &[PlannedTableScan<'_>],
        runtime_exprs: &[*mut pg_sys::Expr],
        scan_target_exprs: &[*mut pg_sys::Expr],
    ) -> Result<*mut pg_sys::List, QueryHostError> {
        if runtime_exprs.len() != query.runtime_values().len() {
            return Err(QueryHostError::invalid_plan(
                "runtime expression count does not match the query runtime layout",
            ));
        }
        if scan_target_exprs.len() != query.tuple_layout().len() {
            return Err(QueryHostError::invalid_plan(
                "scan-target expression count does not match the query tuple layout",
            ));
        }
        let selected_plan = unsafe {
            SelectedQueryPlan::encode(query, execution, parallel_safe, scans)
        }
        .map_err(QueryHostError::invalid_plan)?;
        PlanDataWriter::encode_list(|writer| {
            // SAFETY: the engine codec allocated `selected_plan` in the current
            // planner context, and the caller keeps every expression live there.
            unsafe {
                writer.append_encoded_list(selected_plan);
                writer.append_list(expr_list(runtime_exprs.iter().copied()));
                writer.append_list(expr_list(scan_target_exprs.iter().copied()));
            }
            Ok::<_, PlanDataError>(())
        })
        .map_err(QueryHostError::invalid_plan)
    }

    /// # Safety
    ///
    /// `list` must be this path's live `custom_private` for all of `'plan`.
    pub(super) unsafe fn decode(
        list: &'plan pg_sys::List,
    ) -> Result<Self, QueryHostError> {
        let decode = |reader: &mut PlanDataReader<'plan>| {
            let selected_plan = reader.read_encoded_list()?;
            // SAFETY: the nested list belongs to `list` for all of `'plan`.
            let selected_plan: &'plan pg_sys::List = unsafe { &*selected_plan };
            Ok::<_, PlanDataError>(Self {
                selected_plan,
                runtime_exprs: Self::read_expressions(reader)?,
                scan_target_exprs: Self::read_expressions(reader)?,
            })
        };
        // SAFETY: the caller guarantees that `list` is live for `'plan`.
        unsafe { PlanDataReader::decode_checked_ref(list, 0, decode) }
            .map_err(QueryHostError::invalid_plan)
    }

    /// The engine payload that becomes `CustomScan.custom_private` unchanged.
    #[inline]
    pub(super) fn selected_plan_data(&self) -> *mut pg_sys::List {
        ptr::from_ref(self.selected_plan).cast_mut()
    }

    /// Decode the engine payload for planning that extends its query IR.
    pub(super) fn selected_plan(
        &self,
    ) -> Result<SelectedQueryPlan<'plan>, QueryHostError> {
        // SAFETY: `decode` borrowed this nested list from a live path payload
        // for all of `'plan`.
        unsafe { SelectedQueryPlan::decode(self.selected_plan) }
            .map_err(QueryHostError::invalid_plan)
    }

    #[inline]
    pub(super) fn runtime_exprs(&self) -> &[*mut pg_sys::Expr] {
        &self.runtime_exprs
    }

    #[inline]
    pub(super) fn scan_target_exprs(&self) -> &[*mut pg_sys::Expr] {
        &self.scan_target_exprs
    }

    /// Tuple-layout position whose scan-target expression equals `expression`.
    pub(super) fn scan_target_position(
        &self,
        expression: *mut pg_sys::Expr,
    ) -> Option<usize> {
        // SAFETY: both nodes are live planner expressions; `equal` only reads.
        self.scan_target_exprs.iter().position(|&candidate| unsafe {
            pg_sys::equal(candidate.cast::<c_void>(), expression.cast::<c_void>())
        })
    }

    /// Re-encode this path after an upper operator has wrapped the query IR.
    /// Provider plans, scan-target expressions and existing runtime
    /// expressions remain shared planner-owned nodes; only their containing
    /// PostgreSQL lists are rebuilt.
    ///
    /// # Safety
    ///
    /// `additional_runtime_exprs` must remain live in the current planner
    /// context and correspond exactly to runtime slots appended to `query`.
    pub(super) unsafe fn encode_replacement(
        &self,
        selected: &SelectedQueryPlan<'_>,
        query: &QueryPlanData,
        additional_runtime_exprs: &[*mut pg_sys::Expr],
    ) -> Result<*mut pg_sys::List, QueryHostError> {
        let runtime_exprs =
            [&self.runtime_exprs[..], additional_runtime_exprs].concat();
        unsafe {
            Self::encode(
                query,
                selected.execution_profile(),
                selected.parallel_safe(),
                selected.scans(),
                &runtime_exprs,
                &self.scan_target_exprs,
            )
        }
    }

    fn read_expressions(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Box<[*mut pg_sys::Expr]>, PlanDataError> {
        let list = reader.read_optional_list()?;
        // SAFETY: `read_optional_list` returned NIL or a checked List owned by
        // the live path payload; every index is below its length.
        let length = unsafe { pg_sys::list_length(list) };
        Ok((0..length)
            .map(|index| {
                unsafe { pg_sys::list_nth(list, index) }.cast::<pg_sys::Expr>()
            })
            .collect())
    }
}
