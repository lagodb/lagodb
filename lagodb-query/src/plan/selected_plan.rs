//! Engine-owned selected plan payload: query semantics and provider table scans.

use std::ffi::CStr;
use std::ptr;

use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{
    ScanCost, ScanCostError, TableScanRoute, TableScanRouteKind,
};
use pgrx::pg_sys;

use crate::ExecutionProfile;

use super::codec::{QueryPlanData, QueryPlanDataError};
use super::table_scan_filter::TableScanFilterExplain;

/// One provider table scan with opaque plan data and explain metadata.
pub struct PlannedTableScan<'plan> {
    route: TableScanRoute<'plan>,
    relation_oid: pg_sys::Oid,
    alias: &'plan CStr,
    cost: ScanCost,
    filter_explain: Option<TableScanFilterExplain<'plan>>,
    provider_plan: &'plan pg_sys::List,
}

impl<'plan> PlannedTableScan<'plan> {
    /// Construct one dense scan-table entry around a provider-owned plan record.
    /// Its position in [`SelectedQueryPlan::scans`] is its query-local `ScanId`.
    ///
    /// # Safety
    ///
    /// `provider_plan` must be a live, non-NIL, `copyObject`-safe PostgreSQL
    /// `T_List` in the current planner memory context. It must remain read-only
    /// while this borrowed descriptor exists. The relation alias and any
    /// filter expression text must also remain live while this descriptor is
    /// encoded or inspected.
    pub unsafe fn new(
        route: TableScanRoute<'plan>,
        relation_oid: pg_sys::Oid,
        alias: &'plan CStr,
        cost: ScanCost,
        filter_explain: Option<TableScanFilterExplain<'plan>>,
        provider_plan: &'plan pg_sys::List,
    ) -> Self {
        Self {
            route,
            relation_oid,
            alias,
            cost,
            filter_explain,
            provider_plan,
        }
    }

    #[inline]
    pub const fn route(&self) -> TableScanRoute<'plan> {
        self.route
    }

    #[inline]
    pub const fn relation_oid(&self) -> pg_sys::Oid {
        self.relation_oid
    }

    #[inline]
    pub const fn alias(&self) -> &'plan CStr {
        self.alias
    }

    #[inline]
    pub const fn cost(&self) -> ScanCost {
        self.cost
    }

    #[inline]
    pub const fn filter_explain(&self) -> Option<TableScanFilterExplain<'plan>> {
        self.filter_explain
    }

    #[inline]
    pub(crate) fn provider_plan(&self) -> *const pg_sys::List {
        ptr::from_ref(self.provider_plan)
    }
}

/// Self-contained engine plan-data frame: query semantics, execution profile
/// and opaque provider scan frames.
///
/// Host-owned runtime bindings and scan-target expressions are not part of
/// this payload.
pub struct SelectedQueryPlan<'plan> {
    query: QueryPlanData,
    execution: ExecutionProfile,
    parallel_safe: bool,
    scans: Box<[PlannedTableScan<'plan>]>,
}

impl<'plan> SelectedQueryPlan<'plan> {
    /// # Safety
    ///
    /// Every provider plan must satisfy [`PlannedTableScan::new`].
    pub unsafe fn encode(
        query: &QueryPlanData,
        execution: ExecutionProfile,
        parallel_safe: bool,
        scans: &[PlannedTableScan<'plan>],
    ) -> Result<*mut pg_sys::List, SelectedQueryPlanError> {
        Self::validate_scans(query, scans)?;
        let query_plan = query.encode(scans.len())?;
        PlanDataWriter::encode_list(|writer| {
            writer
                .append_bool(parallel_safe)
                .append_count(execution.maximum_batch_rows().get())
                .append_count(scans.len());
            for scan in scans {
                writer.append_nested(|record| {
                    record
                        .append_i32(scan.route().kind().code())
                        .append_cstr(scan.route().name())
                        .append_oid(scan.relation_oid())
                        .append_cstr(scan.alias())
                        .append_i64(scan.cost().rows_read().to_bits() as i64)
                        .append_i64(scan.cost().bytes_read().to_bits() as i64)
                        .append_i64(scan.cost().startup_cost().to_bits() as i64);
                    record.append_bool(scan.filter_explain().is_some());
                    if let Some(filter) = scan.filter_explain() {
                        record
                            .append_cstr(filter.exact_expression())
                            .append_bool(filter.pushed_expression().is_some());
                        if let Some(expression) = filter.pushed_expression() {
                            record.append_cstr(expression);
                        }
                    }
                    // SAFETY: this method requires every provider plan to be a
                    // live copyObject-safe List in the planner context.
                    unsafe {
                        record.append_encoded_list(scan.provider_plan().cast_mut())
                    };
                });
            }
            // SAFETY: `query_plan` was allocated by the query codec in the
            // same live planner context.
            unsafe { writer.append_encoded_list(query_plan) };
            Ok(())
        })
    }

    /// # Safety
    ///
    /// `list` must point to a live PostgreSQL plan-data `T_List` for the
    /// complete synchronous decode and subsequent scan preparation.
    pub unsafe fn decode(
        list: &'plan pg_sys::List,
    ) -> Result<Self, SelectedQueryPlanError> {
        let decode = |reader: &mut PlanDataReader<'plan>| {
            let parallel_safe = reader.read_bool()?;
            // Encoded from the selected plan's validated profile.
            let execution = ExecutionProfile::restore(reader.read_count()?);
            let scan_count = reader.read_count()?;
            let mut scans = Vec::with_capacity(scan_count);
            for _ in 0..scan_count {
                scans.push(reader.read_nested(|record| {
                    let route_kind_code = record.read_i32()?;
                    let route_kind = TableScanRouteKind::from_code(route_kind_code)
                        .ok_or(
                        SelectedQueryPlanError::UnknownTableScanRouteKind(
                            route_kind_code,
                        ),
                    )?;
                    let route = TableScanRoute::new(route_kind, record.read_cstr()?);
                    let relation_oid = record.read_oid()?;
                    let alias = record.read_cstr()?;
                    let cost = ScanCost::try_new(
                        f64::from_bits(record.read_i64()? as u64),
                        f64::from_bits(record.read_i64()? as u64),
                        f64::from_bits(record.read_i64()? as u64),
                    )?;
                    let filter_explain = if record.read_bool()? {
                        Some(TableScanFilterExplain::new(
                            record.read_cstr()?,
                            if record.read_bool()? {
                                Some(record.read_cstr()?)
                            } else {
                                None
                            },
                        ))
                    } else {
                        None
                    };
                    let provider_plan = record.read_encoded_list()?;
                    // SAFETY: the nested list belongs to `list`; this decode
                    // is bounded by the input reference's `'plan` lifetime.
                    let provider_plan: &'plan pg_sys::List =
                        unsafe { &*provider_plan };
                    Ok::<_, SelectedQueryPlanError>(PlannedTableScan {
                        route,
                        relation_oid,
                        alias,
                        cost,
                        filter_explain,
                        provider_plan,
                    })
                })?);
            }
            let query_plan = reader.read_encoded_list()?;
            // SAFETY: `read_encoded_list` returned a checked nested List
            // borrowed from the live selected-plan payload.
            let query = unsafe { QueryPlanData::decode(query_plan, scan_count) }?;
            let scans = scans.into_boxed_slice();
            Self::validate_scans(&query, &scans)?;
            Ok(Self {
                query,
                execution,
                parallel_safe,
                scans,
            })
        };
        // SAFETY: the caller guarantees that `list` is live for this decode.
        unsafe { PlanDataReader::decode_checked_ref(list, 0, decode) }
    }

    fn validate_scans(
        query: &QueryPlanData,
        scans: &[PlannedTableScan<'_>],
    ) -> Result<(), SelectedQueryPlanError> {
        query.validate(scans.len())?;
        Ok(())
    }

    #[inline]
    pub fn scans(&self) -> &[PlannedTableScan<'plan>] {
        &self.scans
    }

    #[inline]
    pub const fn query(&self) -> &QueryPlanData {
        &self.query
    }

    #[inline]
    pub const fn execution_profile(&self) -> ExecutionProfile {
        self.execution
    }

    #[inline]
    pub const fn parallel_safe(&self) -> bool {
        self.parallel_safe
    }

    /// Largest provider estimate of rows examined by any table scan after
    /// provider pruning. This is physical scan work, not table cardinality or
    /// query output rows.
    pub fn largest_scan_rows(&self) -> f64 {
        self.scans
            .iter()
            .map(|scan| scan.cost().rows_read())
            .fold(0.0, f64::max)
    }

    pub fn into_parts(
        self,
    ) -> (
        QueryPlanData,
        ExecutionProfile,
        Box<[PlannedTableScan<'plan>]>,
    ) {
        (self.query, self.execution, self.scans)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SelectedQueryPlanError {
    #[error("selected query plan-data primitive failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("query engine plan is invalid: {0}")]
    QueryPlan(#[from] QueryPlanDataError),
    #[error("query engine plan is invalid: {0}")]
    InvalidPlan(#[from] super::QueryPlanError),
    #[error("selected query plan contains unknown table-scan route kind {0}")]
    UnknownTableScanRouteKind(i32),
    #[error("table scan cost facts are invalid: {0}")]
    InvalidScanCost(#[from] ScanCostError),
}
