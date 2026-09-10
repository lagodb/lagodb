//! Selected path combining query semantics with provider table scans.

use std::ffi::CStr;
use std::ptr;

use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{
    ScanCost, ScanCostError, TableScanRoute, TableScanRouteKind,
};
use pgrx::pg_sys;

use crate::{ExecutionProfile, ExecutionProfileError};

use super::table_scan_filter::TableScanFilterExplain;
use super::{QueryPlanData, QueryPlanDataError};

const PATH_PAYLOAD: i32 = 1;
const EXECUTION_PAYLOAD: i32 = 2;

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

/// Query plan carrier used first by a selected path and then by executor Begin.
pub struct SelectedQueryPlan<'plan> {
    query: QueryPlanData,
    execution: ExecutionProfile,
    scans: Box<[PlannedTableScan<'plan>]>,
    runtime_exprs: *mut pg_sys::List,
    scan_target_exprs: *mut pg_sys::List,
}

impl<'plan> SelectedQueryPlan<'plan> {
    /// Encode a planner-path carrier. Runtime expressions and raw scan-target
    /// expressions remain PostgreSQL nodes until `PlanCustomPath` moves them to
    /// `CustomScan.custom_exprs` and `CustomScan.custom_scan_tlist` respectively.
    ///
    /// # Safety
    ///
    /// Every provider plan must satisfy [`PlannedTableScan::new`].
    pub unsafe fn encode_path(
        query: &QueryPlanData,
        execution: ExecutionProfile,
        scans: &[PlannedTableScan<'plan>],
        runtime_exprs: &[*mut pg_sys::Expr],
        scan_target_exprs: &[*mut pg_sys::Expr],
    ) -> Result<*mut pg_sys::List, SelectedQueryPlanError> {
        if runtime_exprs.len() != query.runtime_values().len() {
            return Err(SelectedQueryPlanError::RuntimeExpressionCount);
        }
        if scan_target_exprs.len() != query.tuple_layout().len() {
            return Err(SelectedQueryPlanError::ScanTargetExpressionCount);
        }
        unsafe {
            Self::encode(
                PATH_PAYLOAD,
                query,
                execution,
                scans,
                Some((runtime_exprs, scan_target_exprs)),
            )
        }
    }

    /// Encode executor plan data without PostgreSQL expression nodes. Runtime
    /// expressions and scan-target expressions have their single owners in the
    /// corresponding `CustomScan` fields.
    ///
    /// # Safety
    ///
    /// Every provider plan must satisfy [`PlannedTableScan::new`].
    pub unsafe fn encode_execution(
        query: &QueryPlanData,
        execution: ExecutionProfile,
        scans: &[PlannedTableScan<'plan>],
    ) -> Result<*mut pg_sys::List, SelectedQueryPlanError> {
        unsafe { Self::encode(EXECUTION_PAYLOAD, query, execution, scans, None) }
    }

    unsafe fn encode(
        payload_kind: i32,
        query: &QueryPlanData,
        execution: ExecutionProfile,
        scans: &[PlannedTableScan<'plan>],
        planner_exprs: Option<(&[*mut pg_sys::Expr], &[*mut pg_sys::Expr])>,
    ) -> Result<*mut pg_sys::List, SelectedQueryPlanError> {
        Self::validate_scans(query, scans)?;
        let query_plan = query.encode(scans.len())?;
        PlanDataWriter::encode_list(|writer| {
            writer
                .append_i32(payload_kind)
                .append_count(execution.maximum_batch_rows().get())
                .append_count(scans.len());
            if let Some((runtime_exprs, scan_target_exprs)) = planner_exprs {
                writer.append_count(runtime_exprs.len());
                if !runtime_exprs.is_empty() {
                    let mut expressions = ptr::null_mut();
                    for expression in runtime_exprs {
                        expressions = unsafe {
                            pg_sys::lappend(expressions, expression.cast())
                        };
                    }
                    unsafe { writer.append_encoded_list(expressions) };
                }
                writer.append_count(scan_target_exprs.len());
                if !scan_target_exprs.is_empty() {
                    let mut expressions = ptr::null_mut();
                    for expression in scan_target_exprs {
                        expressions = unsafe {
                            pg_sys::lappend(expressions, expression.cast())
                        };
                    }
                    unsafe { writer.append_encoded_list(expressions) };
                }
            }
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

    /// Decode a planner-path carrier and reject execution-only payloads.
    ///
    /// # Safety
    ///
    /// `list` must point to a live PostgreSQL plan-data `T_List` for the
    /// complete synchronous decode and subsequent scan preparation.
    pub unsafe fn decode_path(
        list: &'plan pg_sys::List,
    ) -> Result<Self, SelectedQueryPlanError> {
        unsafe { Self::decode(list, PATH_PAYLOAD) }
    }

    /// Decode an execution carrier and reject path-only payloads.
    ///
    /// # Safety
    ///
    /// `list` must point to live PostgreSQL plan data for the complete decode
    /// and subsequent scan preparation.
    pub unsafe fn decode_execution(
        list: &'plan pg_sys::List,
    ) -> Result<Self, SelectedQueryPlanError> {
        unsafe { Self::decode(list, EXECUTION_PAYLOAD) }
    }

    unsafe fn decode(
        list: &'plan pg_sys::List,
        expected_payload_kind: i32,
    ) -> Result<Self, SelectedQueryPlanError> {
        let decode = |reader: &mut PlanDataReader<'plan>| {
            let payload_kind = reader.read_i32()?;
            if payload_kind != expected_payload_kind {
                return Err(SelectedQueryPlanError::WrongPayloadKind {
                    expected: expected_payload_kind,
                    found: payload_kind,
                });
            }
            let execution = ExecutionProfile::try_new(reader.read_count()?)?;
            let scan_count = reader.read_count()?;
            let (runtime_expr_count, runtime_exprs, scan_target_exprs) =
                if payload_kind == PATH_PAYLOAD {
                    let runtime_expr_count = reader.read_count()?;
                    let runtime_exprs = if runtime_expr_count == 0 {
                        ptr::null_mut()
                    } else {
                        let expressions = reader.read_encoded_list()?;
                        if unsafe { pg_sys::list_length(expressions) } as usize
                            != runtime_expr_count
                        {
                            return Err(
                                SelectedQueryPlanError::RuntimeExpressionCount,
                            );
                        }
                        expressions
                    };
                    let scan_target_expr_count = reader.read_count()?;
                    let scan_target_exprs = if scan_target_expr_count == 0 {
                        ptr::null_mut()
                    } else {
                        let expressions = reader.read_encoded_list()?;
                        if unsafe { pg_sys::list_length(expressions) } as usize
                            != scan_target_expr_count
                        {
                            return Err(
                                SelectedQueryPlanError::ScanTargetExpressionCount,
                            );
                        }
                        expressions
                    };
                    (Some(runtime_expr_count), runtime_exprs, scan_target_exprs)
                } else {
                    (None, ptr::null_mut(), ptr::null_mut())
                };
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
            if runtime_expr_count
                .is_some_and(|count| query.runtime_values().len() != count)
            {
                return Err(SelectedQueryPlanError::RuntimeExpressionCount);
            }
            if payload_kind == PATH_PAYLOAD
                && unsafe { pg_sys::list_length(scan_target_exprs) } as usize
                    != query.tuple_layout().len()
            {
                return Err(SelectedQueryPlanError::ScanTargetExpressionCount);
            }
            let scans = scans.into_boxed_slice();
            Self::validate_scans(&query, &scans)?;
            Ok(Self {
                query,
                execution,
                scans,
                runtime_exprs,
                scan_target_exprs,
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
    pub const fn runtime_exprs(&self) -> *mut pg_sys::List {
        self.runtime_exprs
    }

    #[inline]
    pub const fn scan_target_exprs(&self) -> *mut pg_sys::List {
        self.scan_target_exprs
    }

    /// Return the planner expression aligned with one physical query output.
    pub fn scan_target_expr(&self, index: usize) -> Option<*mut pg_sys::Expr> {
        (index < self.query.tuple_layout().len()).then(|| unsafe {
            pg_sys::list_nth(self.scan_target_exprs, index as i32)
                .cast::<pg_sys::Expr>()
        })
    }

    /// Re-encode this selected path after an upper operator has wrapped the
    /// query IR. Provider plans, scan output expressions and existing runtime
    /// expressions remain shared planner-owned nodes; only their containing
    /// PostgreSQL lists are rebuilt.
    ///
    /// # Safety
    ///
    /// `additional_runtime_exprs` must remain live in the current planner
    /// context and correspond exactly to runtime slots appended to `query`.
    pub unsafe fn encode_replacement_path(
        &self,
        query: &QueryPlanData,
        additional_runtime_exprs: &[*mut pg_sys::Expr],
    ) -> Result<*mut pg_sys::List, SelectedQueryPlanError> {
        let existing_count =
            unsafe { pg_sys::list_length(self.runtime_exprs) } as usize;
        let mut runtime_exprs =
            Vec::with_capacity(existing_count + additional_runtime_exprs.len());
        for index in 0..existing_count {
            runtime_exprs.push(
                unsafe { pg_sys::list_nth(self.runtime_exprs, index as i32) }
                    .cast::<pg_sys::Expr>(),
            );
        }
        runtime_exprs.extend_from_slice(additional_runtime_exprs);
        let target_count = self.query.tuple_layout().len();
        let mut scan_target_exprs = Vec::with_capacity(target_count);
        for index in 0..target_count {
            scan_target_exprs.push(
                self.scan_target_expr(index)
                    .expect("decoded selected path has a complete scan target list"),
            );
        }
        unsafe {
            Self::encode_path(
                query,
                self.execution,
                &self.scans,
                &runtime_exprs,
                &scan_target_exprs,
            )
        }
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
    #[error(
        "selected query runtime expression count does not match its expression list"
    )]
    RuntimeExpressionCount,
    #[error(
        "selected query scan-target expression count does not match its tuple layout"
    )]
    ScanTargetExpressionCount,
    #[error(
        "selected query payload kind mismatch: expected {expected}, found {found}"
    )]
    WrongPayloadKind { expected: i32, found: i32 },
    #[error("table scan cost facts are invalid: {0}")]
    InvalidScanCost(#[from] ScanCostError),
    #[error("query execution profile is invalid: {0}")]
    InvalidExecutionProfile(#[from] ExecutionProfileError),
}
