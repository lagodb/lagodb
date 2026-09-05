//! Selected path combining query semantics with provider table scans.

use std::ptr;

use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{
    ProviderId, ScanEstimate, ScanEstimateError, ScanId,
};
use pgrx::pg_sys;

use crate::{ExecutionProfile, ExecutionProfileError};

use super::table_scan_filter::TableScanFilterExplain;
use super::{QueryPlanData, QueryPlanDataError};

const PATH_PAYLOAD: i32 = 1;
const EXECUTION_PAYLOAD: i32 = 2;

/// Contiguous query-runtime bindings owned by one table-scan predicate.
///
/// Query expressions are evaluated once in their global layout. Each scan
/// receives only this view, while its provider plan keeps a scan-local layout
/// whose value identities start at zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableScanRuntimeBindings {
    start: usize,
    count: usize,
}

impl TableScanRuntimeBindings {
    pub const fn empty() -> Self {
        Self { start: 0, count: 0 }
    }

    pub fn try_new(start: usize, count: usize) -> Option<Self> {
        start.checked_add(count).map(|_| Self { start, count })
    }

    #[inline]
    pub const fn start(self) -> usize {
        self.start
    }

    #[inline]
    pub const fn len(self) -> usize {
        self.count
    }

    #[inline]
    pub const fn is_empty(self) -> bool {
        self.count == 0
    }

    fn end(self) -> usize {
        self.start + self.count
    }

    fn fits(self, runtime_value_count: usize) -> bool {
        self.end() <= runtime_value_count
    }

    pub(crate) fn select<T>(self, values: &[T]) -> &[T] {
        values.get(self.start..self.end()).expect(
            "selected-plan validation bounds every table-scan runtime binding",
        )
    }
}

/// One provider table scan with opaque plan data and explain metadata.
pub struct PlannedTableScan<'plan> {
    provider: ProviderId,
    scan: ScanId,
    estimate: ScanEstimate,
    runtime_bindings: TableScanRuntimeBindings,
    filter_explain: Option<TableScanFilterExplain<'plan>>,
    provider_plan: &'plan pg_sys::List,
}

impl<'plan> PlannedTableScan<'plan> {
    /// Bind a provider-owned plan record to one query-local scan.
    ///
    /// # Safety
    ///
    /// `provider_plan` must be a live, non-NIL, `copyObject`-safe PostgreSQL
    /// `T_List` in the current planner memory context. It must remain read-only
    /// while this borrowed descriptor exists. `runtime_bindings` must select
    /// the global query values in the same order as the scan-local predicate
    /// layout encoded in `provider_plan`. Any filter expression text must also
    /// remain live while this descriptor is encoded or inspected.
    pub unsafe fn new(
        provider: ProviderId,
        scan: ScanId,
        estimate: ScanEstimate,
        runtime_bindings: TableScanRuntimeBindings,
        filter_explain: Option<TableScanFilterExplain<'plan>>,
        provider_plan: &'plan pg_sys::List,
    ) -> Self {
        Self {
            provider,
            scan,
            estimate,
            runtime_bindings,
            filter_explain,
            provider_plan,
        }
    }

    #[inline]
    pub const fn provider(&self) -> ProviderId {
        self.provider
    }

    #[inline]
    pub const fn scan(&self) -> ScanId {
        self.scan
    }

    #[inline]
    pub const fn estimate(&self) -> ScanEstimate {
        self.estimate
    }

    #[inline]
    pub const fn runtime_bindings(&self) -> TableScanRuntimeBindings {
        self.runtime_bindings
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
                let mut expressions = ptr::null_mut();
                for expression in scan_target_exprs {
                    expressions =
                        unsafe { pg_sys::lappend(expressions, expression.cast()) };
                }
                unsafe { writer.append_encoded_list(expressions) };
            }
            for scan in scans {
                writer.append_nested(|record| {
                    record
                        .append_count(scan.provider().index())
                        .append_count(scan.scan().index())
                        .append_i64(scan.estimate().estimated_rows().to_bits() as i64)
                        .append_i64(
                            scan.estimate().estimated_scan_bytes().to_bits() as i64
                        )
                        .append_count(scan.runtime_bindings().start())
                        .append_count(scan.runtime_bindings().len())
                        .append_bool(scan.filter_explain().is_some());
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
                    let scan_target_exprs = reader.read_encoded_list()?;
                    if unsafe { pg_sys::list_length(scan_target_exprs) } as usize
                        != scan_target_expr_count
                    {
                        return Err(
                            SelectedQueryPlanError::ScanTargetExpressionCount,
                        );
                    }
                    (Some(runtime_expr_count), runtime_exprs, scan_target_exprs)
                } else {
                    (None, ptr::null_mut(), ptr::null_mut())
                };
            let mut scans = Vec::with_capacity(scan_count);
            for expected_index in 0..scan_count {
                scans.push(reader.read_nested(|record| {
                    let provider = ProviderId::from_index(record.read_count()?);
                    let scan_index = record.read_count()?;
                    if scan_index != expected_index {
                        return Err(SelectedQueryPlanError::NonDenseScanIdentity {
                            expected: expected_index,
                            found: scan_index,
                        });
                    }
                    let estimate = ScanEstimate::try_new(
                        f64::from_bits(record.read_i64()? as u64),
                        f64::from_bits(record.read_i64()? as u64),
                    )?;
                    let runtime_bindings = TableScanRuntimeBindings::try_new(
                        record.read_count()?,
                        record.read_count()?,
                    )
                    .ok_or(SelectedQueryPlanError::RuntimeBindingRangeOverflow)?;
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
                        provider,
                        scan: ScanId::from_index(scan_index),
                        estimate,
                        runtime_bindings,
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
        for (expected, scan) in scans.iter().enumerate() {
            if scan.scan().index() != expected {
                return Err(SelectedQueryPlanError::NonDenseScanIdentity {
                    expected,
                    found: scan.scan().index(),
                });
            }
            if !scan.runtime_bindings().fits(query.runtime_values().len()) {
                return Err(SelectedQueryPlanError::ScanRuntimeValuesOutOfBounds {
                    scan: scan.scan().index(),
                    start: scan.runtime_bindings().start(),
                    count: scan.runtime_bindings().len(),
                    total: query.runtime_values().len(),
                });
            }
        }
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
    #[error("scan identities must be dense; expected {expected}, found {found}")]
    NonDenseScanIdentity { expected: usize, found: usize },
    #[error(
        "selected query runtime expression count does not match its expression list"
    )]
    RuntimeExpressionCount,
    #[error(
        "selected query scan-target expression count does not match its tuple layout"
    )]
    ScanTargetExpressionCount,
    #[error("table scan runtime binding range overflows usize")]
    RuntimeBindingRangeOverflow,
    #[error(
        "table scan {scan} runtime binding range {start}..+{count} exceeds query layout length {total}"
    )]
    ScanRuntimeValuesOutOfBounds {
        scan: usize,
        start: usize,
        count: usize,
        total: usize,
    },
    #[error(
        "selected query payload kind mismatch: expected {expected}, found {found}"
    )]
    WrongPayloadKind { expected: i32, found: i32 },
    #[error("table scan estimate is invalid: {0}")]
    InvalidScanEstimate(#[from] ScanEstimateError),
    #[error("query execution profile is invalid: {0}")]
    InvalidExecutionProfile(#[from] ExecutionProfileError),
}
