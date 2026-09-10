use std::mem::size_of;
use std::ptr;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::expr::ExpressionCodecError;
use lagodb_core::expr::pushdown::{FilterPushdown, PredicatePlan};
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{ScanCost, ScanId};
use lagodb_core::runtime_api::{
    RuntimePredicateCodecError, RuntimePredicateUpdateAction,
    RuntimePruningPredicate, TABLE_SCAN_PROJECTION_COLUMNS,
    TABLE_SCAN_PROJECTION_ROW_COUNT, TableScanPlanningRequest, TableScanRoutes,
    TableScanRuntimePredicate, TableScanStreamRequest, TableScanTaskMetrics,
    TableScanTaskPlanningRequest,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

/// Projection requested from a provider query source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanProjection<'a> {
    RowCount,
    Columns(&'a [pg_sys::AttrNumber]),
}

/// Borrowed provider-facing view of one query-source planning request.
pub struct ScanPlanningContext<'a> {
    request: &'a TableScanPlanningRequest,
    projection: ScanProjection<'a>,
    pruning_selectivity: f64,
}

impl<'a> ScanPlanningContext<'a> {
    pub(super) fn try_new(
        request: &'a TableScanPlanningRequest,
    ) -> Result<Self, PgReportError> {
        if request.struct_size != size_of::<TableScanPlanningRequest>() as u32 {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan received an incompatible planning request",
            ));
        }
        let projection = match request.projection_kind {
            TABLE_SCAN_PROJECTION_ROW_COUNT => ScanProjection::RowCount,
            TABLE_SCAN_PROJECTION_COLUMNS => {
                if request.projected_attno_count == 0
                    || request.projected_attnos.is_null()
                {
                    return Err(PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        "column table scan received an empty projection",
                    ));
                }
                let columns = unsafe {
                    core::slice::from_raw_parts(
                        request.projected_attnos,
                        request.projected_attno_count,
                    )
                };
                if columns.iter().any(|attno| *attno <= 0) {
                    return Err(PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        "column table scan received an invalid attribute number",
                    ));
                }
                ScanProjection::Columns(columns)
            }
            _ => {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "table scan received an unknown projection kind",
                ));
            }
        };
        Ok(Self {
            request,
            projection,
            pruning_selectivity: 1.0,
        })
    }

    pub(super) fn set_pruning_selectivity(&mut self, selectivity: f64) {
        self.pruning_selectivity = selectivity.clamp(0.0, 1.0);
    }

    /// Estimate the provider-approved pruning expression with PostgreSQL's
    /// relation statistics. The adapter calls this once during planning, never
    /// from execution or a per-row path.
    pub(super) unsafe fn estimate_pruning_selectivity(
        &self,
        expression: *mut pg_sys::Expr,
    ) -> f64 {
        let clauses = unsafe { pg_sys::lappend(ptr::null_mut(), expression.cast()) };
        let selectivity = unsafe {
            pg_sys::clauselist_selectivity(
                self.request.root,
                clauses,
                self.request.range_table_index as i32,
                pg_sys::JoinType::JOIN_INNER,
                ptr::null_mut(),
            )
        };
        // `lappend` owns only its list cell; the expression remains
        // planner-owned. Provider negotiation runs for every candidate, so do
        // not retain one cell until planner-context teardown per attempt.
        unsafe { pg_sys::list_free(clauses) };
        selectivity.clamp(0.0, 1.0)
    }

    /// PostgreSQL estimate for the provider-approved, costable pruning
    /// expression. `1.0` means that no pruning may reduce physical work.
    #[inline]
    pub const fn pruning_selectivity(&self) -> f64 {
        self.pruning_selectivity
    }

    #[inline]
    pub const fn projection(&self) -> ScanProjection<'a> {
        self.projection
    }

    pub(crate) const fn predicate_expression(&self) -> Option<*mut pg_sys::Node> {
        if self.request.predicate_expression.is_null() {
            None
        } else {
            Some(self.request.predicate_expression)
        }
    }

    #[inline]
    pub const fn range_table_index(&self) -> pg_sys::Index {
        self.request.range_table_index
    }

    #[inline]
    pub const fn effective_user_id(&self) -> pg_sys::Oid {
        self.request.effective_user_id
    }

    #[inline]
    pub fn scan(&self) -> ScanId {
        ScanId::from_index(self.request.scan_index)
    }

    #[inline]
    pub fn relation_oid(&self) -> pg_sys::Oid {
        // SAFETY: request borrows PostgreSQL's live RangeTblEntry.
        unsafe { (*self.request.range_table_entry).relid }
    }

    #[inline]
    pub fn tablespace_oid(&self) -> pg_sys::Oid {
        // SAFETY: relation_oid comes from the live planner RangeTblEntry.
        unsafe { pg_sys::get_rel_tablespace(self.relation_oid()) }
    }

    #[inline]
    pub fn relation_rows(&self) -> f64 {
        // SAFETY: relation is the live RelOptInfo supplied for this callback.
        unsafe { (*self.request.relation).tuples }
    }

    #[inline]
    pub fn relation_physical_bytes(&self) -> f64 {
        // SAFETY: same live RelOptInfo invariant as relation_rows.
        let pages = unsafe { (*self.request.relation).pages } as f64;
        pages * pg_sys::BLCKSZ as f64
    }
}

/// Provider-owned scan plan paired with validated physical cost facts.
pub struct PlannedScan<P> {
    plan: P,
    cost: ScanCost,
}

impl<P> PlannedScan<P> {
    #[inline]
    #[must_use]
    pub const fn new(plan: P, cost: ScanCost) -> Self {
        Self { plan, cost }
    }

    #[inline]
    pub fn into_parts(self) -> (P, ScanCost) {
        (self.plan, self.cost)
    }
}

/// Provider decision for one relation scan.
pub enum ScanSupport<T> {
    Unsupported,
    Planned(T),
}

/// Run-local provider task plan and the facts derived from that exact task set.
pub struct PlannedScanTasks<P> {
    plan: P,
    metrics: TableScanTaskMetrics,
}

/// Owned inputs for one run-local physical task-planning call.
pub struct ScanTaskPlanningOptions {
    projection: Box<[usize]>,
}

impl ScanTaskPlanningOptions {
    pub(super) fn try_from_request(
        request: &TableScanTaskPlanningRequest,
    ) -> Result<Self, PgReportError> {
        if request.struct_size != size_of::<TableScanTaskPlanningRequest>() as u32 {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan received incompatible task-planning options",
            ));
        }
        if request.projected_column_count != 0 && request.projected_columns.is_null()
        {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan received invalid task-planning arrays",
            ));
        }
        let projection = if request.projected_column_count == 0 {
            Vec::new().into_boxed_slice()
        } else {
            unsafe {
                core::slice::from_raw_parts(
                    request.projected_columns,
                    request.projected_column_count,
                )
            }
            .into()
        };
        Ok(Self { projection })
    }

    #[inline]
    pub fn projection(&self) -> &[usize] {
        &self.projection
    }
}

impl<P> PlannedScanTasks<P> {
    #[inline]
    #[must_use]
    pub const fn new(plan: P, metrics: TableScanTaskMetrics) -> Self {
        Self { plan, metrics }
    }

    #[inline]
    pub fn into_parts(self) -> (P, TableScanTaskMetrics) {
        (self.plan, self.metrics)
    }
}

/// Immutable batch shape for opening a run-local stream.
#[derive(Debug, Clone, Copy)]
pub struct ScanStreamOptions {
    maximum_batch_rows: u64,
    evolving_predicate: *const TableScanRuntimePredicate,
}

impl ScanStreamOptions {
    pub(super) fn try_from_request(
        request: &TableScanStreamRequest,
    ) -> Result<Self, PgReportError> {
        if request.struct_size != size_of::<TableScanStreamRequest>() as u32
            || request.maximum_batch_rows == 0
            || request.stream_error.is_null()
        {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan received invalid stream limits",
            ));
        }
        Ok(Self {
            maximum_batch_rows: request.maximum_batch_rows,
            evolving_predicate: request.evolving_predicate,
        })
    }

    #[inline]
    pub const fn maximum_batch_rows(self) -> u64 {
        self.maximum_batch_rows
    }

    #[inline]
    pub const fn has_evolving_predicate(self) -> bool {
        !self.evolving_predicate.is_null()
    }

    /// Read a new evolving predicate snapshot at a provider task boundary.
    /// `last_generation` is the most recently consumed generation; unchanged
    /// state returns `None` without decoding or allocation.
    pub fn runtime_predicate_update(
        self,
        last_generation: u64,
    ) -> Result<Option<RuntimePredicateUpdate>, RuntimePredicateCodecError> {
        let Some(slot) =
            (unsafe { runtime_predicate_slot(self.evolving_predicate) })?
        else {
            return Ok(None);
        };
        if slot.generation == last_generation {
            return Ok(None);
        }
        let update = match slot
            .action()
            .ok_or(RuntimePredicateCodecError::UnknownUpdateAction)?
        {
            RuntimePredicateUpdateAction::Replace => {
                RuntimePredicateUpdate::Replace {
                    generation: slot.generation,
                    predicate: decode_runtime_predicate(&slot)?,
                }
            }
            RuntimePredicateUpdateAction::Clear => {
                if !slot.data.is_null() || slot.data_len != 0 {
                    return Err(RuntimePredicateCodecError::InvalidClearPayload);
                }
                RuntimePredicateUpdate::Clear {
                    generation: slot.generation,
                }
            }
        };
        Ok(Some(update))
    }
}

// SAFETY: the raw slot is borrowed from the current-thread serial engine and
// is read only inside its serialized Arrow stream callback.
unsafe impl Send for ScanStreamOptions {}

/// One newly observed provider-neutral predicate generation.
pub enum RuntimePredicateUpdate {
    /// Replace the previously consumed supplemental predicate.
    Replace {
        generation: u64,
        predicate: RuntimePruningPredicate<'static>,
    },
    /// Remove the previously consumed supplemental predicate.
    Clear { generation: u64 },
}

impl RuntimePredicateUpdate {
    #[inline]
    pub const fn generation(&self) -> u64 {
        match self {
            Self::Replace { generation, .. } | Self::Clear { generation } => {
                *generation
            }
        }
    }
}

/// Copy a predicate slot whose lifetime is governed by the table-scan ABI.
///
/// # Safety
///
/// A non-null pointer must name the engine-owned slot retained for the current
/// synchronous callback or Arrow stream lifetime.
pub(super) unsafe fn runtime_predicate_slot(
    predicate: *const TableScanRuntimePredicate,
) -> Result<Option<TableScanRuntimePredicate>, RuntimePredicateCodecError> {
    let Some(predicate) = (unsafe { predicate.as_ref() }) else {
        return Ok(None);
    };
    if predicate.struct_size != size_of::<TableScanRuntimePredicate>() as u32 {
        return Err(RuntimePredicateCodecError::IncompatibleLayout);
    }
    Ok(Some(*predicate))
}

pub(super) fn decode_runtime_predicate(
    predicate: &TableScanRuntimePredicate,
) -> Result<RuntimePruningPredicate<'static>, RuntimePredicateCodecError> {
    match predicate
        .action()
        .ok_or(RuntimePredicateCodecError::UnknownUpdateAction)?
    {
        RuntimePredicateUpdateAction::Replace => {}
        RuntimePredicateUpdateAction::Clear => {
            return Err(RuntimePredicateCodecError::ExpectedReplacement);
        }
    }
    if predicate.data_len == 0 {
        return Err(RuntimePredicateCodecError::Truncated);
    }
    if predicate.data.is_null() {
        return Err(RuntimePredicateCodecError::NullData);
    }
    // SAFETY: `runtime_predicate_slot` validated the fixed-layout slot, and the
    // exact-build ABI requires its non-null data/count pair to remain live for
    // this serialized callback.
    let data =
        unsafe { core::slice::from_raw_parts(predicate.data, predicate.data_len) };
    RuntimePruningPredicate::decode(data)
}

/// Run-local provider stream consumed by the Arrow C Stream exporter.
pub trait TableScanStream: Send + 'static {
    type Error: SqlStateError;

    fn schema(&self) -> SchemaRef;

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error>;
}

/// Format-specific table-scan lifecycle implemented inside a provider DSO.
///
/// A bound scan is statement-owned and released exactly once after every
/// run-local task plan and stream has been dropped. It must not retain borrowed
/// PostgreSQL executor pointers, and its destructor must not panic. The
/// `Send + Sync` bounds satisfy DataFusion's plan traits; the serial engine
/// still polls and drops these values only on the owning backend thread.
pub trait TableScanProvider: Send + Sync + 'static {
    /// PostgreSQL storage objects routed to this callback implementation.
    const ROUTES: TableScanRoutes;

    type Filter: FilterPushdown;
    type Predicate: 'static;
    type ScanPlan: 'static;
    type BoundScan: Send + Sync + 'static;
    type PlannedTasks: Send + Sync + 'static;
    type SerialStream: TableScanStream<Error = Self::Error>;
    type Error: SqlStateError
        + From<PlanDataError>
        + From<ExpressionCodecError>
        + From<RuntimePredicateCodecError>
        + From<<Self::Filter as FilterPushdown>::Error>;

    fn plan_scan(
        &self,
        context: &ScanPlanningContext<'_>,
    ) -> Result<ScanSupport<PlannedScan<Self::ScanPlan>>, Self::Error>;

    fn encode_scan_plan(
        &self,
        plan: &Self::ScanPlan,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error>;

    fn decode_scan_plan(
        &self,
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self::ScanPlan, Self::Error>;

    fn bind_scan(
        &self,
        plan: &Self::ScanPlan,
    ) -> Result<Self::BoundScan, Self::Error>;

    /// Return the immutable Arrow schema established by binding without
    /// opening a run-local stream. Fields must correspond positionally to the
    /// statement projection. A run-local stream emits the exact subprojection
    /// requested through [`ScanTaskPlanningOptions::projection`]. This is a
    /// provider invariant; the query engine does not repeat
    /// PostgreSQL-to-Arrow type validation on the batch or row path.
    fn bound_schema(&self, bound: &Self::BoundScan) -> SchemaRef;

    /// Negotiate a concrete static or dynamic predicate against the immutable
    /// statement-bound schema and snapshot. This is the sole provider
    /// capability policy used by support probing and final task planning.
    fn plan_predicate(
        &self,
        bound: &Self::BoundScan,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    /// Plan physical tasks for one execution run. This is intentionally
    /// separate from [`Self::bind_scan`]: DataFusion needs the bound schema to
    /// optimize the plan before storage task planning begins.
    fn plan_scan_tasks(
        &self,
        bound: &Self::BoundScan,
        options: &ScanTaskPlanningOptions,
        static_predicates: &[&Self::Predicate],
        runtime_predicate: Option<&Self::Predicate>,
    ) -> Result<PlannedScanTasks<Self::PlannedTasks>, Self::Error>;

    fn open_serial_stream(
        &self,
        bound: &Self::BoundScan,
        planned: &Self::PlannedTasks,
        options: ScanStreamOptions,
    ) -> Result<Self::SerialStream, Self::Error>;
}
