use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::expr::ExpressionCodecError;
use lagodb_core::expr::pushdown::{
    CostedConjunct, FilterPlanningContext, FilterPushdown, PredicatePlan,
    PruningEstimate, RelationFilterEstimator,
};
use lagodb_core::handles::ExecutorHandle;
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{ScanCost, ScanId};
use lagodb_core::runtime_api::{
    RuntimePredicateCodecError, RuntimePredicateUpdateAction,
    RuntimePruningPredicate, TableScanPlanningRequest, TableScanRoutes,
    TableScanRuntimePredicate, TableScanStreamRequest, TableScanTaskMetrics,
    TableScanTaskPlanningRequest,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

/// Projection requested from a provider table scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanProjection<'a> {
    RowCount,
    Columns(&'a [pg_sys::AttrNumber]),
}

/// Borrowed provider-facing view of one table-scan planning request.
pub struct ScanPlanningContext<'a> {
    request: &'a TableScanPlanningRequest<'a>,
    projection: ScanProjection<'a>,
    /// Provider-approved pruning conjuncts, each with the costing the
    /// provider gives it alone, and the provider's pruning units.
    pruning_conjuncts: Vec<CostedConjunct>,
    pruning_units: Option<f64>,
}

impl<'a> ScanPlanningContext<'a> {
    pub(super) fn new(request: &'a TableScanPlanningRequest<'a>) -> Self {
        // A nonempty projection lists planner-validated user attnos (>= 1).
        let attnos = request.projected_attnos.as_slice();
        let projection = if attnos.is_empty() {
            ScanProjection::RowCount
        } else {
            ScanProjection::Columns(attnos)
        };
        Self {
            request,
            projection,
            pruning_conjuncts: Vec::new(),
            pruning_units: None,
        }
    }

    /// Record the provider-approved pruning conjuncts and pruning units that
    /// [`Self::pruning`] and [`Self::layout_fraction`] estimate.
    ///
    /// # Safety
    ///
    /// Every conjunct is a planner-owned qual of this scan's relation that
    /// outlives this planning callback.
    pub(super) unsafe fn set_pruning_conjuncts(
        &mut self,
        conjuncts: Vec<CostedConjunct>,
        units: Option<f64>,
    ) {
        self.pruning_conjuncts = conjuncts;
        self.pruning_units = units;
    }

    /// Share of the relation the costed pruning conjuncts leave to scan,
    /// layout and statistics pruning together, computed on each call.
    /// [`PruningEstimate::NONE`] means that no pruning may reduce physical
    /// work.
    pub fn pruning(&self) -> PruningEstimate {
        self.filter_estimator()
            .pruning(self.pruning_conjuncts.iter().copied(), self.pruning_units)
    }

    /// Share of the relation the layout-pruned conjuncts alone leave to scan,
    /// for a relation whose statistics do not follow its physical order. It
    /// looks up no column correlation.
    pub fn layout_fraction(&self) -> f64 {
        self.filter_estimator()
            .layout_selectivity(self.pruning_conjuncts.iter().copied())
    }

    fn filter_estimator(&self) -> RelationFilterEstimator {
        // SAFETY: the request carries the live planner state that owns this
        // scan's range-table entry, and `set_pruning_conjuncts` admits only
        // its planner-owned quals.
        unsafe {
            RelationFilterEstimator::new(
                self.request.root,
                self.request.range_table_index,
            )
        }
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
    pub const fn check_as_user_id(&self) -> pg_sys::Oid {
        self.request.check_as_user_id
    }

    #[inline]
    pub fn effective_user_id(&self) -> pg_sys::Oid {
        let check_as_user_id = self.check_as_user_id();
        if check_as_user_id == pg_sys::InvalidOid {
            // SAFETY: planning callbacks run in a PostgreSQL backend with an
            // established effective user.
            unsafe { pg_sys::GetUserId() }
        } else {
            check_as_user_id
        }
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
    pub fn relation_kind(&self) -> u8 {
        // SAFETY: request borrows PostgreSQL's live RangeTblEntry.
        unsafe { (*self.request.range_table_entry).relkind as u8 }
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

    /// Live PostgreSQL relation-planning state for this synchronous callback.
    #[inline]
    pub fn relation(&self) -> &pg_sys::RelOptInfo {
        // SAFETY: the request borrows the live relation supplied by the
        // PostgreSQL planning hook for the duration of this callback.
        unsafe { &*self.request.relation }
    }
}

/// Provider-owned scan plan, projected output schema, and physical cost facts.
/// The schema follows the requested attribute order and uses the same mapping
/// as `bound_schema`; producing it must not bind a scan or plan file tasks.
pub struct PlannedScan<P> {
    plan: P,
    cost: ScanCost,
    schema: SchemaRef,
}

impl<P> PlannedScan<P> {
    #[inline]
    #[must_use]
    pub const fn new(plan: P, cost: ScanCost, schema: SchemaRef) -> Self {
        Self { plan, cost, schema }
    }

    #[inline]
    pub fn into_parts(self) -> (P, ScanCost, SchemaRef) {
        (self.plan, self.cost, self.schema)
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
    exact_rows: Option<u64>,
}

/// Owned inputs for one run-local physical task-planning call.
pub struct ScanTaskPlanningOptions {
    projection: Box<[usize]>,
    request_exact_rows: bool,
}

impl ScanTaskPlanningOptions {
    pub(super) fn from_request(request: &TableScanTaskPlanningRequest<'_>) -> Self {
        Self {
            projection: request.projected_columns.as_slice().into(),
            request_exact_rows: request.request_exact_rows != 0,
        }
    }

    #[inline]
    pub fn projection(&self) -> &[usize] {
        &self.projection
    }

    /// Whether the caller needs exact output cardinality. A provider may still
    /// return no statistics when it cannot prove the count from its tasks.
    #[inline]
    pub const fn requests_exact_rows(&self) -> bool {
        self.request_exact_rows
    }
}

impl<P> PlannedScanTasks<P> {
    #[inline]
    #[must_use]
    pub const fn new(plan: P, metrics: TableScanTaskMetrics) -> Self {
        Self {
            plan,
            metrics,
            exact_rows: None,
        }
    }

    /// Exact output cardinality, including the effects of predicates and deletes.
    /// Providers must leave this unavailable when file metrics cannot prove it.
    #[must_use]
    pub const fn with_exact_rows(mut self, exact_rows: Option<u64>) -> Self {
        self.exact_rows = exact_rows;
        self
    }

    #[inline]
    pub fn into_parts(self) -> (P, TableScanTaskMetrics, Option<u64>) {
        (self.plan, self.metrics, self.exact_rows)
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
        if request.maximum_batch_rows == 0 {
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
        // SAFETY: a non-null evolving slot is engine-owned and stays live until
        // the Arrow stream release callback returns.
        let Some(&slot) = (unsafe { self.evolving_predicate.as_ref() }) else {
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

// SAFETY: the raw slot is borrowed from the current-thread engine and
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
    if predicate.data.is_null() {
        return Err(RuntimePredicateCodecError::NullData);
    }
    // SAFETY: the exact-build ABI requires the slot's non-null data/count pair
    // to remain live for this serialized callback.
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
/// `Send + Sync` bounds satisfy DataFusion's plan traits; the query engine
/// still polls and drops these values only on the owning backend thread.
pub trait TableScanProvider: Send + Sync + 'static {
    /// PostgreSQL storage objects routed to this callback implementation.
    const ROUTES: TableScanRoutes;

    type Filter: FilterPushdown;
    type Predicate: 'static;
    type ScanPlan: 'static;
    type BoundScan: Send + Sync + 'static;
    type PlannedTasks: Send + Sync + 'static;
    type Stream: TableScanStream<Error = Self::Error>;
    type Error: SqlStateError
        + From<PlanDataError>
        + From<ExpressionCodecError>
        + From<RuntimePredicateCodecError>
        + From<<Self::Filter as FilterPushdown>::Error>;

    /// Return whether this provider owns the supplied valid foreign server.
    /// The runtime calls this only during foreign-table path planning and only
    /// for providers that advertise a foreign-data-wrapper route. The server
    /// may belong to a different provider.
    fn owns_foreign_server(&self, _server_oid: pg_sys::Oid) -> bool {
        false
    }

    /// Construct the predicate planner for this table-scan planning attempt.
    /// Providers may override this to supply relation-scoped metadata while
    /// keeping the shared filter contract independent of storage adapters.
    fn begin_filter_planning(
        &self,
        _scan: &ScanPlanningContext<'_>,
        context: &FilterPlanningContext,
    ) -> Result<<Self::Filter as FilterPushdown>::Planner, Self::Error> {
        Self::Filter::begin_filter_planning(context).map_err(Into::into)
    }

    /// Plan the requested source projection and expose its Arrow representation.
    /// The query engine owns expression admission against that schema; storage
    /// predicate negotiation remains independent of exact query execution.
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

    /// Bind the complete source identity, schema and data visibility for this
    /// execution. Later task planning and rescans reuse this source; physical
    /// plan preparation may be deferred without refreshing its read view.
    fn bind_scan(
        &self,
        plan: &Self::ScanPlan,
        executor: &ExecutorHandle<'_>,
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

    /// Plan physical tasks separately from [`Self::bind_scan`], so binding a
    /// schema does not enumerate files. Metadata COUNT candidates request tasks
    /// before optimization; ordinary scans request them during physical source
    /// preparation or when opening their stream. Repeated requests with the
    /// same projection and predicates can reuse the bound source's task cache.
    fn plan_scan_tasks(
        &self,
        bound: &Self::BoundScan,
        options: &ScanTaskPlanningOptions,
        static_predicates: &[&Self::Predicate],
        runtime_predicate: Option<&Self::Predicate>,
    ) -> Result<PlannedScanTasks<Self::PlannedTasks>, Self::Error>;

    fn open_stream(
        &self,
        bound: &Self::BoundScan,
        planned: &Self::PlannedTasks,
        options: ScanStreamOptions,
    ) -> Result<Self::Stream, Self::Error>;
}
