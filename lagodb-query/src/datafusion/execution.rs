//! Current-thread DataFusion lifecycle for query offload.

mod resources;

use std::error::Error;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::DataFusionError;
use lagodb_arrow::{
    ArrowColumnDecoder, BoundBatch, ColumnRule, DatumCodec, DecodedColumn,
    PgColumnType, resolve_column_rule,
};
use lagodb_core::batch::BatchRowDecoder;
use lagodb_core::customscan::custom_exprs::PgExpressionSectionsError;
use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::expr::RuntimeValueStateError;
use lagodb_core::tuple::SlotColumns;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{PgMemoryContexts, pg_sys};

use crate::plan::{
    PlanExplainNode, PlannedTableScan, QueryFragment, QueryPlanData, QueryTupleLayout,
};

use super::metrics::{
    ExecutionMetrics, ExecutionMetricsSnapshot, QueryExecutionMode,
};
use super::plan_compiler::DataFusionPlanError;
use super::scan_callbacks::SerialTableScanCallbacks;
use super::{ParallelQueryOptions, QueryExecutionLimits};
use resources::QueryExecutionResources;

/// Whether LagoDB records counters and retains physical-plan metrics for
/// PostgreSQL instrumentation.
///
/// This switch does not control DataFusion 55's intrinsic operator metrics:
/// standard physical operators create their own timers and update them while
/// executing. `Disabled` guarantees that ordinary PostgreSQL execution adds no
/// LagoDB scan counters or physical-plan metric retention. EXPLAIN `TIMING OFF`
/// suppresses DataFusion duration metrics when the retained plan is rendered;
/// eliminating DataFusion's internal timer reads requires upstream engine
/// support rather than a LagoDB execution-mode flag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionMetricsMode {
    /// Do not collect LagoDB counters or retain physical-plan metrics.
    Disabled,
    /// Collect and retain metrics requested by PostgreSQL instrumentation.
    Enabled,
}

/// Arrow output columns bound once to the plan's PostgreSQL slot layout.
pub(super) struct QueryOutputDecoder {
    decoder: ArrowColumnDecoder,
    nullable: Box<[bool]>,
    width: usize,
    requires_datum_context: bool,
}

impl QueryOutputDecoder {
    fn try_new(
        layout: &QueryTupleLayout,
        schema: &SchemaRef,
    ) -> Result<Self, QueryExecutionError> {
        if schema.fields().len() != layout.len() {
            return Err(QueryExecutionError::InvalidQueryOutput {
                columns: schema.fields().len(),
                rows: 0,
            });
        }
        let mut columns = Vec::with_capacity(layout.len());
        let mut nullable = Vec::with_capacity(layout.len());
        for (position, (field, slot)) in
            schema.fields().iter().zip(layout.slots()).enumerate()
        {
            let pg_type =
                PgColumnType::from_pg_type(slot.type_oid()).ok_or_else(|| {
                    PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
                        format!(
                            "query output type {:?} has no Arrow conversion",
                            slot.type_oid()
                        ),
                    )
                })?;
            let (rule, codec) = match (slot.type_oid(), field.data_type()) {
                (pg_sys::FLOAT4OID, DataType::Float64) => {
                    (ColumnRule::F64, DatumCodec::float4_from_float64())
                }
                (pg_sys::NUMERICOID, DataType::Binary) => {
                    // SAFETY: only LagoDB's numeric UDAF emits this
                    // complete detoasted PostgreSQL NUMERIC varlena.
                    (ColumnRule::Binary, unsafe {
                        DatumCodec::postgres_numeric_varlena()
                    })
                }
                (pg_sys::NUMERICOID, DataType::Int64) => {
                    (ColumnRule::I64, DatumCodec::numeric_from_int64())
                }
                (pg_sys::NUMERICOID, DataType::Float64) => {
                    (ColumnRule::F64, DatumCodec::numeric_from_float64())
                }
                _ => (
                    resolve_column_rule(field.data_type(), pg_type)
                        .map_err(PgReportError::from_domain_error)?,
                    DatumCodec::standard(slot.type_oid())
                        .map_err(PgReportError::from_domain_error)?,
                ),
            };
            // SAFETY: the output layout is constructed from the CustomScan
            // target list that PostgreSQL uses to create the destination slot.
            // `position` is bounded by that validated layout for this decoder.
            columns.push(
                unsafe {
                    DecodedColumn::new(
                        rule,
                        position,
                        position,
                        slot.type_oid(),
                        codec,
                    )
                }
                .map_err(PgReportError::from_domain_error)?,
            );
            nullable.push(slot.nullable());
        }
        // Resolve this once with PostgreSQL's type metadata. By-value datums
        // cannot retain allocations in the current memory context, so the
        // row hot path only switches contexts when an output can be by-ref.
        let requires_datum_context = layout
            .slots()
            .iter()
            .any(|slot| unsafe { !pg_sys::get_typbyval(slot.type_oid()) });
        Ok(Self {
            decoder: ArrowColumnDecoder::new(columns),
            nullable: nullable.into_boxed_slice(),
            width: layout.len(),
            requires_datum_context,
        })
    }

    fn bind(&self, batch: RecordBatch) -> Result<BoundBatch, PgReportError> {
        self.decoder.bind(batch)
    }

    /// # Safety
    ///
    /// `slot` must be the scan slot built by PostgreSQL from the same target
    /// list that produced this decoder's query layout.
    unsafe fn write_row(
        &self,
        bound: &BoundBatch,
        row: usize,
        slot: *mut pg_sys::TupleTableSlot,
        datum_context: pg_sys::MemoryContext,
    ) -> Result<(), PgReportError> {
        let mut columns = unsafe { SlotColumns::new(slot, datum_context) };
        // SAFETY: decoder construction bound every destination to the
        // CustomScan output layout; batch iteration proves `row` exists.
        let mut write =
            || unsafe { self.decoder.write_row_unchecked(bound, row, &mut columns) };
        if self.requires_datum_context {
            unsafe { PgMemoryContexts::For(datum_context).switch_to(|_| write()) }
        } else {
            write()
        }
    }

    #[inline]
    const fn width(&self) -> usize {
        self.width
    }

    fn accepts_nulls(&self, batch: &RecordBatch) -> bool {
        batch
            .columns()
            .iter()
            .zip(self.nullable.iter())
            .all(|(column, nullable)| *nullable || column.null_count() == 0)
    }
}

/// Begin-owned query state with statement resources and at most one lazy run.
pub struct QueryExecution {
    output: QueryOutputDecoder,
    metrics: Option<Arc<ExecutionMetrics>>,
    resources: Option<QueryExecutionResources>,
    backend_thread: PhantomData<Rc<()>>,
}

/// Begin-time inputs selected by the PG host for one complete query fragment.
pub struct QueryExecutionRequest<'a> {
    pub query: QueryPlanData,
    pub scans: &'a [PlannedTableScan<'a>],
    pub limits: QueryExecutionLimits,
    pub metrics_mode: ExecutionMetricsMode,
    pub callbacks: &'a [SerialTableScanCallbacks],
    pub parallel: Option<ParallelQueryOptions>,
    pub runtime_exprs: *mut pg_sys::List,
    pub parent: *mut pg_sys::PlanState,
}

impl QueryExecution {
    pub fn try_new(
        request: QueryExecutionRequest<'_>,
    ) -> Result<Self, QueryExecutionError> {
        let metrics = (request.metrics_mode == ExecutionMetricsMode::Enabled)
            .then(|| Arc::new(ExecutionMetrics::new(request.scans.len())));
        let (resources, output) =
            QueryExecutionResources::prepare(request, metrics.as_ref())?;
        Ok(Self {
            output,
            metrics,
            resources: Some(resources),
            backend_thread: PhantomData,
        })
    }

    /// Write the next query result through the pre-bound Arrow decoder.
    ///
    /// # Safety
    ///
    /// `slot` must be the live scan slot created by PostgreSQL from this
    /// CustomScan's target list.
    pub unsafe fn next_into_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
        datum_context: pg_sys::MemoryContext,
    ) -> Result<bool, QueryExecutionError> {
        let resources = self
            .resources
            .as_mut()
            .expect("active query execution owns its resources");
        let produced =
            unsafe { resources.next_into_slot(&self.output, slot, datum_context) }?;
        Ok(produced)
    }

    /// # Safety
    ///
    /// `changed_parameters` is NULL or the live `PlanState::chgParam` bitmap
    /// supplied during PostgreSQL's `ExecReScan` callback.
    pub unsafe fn rescan(
        &mut self,
        changed_parameters: *mut pg_sys::Bitmapset,
    ) -> Result<(), QueryExecutionError> {
        unsafe {
            self.resources
                .as_mut()
                .expect("active query execution owns its resources")
                .rescan(changed_parameters)
        }
    }

    pub fn close(mut self) -> Result<(), QueryExecutionError> {
        self.resources
            .take()
            .expect("active query execution owns its resources")
            .close()
    }

    pub fn abort(mut self) {
        if let Some(resources) = self.resources.take() {
            resources.abort();
        }
    }

    pub fn metrics(&self) -> Option<ExecutionMetricsSnapshot> {
        self.metrics.as_ref().map(|metrics| {
            metrics.snapshot(
                self.resources
                    .as_ref()
                    .expect("active query execution owns its resources")
                    .peak_reserved(),
            )
        })
    }

    pub fn physical_plan_analyze(
        &self,
        include_timing: bool,
    ) -> Option<PlanExplainNode> {
        self.resources
            .as_ref()
            .expect("active query execution owns its resources")
            .physical_plan_analyze(include_timing)
    }

    pub fn physical_plan_explain(&self) -> PlanExplainNode {
        self.resources
            .as_ref()
            .expect("active query execution owns its resources")
            .physical_plan_explain()
    }

    pub fn planned_mode(&self) -> QueryExecutionMode {
        if self
            .resources
            .as_ref()
            .expect("active query execution owns its resources")
            .planned_parallel()
        {
            QueryExecutionMode::Parallel
        } else {
            QueryExecutionMode::Serial
        }
    }

    pub fn fragment(&self) -> &QueryFragment {
        self.resources
            .as_ref()
            .expect("active query execution owns its resources")
            .fragment()
    }
}

impl Drop for QueryExecution {
    fn drop(&mut self) {
        if let Some(resources) = self.resources.take() {
            let _ = resources.close();
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryExecutionError {
    #[error("query execution limits must all be non-zero")]
    InvalidLimits,
    #[error("failed to create current-thread query runtime: {0}")]
    Runtime(#[source] io::Error),
    #[error("DataFusion query execution failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("table scan binding failed: {0}")]
    ScanBind(#[source] PgReportError),
    #[error("failed to initialize query runtime values: {0}")]
    RuntimeValues(#[source] RuntimeValueStateError),
    #[error("invalid PostgreSQL expression sections: {0}")]
    ExpressionSections(#[source] PgExpressionSectionsError),
    #[error("selected query plan has {scans} scans but {callbacks} callback tables")]
    ScanCallbackCount { scans: usize, callbacks: usize },
    #[error("DataFusion query output has {columns} columns and {rows} rows")]
    InvalidQueryOutput { columns: usize, rows: usize },
    #[error("failed to convert the DataFusion result batch: {0}")]
    OutputConversion(#[from] PgReportError),
    #[error("bound table scan {scan} remained shared while closing execution")]
    BoundScanStillShared { scan: usize },
    #[error("query fragment is missing metadata for table scan {scan}")]
    MissingScanMetadata { scan: usize },
    #[error("table scan release failed: {0}")]
    ScanRelease(#[source] PgReportError),
    #[error("parallel query host failed: {0}")]
    ParallelHost(#[source] PgReportError),
    #[error("query initialization failed: {primary}; cleanup failure: {cleanup:?}")]
    Initialization {
        #[source]
        primary: Box<QueryExecutionError>,
        cleanup: Option<Box<QueryExecutionError>>,
    },
}

impl SqlStateError for QueryExecutionError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::DataFusion(error) => Self::datafusion_sqlstate(error),
            Self::ScanBind(error)
            | Self::ScanRelease(error)
            | Self::ParallelHost(error)
            | Self::OutputConversion(error) => error.sql_error_code(),
            Self::Initialization { primary, .. } => primary.sql_error_code(),
            Self::InvalidQueryOutput { .. } => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
            Self::InvalidLimits
            | Self::Runtime(_)
            | Self::RuntimeValues(_)
            | Self::ExpressionSections(_)
            | Self::ScanCallbackCount { .. }
            | Self::MissingScanMetadata { .. }
            | Self::BoundScanStillShared { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
        }
    }
}

impl From<DataFusionPlanError> for QueryExecutionError {
    fn from(error: DataFusionPlanError) -> Self {
        match error {
            DataFusionPlanError::DataFusion(error) => Self::DataFusion(error),
            DataFusionPlanError::MissingScan { index } => {
                Self::DataFusion(DataFusionError::Internal(format!(
                    "query fragment references missing table scan {index}"
                )))
            }
            error => Self::DataFusion(DataFusionError::Internal(error.to_string())),
        }
    }
}

impl QueryExecutionError {
    /// Convert at the query-offload boundary while preserving a provider's
    /// SQLSTATE, DETAIL, and HINT.
    pub fn into_report(self) -> PgReportError {
        match self {
            Self::DataFusion(error) => Self::datafusion_report(error),
            Self::ScanBind(error)
            | Self::OutputConversion(error)
            | Self::ParallelHost(error)
            | Self::ScanRelease(error) => error,
            Self::Initialization { primary, cleanup } => {
                let cleanup = cleanup.map(|error| {
                    format!("query initialization cleanup failed: {error}")
                });
                (*primary)
                    .into_report()
                    .contextualize("query initialization failed", cleanup)
            }
            error => PgReportError::from_domain_error(error),
        }
    }

    fn datafusion_sqlstate(error: &DataFusionError) -> PgSqlErrorCode {
        if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
            return PgSqlErrorCode::ERRCODE_OUT_OF_MEMORY;
        }
        Self::provider_error(error)
            .map_or(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, |error| {
                error.sql_error_code()
            })
    }

    fn datafusion_report(error: DataFusionError) -> PgReportError {
        if let Some(provider) = Self::provider_error(&error) {
            return PgReportError::from_parts(
                provider.sql_error_code(),
                provider.message(),
                provider.detail().map(str::to_owned),
                provider.hint().map(str::to_owned),
            );
        }
        PgReportError::from_domain_error(Self::DataFusion(error))
    }

    fn provider_error(error: &DataFusionError) -> Option<&PgReportError> {
        let mut current: Option<&(dyn Error + 'static)> = Some(error);
        while let Some(error) = current {
            if let Some(provider) = error.downcast_ref::<PgReportError>() {
                return Some(provider);
            }
            current = error.source();
        }
        None
    }
}
