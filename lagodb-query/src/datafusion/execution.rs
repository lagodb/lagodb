//! Current-thread DataFusion lifecycle for serial aggregate queries.

mod resources;

use std::error::Error;
use std::ffi::CStr;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::PeakRecordingPool;
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

use crate::plan::{PlannedTableScan, QueryPlanData, QueryTupleLayout};

use super::SerialExecutionLimits;
use super::metrics::{ExecutionMetrics, ExecutionMetricsSnapshot};
use super::plan_compiler::DataFusionPlanError;
use super::scan_callbacks::SerialTableScanCallbacks;
use resources::QueryExecutionResources;

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
pub struct SerialQueryExecution {
    output: QueryOutputDecoder,
    output_rows: u64,
    metrics: Arc<ExecutionMetrics>,
    memory: Arc<PeakRecordingPool>,
    resources: Option<QueryExecutionResources>,
    backend_thread: PhantomData<Rc<()>>,
}

impl SerialQueryExecution {
    pub fn try_new(
        query: QueryPlanData,
        scans: &[PlannedTableScan<'_>],
        limits: SerialExecutionLimits,
        callbacks: &[SerialTableScanCallbacks],
        runtime_exprs: *mut pg_sys::List,
        parent: *mut pg_sys::PlanState,
    ) -> Result<Self, QueryExecutionError> {
        let metrics = Arc::new(ExecutionMetrics::default());
        let (resources, memory, output) = QueryExecutionResources::prepare(
            query,
            scans,
            callbacks,
            limits,
            &metrics,
            runtime_exprs,
            parent,
        )?;
        Ok(Self {
            output,
            output_rows: 0,
            metrics,
            memory,
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
        self.output_rows += u64::from(produced);
        Ok(produced)
    }

    pub fn rescan(&mut self) -> Result<(), QueryExecutionError> {
        self.resources
            .as_mut()
            .expect("active query execution owns its resources")
            .rescan()
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

    pub fn metrics(&self) -> ExecutionMetricsSnapshot {
        self.metrics
            .snapshot(self.memory.peak_reserved(), self.output_rows)
    }

    pub fn physical_operators(&self) -> &CStr {
        self.resources
            .as_ref()
            .expect("active query execution owns its resources")
            .physical_operators()
    }
}

impl Drop for SerialQueryExecution {
    fn drop(&mut self) {
        if let Some(resources) = self.resources.take() {
            let _ = resources.close();
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryExecutionError {
    #[error("serial query execution limits must all be non-zero")]
    InvalidLimits,
    #[error("failed to create current-thread query runtime: {0}")]
    Runtime(#[source] io::Error),
    #[error("DataFusion query execution failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("table scan preparation failed: {0}")]
    ScanPrepare(#[source] PgReportError),
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
    #[error("prepared table scan {scan} remained shared while closing execution")]
    PreparedScanStillShared { scan: usize },
    #[error("query fragment is missing metadata for table scan {scan}")]
    MissingScanMetadata { scan: usize },
    #[error("table scan release failed: {0}")]
    ScanRelease(#[source] PgReportError),
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
            Self::ScanPrepare(error)
            | Self::ScanRelease(error)
            | Self::OutputConversion(error) => error.sql_error_code(),
            Self::Initialization { primary, .. } => primary.sql_error_code(),
            Self::InvalidQueryOutput { .. } => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
            Self::InvalidLimits
            | Self::Runtime(_)
            | Self::RuntimeValues(_)
            | Self::ExpressionSections(_)
            | Self::ScanCallbackCount { .. }
            | Self::MissingScanMetadata { .. }
            | Self::PreparedScanStillShared { .. } => {
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
            Self::ScanPrepare(error)
            | Self::OutputConversion(error)
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
