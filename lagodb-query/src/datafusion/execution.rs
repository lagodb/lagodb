//! Current-thread DataFusion lifecycle for S1M scalar `COUNT(*)`.

mod resources;

use std::error::Error;
use std::ffi::CStr;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::PeakRecordingPool;
use lagodb_core::diag::PgReportError;
use pg_arrow_conv::{ArrowColumnDecoder, ColumnRule, DatumCodec, DecodedColumn};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::plan::{DecodedQuerySource, QueryPlanData};

use super::PhysicalPlanAuditError;
use super::SerialExecutionLimits;
use super::metrics::{ExecutionMetrics, ExecutionMetricsSnapshot};
use super::plan_compiler::DataFusionPlanError;
use super::source::ExternalSourceStatistics;
use super::source_ffi::SerialSourceCallbacks;
use resources::CountExecutionResources;

/// Begin-owned COUNT state with statement resources and at most one lazy run.
pub struct SerialCountExecution {
    query: QueryPlanData,
    output: ArrowColumnDecoder,
    metrics: Arc<ExecutionMetrics>,
    memory: Arc<PeakRecordingPool>,
    resources: Option<CountExecutionResources>,
    // The complete PG-sensitive lifecycle must remain on the backend thread
    // that constructed it. The marker has no runtime representation or cost.
    backend_thread: PhantomData<Rc<()>>,
}

impl SerialCountExecution {
    pub fn try_new(
        query: QueryPlanData,
        source: DecodedQuerySource<'_>,
        limits: SerialExecutionLimits,
        callbacks: SerialSourceCallbacks,
    ) -> Result<Self, QueryExecutionError> {
        let source_id = source.source();
        if query.fragment().scalar_count_source() != source_id {
            return Err(QueryExecutionError::SourceMismatch);
        }
        let output_slot = query.tuple_layout().slot();
        let output_codec = DatumCodec::standard(output_slot.type_oid())
            .map_err(PgReportError::from_domain_error)?;
        // SAFETY: QueryPlanData validation fixes the only destination at slot 0
        // with PostgreSQL INT8OID. CountAll produces Arrow Int64 at column 0,
        // and Begin validates the live scan slot against this same tuple layout.
        let output_column = unsafe {
            DecodedColumn::new(
                ColumnRule::I64,
                0,
                0,
                output_slot.type_oid(),
                output_codec,
            )
        }
        .map_err(PgReportError::from_domain_error)?;
        let metrics = Arc::new(ExecutionMetrics::default());
        let planned_estimate = source.estimate();
        let statistics = ExternalSourceStatistics {
            estimated_rows: estimate_to_usize(planned_estimate.estimated_rows()),
        };
        // SAFETY: `DecodedQuerySource` can only be produced by validating a live
        // executor-owned plan envelope and keeps its provider frame borrowed for
        // this call. `SerialSourceCallbacks` can only be constructed from the
        // matching validated backend-lifetime descriptor and is thread-bound.
        let prepared =
            unsafe { callbacks.prepare(source_id, source.provider_plan()) }
                .map_err(QueryExecutionError::SourcePrepare)?;
        let (resources, memory) = CountExecutionResources::prepare(
            query.fragment(),
            source_id,
            statistics,
            limits,
            prepared,
            &metrics,
        )?;
        Ok(Self {
            query,
            output: ArrowColumnDecoder::new(vec![output_column]),
            metrics,
            memory,
            resources: Some(resources),
            backend_thread: PhantomData,
        })
    }

    /// Write the single scalar result through the shared Arrow-to-PG converter.
    ///
    /// All input remains behind the Arrow batch boundary. The final batch is
    /// bound once, then its sole row is written directly into the scan slot.
    ///
    /// # Safety
    ///
    /// `slot` must be the live one-column scan slot validated against
    /// `self.query.tuple_layout()` during AggregateScan Begin.
    pub unsafe fn next_into_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
        datum_context: pg_sys::MemoryContext,
    ) -> Result<bool, QueryExecutionError> {
        let resources = self
            .resources
            .as_mut()
            .expect("active COUNT execution owns its resources");
        let produced = unsafe {
            resources.next_into_slot(
                self.query.tuple_layout().len(),
                &mut self.output,
                slot,
                datum_context,
            )
        }?;
        if produced {
            self.metrics.record_output_row();
        }
        Ok(produced)
    }

    pub fn rescan(&mut self) -> Result<(), QueryExecutionError> {
        self.resources
            .as_mut()
            .expect("active COUNT execution owns its resources")
            .rescan()
    }

    pub fn close(mut self) -> Result<(), QueryExecutionError> {
        self.resources
            .take()
            .expect("active COUNT execution owns its resources")
            .close()
    }

    #[inline]
    pub const fn query(&self) -> &QueryPlanData {
        &self.query
    }

    pub fn metrics(&self) -> ExecutionMetricsSnapshot {
        self.metrics.snapshot(self.memory.peak_reserved())
    }

    pub fn physical_operators(&self) -> &CStr {
        self.resources
            .as_ref()
            .expect("active COUNT execution owns its resources")
            .physical_operators()
    }
}

impl Drop for SerialCountExecution {
    fn drop(&mut self) {
        // Explicit close takes the sole resource owner before this Drop runs.
        // An unwind reaches this fallback with resources still present; the
        // provider contract requires release to be non-panicking.
        if let Some(resources) = self.resources.take() {
            let _ = resources.close();
        }
    }
}

fn estimate_to_usize(value: f64) -> usize {
    value.min(usize::MAX as f64) as usize
}

#[derive(Debug, thiserror::Error)]
pub enum QueryExecutionError {
    #[error("serial query execution limits must all be non-zero")]
    InvalidLimits,
    #[error("failed to create current-thread query runtime: {0}")]
    Runtime(#[source] std::io::Error),
    #[error("DataFusion query execution failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("query fragment and decoded source identities differ")]
    SourceMismatch,
    #[error("query source preparation failed: {0}")]
    SourcePrepare(#[source] PgReportError),
    #[error("DataFusion COUNT output has {columns} columns and {rows} rows")]
    InvalidCountOutput { columns: usize, rows: usize },
    #[error("DataFusion COUNT output did not contain a result row")]
    MissingCountOutput,
    #[error("failed to convert the DataFusion result batch: {0}")]
    OutputConversion(#[from] PgReportError),
    #[error(
        "prepared query source remained shared after closing execution resources"
    )]
    PreparedSourceStillShared,
    #[error("S1M physical plan is outside its memory audit: {0}")]
    PhysicalPlanAudit(#[from] PhysicalPlanAuditError),
    #[error("query source release failed: {0}")]
    SourceRelease(#[source] PgReportError),
    #[error("query initialization failed: {primary}; cleanup failure: {cleanup:?}")]
    Initialization {
        #[source]
        primary: Box<QueryExecutionError>,
        cleanup: Option<Box<QueryExecutionError>>,
    },
}

impl lagodb_core::diag::SqlStateError for QueryExecutionError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::DataFusion(error) => Self::datafusion_sqlstate(error),
            Self::SourcePrepare(error) => error.sql_error_code(),
            Self::SourceRelease(error) => error.sql_error_code(),
            Self::OutputConversion(error) => error.sql_error_code(),
            Self::Initialization { primary, .. } => primary.sql_error_code(),
            Self::InvalidCountOutput { .. } | Self::MissingCountOutput => {
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION
            }
            Self::InvalidLimits
            | Self::Runtime(_)
            | Self::SourceMismatch
            | Self::PreparedSourceStillShared
            | Self::PhysicalPlanAudit(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}

impl From<DataFusionPlanError> for QueryExecutionError {
    fn from(error: DataFusionPlanError) -> Self {
        match error {
            DataFusionPlanError::DataFusion(error) => Self::DataFusion(error),
            DataFusionPlanError::Audit(error) => Self::PhysicalPlanAudit(error),
        }
    }
}

impl QueryExecutionError {
    /// Convert at the AggregateScan boundary while preserving a provider's
    /// fixed-layout SQLSTATE, DETAIL, and HINT.
    pub fn into_report(self) -> PgReportError {
        match self {
            Self::DataFusion(error) => Self::datafusion_report(error),
            Self::SourcePrepare(error)
            | Self::OutputConversion(error)
            | Self::SourceRelease(error) => error,
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
