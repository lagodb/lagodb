use std::mem::size_of;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::expr::ExpressionCodecError;
use lagodb_core::expr::pushdown::FilterPushdown;
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{ScanEstimate, ScanId};
use lagodb_core::runtime_api::{
    TABLE_SCAN_PROJECTION_COLUMNS, TABLE_SCAN_PROJECTION_ROW_COUNT,
    TableScanPlanningRequest, TableScanStreamRequest,
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
        })
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
    pub fn access_method_oid(&self) -> pg_sys::Oid {
        // SAFETY: relation_oid comes from the live planner RangeTblEntry.
        unsafe { pg_sys::get_rel_relam(self.relation_oid()) }
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

/// Provider-owned scan plan paired with validated physical estimates.
pub struct PlannedScan<P> {
    plan: P,
    estimate: ScanEstimate,
}

impl<P> PlannedScan<P> {
    #[inline]
    #[must_use]
    pub const fn new(plan: P, estimate: ScanEstimate) -> Self {
        Self { plan, estimate }
    }

    #[inline]
    pub fn into_parts(self) -> (P, ScanEstimate) {
        (self.plan, self.estimate)
    }
}

/// Provider decision for one relation scan.
pub enum ScanSupport<T> {
    NotOwned,
    Unsupported,
    Planned(T),
}

/// Immutable batch shape for opening a run-local stream.
#[derive(Debug, Clone, Copy)]
pub struct ScanStreamOptions {
    maximum_batch_rows: u64,
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
        })
    }

    #[inline]
    pub const fn maximum_batch_rows(self) -> u64 {
        self.maximum_batch_rows
    }
}

/// Run-local provider stream consumed by the Arrow C Stream exporter.
pub trait TableScanStream: Send + 'static {
    type Error: SqlStateError;

    fn schema(&self) -> SchemaRef;

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error>;
}

/// Format-specific table-scan lifecycle implemented inside a provider DSO.
///
/// A prepared scan is statement-owned and released exactly once after every
/// run-local stream has been dropped. It must not retain borrowed PostgreSQL
/// executor pointers, and its destructor must not panic. The `Send + Sync`
/// bounds satisfy DataFusion's plan traits; the serial engine still
/// polls and drops these values only on the owning backend thread.
pub trait TableScanProvider: Send + Sync + 'static {
    type Filter: FilterPushdown;
    type ScanPlan: 'static;
    type PreparedScan: Send + Sync + 'static;
    type SerialStream: TableScanStream<Error = Self::Error>;
    type Error: SqlStateError
        + From<PlanDataError>
        + From<ExpressionCodecError>
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
        scan: ScanId,
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self::ScanPlan, Self::Error>;

    fn prepare_scan(
        &self,
        plan: &Self::ScanPlan,
        predicate: Option<&<Self::Filter as FilterPushdown>::BoundPredicate>,
    ) -> Result<Self::PreparedScan, Self::Error>;

    /// Return the immutable Arrow schema established by preparation without
    /// opening a run-local stream.
    fn prepared_schema(&self, prepared: &Self::PreparedScan) -> SchemaRef;

    fn open_serial_stream(
        &self,
        prepared: &Self::PreparedScan,
        options: ScanStreamOptions,
    ) -> Result<Self::SerialStream, Self::Error>;
}
