//! Typed provider adapter for the query-source descriptor and Arrow C Stream.

use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr;

use arrow_array::RecordBatch;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::SchemaRef;
use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{SourceEstimate, SourceId};
use lagodb_core::runtime_api::{
    FFI_OPERATION_OK, FfiErrorRecord, PlannedQuerySource, QUERY_SOURCE_FAILED,
    QUERY_SOURCE_NOT_OWNED, QUERY_SOURCE_PLANNED, QUERY_SOURCE_PROJECTION_COUNT_ROWS,
    QUERY_SOURCE_UNSUPPORTED, QuerySourceDescriptor, QuerySourcePlanningRequest,
    QuerySourcePrepareRequest, QuerySourceStreamRequest,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

mod ffi_stream;

/// Export a provider stream through LagoDB's sole Arrow C Stream boundary.
///
/// Providers normally reach this through [`QuerySourceAdapter`]. The explicit
/// API exists for provider adapters that need to compose their stream before
/// descriptor registration; they must still use this exporter so panic
/// containment, fixed-layout errors, and release ownership do not fork into a
/// second implementation.
///
/// # Safety
///
/// `error` must point to writable engine-owned storage that remains live until
/// the returned stream's release callback has completed. All callbacks must be
/// invoked serially on the PostgreSQL backend thread.
pub unsafe fn export_query_source_stream<S: QuerySourceStream>(
    stream: S,
    error: *mut FfiErrorRecord,
) -> FFI_ArrowArrayStream {
    unsafe { ffi_stream::export(stream, error) }
}

/// Source projection requested by the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceProjection {
    CountRows,
}

/// Borrowed, provider-facing view of one source planning request.
pub struct SourcePlanningContext<'a> {
    request: &'a QuerySourcePlanningRequest,
    projection: SourceProjection,
}

impl<'a> SourcePlanningContext<'a> {
    fn try_new(
        request: &'a QuerySourcePlanningRequest,
    ) -> Result<Self, PgReportError> {
        if request.struct_size != size_of::<QuerySourcePlanningRequest>() as u32 {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "query source received an incompatible planning request",
            ));
        }
        let projection = match request.projection_kind {
            QUERY_SOURCE_PROJECTION_COUNT_ROWS => SourceProjection::CountRows,
            _ => {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "query source received an unknown projection kind",
                ));
            }
        };
        Ok(Self {
            request,
            projection,
        })
    }

    #[inline]
    pub const fn projection(&self) -> SourceProjection {
        self.projection
    }

    #[inline]
    pub fn source(&self) -> SourceId {
        SourceId::from_index(self.request.source_index)
    }

    #[inline]
    pub fn relation_oid(&self) -> pg_sys::Oid {
        // SAFETY: `try_new` borrows the live planning request supplied by the
        // PostgreSQL path callback; its RangeTblEntry remains live for it.
        unsafe { (*self.request.range_table_entry).relid }
    }

    #[inline]
    pub fn access_method_oid(&self) -> pg_sys::Oid {
        // SAFETY: the relation OID comes from the live planner RangeTblEntry.
        unsafe { pg_sys::get_rel_relam(self.relation_oid()) }
    }

    #[inline]
    pub fn tablespace_oid(&self) -> pg_sys::Oid {
        // SAFETY: the relation OID comes from the live planner RangeTblEntry.
        unsafe { pg_sys::get_rel_tablespace(self.relation_oid()) }
    }

    #[inline]
    pub fn relation_rows(&self) -> f64 {
        // SAFETY: `relation` is the live RelOptInfo supplied by PostgreSQL for
        // the duration of this planning callback.
        unsafe { (*self.request.relation).tuples }
    }

    #[inline]
    pub fn relation_physical_bytes(&self) -> f64 {
        // SAFETY: same live RelOptInfo invariant as `relation_rows`.
        let pages = unsafe { (*self.request.relation).pages } as f64;
        pages * pg_sys::BLCKSZ as f64
    }
}

/// Provider-owned source plan paired with validated physical estimates.
pub struct PlannedSource<P> {
    plan: P,
    estimate: SourceEstimate,
}

impl<P> PlannedSource<P> {
    #[inline]
    #[must_use]
    pub const fn new(plan: P, estimate: SourceEstimate) -> Self {
        Self { plan, estimate }
    }

    #[inline]
    pub fn into_parts(self) -> (P, SourceEstimate) {
        (self.plan, self.estimate)
    }
}

/// Provider decision for one relation leaf.
pub enum SourceSupport<T> {
    NotOwned,
    Unsupported,
    Planned(T),
}

/// Immutable batch shape for opening a run-local stream.
#[derive(Debug, Clone, Copy)]
pub struct SourceStreamOptions {
    maximum_batch_rows: u64,
}

impl SourceStreamOptions {
    fn try_from_request(
        request: &QuerySourceStreamRequest,
    ) -> Result<Self, PgReportError> {
        if request.struct_size != size_of::<QuerySourceStreamRequest>() as u32
            || request.maximum_batch_rows == 0
            || request.stream_error.is_null()
        {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "query source received invalid stream limits",
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

/// Run-local provider stream consumed by the project Arrow exporter.
///
/// Keeping the domain error type until this boundary lets the exporter write
/// SQLSTATE, detail, and hint into the fixed-layout runtime error record before
/// Arrow's C interface reduces failures to an errno.
pub trait QuerySourceStream: Send + 'static {
    type Error: SqlStateError;

    fn schema(&self) -> SchemaRef;

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error>;
}

/// Format-specific source lifecycle implemented inside a provider DSO.
///
/// Prepared sources and streams are released exactly once by the adapter and
/// their destructors must not panic. FFI containment is the last-resort safety
/// boundary for a provider contract violation, not a normal cleanup mechanism.
pub trait QuerySourceProvider: Send + Sync + 'static {
    type SourcePlan: 'static;
    type PreparedSource: Send + Sync + 'static;
    type SerialStream: QuerySourceStream<Error = Self::Error>;
    type Error: SqlStateError + From<PlanDataError>;

    fn plan_source(
        &self,
        context: &SourcePlanningContext<'_>,
    ) -> Result<SourceSupport<PlannedSource<Self::SourcePlan>>, Self::Error>;

    fn encode_source_plan(
        &self,
        plan: &Self::SourcePlan,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error>;

    fn decode_source_plan(
        &self,
        source: SourceId,
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self::SourcePlan, Self::Error>;

    fn prepare_source(
        &self,
        plan: &Self::SourcePlan,
    ) -> Result<Self::PreparedSource, Self::Error>;

    fn open_serial_stream(
        &self,
        prepared: &Self::PreparedSource,
        options: SourceStreamOptions,
    ) -> Result<Self::SerialStream, Self::Error>;
}

/// Generates the complete C-compatible descriptor for one typed provider.
pub struct QuerySourceAdapter<P>(PhantomData<P>);

impl<P: QuerySourceProvider> QuerySourceAdapter<P> {
    fn descriptor(provider: &'static P) -> QuerySourceDescriptor {
        // SAFETY: every callback below is generated from the typed provider
        // contract, contains callback panics/errors, and keeps `provider` live
        // forever.
        unsafe {
            QuerySourceDescriptor::new(
                ptr::from_ref(provider).cast_mut().cast(),
                Self::plan_source,
                Self::prepare_source,
                Self::open_serial_stream,
                Self::release_prepared,
            )
        }
    }

    /// Register the provider's typed query-source facet.
    pub fn register(provider: &'static P) {
        // SAFETY: `descriptor` is exclusively generated from the typed adapter
        // contract and `provider` has backend-static lifetime.
        unsafe {
            lagodb_core::hooks::register_query_source(Self::descriptor(provider))
        }
    }

    unsafe fn provider(context: *mut c_void) -> &'static P {
        // SAFETY: `descriptor` stores the address of the backend-static typed
        // provider and raw registration is not used by this adapter.
        unsafe { &*context.cast::<P>() }
    }

    unsafe extern "C-unwind" fn plan_source(
        provider_context: *mut c_void,
        request: *const QuerySourcePlanningRequest,
        output: *mut PlannedQuerySource,
        error: *mut FfiErrorRecord,
    ) -> u32 {
        let mut outcome = QUERY_SOURCE_NOT_OWNED;
        let operation = || {
            // SAFETY: the request remains live for the synchronous descriptor
            // callback.
            let planning = SourcePlanningContext::try_new(unsafe { &*request })?;
            // SAFETY: this adapter installed the backend-static context.
            let provider = unsafe { Self::provider(provider_context) };
            match provider
                .plan_source(&planning)
                .map_err(PgReportError::from_domain_error)?
            {
                SourceSupport::NotOwned => outcome = QUERY_SOURCE_NOT_OWNED,
                SourceSupport::Unsupported => outcome = QUERY_SOURCE_UNSUPPORTED,
                SourceSupport::Planned(planned) => {
                    let (plan, estimate) = planned.into_parts();
                    let plan_data = PlanDataWriter::encode_list(|writer| {
                        provider.encode_source_plan(&plan, writer)
                    })
                    .map_err(PgReportError::from_domain_error)?;
                    // SAFETY: `output` is runtime-owned writable storage for
                    // this synchronous callback.
                    unsafe {
                        *output = PlannedQuerySource {
                            struct_size: size_of::<PlannedQuerySource>() as u32,
                            plan_data,
                            estimated_rows: estimate.estimated_rows(),
                            estimated_scan_bytes: estimate.estimated_scan_bytes(),
                        };
                    }
                    outcome = QUERY_SOURCE_PLANNED;
                }
            }
            Ok(())
        };
        // SAFETY: the runtime supplies live request/output/error storage and
        // invokes this adapter on the PostgreSQL backend thread.
        let status = unsafe { (&mut *error).capture(operation) };
        if status == FFI_OPERATION_OK {
            outcome
        } else {
            QUERY_SOURCE_FAILED
        }
    }

    unsafe extern "C-unwind" fn prepare_source(
        context: *mut c_void,
        request: *const QuerySourcePrepareRequest,
        prepared: *mut *mut c_void,
        error: *mut FfiErrorRecord,
    ) -> u32 {
        let operation = || {
            // SAFETY: request storage remains live for this call.
            let request = unsafe { &*request };
            if request.struct_size != size_of::<QuerySourcePrepareRequest>() as u32 {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "query source received an incompatible prepare request",
                ));
            }
            // SAFETY: this adapter installed the backend-static context.
            let provider = unsafe { Self::provider(context) };
            let source = SourceId::from_index(request.source_index);
            // SAFETY: plan_data is the live checked List supplied by the
            // runtime's executor context.
            let plan = unsafe {
                PlanDataReader::decode_checked_list(request.plan_data, 0, |reader| {
                    provider.decode_source_plan(source, reader)
                })
            }
            .map_err(PgReportError::from_domain_error)?;
            let source = provider
                .prepare_source(&plan)
                .map_err(PgReportError::from_domain_error)?;
            // SAFETY: the runtime supplies writable opaque-handle storage and
            // assumes ownership through `release_prepared`.
            unsafe { *prepared = Box::into_raw(Box::new(source)).cast() };
            Ok(())
        };
        // SAFETY: the runtime supplies live request/output/error storage and
        // invokes this adapter on the PostgreSQL backend thread.
        unsafe { (&mut *error).capture(operation) }
    }

    unsafe extern "C-unwind" fn open_serial_stream(
        context: *mut c_void,
        prepared: *mut c_void,
        request: *const QuerySourceStreamRequest,
        output: *mut c_void,
        error: *mut FfiErrorRecord,
    ) -> u32 {
        let operation = || {
            // SAFETY: this adapter installed the backend-static context.
            let provider = unsafe { Self::provider(context) };
            // SAFETY: the pointer was created by `prepare_source` for P.
            let source = unsafe { &*prepared.cast::<P::PreparedSource>() };
            // SAFETY: request storage remains live for this call and its stream
            // error slot is validated before export.
            let request = unsafe { &*request };
            let options = SourceStreamOptions::try_from_request(request)?;
            let reader = provider
                .open_serial_stream(source, options)
                .map_err(PgReportError::from_domain_error)?;
            // SAFETY: output is caller-owned Arrow stream storage and the
            // engine keeps `stream_error` live until release.
            unsafe {
                output
                    .cast::<FFI_ArrowArrayStream>()
                    .write(export_query_source_stream(reader, request.stream_error));
            }
            Ok(())
        };
        // SAFETY: the runtime supplies the matching prepared handle and live
        // request/output/error storage on the backend thread.
        unsafe { (&mut *error).capture(operation) }
    }

    unsafe extern "C-unwind" fn release_prepared(
        _context: *mut c_void,
        prepared: *mut c_void,
        error: *mut FfiErrorRecord,
    ) -> u32 {
        // SAFETY: the runtime supplies the unique handle returned by this
        // adapter and writable error storage on the backend thread.
        // Reconstruct ownership before entering the protected operation so
        // every contained failure still drops it while unwinding through
        // `PgTryBuilder`.
        let prepared = unsafe { Box::from_raw(prepared.cast::<P::PreparedSource>()) };
        unsafe {
            (&mut *error).capture(move || {
                drop(prepared);
                Ok(())
            })
        }
    }
}
