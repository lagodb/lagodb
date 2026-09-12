//! Engine-side ownership of provider table-scan callbacks and lifecycle values.

mod bound_scan;
mod stream_reader;

use std::ffi::c_void;
use std::marker::PhantomData;
use std::rc::Rc;

use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{
    BindTableScan, CALLBACK_FAILED, CALLBACK_OK, CallbackErrorReport,
    GetBoundTableScanSchema, NegotiateTableScanPredicate, OpenTableScanStream,
    PlanTableScanTasks, ReleaseBoundTableScan, ReleasePlannedTableScan,
    ReleaseTableScanPredicate, TableScanDescriptor,
};
use pgrx::prelude::PgSqlErrorCode;

pub(super) use bound_scan::{BoundTableScanHandle, NegotiatedTableScanPredicate};
pub(super) use stream_reader::ProviderStreamReader;

/// Backend-thread-bound callbacks for one validated provider table scan.
///
/// This wrapper can cross the `lagodb-base`/`lagodb-query` crate boundary, but
/// cannot cross a thread boundary. It exposes no opaque handle or stream;
/// [`SerialQueryExecution`](super::SerialQueryExecution) consumes it while
/// constructing the sole PostgreSQL-owned execution lifecycle.
#[derive(Clone, Copy)]
pub struct SerialTableScanCallbacks {
    context: *mut c_void,
    bind_scan: BindTableScan,
    get_bound_schema: GetBoundTableScanSchema,
    negotiate_predicate: NegotiateTableScanPredicate,
    plan_scan_tasks: PlanTableScanTasks,
    open_serial_stream: OpenTableScanStream,
    release_predicate: ReleaseTableScanPredicate,
    release_planned: ReleasePlannedTableScan,
    release_bound: ReleaseBoundTableScan,
    backend_thread: PhantomData<Rc<()>>,
}

impl SerialTableScanCallbacks {
    /// Construct from a descriptor already validated by the runtime directory.
    ///
    /// # Safety
    ///
    /// The callback code and context must remain live for the backend lifetime.
    /// Callbacks must either originate from the typed provider adapter or from
    /// an unsafe raw registration that upholds the identical contract. The
    /// returned wrapper must be created, consumed, and dropped on the current
    /// PostgreSQL backend thread.
    pub unsafe fn from_validated_descriptor(
        descriptor: &TableScanDescriptor,
    ) -> Option<Self> {
        Some(Self {
            context: descriptor.context(),
            bind_scan: descriptor.bind_scan()?,
            get_bound_schema: descriptor.get_bound_schema()?,
            negotiate_predicate: descriptor.negotiate_predicate()?,
            plan_scan_tasks: descriptor.plan_scan_tasks()?,
            open_serial_stream: descriptor.open_serial_stream()?,
            release_predicate: descriptor.release_predicate()?,
            release_planned: descriptor.release_planned()?,
            release_bound: descriptor.release_bound()?,
            backend_thread: PhantomData,
        })
    }

    fn operation_result(
        self,
        status: u32,
        error: &CallbackErrorReport,
        operation: &'static str,
    ) -> Result<(), PgReportError> {
        match status {
            CALLBACK_OK => Ok(()),
            CALLBACK_FAILED => {
                // SAFETY: callbacks allocate error text in the active backend
                // context and this method consumes it synchronously.
                Err(unsafe { error.to_error(operation) })
            }
            status => Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("{operation} returned unknown status {status}"),
            )),
        }
    }
}
