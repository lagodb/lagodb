//! Statement binding and QueryRun task-plan ownership.

mod stream;

use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::fmt;
use std::mem::size_of;
use std::ptr::{self, NonNull};
use std::sync::{Arc, Mutex};

use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{Schema, SchemaRef};
use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{
    BoundTableScanResult, CallbackErrorReport, PlannedTableScanTasks,
    PredicateSupport, RuntimePruningPredicate, TableScanBindRequest,
    TableScanPredicateResult, TableScanRuntimePredicate, TableScanTaskMetrics,
    TableScanTaskPlanningRequest,
};
use pgrx::{pg_sys, prelude::PgSqlErrorCode};

use super::TableScanCallbacks;

pub(super) struct StreamErrorSlot(UnsafeCell<CallbackErrorReport>);

impl StreamErrorSlot {
    pub(super) fn new() -> Self {
        Self(UnsafeCell::new(CallbackErrorReport::default()))
    }

    pub(super) fn as_mut_ptr(&self) -> *mut CallbackErrorReport {
        self.0.get()
    }

    fn is_set(&self) -> bool {
        // SAFETY: Arrow callbacks and engine inspection are serialized on the
        // PostgreSQL backend main thread.
        unsafe { (*self.0.get()).is_set() }
    }

    pub(super) fn take_error(
        &self,
        operation: &'static str,
    ) -> Option<PgReportError> {
        if !self.is_set() {
            return None;
        }
        // SAFETY: the exporter wrote this record synchronously in the
        // still-live executor memory context, and callback/consumer access is
        // serialized on the backend main thread.
        let error = unsafe { (*self.0.get()).to_error(operation) };
        // SAFETY: the same serialized access permits clearing the consumed
        // record before the next callback.
        unsafe { *self.0.get() = CallbackErrorReport::default() };
        Some(error)
    }
}

// SAFETY: the slot exists only to satisfy DataFusion's `Send` stream contract.
// LagoDB invokes its exporter and reads the record on one backend main thread.
unsafe impl Send for StreamErrorSlot {}
// SAFETY: each Arrow stream is polled on one current-thread runtime, so its
// callback writes and reader access never overlap. The reader retains this
// slot through stream release.
unsafe impl Sync for StreamErrorSlot {}

impl TableScanCallbacks {
    /// Bind an immutable scan handle on the PostgreSQL backend thread.
    ///
    /// # Safety
    ///
    /// `plan_data` must be a live, read-only provider plan frame in the active
    /// executor memory context.
    pub(in crate::datafusion) unsafe fn bind(
        self,
        plan_data: *const pg_sys::List,
    ) -> Result<BoundTableScanHandle, PgReportError> {
        let request = TableScanBindRequest::new(plan_data);
        let mut output = BoundTableScanResult::default();
        let mut error = CallbackErrorReport::default();
        // SAFETY: this method's contract supplies live plan data; registration
        // guarantees backend-live context/callback pointers and the stack
        // outputs remain writable for the synchronous call.
        let status = unsafe {
            (self.bind_scan)(self.context, &request, &mut output, &mut error)
        };
        self.operation_result(status, &error, "table scan bind")?;
        if output.struct_size != size_of::<BoundTableScanResult>() as u32 {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan bind returned an incompatible result",
            ));
        }
        let handle = NonNull::new(output.bound).ok_or_else(|| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan bind returned a null handle",
            )
        })?;
        Ok(BoundTableScanHandle {
            callbacks: self,
            handle: Some(handle),
            active_plan: Mutex::new(None),
        })
    }
}

/// Statement-owned opaque scan binding.
pub(in crate::datafusion) struct BoundTableScanHandle {
    callbacks: TableScanCallbacks,
    handle: Option<NonNull<c_void>>,
    active_plan: Mutex<Option<Arc<PlannedTableScanHandle>>>,
}

// SAFETY: the typed `TableScanAdapter` proves the opaque handle/callback type
// pairing and the concrete provider bounds before the DSO boundary; raw
// registration must uphold the same pairing and backend-lifetime contract.
// The current-thread runtime polls and drops every run-local stream before
// releasing the handle on its owning PostgreSQL backend thread.
unsafe impl Send for BoundTableScanHandle {}
// SAFETY: DataFusion may share the private handle inside its plan, but the
// thread-bound owner and current-thread runtime serialize every callback. The
// handle also rejects opening a second stream while the first remains live.
unsafe impl Sync for BoundTableScanHandle {}

/// Provider-owned immutable predicate planned against one bound scan.
pub(in crate::datafusion) struct NegotiatedTableScanPredicate {
    callbacks: TableScanCallbacks,
    handle: Option<NonNull<c_void>>,
    support: PredicateSupport,
}

// SAFETY: the typed provider requires its predicate artifact to be `'static`;
// the current-thread runtime only borrows the opaque pointer in serialized
// callbacks and releases it on the owning backend thread.
unsafe impl Send for NegotiatedTableScanPredicate {}
// SAFETY: sharing only permits immutable task-planning borrows. Callback
// execution remains serialized by the owning query runtime.
unsafe impl Sync for NegotiatedTableScanPredicate {}

impl NegotiatedTableScanPredicate {
    pub(in crate::datafusion) const fn support(&self) -> PredicateSupport {
        self.support
    }

    pub(in crate::datafusion) fn as_ptr(&self) -> *const c_void {
        self.handle
            .expect("negotiated table-scan predicate is open")
            .as_ptr()
    }

    fn release(&mut self) -> Result<(), PgReportError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        let mut error = CallbackErrorReport::default();
        let status = unsafe {
            (self.callbacks.release_predicate)(
                self.callbacks.context,
                handle.as_ptr(),
                &mut error,
            )
        };
        self.callbacks.operation_result(
            status,
            &error,
            "table scan predicate release",
        )
    }

    pub(in crate::datafusion) fn close(mut self) -> Result<(), PgReportError> {
        self.release()
    }
}

impl Drop for NegotiatedTableScanPredicate {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

impl fmt::Debug for NegotiatedTableScanPredicate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NegotiatedTableScanPredicate")
            .field("is_open", &self.handle.is_some())
            .field("support", &self.support)
            .finish()
    }
}

impl BoundTableScanHandle {
    pub(super) fn as_ptr(&self) -> *mut c_void {
        self.handle.expect("bound table scan is open").as_ptr()
    }

    pub(in crate::datafusion) fn schema(&self) -> Result<SchemaRef, PgReportError> {
        let mut schema = FFI_ArrowSchema::empty();
        let mut error = CallbackErrorReport::default();
        let status = unsafe {
            (self.callbacks.get_bound_schema)(
                self.callbacks.context,
                self.handle.expect("bound table scan is open").as_ptr(),
                (&mut schema as *mut FFI_ArrowSchema).cast(),
                &mut error,
            )
        };
        self.callbacks
            .operation_result(status, &error, "table scan schema")?;
        Schema::try_from(&schema).map(Arc::new).map_err(|error| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("table scan returned an invalid Arrow schema: {error}"),
            )
        })
    }

    /// Finish the previously dropped stream and surface any error its
    /// Arrow release callback recorded before another run is started.
    pub(in crate::datafusion) fn finish_run(&self) -> Result<(), PgReportError> {
        let mut active_plan = self.active_plan.lock().map_err(|_| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan active-plan state was poisoned",
            )
        })?;
        let Some(active) = active_plan.as_ref() else {
            return Ok(());
        };
        if Arc::strong_count(active) != 1 {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "planned table scan remained shared after its stream was dropped",
            ));
        }
        let active = active_plan
            .take()
            .expect("exclusive planned scan remains installed");
        let Ok(active) = Arc::try_unwrap(active) else {
            unreachable!("planned scan strong count was checked while locked")
        };
        active.close()
    }

    pub(in crate::datafusion) fn plan_tasks(
        &self,
        projection: &[usize],
        static_predicates: &[*const c_void],
        fixed_predicate: *const c_void,
    ) -> Result<(PlannedTableScanHandle, TableScanTaskMetrics), PgReportError> {
        let request = TableScanTaskPlanningRequest::new(
            projection,
            static_predicates,
            fixed_predicate,
        );
        let mut output = PlannedTableScanTasks::default();
        let mut error = CallbackErrorReport::default();
        let status = unsafe {
            (self.callbacks.plan_scan_tasks)(
                self.callbacks.context,
                self.handle.expect("bound table scan is open").as_ptr(),
                &request,
                &mut output,
                &mut error,
            )
        };
        self.callbacks.operation_result(
            status,
            &error,
            "table scan task planning",
        )?;
        if output.struct_size != size_of::<PlannedTableScanTasks>() as u32 {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan task planning returned an incompatible result",
            ));
        }
        let handle = NonNull::new(output.planned).ok_or_else(|| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan task planning returned a null handle",
            )
        })?;
        Ok((
            PlannedTableScanHandle {
                callbacks: self.callbacks,
                handle: Some(handle),
                stream_error: Arc::new(StreamErrorSlot::new()),
            },
            output.metrics,
        ))
    }

    pub(in crate::datafusion) fn negotiate_predicate(
        &self,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> Result<Option<NegotiatedTableScanPredicate>, PgReportError> {
        let encoded = predicate.encode();
        let input = TableScanRuntimePredicate::replacement(0, &encoded);
        let mut output = TableScanPredicateResult::default();
        let mut error = CallbackErrorReport::default();
        let status = unsafe {
            (self.callbacks.negotiate_predicate)(
                self.callbacks.context,
                self.handle.expect("bound table scan is open").as_ptr(),
                ptr::from_ref(&input),
                &mut output,
                &mut error,
            )
        };
        self.callbacks.operation_result(
            status,
            &error,
            "table scan predicate negotiation",
        )?;
        if output.struct_size != size_of::<TableScanPredicateResult>() as u32 {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan predicate negotiation returned an incompatible result",
            ));
        }
        let support = PredicateSupport::from_code(output.support);
        let handle = NonNull::new(output.predicate);
        match (support, handle) {
            (None, None) if output.support == 0 => Ok(None),
            (Some(support), Some(handle)) => Ok(Some(NegotiatedTableScanPredicate {
                callbacks: self.callbacks,
                handle: Some(handle),
                support,
            })),
            (_, Some(handle)) => {
                let cleanup = NegotiatedTableScanPredicate {
                    callbacks: self.callbacks,
                    handle: Some(handle),
                    support: PredicateSupport::Conservative,
                }
                .close()
                .err();
                let primary = PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "table scan predicate negotiation returned an invalid result",
                );
                Err(match cleanup {
                    Some(cleanup) => primary.contextualize(
                        "table scan predicate negotiation failed",
                        Some(format!("predicate cleanup also failed: {cleanup}")),
                    ),
                    None => primary,
                })
            }
            _ => Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan predicate negotiation returned an invalid result",
            )),
        }
    }

    pub(in crate::datafusion) fn close(mut self) -> Result<(), PgReportError> {
        self.release()
    }

    fn release(&mut self) -> Result<(), PgReportError> {
        let Some(handle) = self.handle else {
            return Ok(());
        };
        let run_error = self.finish_run().err();
        if self
            .active_plan
            .lock()
            .map_or(true, |active| active.is_some())
        {
            return Err(run_error.expect("an active run prevented its cleanup"));
        }
        self.handle = None;
        let mut error = CallbackErrorReport::default();
        // SAFETY: `handle` was produced by this registered descriptor and is
        // consumed exactly once; the stack error record is writable.
        let status = unsafe {
            (self.callbacks.release_bound)(
                self.callbacks.context,
                handle.as_ptr(),
                &mut error,
            )
        };
        let bound_result = self.callbacks.operation_result(
            status,
            &error,
            "bound table scan release",
        );
        match (run_error, bound_result) {
            (Some(run), Err(bound)) => Err(run.contextualize(
                "table scan run cleanup failed",
                Some(format!("bound scan release also failed: {bound}")),
            )),
            (Some(error), Ok(())) | (None, Err(error)) => Err(error),
            (None, Ok(())) => Ok(()),
        }
    }
}

pub(in crate::datafusion) struct PlannedTableScanHandle {
    callbacks: TableScanCallbacks,
    handle: Option<NonNull<c_void>>,
    pub(super) stream_error: Arc<StreamErrorSlot>,
}

impl PlannedTableScanHandle {
    pub(super) fn as_ptr(&self) -> *mut c_void {
        self.handle.expect("planned table scan is open").as_ptr()
    }
}

// SAFETY: the same exact-build type pairing and current-thread execution proof
// as `BoundTableScanHandle` applies to this run-local provider handle.
unsafe impl Send for PlannedTableScanHandle {}
// SAFETY: the handle is shared only between one DataFusion stream and the
// statement owner's active-plan slot; callback access remains serialized.
unsafe impl Sync for PlannedTableScanHandle {}

impl PlannedTableScanHandle {
    pub(in crate::datafusion) fn close(mut self) -> Result<(), PgReportError> {
        let stream_error = self.stream_error.take_error("table scan stream release");
        let Some(handle) = self.handle.take() else {
            return stream_error.map_or(Ok(()), Err);
        };
        let mut error = CallbackErrorReport::default();
        let status = unsafe {
            (self.callbacks.release_planned)(
                self.callbacks.context,
                handle.as_ptr(),
                &mut error,
            )
        };
        let release = self.callbacks.operation_result(
            status,
            &error,
            "planned table scan release",
        );
        match (stream_error, release) {
            (Some(stream), Err(planned)) => Err(stream.contextualize(
                "table scan stream release failed",
                Some(format!("planned task release also failed: {planned}")),
            )),
            (Some(error), Ok(())) | (None, Err(error)) => Err(error),
            (None, Ok(())) => Ok(()),
        }
    }

    fn release_fallback(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let mut error = CallbackErrorReport::default();
        let _ = unsafe {
            (self.callbacks.release_planned)(
                self.callbacks.context,
                handle.as_ptr(),
                &mut error,
            )
        };
    }
}

impl Drop for PlannedTableScanHandle {
    fn drop(&mut self) {
        self.release_fallback();
    }
}

impl Drop for BoundTableScanHandle {
    fn drop(&mut self) {
        // Normal query lifecycle calls `close` at its PostgreSQL error boundary.
        // This fallback still guarantees exactly-once release during unwinding.
        let _ = self.release();
    }
}

impl fmt::Debug for BoundTableScanHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundTableScanHandle")
            .field("is_open", &self.handle.is_some())
            .finish()
    }
}
