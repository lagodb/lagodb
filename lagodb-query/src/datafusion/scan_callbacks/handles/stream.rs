//! Stream opening under the bound-scan lifecycle owner.

use std::ffi::c_void;
use std::sync::Arc;

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{
    CallbackErrorReport, TableScanRuntimePredicate, TableScanStreamRequest,
    TableScanTaskMetrics,
};
use pgrx::prelude::PgSqlErrorCode;

use super::{BoundTableScanHandle, PlannedTableScanHandle};
use crate::datafusion::scan_callbacks::ProviderStreamReader;

impl BoundTableScanHandle {
    pub(in crate::datafusion) fn open_stream(
        &self,
        projection: &[usize],
        static_predicates: &[*const c_void],
        maximum_batch_rows: u64,
        fixed_predicate: *const c_void,
        evolving_predicate: *const TableScanRuntimePredicate,
    ) -> Result<(ProviderStreamReader, TableScanTaskMetrics), PgReportError> {
        let mut active_plan = self.active_plan.lock().map_err(|_| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan active-plan state was poisoned",
            )
        })?;
        if active_plan.is_some() {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan opened more than one run-local task plan",
            ));
        }
        let (planned, metrics) =
            self.plan_tasks(projection, static_predicates, fixed_predicate)?;
        let request = TableScanStreamRequest::new(
            maximum_batch_rows,
            planned.stream_error.as_mut_ptr(),
            evolving_predicate,
        );
        let mut stream = FFI_ArrowArrayStream::empty();
        let mut error = CallbackErrorReport::default();
        // SAFETY: the bound and planned handles are open; request/error storage
        // outlives the returned stream and output is writable for this call.
        let status = unsafe {
            (self.callbacks.open_stream)(
                self.callbacks.context,
                self.as_ptr(),
                planned.as_ptr(),
                &request,
                (&mut stream as *mut FFI_ArrowArrayStream).cast(),
                &mut error,
            )
        };
        if let Err(primary) =
            self.callbacks
                .operation_result(status, &error, "table scan stream open")
        {
            return Err(Self::combine_open_cleanup(primary, planned));
        }
        match ArrowArrayStreamReader::try_new(stream) {
            Ok(reader) => {
                let planned = Arc::new(planned);
                *active_plan = Some(Arc::clone(&planned));
                Ok((ProviderStreamReader::new(reader, planned), metrics))
            }
            Err(error) => {
                let primary = match planned
                    .stream_error
                    .take_error("table scan stream schema")
                {
                    Some(error) => error,
                    None => PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                        format!(
                            "table scan returned an invalid Arrow C Stream: {error}"
                        ),
                    ),
                };
                Err(Self::combine_open_cleanup(primary, planned))
            }
        }
    }

    fn combine_open_cleanup(
        primary: PgReportError,
        planned: PlannedTableScanHandle,
    ) -> PgReportError {
        match planned.close() {
            Ok(()) => primary,
            Err(cleanup) => primary.contextualize(
                "table scan stream initialization failed",
                Some(format!("planned task cleanup also failed: {cleanup}")),
            ),
        }
    }
}
