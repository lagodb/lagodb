use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::ffi::FFI_ArrowSchema;
use lagodb_core::diag::PgReportError;
use lagodb_core::expr::pushdown::{
    FilterPlanningContext, FilterPushdown, PredicatePlan, QueryExpressionNormalizer,
    QueryExpressionScope, QueryPruningPlanner,
};
use lagodb_core::hooks::register_table_scan;
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::runtime_api::{
    BoundTableScanResult, CALLBACK_OK, CallbackErrorReport, PlannedTableScanResult,
    PlannedTableScanTasks, PredicateSupport, TABLE_SCAN_FAILED, TABLE_SCAN_PLANNED,
    TABLE_SCAN_UNSUPPORTED, TableScanBindRequest, TableScanDescriptor,
    TableScanPlanningRequest, TableScanPredicateResult, TableScanRuntimePredicate,
    TableScanStreamRequest, TableScanTaskPlanningRequest,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::contract::{
    ScanPlanningContext, ScanStreamOptions, ScanSupport, ScanTaskPlanningOptions,
    TableScanProvider, decode_runtime_predicate, runtime_predicate_slot,
};
use super::stream_export;

/// Complete C-compatible descriptor adapter for one typed provider.
pub struct TableScanAdapter<P>(PhantomData<P>);

struct AdapterPruning {
    expression: *mut pg_sys::Expr,
}

impl<P: TableScanProvider> TableScanAdapter<P> {
    fn descriptor(provider: &'static P) -> TableScanDescriptor {
        // SAFETY: typed adapter contains all callbacks and provider is static.
        unsafe {
            TableScanDescriptor::new(
                P::ROUTES,
                ptr::from_ref(provider).cast_mut().cast(),
                Self::plan_scan,
                Self::bind_scan,
                Self::get_bound_schema,
                Self::negotiate_predicate,
                Self::plan_scan_tasks,
                Self::open_serial_stream,
                Self::release_predicate,
                Self::release_planned,
                Self::release_bound,
            )
        }
    }

    pub fn register(provider: &'static P) {
        // SAFETY: descriptor is generated solely from the typed adapter.
        unsafe { register_table_scan(Self::descriptor(provider)) }
    }

    unsafe fn provider(context: *mut c_void) -> &'static P {
        // SAFETY: descriptor stores the backend-static provider address.
        unsafe { &*context.cast::<P>() }
    }

    unsafe extern "C-unwind" fn plan_scan(
        provider_context: *mut c_void,
        request: *const TableScanPlanningRequest,
        output: *mut PlannedTableScanResult,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let mut outcome = TABLE_SCAN_UNSUPPORTED;
        let operation = || {
            // SAFETY: request remains live for this synchronous callback.
            let mut planning = ScanPlanningContext::try_new(unsafe { &*request })?;
            // SAFETY: this adapter installed the backend-static context.
            let provider = unsafe { Self::provider(provider_context) };
            let pruning = Self::negotiate_pruning(&mut planning)?;
            match provider
                .plan_scan(&planning)
                .map_err(PgReportError::from_domain_error)?
            {
                ScanSupport::Unsupported => outcome = TABLE_SCAN_UNSUPPORTED,
                ScanSupport::Planned(planned) => {
                    let (plan, cost) = planned.into_parts();
                    let plan_data = PlanDataWriter::encode_list(|writer| {
                        provider.encode_scan_plan(&plan, writer)
                    })
                    .map_err(PgReportError::from_domain_error)?;
                    let pruning_expression =
                        pruning.map_or(ptr::null_mut(), |pruning| pruning.expression);
                    // SAFETY: output is runtime-owned writable storage.
                    unsafe {
                        *output = PlannedTableScanResult {
                            struct_size: size_of::<PlannedTableScanResult>() as u32,
                            plan_data,
                            pruning_expression,
                            rows_read: cost.rows_read(),
                            bytes_read: cost.bytes_read(),
                            startup_cost: cost.startup_cost(),
                        };
                    }
                    outcome = TABLE_SCAN_PLANNED;
                }
            }
            Ok(())
        };
        // SAFETY: runtime supplies live request/output/error storage.
        let status = unsafe { (&mut *error).capture(operation) };
        if status == CALLBACK_OK {
            outcome
        } else {
            TABLE_SCAN_FAILED
        }
    }

    fn negotiate_pruning(
        planning: &mut ScanPlanningContext<'_>,
    ) -> Result<Option<AdapterPruning>, PgReportError> {
        let Some(expression) = planning.predicate_expression() else {
            return Ok(None);
        };
        let filter_context = FilterPlanningContext::new(
            planning.relation_oid(),
            planning.range_table_index(),
            planning.tablespace_oid(),
            planning.effective_user_id(),
        );
        let mut filter_planner = P::Filter::begin_filter_planning(&filter_context)
            .map_err(PgReportError::from_domain_error)?;
        let normalizer =
            QueryExpressionNormalizer::new(QueryExpressionScope::for_relation(
                planning.range_table_index(),
                planning.scan(),
            ));
        let Some(negotiated) = (unsafe {
            QueryPruningPlanner::new(&normalizer)
                .negotiate(expression, &mut filter_planner)
        })
        .map_err(PgReportError::from_domain_error)?
        else {
            return Ok(None);
        };
        let (normalized, predicate, costing) = negotiated.into_parts();
        if costing.is_costed() {
            planning.set_pruning_selectivity(unsafe {
                planning.estimate_pruning_selectivity(normalized.pushed_expr())
            });
        }
        let (_, _, pruning_expression) = normalized.into_parts();
        // PostgreSQL planning uses this artifact only to establish capability,
        // costing, and EXPLAIN identity. DataFusion negotiates the final filter
        // again against the statement-bound scan and owns that execution
        // artifact through the physical plan.
        drop(predicate);
        Ok(Some(AdapterPruning {
            expression: pruning_expression,
        }))
    }

    unsafe extern "C-unwind" fn bind_scan(
        context: *mut c_void,
        request: *const TableScanBindRequest,
        output: *mut BoundTableScanResult,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            // SAFETY: request remains live for this synchronous callback.
            let request = unsafe { &*request };
            if request.struct_size != size_of::<TableScanBindRequest>() as u32 {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "table scan received an incompatible bind request",
                ));
            }
            // SAFETY: adapter installed context and plan_data is live.
            let provider = unsafe { Self::provider(context) };
            let plan = unsafe {
                PlanDataReader::decode_checked_list(
                    request.plan_data.cast_mut(),
                    0,
                    |reader| provider.decode_scan_plan(reader),
                )
            }
            .map_err(PgReportError::from_domain_error)?;
            let scan = provider
                .bind_scan(&plan)
                .map_err(PgReportError::from_domain_error)?;
            // SAFETY: runtime assumes ownership through release_bound.
            unsafe {
                *output = BoundTableScanResult {
                    struct_size: size_of::<BoundTableScanResult>() as u32,
                    bound: Box::into_raw(Box::new(scan)).cast(),
                };
            }
            Ok(())
        };
        // SAFETY: runtime supplies live request/output/error storage.
        unsafe { (&mut *error).capture(operation) }
    }

    unsafe extern "C-unwind" fn open_serial_stream(
        context: *mut c_void,
        bound: *mut c_void,
        planned: *mut c_void,
        request: *const TableScanStreamRequest,
        output: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            // SAFETY: context, bound, and planned originate from this adapter.
            let provider = unsafe { Self::provider(context) };
            let scan = unsafe { &*bound.cast::<P::BoundScan>() };
            let tasks = unsafe { &*planned.cast::<P::PlannedTasks>() };
            // SAFETY: request and its stream error slot remain live.
            let request = unsafe { &*request };
            let options = ScanStreamOptions::try_from_request(request)?;
            let stream = provider
                .open_serial_stream(scan, tasks, options)
                .map_err(PgReportError::from_domain_error)?;
            // SAFETY: output is caller-owned Arrow stream storage.
            unsafe {
                output
                    .cast::<FFI_ArrowArrayStream>()
                    .write(stream_export::export(stream, request.stream_error));
            }
            Ok(())
        };
        // SAFETY: runtime supplies matching live callback storage.
        unsafe { (&mut *error).capture(operation) }
    }

    unsafe extern "C-unwind" fn get_bound_schema(
        context: *mut c_void,
        bound: *mut c_void,
        output: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            let provider = unsafe { Self::provider(context) };
            let scan = unsafe { &*bound.cast::<P::BoundScan>() };
            let schema = FFI_ArrowSchema::try_from(
                provider.bound_schema(scan).as_ref(),
            )
            .map_err(|error| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!(
                        "table scan returned an unexportable Arrow schema: {error}"
                    ),
                )
            })?;
            unsafe { output.cast::<FFI_ArrowSchema>().write(schema) };
            Ok(())
        };
        unsafe { (&mut *error).capture(operation) }
    }

    unsafe extern "C-unwind" fn negotiate_predicate(
        context: *mut c_void,
        bound: *mut c_void,
        predicate: *const TableScanRuntimePredicate,
        output: *mut TableScanPredicateResult,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            let provider = unsafe { Self::provider(context) };
            let scan = unsafe { &*bound.cast::<P::BoundScan>() };
            let slot = unsafe { runtime_predicate_slot(predicate) }
                .map_err(P::Error::from)
                .map_err(PgReportError::from_domain_error)?
                .ok_or_else(|| {
                    PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        "table scan received a null predicate negotiation request",
                    )
                })?;
            let predicate = decode_runtime_predicate(&slot)
                .map_err(P::Error::from)
                .map_err(PgReportError::from_domain_error)?;
            let (support, predicate) = match provider
                .plan_predicate(scan, &predicate)
                .map_err(PgReportError::from_domain_error)?
            {
                PredicatePlan::Unsupported => (0, ptr::null_mut()),
                PredicatePlan::Partial(predicate)
                | PredicatePlan::Conservative(predicate) => (
                    PredicateSupport::Conservative.code(),
                    Box::into_raw(Box::new(predicate)).cast(),
                ),
                PredicatePlan::Exact(predicate)
                | PredicatePlan::ExactNoComplement(predicate) => (
                    PredicateSupport::Exact.code(),
                    Box::into_raw(Box::new(predicate)).cast(),
                ),
            };
            unsafe {
                *output = TableScanPredicateResult {
                    struct_size: size_of::<TableScanPredicateResult>() as u32,
                    support,
                    predicate,
                };
            }
            Ok(())
        };
        unsafe { (&mut *error).capture(operation) }
    }

    unsafe extern "C-unwind" fn plan_scan_tasks(
        context: *mut c_void,
        bound: *mut c_void,
        request: *const TableScanTaskPlanningRequest,
        output: *mut PlannedTableScanTasks,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            let provider = unsafe { Self::provider(context) };
            let scan = unsafe { &*bound.cast::<P::BoundScan>() };
            let request = unsafe { &*request };
            let options = ScanTaskPlanningOptions::try_from_request(request)?;
            if request.static_predicate_count != 0
                && request.static_predicates.is_null()
            {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "table scan received an invalid static predicate handle array",
                ));
            }
            let static_predicates = if request.static_predicate_count == 0 {
                Vec::new()
            } else {
                unsafe {
                    core::slice::from_raw_parts(
                        request.static_predicates,
                        request.static_predicate_count,
                    )
                }
                .iter()
                .map(|predicate| {
                    let predicate = *predicate;
                    if predicate.is_null() {
                        return Err(PgReportError::from_message(
                            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                            "table scan received a null static predicate handle",
                        ));
                    }
                    Ok(unsafe { &*predicate.cast::<P::Predicate>() })
                })
                .collect::<Result<Vec<_>, _>>()?
            };
            let runtime_predicate = if request.runtime_predicate.is_null() {
                None
            } else {
                Some(unsafe { &*request.runtime_predicate.cast::<P::Predicate>() })
            };
            let (planned, metrics) = provider
                .plan_scan_tasks(
                    scan,
                    &options,
                    &static_predicates,
                    runtime_predicate,
                )
                .map_err(PgReportError::from_domain_error)?
                .into_parts();
            unsafe {
                *output = PlannedTableScanTasks {
                    struct_size: size_of::<PlannedTableScanTasks>() as u32,
                    planned: Box::into_raw(Box::new(planned)).cast(),
                    metrics,
                };
            }
            Ok(())
        };
        unsafe { (&mut *error).capture(operation) }
    }

    unsafe extern "C-unwind" fn release_planned(
        _context: *mut c_void,
        planned: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let planned = unsafe { Box::from_raw(planned.cast::<P::PlannedTasks>()) };
        unsafe {
            (&mut *error).capture(move || {
                drop(planned);
                Ok(())
            })
        }
    }

    unsafe extern "C-unwind" fn release_predicate(
        _context: *mut c_void,
        predicate: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let predicate = unsafe { Box::from_raw(predicate.cast::<P::Predicate>()) };
        unsafe {
            (&mut *error).capture(move || {
                drop(predicate);
                Ok(())
            })
        }
    }

    unsafe extern "C-unwind" fn release_bound(
        _context: *mut c_void,
        bound: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        // SAFETY: runtime returns the unique handle created by bind_scan.
        let bound = unsafe { Box::from_raw(bound.cast::<P::BoundScan>()) };
        unsafe {
            (&mut *error).capture(move || {
                drop(bound);
                Ok(())
            })
        }
    }
}
