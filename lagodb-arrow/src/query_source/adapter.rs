use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::ffi::FFI_ArrowSchema;
use lagodb_core::diag::PgReportError;
use lagodb_core::expr::pushdown::{
    FilterBindResult, FilterPlanningContext, FilterPushdown,
    QueryExpressionNormalizer, QueryExpressionScope, QueryPruningPlanner,
};
use lagodb_core::expr::{
    ExpressionPlanDataDecode, ExpressionPlanDataEncode, RuntimeValue,
    RuntimeValueBindings, RuntimeValueLayout,
};
use lagodb_core::hooks::register_table_scan;
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::ScanId;
use lagodb_core::runtime_api::{
    CALLBACK_OK, CallbackErrorReport, PlannedTableScanResult, TABLE_SCAN_FAILED,
    TABLE_SCAN_NOT_OWNED, TABLE_SCAN_PLANNED, TABLE_SCAN_UNSUPPORTED,
    TableScanDescriptor, TableScanPlanningRequest, TableScanPrepareRequest,
    TableScanStreamRequest,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::contract::{
    ScanPlanningContext, ScanStreamOptions, ScanSupport, TableScanProvider,
};
use super::stream_export;

/// Complete C-compatible descriptor adapter for one typed provider.
pub struct TableScanAdapter<P>(PhantomData<P>);

impl<P: TableScanProvider> TableScanAdapter<P> {
    fn descriptor(provider: &'static P) -> TableScanDescriptor {
        // SAFETY: typed adapter contains all callbacks and provider is static.
        unsafe {
            TableScanDescriptor::new(
                ptr::from_ref(provider).cast_mut().cast(),
                Self::plan_scan,
                Self::prepare_scan,
                Self::get_prepared_schema,
                Self::open_serial_stream,
                Self::release_prepared,
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
        let mut outcome = TABLE_SCAN_NOT_OWNED;
        let operation = || {
            // SAFETY: request remains live for this synchronous callback.
            let planning = ScanPlanningContext::try_new(unsafe { &*request })?;
            // SAFETY: this adapter installed the backend-static context.
            let provider = unsafe { Self::provider(provider_context) };
            match provider
                .plan_scan(&planning)
                .map_err(PgReportError::from_domain_error)?
            {
                ScanSupport::NotOwned => outcome = TABLE_SCAN_NOT_OWNED,
                ScanSupport::Unsupported => outcome = TABLE_SCAN_UNSUPPORTED,
                ScanSupport::Planned(planned) => {
                    let (plan, estimate) = planned.into_parts();
                    let scan_plan = PlanDataWriter::encode_list(|writer| {
                        provider.encode_scan_plan(&plan, writer)
                    })
                    .map_err(PgReportError::from_domain_error)?;
                    let pruning =
                        if let Some(expression) = planning.predicate_expression() {
                            let filter_context = FilterPlanningContext::new(
                                planning.relation_oid(),
                                planning.range_table_index(),
                                planning.tablespace_oid(),
                                planning.effective_user_id(),
                            );
                            let mut filter_planner =
                                P::Filter::begin_filter_planning(&filter_context)
                                    .map_err(PgReportError::from_domain_error)?;
                            let normalizer = QueryExpressionNormalizer::new(
                                QueryExpressionScope::for_relation(
                                    planning.range_table_index(),
                                    planning.scan(),
                                ),
                            );
                            let negotiated = (unsafe {
                                QueryPruningPlanner::new(&normalizer)
                                    .negotiate(expression, &mut filter_planner)
                            })
                            .map_err(PgReportError::from_domain_error)?;
                            if let Some(negotiated) = negotiated {
                                let (normalized, predicate) = negotiated.into_parts();
                                let pruning_fragment = normalized
                                    .fragment()
                                    .encode_plan_data()
                                    .map_err(P::Error::from)
                                    .map_err(PgReportError::from_domain_error)?;
                                let (fragment, bindings, pruning_expression) =
                                    normalized.into_parts();
                                let (_, layout) = fragment.into_parts();
                                let mut pruning_binding_exprs = ptr::null_mut();
                                for binding in bindings {
                                    pruning_binding_exprs = unsafe {
                                        pg_sys::lappend(
                                            pruning_binding_exprs,
                                            binding.expr().cast(),
                                        )
                                    };
                                }
                                let encoded = PlanDataWriter::encode_list(|writer| {
                                    P::Filter::encode_planned(&predicate, writer)
                                        .map_err(P::Error::from)
                                })
                                .map_err(PgReportError::from_domain_error)?;
                                Some((
                                    layout,
                                    encoded,
                                    pruning_fragment,
                                    pruning_binding_exprs,
                                    pruning_expression,
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                    let plan_data = PlanDataWriter::encode_list(|writer| {
                        unsafe { writer.append_encoded_list(scan_plan) };
                        writer.append_bool(pruning.is_some());
                        if let Some((layout, encoded_filter, ..)) = &pruning {
                            writer.append_nested(|record| {
                                layout.encode_plan_data(record)
                            });
                            unsafe { writer.append_encoded_list(*encoded_filter) };
                        }
                        Ok::<_, P::Error>(())
                    })
                    .map_err(PgReportError::from_domain_error)?;
                    let (pruning_fragment, pruning_binding_exprs, pruning_expression) =
                        pruning.map_or(
                            (ptr::null_mut(), ptr::null_mut(), ptr::null_mut()),
                            |(_, _, fragment, bindings, expression)| {
                                (fragment, bindings, expression)
                            },
                        );
                    // SAFETY: output is runtime-owned writable storage.
                    unsafe {
                        *output = PlannedTableScanResult {
                            struct_size: size_of::<PlannedTableScanResult>() as u32,
                            plan_data,
                            pruning_fragment,
                            pruning_binding_exprs,
                            pruning_expression,
                            estimated_rows: estimate.estimated_rows(),
                            estimated_scan_bytes: estimate.estimated_scan_bytes(),
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

    unsafe extern "C-unwind" fn prepare_scan(
        context: *mut c_void,
        request: *const TableScanPrepareRequest,
        prepared: *mut *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            // SAFETY: request remains live for this synchronous callback.
            let request = unsafe { &*request };
            if request.struct_size != size_of::<TableScanPrepareRequest>() as u32 {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "table scan received an incompatible prepare request",
                ));
            }
            // SAFETY: adapter installed context and plan_data is live.
            let provider = unsafe { Self::provider(context) };
            let scan = ScanId::from_index(request.scan_index);
            let (plan, planned_filter) = unsafe {
                PlanDataReader::decode_checked_list(
                    request.plan_data.cast_mut(),
                    0,
                    |reader| {
                        let scan_plan = reader.read_encoded_list()?;
                        let plan = PlanDataReader::decode_checked_list(
                            scan_plan,
                            0,
                            |record| provider.decode_scan_plan(scan, record),
                        )?;
                        let filter = if reader.read_bool()? {
                            let layout = reader.read_nested(|record| {
                                RuntimeValueLayout::decode_plan_data(record, ())
                            })?;
                            let predicate = reader.read_nested(|record| {
                                P::Filter::decode_planned(record, layout.len())
                                    .map_err(P::Error::from)
                            })?;
                            Some((predicate, layout))
                        } else {
                            None
                        };
                        Ok::<_, P::Error>((plan, filter))
                    },
                )
            }
            .map_err(PgReportError::from_domain_error)?;
            let expected_value_count = planned_filter
                .as_ref()
                .map_or(0, |(_, layout)| layout.len());
            if request.runtime_value_count != expected_value_count
                || (request.runtime_value_count != 0
                    && request.runtime_values.is_null())
            {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "table scan runtime values do not match its predicate layouts",
                ));
            }
            let raw_values = if request.runtime_value_count == 0 {
                &[]
            } else {
                unsafe {
                    core::slice::from_raw_parts(
                        request.runtime_values,
                        request.runtime_value_count,
                    )
                }
            };
            let bound_filter = if let Some((predicate, layout)) = &planned_filter {
                let values = raw_values
                    .iter()
                    .zip(layout.values())
                    .map(|(value, &metadata)| unsafe {
                        RuntimeValue::from_raw(value.datum, value.is_null, metadata)
                    })
                    .collect::<Vec<_>>();
                match P::Filter::bind_filter(
                    predicate,
                    RuntimeValueBindings::new(&values),
                )
                .map_err(PgReportError::from_domain_error)?
                {
                    FilterBindResult::Bound(predicate) => Some(predicate),
                    FilterBindResult::ValueNotRepresentable => None,
                }
            } else {
                None
            };
            let scan = provider
                .prepare_scan(&plan, bound_filter.as_ref())
                .map_err(PgReportError::from_domain_error)?;
            // SAFETY: runtime assumes ownership through release_prepared.
            unsafe { *prepared = Box::into_raw(Box::new(scan)).cast() };
            Ok(())
        };
        // SAFETY: runtime supplies live request/output/error storage.
        unsafe { (&mut *error).capture(operation) }
    }

    unsafe extern "C-unwind" fn open_serial_stream(
        context: *mut c_void,
        prepared: *mut c_void,
        request: *const TableScanStreamRequest,
        output: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            // SAFETY: context and prepared originate from this adapter.
            let provider = unsafe { Self::provider(context) };
            let scan = unsafe { &*prepared.cast::<P::PreparedScan>() };
            // SAFETY: request and its stream error slot remain live.
            let request = unsafe { &*request };
            let options = ScanStreamOptions::try_from_request(request)?;
            let stream = provider
                .open_serial_stream(scan, options)
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

    unsafe extern "C-unwind" fn get_prepared_schema(
        context: *mut c_void,
        prepared: *mut c_void,
        output: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            let provider = unsafe { Self::provider(context) };
            let scan = unsafe { &*prepared.cast::<P::PreparedScan>() };
            let schema = FFI_ArrowSchema::try_from(
                provider.prepared_schema(scan).as_ref(),
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

    unsafe extern "C-unwind" fn release_prepared(
        _context: *mut c_void,
        prepared: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        // SAFETY: runtime returns the unique handle created by prepare_scan.
        let prepared = unsafe { Box::from_raw(prepared.cast::<P::PreparedScan>()) };
        unsafe {
            (&mut *error).capture(move || {
                drop(prepared);
                Ok(())
            })
        }
    }
}
