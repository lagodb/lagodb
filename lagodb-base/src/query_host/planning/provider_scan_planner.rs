//! Planning adapter for one storage-routed provider table scan.

use std::ffi::CStr;
use std::mem::size_of;

use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::{ScanCost, TableScanRoute};
use lagodb_core::runtime_api::{
    CallbackErrorReport, PlannedTableScanResult, TABLE_SCAN_FAILED,
    TABLE_SCAN_PLANNED, TABLE_SCAN_UNSUPPORTED, TableScanPlanningRequest,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::super::table_scan_registry::TableScanRegistry;

pub(super) struct PlannedScanRecord {
    pub(super) route: TableScanRoute<'static>,
    pub(super) plan_data: *mut pg_sys::List,
    pub(super) pruning: Option<PlannedScanPruning>,
    pub(super) cost: ScanCost,
}

pub(super) struct PlannedScanPruning {
    pub(super) expression: *mut pg_sys::Expr,
}

pub(super) struct ProviderScanPlanner;

impl ProviderScanPlanner {
    pub(super) fn plan(
        request: TableScanPlanningRequest,
    ) -> Result<Option<PlannedScanRecord>, PgReportError> {
        // The candidate gate admits only a PostgreSQL base relation. Its RTE
        // therefore supplies the relation OID and its catalog row supplies one
        // valid table-AM OID.
        let relation_oid = unsafe { (*request.range_table_entry).relid };
        let access_method_oid = unsafe { pg_sys::get_rel_relam(relation_oid) };
        // PostgreSQL returns a palloc-owned copy of the AM name.
        let access_method_name = unsafe { pg_sys::get_am_name(access_method_oid) };
        let registered = {
            // SAFETY: `get_am_name` returned a live NUL-terminated name.
            let route = TableScanRoute::access_method(unsafe {
                CStr::from_ptr(access_method_name)
            });
            TableScanRegistry::resolve(route)
        };
        // SAFETY: `access_method_name` is the palloc-owned result above and no
        // borrowed route escapes this block; the registry returns its own
        // backend-lifetime route pointer.
        unsafe { pg_sys::pfree(access_method_name.cast()) };
        let Some(registered) = registered else {
            return Ok(None);
        };

        let mut output = PlannedTableScanResult::default();
        let mut error = CallbackErrorReport::default();
        match registered.plan(&request, &mut output, &mut error) {
            TABLE_SCAN_UNSUPPORTED => Ok(None),
            TABLE_SCAN_PLANNED => {
                Self::decode_planned(registered.route(), &request, output).map(Some)
            }
            TABLE_SCAN_FAILED => {
                // SAFETY: `FAILED` requires the provider callback to populate
                // the fixed-layout error record synchronously.
                Err(unsafe { error.to_error("table scan planning") })
            }
            status => Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!(
                    "table-scan route {:?} returned unknown planning status {status}",
                    registered.route(),
                ),
            )),
        }
    }

    fn decode_planned(
        route: TableScanRoute<'static>,
        request: &TableScanPlanningRequest,
        output: PlannedTableScanResult,
    ) -> Result<PlannedScanRecord, PgReportError> {
        let expected_size = u32::try_from(size_of::<PlannedTableScanResult>())
            .expect("planned table-scan result size exceeds u32");
        // SAFETY: each node pointer is checked for null before its tag is read;
        // provider output remains live in the planner context.
        if output.struct_size != expected_size
            || output.plan_data.is_null()
            || unsafe { (*output.plan_data).type_ } != pg_sys::NodeTag::T_List
            || (request.predicate_expression.is_null()
                && !output.pruning_expression.is_null())
        {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan returned invalid plan data",
            ));
        }
        let cost = ScanCost::try_new(
            output.rows_read,
            output.bytes_read,
            output.startup_cost,
        )
        .map_err(|error| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("table scan returned invalid statistics: {error}"),
            )
        })?;
        let pruning =
            (!output.pruning_expression.is_null()).then_some(PlannedScanPruning {
                expression: output.pruning_expression,
            });
        Ok(PlannedScanRecord {
            route,
            plan_data: output.plan_data,
            pruning,
            cost,
        })
    }
}
