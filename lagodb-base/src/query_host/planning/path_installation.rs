//! Installation of one fully planned provider-neutral query path.

use std::ffi::CStr;
use std::{mem, ptr};

use lagodb_core::runtime_api::TableScanPlanningRequest;
use lagodb_query::ExecutionProfile;
use lagodb_query::plan::{
    CostingContext, PlanCost, PlannedTableScan, QueryCostEstimator, ScanCostTable,
    SelectedQueryPlan, TableScanFilterExplain,
};
use pgrx::pg_sys;

use crate::gucs::{QueryOffloadMode, query_execution_profile, query_offload_mode};
use crate::query_host::error::QueryHostError;

use super::materialize;
use super::planned_query::{PlannedQuery, PlannedScanInput};
use super::provider_scan_planner::{PlannedScanRecord, ProviderScanPlanner};

struct ProviderScanPlan {
    input: PlannedScanInput,
    provider: PlannedScanRecord,
}

/// Every scan follows the same registry protocol, so execution does not
/// depend on all participants being implemented by the same provider.
pub(super) struct QueryPathInstallation {
    output_rel: *mut pg_sys::RelOptInfo,
    path_target: *mut pg_sys::PathTarget,
    planned: PlannedQuery,
    output_rows: f64,
    parameter_info: *mut pg_sys::ParamPathInfo,
}

impl QueryPathInstallation {
    pub(super) fn new(
        output_rel: *mut pg_sys::RelOptInfo,
        path_target: *mut pg_sys::PathTarget,
        planned: PlannedQuery,
        output_rows: f64,
    ) -> Self {
        Self {
            output_rel,
            path_target,
            planned,
            output_rows,
            parameter_info: ptr::null_mut(),
        }
    }

    #[must_use]
    pub(super) fn with_parameter_info(
        mut self,
        parameter_info: *mut pg_sys::ParamPathInfo,
    ) -> Self {
        self.parameter_info = parameter_info;
        self
    }

    pub(super) fn install(mut self) -> Result<(), QueryHostError> {
        if self.declines_auto_fallback() {
            return Ok(());
        }
        let inputs = mem::take(&mut self.planned.scans).into_vec();
        let mut scans = Vec::with_capacity(inputs.len());
        for input in inputs {
            let request = self.provider_request(&input);
            let Some(provider) = ProviderScanPlanner::plan(request)? else {
                return Ok(());
            };
            scans.push(ProviderScanPlan { input, provider });
        }
        let execution = query_execution_profile();
        let cost = self.cost(&scans, execution)?;
        self.encode_and_install(scans, execution, cost)
    }

    fn declines_auto_fallback(&self) -> bool {
        query_offload_mode() == QueryOffloadMode::Auto
            && self.planned.query.fragment().postgres_fallback_count() != 0
    }

    fn provider_request(&self, input: &PlannedScanInput) -> TableScanPlanningRequest {
        let predicate_expression = input
            .table_scan_filter
            .as_ref()
            .map_or(ptr::null_mut(), |filter| filter.source_expression());
        let relation_user = unsafe { (*input.input_rel).userid };
        let effective_user = if relation_user == pg_sys::InvalidOid {
            unsafe { pg_sys::GetUserId() }
        } else {
            relation_user
        };
        if input.projected_columns.is_empty() {
            TableScanPlanningRequest::row_count(
                input.scan.index(),
                predicate_expression,
                effective_user,
                input.root,
                input.input_rel,
                input.range_table_index,
                input.range_table_entry,
            )
        } else {
            TableScanPlanningRequest::columns(
                input.scan.index(),
                &input.projected_columns,
                predicate_expression,
                effective_user,
                input.root,
                input.input_rel,
                input.range_table_index,
                input.range_table_entry,
            )
        }
    }

    fn cost(
        &self,
        scans: &[ProviderScanPlan],
        execution: ExecutionProfile,
    ) -> Result<PlanCost, QueryHostError> {
        match query_offload_mode() {
            QueryOffloadMode::Off => unreachable!("mode was gated before planning"),
            QueryOffloadMode::Force => Ok(PlanCost::forced()),
            QueryOffloadMode::Auto => {
                let estimates = scans
                    .iter()
                    .map(|scan| scan.provider.cost)
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let scan_rows = scans
                    .iter()
                    .map(|scan| scan.input.estimated_rows)
                    .collect::<Vec<_>>();
                let context = CostingContext::try_new(
                    execution,
                    unsafe { pg_sys::cpu_tuple_cost },
                    unsafe { pg_sys::cpu_operator_cost },
                    unsafe { pg_sys::seq_page_cost },
                    pg_sys::BLCKSZ as usize,
                )
                .map_err(QueryHostError::invalid_plan)?;
                QueryCostEstimator::new(
                    context,
                    &ScanCostTable::from_dense(estimates),
                    &scan_rows,
                )
                .estimate(self.planned.query.fragment())
                .map(|estimate| estimate.cost())
                .map_err(QueryHostError::invalid_plan)
            }
        }
    }

    fn encode_and_install(
        self,
        scans: Vec<ProviderScanPlan>,
        execution: ExecutionProfile,
        cost: PlanCost,
    ) -> Result<(), QueryHostError> {
        let filter_texts = scans
            .iter()
            .map(|scan| {
                scan.input
                    .table_scan_filter
                    .as_ref()
                    .map(|filter| unsafe {
                        filter.explain_texts(
                            scan.provider
                                .pruning
                                .as_ref()
                                .map(|pruning| pruning.expression),
                            scan.input.range_table_index,
                            scan.input.range_table_entry,
                        )
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let planned_scans = scans
            .iter()
            .zip(&filter_texts)
            .map(|(scan, text)| {
                let filter_explain = text.as_ref().map(|(exact, pruning)| {
                    TableScanFilterExplain::new(exact.as_c_str(), pruning.as_deref())
                });
                let range_table_entry = unsafe { &*scan.input.range_table_entry };
                let alias =
                    unsafe { CStr::from_ptr((*range_table_entry.eref).aliasname) };
                unsafe {
                    PlannedTableScan::new(
                        scan.provider.route,
                        range_table_entry.relid,
                        alias,
                        scan.provider.cost,
                        filter_explain,
                        &*scan.provider.plan_data,
                    )
                }
            })
            .collect::<Vec<_>>();
        let runtime_exprs = self
            .planned
            .runtime_exprs
            .iter()
            .map(|binding| binding.expr())
            .collect::<Vec<_>>();
        let selected_plan = unsafe {
            SelectedQueryPlan::encode_path(
                &self.planned.query,
                execution,
                &planned_scans,
                &runtime_exprs,
                &self.planned.scan_target_exprs,
            )
        }
        .map_err(QueryHostError::invalid_plan)?;
        unsafe {
            materialize::add_path(
                self.output_rel,
                self.path_target,
                selected_plan,
                cost,
                self.output_rows,
                self.parameter_info,
                ptr::null_mut(),
            )
        };
        Ok(())
    }
}
