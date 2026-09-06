//! Installation of one fully planned query-offload path.

use std::ptr;

use lagodb_core::query_contract::ScanId;
use lagodb_core::runtime_api::TableScanPlanningRequest;
use lagodb_query::ExecutionProfile;
use lagodb_query::plan::{
    CostingContext, PlanCost, PlannedTableScan, QueryCostEstimator, ScanCatalog,
    ScanEstimateTable, SelectedQueryPlan, TableScanFilterExplain,
    TableScanRuntimeBindings,
};
use pgrx::pg_sys;

use crate::gucs::QueryOffloadMode;
use crate::query_host::error::QueryHostError;
use crate::runtime_api::table_scan_registry::{PlannedScanRecord, TableScanRegistry};

use super::aggregate_plan::PlannedQuery;
use super::candidate::SingleRelationCandidate;
use super::materialize;

/// One query path moving through provider planning, costing, encoding, and
/// PostgreSQL path installation.
pub(super) struct QueryPathInstallation {
    candidate: SingleRelationCandidate,
    planned: PlannedQuery,
    aggregate_rows: f64,
    output_rows: f64,
}

impl QueryPathInstallation {
    pub(super) fn new(
        candidate: SingleRelationCandidate,
        planned: PlannedQuery,
        aggregate_rows: f64,
        output_rows: f64,
    ) -> Self {
        Self {
            candidate,
            planned,
            aggregate_rows,
            output_rows,
        }
    }

    pub(super) fn install(mut self) -> Result<(), QueryHostError> {
        if self.declines_auto_fallback() {
            return Ok(());
        }
        let catalog = ScanCatalog::for_relation(self.candidate.range_table_index);
        let scan = catalog
            .scan_for_rti(self.candidate.range_table_index)
            .expect("scan catalog contains its construction RTI");
        let request = self.provider_request(scan);
        let Some(planned_scan) = TableScanRegistry::plan(scan, request)? else {
            return Ok(());
        };
        let runtime_bindings = self.bind_pruning_runtime_values(&planned_scan)?;
        let execution = crate::gucs::query_execution_profile();
        let cost = self.cost(&planned_scan, execution)?;
        self.encode_and_install(scan, planned_scan, runtime_bindings, execution, cost)
    }

    fn declines_auto_fallback(&self) -> bool {
        crate::gucs::query_offload_mode() == QueryOffloadMode::Auto
            && self
                .planned
                .query
                .fragment()
                .summary()
                .postgres_expression_fallbacks()
                != 0
    }

    fn provider_request(&self, scan: ScanId) -> TableScanPlanningRequest {
        let predicate_expression = self
            .planned
            .table_scan_filter
            .as_ref()
            .map_or(ptr::null_mut(), |filter| filter.source_expression());
        let relation_user = unsafe { (*self.candidate.input_rel).userid };
        let effective_user = if relation_user == pg_sys::InvalidOid {
            unsafe { pg_sys::GetUserId() }
        } else {
            relation_user
        };
        if self.planned.projected_columns.is_empty() {
            TableScanPlanningRequest::row_count(
                scan.index(),
                predicate_expression,
                effective_user,
                self.candidate.root,
                self.candidate.input_rel,
                self.candidate.range_table_index,
                self.candidate.range_table_entry,
            )
        } else {
            TableScanPlanningRequest::columns(
                scan.index(),
                &self.planned.projected_columns,
                predicate_expression,
                effective_user,
                self.candidate.root,
                self.candidate.input_rel,
                self.candidate.range_table_index,
                self.candidate.range_table_entry,
            )
        }
    }

    fn bind_pruning_runtime_values(
        &mut self,
        scan: &PlannedScanRecord,
    ) -> Result<TableScanRuntimeBindings, QueryHostError> {
        let Some(pruning) = &scan.pruning else {
            return Ok(TableScanRuntimeBindings::empty());
        };
        let metadata = pruning
            .bindings
            .iter()
            .map(|binding| binding.metadata())
            .collect::<Vec<_>>();
        let start = self
            .planned
            .query
            .try_append_runtime_values(&metadata)
            .ok_or_else(|| {
                QueryHostError::invalid_plan(
                    "table-scan pruning runtime layout exceeds addressable memory",
                )
            })?;
        self.planned
            .runtime_exprs
            .extend_from_slice(&pruning.bindings);
        TableScanRuntimeBindings::try_new(start, metadata.len()).ok_or_else(|| {
            QueryHostError::invalid_plan(
                "table-scan pruning runtime binding range overflows",
            )
        })
    }

    fn cost(
        &self,
        scan: &PlannedScanRecord,
        execution: ExecutionProfile,
    ) -> Result<PlanCost, QueryHostError> {
        match crate::gucs::query_offload_mode() {
            QueryOffloadMode::Off => unreachable!("mode was gated before planning"),
            QueryOffloadMode::Force => Ok(PlanCost::forced()),
            QueryOffloadMode::Auto => {
                let scans = ScanEstimateTable::from_dense(Box::new([scan.estimate]));
                let scan_output_rows = [unsafe { (*self.candidate.input_rel).rows }];
                let context = CostingContext::try_new(
                    execution,
                    unsafe { pg_sys::cpu_tuple_cost },
                    unsafe { pg_sys::cpu_operator_cost },
                )
                .map_err(QueryHostError::invalid_plan)?;
                QueryCostEstimator::new(
                    context,
                    &scans,
                    &scan_output_rows,
                    self.aggregate_rows,
                    self.output_rows,
                )
                .estimate(self.planned.query.fragment())
                .map(|estimate| estimate.cost())
                .map_err(QueryHostError::invalid_plan)
            }
        }
    }

    fn encode_and_install(
        self,
        scan: ScanId,
        provider_scan: PlannedScanRecord,
        runtime_bindings: TableScanRuntimeBindings,
        execution: ExecutionProfile,
        cost: PlanCost,
    ) -> Result<(), QueryHostError> {
        let provider_plan = unsafe { &*provider_scan.plan_data };
        let filter_texts = self
            .planned
            .table_scan_filter
            .as_ref()
            .map(|filter| unsafe {
                filter.explain_texts(
                    provider_scan
                        .pruning
                        .as_ref()
                        .map(|pruning| pruning.expression),
                    self.candidate.range_table_index,
                    self.candidate.range_table_entry,
                )
            })
            .transpose()?;
        let filter_explain = filter_texts.as_ref().map(|(exact, pruning)| {
            TableScanFilterExplain::new(exact.as_c_str(), pruning.as_deref())
        });
        let planned_scan = unsafe {
            PlannedTableScan::new(
                provider_scan.provider_id,
                scan,
                provider_scan.estimate,
                runtime_bindings,
                filter_explain,
                provider_plan,
            )
        };
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
                &[planned_scan],
                &runtime_exprs,
                &self.planned.scan_target_exprs,
            )
        }
        .map_err(QueryHostError::invalid_plan)?;
        unsafe {
            materialize::add_path(
                self.candidate.output_rel,
                self.candidate.path_target,
                selected_plan,
                cost,
                self.output_rows,
            )
        };
        Ok(())
    }
}
