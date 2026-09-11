//! Statement-stable DataFusion filters negotiated by the bound provider.

use std::ffi::c_void;
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use lagodb_core::runtime_api::PredicateSupport;

use super::predicate::LogicalPredicateAdapter;
use crate::datafusion::scan_callbacks::{
    BoundTableScanHandle, NegotiatedTableScanPredicate,
};

#[derive(Debug, Clone, Default)]
pub(super) struct StaticFilterSet {
    predicates: Arc<[NegotiatedTableScanPredicate]>,
}

impl StaticFilterSet {
    pub(super) fn support(
        filter: &Expr,
        source_schema: &Schema,
        bound: &BoundTableScanHandle,
    ) -> Result<TableProviderFilterPushDown> {
        let Some(plan) = LogicalPredicateAdapter::new(source_schema).lower(filter)
        else {
            return Ok(TableProviderFilterPushDown::Unsupported);
        };
        let planned = bound
            .negotiate_predicate(plan.predicate())
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(if let Some(planned) = planned {
            let support = planned.support();
            planned
                .close()
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            match (support, plan.is_complete()) {
                (PredicateSupport::Exact, true) => TableProviderFilterPushDown::Exact,
                (PredicateSupport::Exact, false)
                | (PredicateSupport::Conservative, _) => {
                    TableProviderFilterPushDown::Inexact
                }
            }
        } else {
            TableProviderFilterPushDown::Unsupported
        })
    }

    pub(super) fn plan(
        filters: &[Expr],
        source_schema: &Schema,
        bound: &BoundTableScanHandle,
    ) -> Result<Self> {
        let adapter = LogicalPredicateAdapter::new(source_schema);
        let mut predicates = Vec::with_capacity(filters.len());
        for filter in filters {
            let predicate = adapter
                .lower(filter)
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "table scan received a filter outside the provider-neutral predicate IR"
                            .to_owned(),
                    )
                })?
                .into_predicate();
            let Some(planned) = bound
                .negotiate_predicate(&predicate)
                .map_err(|error| DataFusionError::External(Box::new(error)))?
            else {
                return Err(DataFusionError::Plan(
                    "table scan provider rejected a previously advertised filter"
                        .to_owned(),
                ));
            };
            predicates.push(planned);
        }
        Ok(Self {
            predicates: predicates.into(),
        })
    }

    pub(super) fn handles(&self) -> Vec<*const c_void> {
        self.predicates
            .iter()
            .map(NegotiatedTableScanPredicate::as_ptr)
            .collect()
    }
}
