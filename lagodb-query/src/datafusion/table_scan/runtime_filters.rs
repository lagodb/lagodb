//! Dynamic filters installed on one physical table-scan node.

use std::ffi::c_void;
use std::ptr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr::{
    DynamicFilterTracker, DynamicFilterTracking, PhysicalExpr, conjunction_opt,
};
use datafusion::physical_plan::filter::batch_filter;
use datafusion::physical_plan::filter_pushdown::{
    ChildFilterPushdownResult, PushedDown,
};
use lagodb_core::runtime_api::{
    PredicateSupport, RuntimePruningPredicate, TableScanRuntimePredicate,
};

use super::predicate::PhysicalPredicateAdapter;
use crate::datafusion::scan_callbacks::{
    BoundTableScanHandle, NegotiatedTableScanPredicate,
};

/// Scan-local set of DataFusion runtime filters, deduplicated by logical
/// expression identity so repeated physical optimizer passes do not duplicate
/// work.
#[derive(Debug, Clone, Default)]
pub(super) struct RuntimeFilterSet {
    filters: Box<[Arc<dyn PhysicalExpr>]>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl RuntimeFilterSet {
    pub(super) fn len(&self) -> usize {
        self.filters.len()
    }

    pub(super) fn merge(
        &self,
        incoming: &[ChildFilterPushdownResult],
    ) -> (Self, Vec<PushedDown>, bool) {
        let mut filters = self.filters.to_vec();
        let mut results = Vec::with_capacity(incoming.len());
        let mut changed = false;
        for filter in incoming {
            if filter.filter.is::<DynamicFilterPhysicalExpr>() {
                let expression = &filter.filter;
                let id = expression.expression_id();
                match filters
                    .iter_mut()
                    .find(|current| current.expression_id() == id)
                {
                    Some(current) if !Arc::ptr_eq(current, expression) => {
                        *current = Arc::clone(expression);
                        changed = true;
                    }
                    Some(_) => {}
                    None => {
                        filters.push(Arc::clone(expression));
                        changed = true;
                    }
                }
                results.push(PushedDown::Yes);
            } else {
                results.push(filter.any());
            }
        }
        let predicate = conjunction_opt(filters.iter().cloned());
        (
            Self {
                filters: filters.into_boxed_slice(),
                predicate,
            },
            results,
            changed,
        )
    }

    pub(super) fn visit(
        &self,
        visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        if let Some(predicate) = &self.predicate
            && matches!(visitor(predicate)?, TreeNodeRecursion::Stop)
        {
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    }

    /// Classify each filter at the first scan poll. HashJoin filters are
    /// complete at that point and become immutable snapshots; TopK filters
    /// remain watched and are refreshed only after a generation change.
    pub(super) fn activate(
        &self,
        source_position_by_output: &[usize],
        output_schema: SchemaRef,
        bound: &BoundTableScanHandle,
    ) -> Result<ActiveRuntimeFilters> {
        let mut fixed = Vec::new();
        let mut evolving = Vec::new();
        for filter in &self.filters {
            match DynamicFilterTracking::classify(filter) {
                DynamicFilterTracking::AllComplete => {
                    fixed.push(
                        filter.snapshot()?.unwrap_or_else(|| Arc::clone(filter)),
                    );
                }
                DynamicFilterTracking::Watching(tracker) => {
                    evolving.push(EvolvingFilter {
                        expression: Arc::clone(filter),
                        tracker,
                    });
                }
                DynamicFilterTracking::Static => fixed.push(Arc::clone(filter)),
            }
        }
        ActiveRuntimeFilters::new(
            fixed,
            evolving,
            source_position_by_output,
            output_schema,
            bound,
        )
    }
}

struct EvolvingFilter {
    expression: Arc<dyn PhysicalExpr>,
    tracker: DynamicFilterTracker,
}

struct EncodedPredicateSlot {
    abi: TableScanRuntimePredicate,
    encoded: Vec<u8>,
}

// SAFETY: any non-null payload pointer targets this value's owned `Vec`
// allocation and is dereferenced only by the serialized provider callback
// while the owner is live.
unsafe impl Send for EncodedPredicateSlot {}

impl EncodedPredicateSlot {
    fn empty() -> Self {
        Self {
            abi: TableScanRuntimePredicate::clear(0),
            encoded: Vec::new(),
        }
    }

    fn replace(&mut self, generation: u64, predicate: &RuntimePruningPredicate<'_>) {
        predicate.encode_into(&mut self.encoded);
        self.abi = TableScanRuntimePredicate::replacement(generation, &self.encoded);
    }

    fn clear(&mut self, generation: u64) {
        self.encoded.clear();
        self.abi = TableScanRuntimePredicate::clear(generation);
    }

    const fn as_ptr(&self) -> *const TableScanRuntimePredicate {
        ptr::from_ref(&self.abi)
    }
}

pub(super) struct ActiveRuntimeFilters {
    fixed_batch_residual: Option<Arc<dyn PhysicalExpr>>,
    evolving: Vec<EvolvingFilter>,
    current_evolving: Option<Arc<dyn PhysicalExpr>>,
    batch_predicate: Option<Arc<dyn PhysicalExpr>>,
    provider_fixed: Option<NegotiatedTableScanPredicate>,
    provider_evolving: Option<Box<EncodedPredicateSlot>>,
    source_position_by_output: Box<[usize]>,
    output_schema: SchemaRef,
    provider_generation: u64,
}

impl ActiveRuntimeFilters {
    fn new(
        fixed: Vec<Arc<dyn PhysicalExpr>>,
        evolving: Vec<EvolvingFilter>,
        source_position_by_output: &[usize],
        output_schema: SchemaRef,
        bound: &BoundTableScanHandle,
    ) -> Result<Self> {
        let fixed = conjunction_opt(fixed);
        let compiler = PhysicalPredicateAdapter::new(
            source_position_by_output,
            output_schema.as_ref(),
        );
        let (provider_fixed_ir, exact_residual) = fixed
            .as_ref()
            .map(|predicate| compiler.plan(predicate).into_parts())
            .unwrap_or((None, None));
        let provider_fixed = provider_fixed_ir
            .as_ref()
            .filter(|predicate| {
                !matches!(predicate, RuntimePruningPredicate::AlwaysTrue)
            })
            .map(|predicate| {
                bound
                    .negotiate_predicate(predicate)
                    .map_err(|error| DataFusionError::External(Box::new(error)))
            })
            .transpose()?
            .flatten();
        let support = provider_fixed
            .as_ref()
            .map(NegotiatedTableScanPredicate::support);
        let fixed_batch_residual =
            Self::fixed_residual(fixed.as_ref(), exact_residual, support);
        let has_evolving = !evolving.is_empty();
        let mut active = Self {
            fixed_batch_residual,
            evolving,
            current_evolving: None,
            batch_predicate: None,
            provider_fixed,
            provider_evolving: has_evolving
                .then(|| Box::new(EncodedPredicateSlot::empty())),
            source_position_by_output: source_position_by_output.into(),
            output_schema,
            provider_generation: 0,
        };
        active.refresh_evolving(true)?;
        active.rebuild_batch_predicate();
        Ok(active)
    }

    pub(super) fn fixed_provider_predicate(&self) -> *const c_void {
        self.provider_fixed
            .as_ref()
            .map_or(ptr::null_mut(), NegotiatedTableScanPredicate::as_ptr)
    }

    pub(super) fn evolving_provider_predicate(
        &self,
    ) -> *const TableScanRuntimePredicate {
        self.provider_evolving
            .as_deref()
            .map_or(ptr::null(), EncodedPredicateSlot::as_ptr)
    }

    pub(super) fn refresh(&mut self) -> Result<()> {
        if self.refresh_evolving(false)? {
            self.rebuild_batch_predicate();
        }
        Ok(())
    }

    pub(super) fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match &self.batch_predicate {
            Some(predicate) => batch_filter(&batch, predicate),
            None => Ok(batch),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.batch_predicate.is_none()
    }

    fn refresh_evolving(&mut self, force: bool) -> Result<bool> {
        if self.evolving.is_empty() {
            return Ok(false);
        }
        let changed = self
            .evolving
            .iter_mut()
            .fold(force, |changed, filter| filter.tracker.changed() || changed);
        if !changed {
            return Ok(false);
        }
        let snapshots = self
            .evolving
            .iter()
            .map(|filter| {
                filter.expression.snapshot().map(|snapshot| {
                    snapshot.unwrap_or_else(|| Arc::clone(&filter.expression))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.current_evolving = conjunction_opt(snapshots.iter().cloned());
        let compiler = PhysicalPredicateAdapter::new(
            &self.source_position_by_output,
            self.output_schema.as_ref(),
        );
        let provider_expression = conjunction_opt(snapshots.iter().cloned());
        let Some(provider_expression) = provider_expression.as_ref() else {
            return Ok(true);
        };
        // A producer can temporarily publish no representable leaf. Clear the
        // provider predicate rather than retaining an older threshold, which
        // could exclude valid rows. Provider capability negotiation happens
        // again when the stream consumes a replacement generation.
        let provider =
            compiler
                .plan(provider_expression)
                .into_parts()
                .0
                .filter(|predicate| {
                    !matches!(predicate, RuntimePruningPredicate::AlwaysTrue)
                });
        self.provider_generation += 1;
        let slot = self
            .provider_evolving
            .as_mut()
            .expect("evolving filters own a provider predicate slot");
        match provider.as_ref() {
            Some(predicate) => slot.replace(self.provider_generation, predicate),
            None => slot.clear(self.provider_generation),
        }
        Ok(true)
    }

    fn rebuild_batch_predicate(&mut self) {
        self.batch_predicate = conjunction_opt(
            self.fixed_batch_residual
                .iter()
                .chain(self.current_evolving.iter())
                .cloned(),
        );
    }

    fn fixed_residual(
        fixed: Option<&Arc<dyn PhysicalExpr>>,
        exact_residual: Option<Arc<dyn PhysicalExpr>>,
        support: Option<PredicateSupport>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        match support {
            Some(PredicateSupport::Exact) => exact_residual,
            Some(PredicateSupport::Conservative) | None => fixed.cloned(),
        }
    }
}
