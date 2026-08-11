//! Shared Begin/ReScan binding state for planned provider predicates.

use pgrx::pg_sys;

use crate::expr::contract::PushdownContract;
use crate::expr::execution::RuntimeParamRefs;

use super::{
    BoundFilter, BoundFilterSet, FilterBindResult, FilterPushdown, FilterValue,
    FilterValueBindings, FilterValueSlot, PlannedFilterRecord,
};

type BoundFilterSlots<B> = Vec<Option<BoundFilter<B>>>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RuntimeFilterError<E> {
    #[error("planned filter binding expression count does not match its metadata")]
    BindingCountMismatch,
    #[error("provider failed to bind a planned filter: {0}")]
    Provider(E),
    #[error(
        "Exact planned filter {filter_index} rejected a runtime value; the provider binder is not total for the accepted PostgreSQL type"
    )]
    ExactValueNotRepresentable { filter_index: usize },
}

pub(crate) struct RuntimeFilterState<P: FilterPushdown> {
    planned: Vec<PlannedFilterRecord<P::PlannedPredicate>>,
    binding_metadata: Box<[FilterValueSlot]>,
    expr_states: *mut pg_sys::List,
    values: Vec<FilterValue>,
    pending_values: Vec<FilterValue>,
    dynamic_slots: Box<[usize]>,
    stable_records: Box<[bool]>,
    bound: Vec<Option<BoundFilter<P::BoundPredicate>>>,
    pending_bound: Vec<(usize, Option<BoundFilter<P::BoundPredicate>>)>,
    param_refs: RuntimeParamRefs,
}

impl<P: FilterPushdown> RuntimeFilterState<P> {
    /// # Safety
    ///
    /// `binding_exprs` is a live plan-owned `List<Expr>`, and `parent` is the
    /// CustomScan/ForeignScan PlanState that owns the initialized ExprStates.
    pub(crate) unsafe fn initialize(
        planned: Vec<PlannedFilterRecord<P::PlannedPredicate>>,
        binding_metadata: Vec<FilterValueSlot>,
        binding_exprs: *mut pg_sys::List,
        parent: *mut pg_sys::PlanState,
    ) -> Result<Self, RuntimeFilterError<P::Error>> {
        let expression_count = if binding_exprs.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(binding_exprs) as usize }
        };
        if expression_count != binding_metadata.len() {
            return Err(RuntimeFilterError::BindingCountMismatch);
        }
        let expr_states = unsafe { pg_sys::ExecInitExprList(binding_exprs, parent) };
        let mut param_refs =
            unsafe { RuntimeParamRefs::collect_from_list(binding_exprs) };
        let estate = unsafe { (*parent).state };
        let query_context = unsafe { (*estate).es_query_cxt };
        unsafe { param_refs.relocate_exec_param_ids_to(query_context) };
        let stable_records = planned
            .iter()
            .map(|filter| {
                binding_metadata[filter.binding_range.clone()]
                    .iter()
                    .all(|value| value.source_kind.is_rescan_stable())
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let dynamic_slots = binding_metadata
            .iter()
            .enumerate()
            .filter_map(|(index, value)| {
                (!value.source_kind.is_rescan_stable()).then_some(index)
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let dynamic_count = stable_records.iter().filter(|&&stable| !stable).count();
        Ok(Self {
            planned,
            binding_metadata: binding_metadata.into_boxed_slice(),
            expr_states,
            values: Vec::with_capacity(expression_count),
            pending_values: Vec::with_capacity(expression_count),
            dynamic_slots,
            stable_records,
            bound: Vec::new(),
            pending_bound: Vec::with_capacity(dynamic_count),
            param_refs,
        })
    }

    /// Evaluate every slot and bind every planned record once at Begin.
    ///
    /// # Safety
    ///
    /// `econtext` is the live executor ExprContext belonging to `parent` used
    /// during initialization.
    pub(crate) unsafe fn bind_initial(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), RuntimeFilterError<P::Error>> {
        debug_assert!(self.values.is_empty());
        debug_assert!(self.bound.is_empty());
        for (index, &metadata) in self.binding_metadata.iter().enumerate() {
            self.values.push(unsafe {
                Self::evaluate(self.expr_states, index, metadata, econtext)
            });
        }
        self.bound =
            Self::bind_values(&self.planned, &self.binding_metadata, &self.values)?;
        self.pending_values.clone_from(&self.values);
        Ok(())
    }

    /// Reevaluate dynamic slots and atomically replace dynamic records only.
    /// Stable records retain their Begin-time bound predicates.
    ///
    /// # Safety
    ///
    /// `econtext` is the live executor ExprContext used at initialization.
    pub(crate) unsafe fn rebind_dynamic(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), RuntimeFilterError<P::Error>> {
        debug_assert_eq!(self.values.len(), self.binding_metadata.len());
        debug_assert_eq!(self.pending_values.len(), self.values.len());

        for &index in self.dynamic_slots.iter() {
            let metadata = self.binding_metadata[index];
            self.pending_values[index] = unsafe {
                Self::evaluate(self.expr_states, index, metadata, econtext)
            };
        }

        Self::bind_dynamic_records(
            &self.planned,
            &self.stable_records,
            &self.binding_metadata,
            &self.pending_values,
            &mut self.pending_bound,
        )?;

        for (filter_index, replacement) in self.pending_bound.drain(..) {
            self.bound[filter_index] = replacement;
        }
        core::mem::swap(&mut self.values, &mut self.pending_values);
        Ok(())
    }

    unsafe fn evaluate(
        expr_states: *mut pg_sys::List,
        index: usize,
        metadata: FilterValueSlot,
        econtext: *mut pg_sys::ExprContext,
    ) -> FilterValue {
        let state = unsafe { pg_sys::list_nth(expr_states, index as i32) }
            as *mut pg_sys::ExprState;
        let mut is_null = false;
        let datum = unsafe {
            pg_sys::ExecEvalExprSwitchContext(state, econtext, &mut is_null)
        };
        unsafe { FilterValue::from_raw(datum, is_null, metadata) }
    }

    fn bind_values(
        planned: &[PlannedFilterRecord<P::PlannedPredicate>],
        binding_metadata: &[FilterValueSlot],
        values: &[FilterValue],
    ) -> Result<BoundFilterSlots<P::BoundPredicate>, RuntimeFilterError<P::Error>>
    {
        let mut replacement = Vec::with_capacity(planned.len());
        for (filter_index, filter) in planned.iter().enumerate() {
            replacement.push(Self::bind_record(
                filter_index,
                filter,
                binding_metadata,
                values,
            )?);
        }
        Ok(replacement)
    }

    fn bind_record(
        filter_index: usize,
        filter: &PlannedFilterRecord<P::PlannedPredicate>,
        binding_metadata: &[FilterValueSlot],
        values: &[FilterValue],
    ) -> Result<Option<BoundFilter<P::BoundPredicate>>, RuntimeFilterError<P::Error>>
    {
        let result = P::bind_filter(
            &filter.planned,
            FilterValueBindings::new(&values[filter.binding_range.clone()]),
        )
        .map_err(RuntimeFilterError::Provider)?;
        match result {
            FilterBindResult::Bound(predicate) => Ok(Some(BoundFilter {
                predicate,
                rescan_stable: binding_metadata[filter.binding_range.clone()]
                    .iter()
                    .all(|value| value.source_kind.is_rescan_stable()),
                static_values: binding_metadata[filter.binding_range.clone()]
                    .iter()
                    .all(|value| value.source_kind.is_static()),
            })),
            FilterBindResult::ValueNotRepresentable
                if filter.contract.requires_residual() =>
            {
                Ok(None)
            }
            FilterBindResult::ValueNotRepresentable => {
                Err(RuntimeFilterError::ExactValueNotRepresentable { filter_index })
            }
        }
    }

    fn bind_dynamic_records(
        planned: &[PlannedFilterRecord<P::PlannedPredicate>],
        stable_records: &[bool],
        binding_metadata: &[FilterValueSlot],
        values: &[FilterValue],
        pending: &mut Vec<(usize, Option<BoundFilter<P::BoundPredicate>>)>,
    ) -> Result<(), RuntimeFilterError<P::Error>> {
        pending.clear();
        for (filter_index, filter) in planned.iter().enumerate() {
            if stable_records[filter_index] {
                continue;
            }
            match Self::bind_record(filter_index, filter, binding_metadata, values) {
                Ok(bound) => pending.push((filter_index, bound)),
                Err(error) => {
                    pending.clear();
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// # Safety
    ///
    /// `chg_param` must be NULL or point to the current PlanState bitmap.
    pub(crate) unsafe fn filters_changed(
        &self,
        chg_param: *mut pg_sys::Bitmapset,
    ) -> bool {
        unsafe { self.param_refs.changed(chg_param) }
    }

    pub(crate) fn bound(&self) -> BoundFilterSet<'_, P::BoundPredicate> {
        BoundFilterSet::new(&self.bound)
    }

    pub(crate) fn recheck_count(&self) -> usize {
        self.contracts()
            .filter(|contract| contract.requires_recheck())
            .count()
    }

    pub(crate) fn contracts(
        &self,
    ) -> impl ExactSizeIterator<Item = PushdownContract> + '_ {
        self.planned.iter().map(|filter| filter.contract)
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{AtomicUsize, Ordering};

    use pgrx::prelude::PgSqlErrorCode;

    use crate::diag::SqlStateError;
    use crate::expr::contract::PushdownContract;
    use crate::plan_data::{PlanDataReader, PlanDataWriter};

    use super::*;
    use crate::expr::pushdown::{
        FilterFragment, FilterPlan, FilterPlanningContext, FilterPushdownPlanner,
        FilterTypeMetadata, FilterValueSourceKind,
    };

    #[derive(Debug, thiserror::Error)]
    #[error("runtime filter test error")]
    struct TestError;

    impl SqlStateError for TestError {
        fn sql_error_code(&self) -> PgSqlErrorCode {
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
        }
    }

    #[derive(Clone, Copy)]
    enum BindBehavior {
        Bound(u8),
        Counted(&'static AtomicUsize, u8),
        ValueNotRepresentable,
    }

    struct TestPlanner;

    impl FilterPushdownPlanner for TestPlanner {
        type PlannedPredicate = BindBehavior;
        type Error = TestError;

        fn try_plan_filter(
            &mut self,
            _fragment: &FilterFragment,
        ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
            Ok(FilterPlan::Unsupported)
        }
    }

    struct TestProvider;

    impl FilterPushdown for TestProvider {
        type Planner = TestPlanner;
        type PlannedPredicate = BindBehavior;
        type BoundPredicate = u8;
        type Error = TestError;

        fn begin_filter_planning(
            _context: &FilterPlanningContext,
        ) -> Result<Self::Planner, Self::Error> {
            Ok(TestPlanner)
        }

        fn encode_planned(
            _predicate: &Self::PlannedPredicate,
            _writer: &mut PlanDataWriter,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn decode_planned(
            _reader: &mut PlanDataReader<'_>,
            _binding_count: usize,
        ) -> Result<Self::PlannedPredicate, Self::Error> {
            Ok(BindBehavior::Bound(0))
        }

        fn bind_filter(
            predicate: &Self::PlannedPredicate,
            values: FilterValueBindings<'_>,
        ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
            Ok(match predicate {
                BindBehavior::Bound(value) => {
                    assert!(values.is_empty());
                    FilterBindResult::Bound(*value)
                }
                BindBehavior::Counted(counter, value) => {
                    counter.fetch_add(1, Ordering::Relaxed);
                    FilterBindResult::Bound(*value)
                }
                BindBehavior::ValueNotRepresentable => {
                    FilterBindResult::ValueNotRepresentable
                }
            })
        }
    }

    fn value_slot(source_kind: FilterValueSourceKind) -> FilterValueSlot {
        FilterValueSlot {
            value_type: FilterTypeMetadata {
                type_oid: pg_sys::INT4OID,
                typmod: -1,
                collation: pg_sys::Oid::INVALID,
            },
            source_kind,
        }
    }

    fn planned(
        behavior: BindBehavior,
        contract: PushdownContract,
    ) -> PlannedFilterRecord<BindBehavior> {
        PlannedFilterRecord {
            planned: behavior,
            contract,
            binding_range: 0..0,
        }
    }

    #[test]
    fn conservative_unrepresentable_value_omits_current_predicate() {
        let filters = [
            planned(BindBehavior::Bound(7), PushdownContract::ExactRowFilter),
            planned(
                BindBehavior::ValueNotRepresentable,
                PushdownContract::ConservativePruning,
            ),
        ];

        let bound =
            RuntimeFilterState::<TestProvider>::bind_values(&filters, &[], &[])
                .expect("Conservative binding may omit an unrepresentable predicate");

        assert_eq!(bound.iter().filter(|entry| entry.is_some()).count(), 1);
        assert_eq!(bound[0].as_ref().map(|entry| entry.predicate), Some(7));
        assert!(bound[1].is_none());
    }

    #[test]
    fn exact_unrepresentable_value_is_contract_violation() {
        let filters = [
            planned(BindBehavior::Bound(7), PushdownContract::ExactRowFilter),
            planned(
                BindBehavior::ValueNotRepresentable,
                PushdownContract::ExactRowFilter,
            ),
        ];

        let error =
            match RuntimeFilterState::<TestProvider>::bind_values(&filters, &[], &[])
            {
                Err(error) => error,
                Ok(_) => panic!("Exact binding accepted an unrepresentable value"),
            };

        assert!(matches!(
            error,
            RuntimeFilterError::ExactValueNotRepresentable { filter_index: 1 }
        ));
    }

    #[test]
    fn dynamic_rebind_skips_stable_records() {
        static STABLE_BINDS: AtomicUsize = AtomicUsize::new(0);
        static DYNAMIC_BINDS: AtomicUsize = AtomicUsize::new(0);
        STABLE_BINDS.store(0, Ordering::Relaxed);
        DYNAMIC_BINDS.store(0, Ordering::Relaxed);

        let mut stable = planned(
            BindBehavior::Counted(&STABLE_BINDS, 1),
            PushdownContract::ExactRowFilter,
        );
        stable.binding_range = 0..1;
        let mut dynamic = planned(
            BindBehavior::Counted(&DYNAMIC_BINDS, 2),
            PushdownContract::ExactRowFilter,
        );
        dynamic.binding_range = 1..2;
        let metadata = [
            value_slot(FilterValueSourceKind::Constant),
            value_slot(FilterValueSourceKind::ExecParam),
        ];
        let values = metadata.map(|metadata| unsafe {
            FilterValue::from_raw(pg_sys::Datum::from(1usize), false, metadata)
        });
        let mut pending = Vec::new();

        RuntimeFilterState::<TestProvider>::bind_dynamic_records(
            &[stable, dynamic],
            &[true, false],
            &metadata,
            &values,
            &mut pending,
        )
        .expect("dynamic records should bind");

        assert_eq!(STABLE_BINDS.load(Ordering::Relaxed), 0);
        assert_eq!(DYNAMIC_BINDS.load(Ordering::Relaxed), 1);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 1);
    }
}
