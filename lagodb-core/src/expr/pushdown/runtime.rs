//! Shared Begin/ReScan binding state for planned provider predicates.

use pgrx::pg_sys;

use super::{
    BoundFilter, BoundFilterSet, FilterBindResult, FilterPushdown,
    PlannedFilterRecord,
};
use crate::expr::contract::PushdownContract;
use crate::expr::{
    RuntimeValue, RuntimeValueBindings, RuntimeValueLayout, RuntimeValueSpec,
    RuntimeValueState, RuntimeValueStateError,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum RelationFilterBindingError<E> {
    #[error("planned filter binding expression count does not match its metadata")]
    BindingCountMismatch,
    #[error("provider failed to bind a planned filter: {0}")]
    Provider(E),
    #[error(
        "Exact planned filter {filter_index} rejected a runtime value; the provider binder is not total for the accepted PostgreSQL type"
    )]
    ExactValueNotRepresentable { filter_index: usize },
}

impl<E> From<RuntimeValueStateError> for RelationFilterBindingError<E> {
    fn from(_: RuntimeValueStateError) -> Self {
        Self::BindingCountMismatch
    }
}

pub(crate) struct RelationFilterBinding<P: FilterPushdown> {
    planned: Vec<PlannedFilterRecord<P::PlannedPredicate>>,
    values: RuntimeValueState,
    stable_records: Box<[bool]>,
    bound: Vec<Option<BoundFilter<P::BoundPredicate>>>,
    pending_bound: Vec<(usize, Option<BoundFilter<P::BoundPredicate>>)>,
}

impl<P: FilterPushdown> RelationFilterBinding<P> {
    /// # Safety
    ///
    /// `binding_exprs` is a live plan-owned `List<Expr>`, and `parent` is the
    /// CustomScan/ForeignScan PlanState that owns the initialized ExprStates.
    pub(crate) unsafe fn initialize(
        planned: Vec<PlannedFilterRecord<P::PlannedPredicate>>,
        binding_metadata: Vec<RuntimeValueSpec>,
        binding_exprs: *mut pg_sys::List,
        parent: *mut pg_sys::PlanState,
    ) -> Result<Self, RelationFilterBindingError<P::Error>> {
        let expression_count = unsafe { pg_sys::list_length(binding_exprs) as usize };
        Self::validate_binding_count(expression_count, binding_metadata.len())?;
        let stable_records = planned
            .iter()
            .map(|filter| {
                binding_metadata[filter.binding_range.clone()]
                    .iter()
                    .all(|value| value.source_kind.is_rescan_stable())
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let dynamic_count = stable_records.iter().filter(|&&stable| !stable).count();
        let values = unsafe {
            RuntimeValueState::initialize(
                RuntimeValueLayout::new(binding_metadata.into_boxed_slice()),
                binding_exprs,
                parent,
            )
        }?;
        Ok(Self {
            planned,
            values,
            stable_records,
            bound: Vec::new(),
            pending_bound: Vec::with_capacity(dynamic_count),
        })
    }

    /// Bind records whose values are valid for the whole scan before provider
    /// initialization. Dynamic executor parameters are deliberately left
    /// untouched until the scan is started.
    ///
    /// # Safety
    ///
    /// `econtext` is the live executor ExprContext belonging to `parent` used
    /// during initialization.
    pub(crate) unsafe fn bind_stable(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), RelationFilterBindingError<P::Error>> {
        debug_assert!(self.values.values().is_empty());
        debug_assert!(self.bound.is_empty());
        self.bound = (0..self.planned.len()).map(|_| None).collect();
        for (filter_index, filter) in self.planned.iter().enumerate() {
            if !self.stable_records[filter_index] {
                continue;
            }
            let range = filter.binding_range.clone();
            let metadata = &self.values.layout().values()[range.clone()];
            let values = unsafe { self.values.evaluate_range(range, econtext) };
            self.bound[filter_index] =
                Self::bind_record(filter_index, filter, metadata, &values)?;
        }
        Ok(())
    }

    /// Bind all records for executor paths whose parameters are already valid
    /// at their Begin callback.
    pub(crate) unsafe fn bind_initial(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), RelationFilterBindingError<P::Error>> {
        unsafe { self.bind_stable(econtext) }?;
        unsafe { self.bind_dynamic_initial(econtext) }
    }

    /// Bind the records that depend on `PARAM_EXEC` or outer-tuple values once
    /// PostgreSQL has supplied the first valid parameter set.
    ///
    /// # Safety
    ///
    /// `econtext` is the live executor ExprContext belonging to `parent` used
    /// during initialization.
    pub(crate) unsafe fn bind_dynamic_initial(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), RelationFilterBindingError<P::Error>> {
        debug_assert_eq!(self.bound.len(), self.planned.len());
        debug_assert!(self.values.values().is_empty());
        if !self.values.has_dynamic_values() {
            return Ok(());
        }
        unsafe { self.values.bind_initial(econtext) };
        Self::bind_dynamic_records(
            &self.planned,
            &self.stable_records,
            self.values.layout().values(),
            self.values.values(),
            &mut self.pending_bound,
        )?;
        for (filter_index, replacement) in self.pending_bound.drain(..) {
            self.bound[filter_index] = replacement;
        }
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
    ) -> Result<(), RelationFilterBindingError<P::Error>> {
        if !self.values.has_dynamic_values() {
            return Ok(());
        }
        unsafe { self.values.rebind_dynamic(econtext) };
        Self::bind_dynamic_records(
            &self.planned,
            &self.stable_records,
            self.values.layout().values(),
            self.values.values(),
            &mut self.pending_bound,
        )?;

        for (filter_index, replacement) in self.pending_bound.drain(..) {
            self.bound[filter_index] = replacement;
        }
        Ok(())
    }

    fn bind_record(
        filter_index: usize,
        filter: &PlannedFilterRecord<P::PlannedPredicate>,
        binding_metadata: &[RuntimeValueSpec],
        values: &[RuntimeValue],
    ) -> Result<
        Option<BoundFilter<P::BoundPredicate>>,
        RelationFilterBindingError<P::Error>,
    > {
        let result =
            P::bind_filter(&filter.planned, RuntimeValueBindings::new(values))
                .map_err(RelationFilterBindingError::Provider)?;
        match result {
            FilterBindResult::Bound(predicate) => Ok(Some(BoundFilter {
                predicate,
                rescan_stable: binding_metadata
                    .iter()
                    .all(|value| value.source_kind.is_rescan_stable()),
                static_values: binding_metadata
                    .iter()
                    .all(|value| value.source_kind.is_static()),
            })),
            FilterBindResult::ValueNotRepresentable
                if filter.contract.requires_residual() =>
            {
                Ok(None)
            }
            FilterBindResult::ValueNotRepresentable => {
                Err(RelationFilterBindingError::ExactValueNotRepresentable {
                    filter_index,
                })
            }
        }
    }

    fn bind_dynamic_records(
        planned: &[PlannedFilterRecord<P::PlannedPredicate>],
        stable_records: &[bool],
        binding_metadata: &[RuntimeValueSpec],
        values: &[RuntimeValue],
        pending: &mut Vec<(usize, Option<BoundFilter<P::BoundPredicate>>)>,
    ) -> Result<(), RelationFilterBindingError<P::Error>> {
        pending.clear();
        for (filter_index, filter) in planned.iter().enumerate() {
            if stable_records[filter_index] {
                continue;
            }
            let range = filter.binding_range.clone();
            match Self::bind_record(
                filter_index,
                filter,
                &binding_metadata[range.clone()],
                &values[range],
            ) {
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
        unsafe { self.values.values_changed(chg_param) }
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

    fn validate_binding_count(
        expression_count: usize,
        metadata_count: usize,
    ) -> Result<(), RelationFilterBindingError<P::Error>> {
        if expression_count == metadata_count {
            Ok(())
        } else {
            Err(RelationFilterBindingError::BindingCountMismatch)
        }
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
        FilterPlan, FilterPlanningContext, FilterPushdownPlanner, PredicateFragment,
    };
    use crate::expr::{ExprType, RuntimeValueSource};

    type BoundFilterSlots<P> =
        Vec<Option<BoundFilter<<P as FilterPushdown>::BoundPredicate>>>;

    type RuntimeFilterResult<P, T> =
        Result<T, RelationFilterBindingError<<P as FilterPushdown>::Error>>;

    fn bind_values<P: FilterPushdown>(
        planned: &[PlannedFilterRecord<P::PlannedPredicate>],
        binding_metadata: &[RuntimeValueSpec],
        values: &[RuntimeValue],
    ) -> RuntimeFilterResult<P, BoundFilterSlots<P>> {
        planned
            .iter()
            .enumerate()
            .map(|(filter_index, filter)| {
                let range = filter.binding_range.clone();
                RelationFilterBinding::<P>::bind_record(
                    filter_index,
                    filter,
                    &binding_metadata[range.clone()],
                    &values[range],
                )
            })
            .collect()
    }

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
        Error,
    }

    struct TestPlanner;

    impl FilterPushdownPlanner for TestPlanner {
        type PlannedPredicate = BindBehavior;
        type Error = TestError;

        fn try_plan_filter(
            &mut self,
            _fragment: &PredicateFragment,
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
            values: RuntimeValueBindings<'_>,
        ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
            match predicate {
                BindBehavior::Bound(value) => {
                    assert!(values.is_empty());
                    Ok(FilterBindResult::Bound(*value))
                }
                BindBehavior::Counted(counter, value) => {
                    counter.fetch_add(1, Ordering::Relaxed);
                    Ok(FilterBindResult::Bound(*value))
                }
                BindBehavior::ValueNotRepresentable => {
                    Ok(FilterBindResult::ValueNotRepresentable)
                }
                BindBehavior::Error => Err(TestError),
            }
        }
    }

    fn value_slot(source_kind: RuntimeValueSource) -> RuntimeValueSpec {
        RuntimeValueSpec {
            value_type: ExprType {
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

        let bound = bind_values::<TestProvider>(&filters, &[], &[])
            .expect("Conservative binding may omit an unrepresentable predicate");

        assert_eq!(bound.iter().filter(|entry| entry.is_some()).count(), 1);
        assert_eq!(bound[0].as_ref().map(|entry| entry.predicate), Some(7));
        assert!(bound[1].is_none());
    }

    #[test]
    fn binding_expression_count_must_match_metadata() {
        RelationFilterBinding::<TestProvider>::validate_binding_count(2, 2)
            .expect("matching binding counts must be accepted");
        let error =
            RelationFilterBinding::<TestProvider>::validate_binding_count(1, 2)
                .expect_err("mismatched binding counts must be rejected");

        assert!(matches!(
            error,
            RelationFilterBindingError::BindingCountMismatch
        ));
    }

    #[test]
    fn provider_binding_error_is_preserved() {
        let filters = [planned(
            BindBehavior::Error,
            PushdownContract::ExactRowFilter,
        )];

        let error = match bind_values::<TestProvider>(&filters, &[], &[]) {
            Err(error) => error,
            Ok(_) => {
                panic!(
                    "provider binding error was not preserved as RelationFilterBindingError::Provider"
                )
            }
        };

        assert!(matches!(
            error,
            RelationFilterBindingError::Provider(TestError)
        ));
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

        let error = match bind_values::<TestProvider>(&filters, &[], &[]) {
            Err(error) => error,
            Ok(_) => panic!("Exact binding accepted an unrepresentable value"),
        };

        assert!(matches!(
            error,
            RelationFilterBindingError::ExactValueNotRepresentable {
                filter_index: 1
            }
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
            value_slot(RuntimeValueSource::Constant),
            value_slot(RuntimeValueSource::ExecParam),
        ];
        let values = metadata.map(|metadata| unsafe {
            RuntimeValue::from_raw(pg_sys::Datum::from(1usize), false, metadata)
        });
        let mut pending = Vec::new();

        RelationFilterBinding::<TestProvider>::bind_dynamic_records(
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
