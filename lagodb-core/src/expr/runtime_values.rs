//! Executor-owned evaluation state for plan-time runtime value expressions.

use core::{mem, ops::Range};

use pgrx::pg_sys;

use crate::expr::execution::RuntimeParamRefs;
use crate::expr::{RuntimeValue, RuntimeValueLayout, RuntimeValueSpec};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeValueStateError {
    #[error("runtime-value expression count does not match its layout")]
    ExpressionCountMismatch,
}

/// Shared Begin/ReScan value evaluator. Provider predicate binding and query
/// execution both consume the same global evaluation/layout.
pub struct RuntimeValueState {
    layout: RuntimeValueLayout,
    expr_states: *mut pg_sys::List,
    values: Vec<RuntimeValue>,
    pending: Vec<RuntimeValue>,
    dynamic_slots: Box<[usize]>,
    param_refs: RuntimeParamRefs,
}

impl RuntimeValueState {
    /// # Safety
    /// `expressions` is plan-owned and `parent` owns the initialized states.
    pub unsafe fn initialize(
        layout: RuntimeValueLayout,
        expressions: *mut pg_sys::List,
        parent: *mut pg_sys::PlanState,
    ) -> Result<Self, RuntimeValueStateError> {
        let expression_count = unsafe { pg_sys::list_length(expressions) as usize };
        if expression_count != layout.len() {
            return Err(RuntimeValueStateError::ExpressionCountMismatch);
        }
        let expr_states = unsafe { pg_sys::ExecInitExprList(expressions, parent) };
        let mut param_refs =
            unsafe { RuntimeParamRefs::collect_from_list(expressions) };
        let estate = unsafe { (*parent).state };
        let query_context = unsafe { (*estate).es_query_cxt };
        unsafe { param_refs.relocate_exec_param_ids_to(query_context) };
        let dynamic_slots = layout
            .values()
            .iter()
            .enumerate()
            .filter_map(|(index, value)| {
                (!value.source_kind.is_rescan_stable()).then_some(index)
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            layout,
            expr_states,
            values: Vec::with_capacity(expression_count),
            pending: Vec::with_capacity(expression_count),
            dynamic_slots,
            param_refs,
        })
    }

    #[inline]
    pub fn layout(&self) -> &RuntimeValueLayout {
        &self.layout
    }

    #[inline]
    pub fn values(&self) -> &[RuntimeValue] {
        &self.values
    }

    #[inline]
    pub fn has_dynamic_values(&self) -> bool {
        !self.dynamic_slots.is_empty()
    }

    /// Evaluate an isolated stable range without touching dynamic expressions.
    pub(crate) unsafe fn evaluate_range(
        &self,
        range: Range<usize>,
        econtext: *mut pg_sys::ExprContext,
    ) -> Vec<RuntimeValue> {
        range
            .map(|index| unsafe {
                self.evaluate(index, self.layout.values()[index], econtext)
            })
            .collect()
    }

    /// Evaluate statement-stable values and install typed NULL placeholders
    /// for dynamic values. A nested-loop outer parameter is not valid during
    /// `BeginCustomScan`; its placeholder is used only to compile and validate
    /// the immutable query template's output schema. [`Self::rebind_dynamic`]
    /// replaces every placeholder before the first run is opened.
    ///
    /// # Safety
    ///
    /// `econtext` must be live and valid for the expression states initialized
    /// by [`Self::initialize`], with every referenced parameter and tuple slot
    /// populated for stable expressions. Dynamic expression inputs need not be
    /// populated until the first execution call.
    pub unsafe fn bind_initial_template(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) {
        self.values.clear();
        for (index, &metadata) in self.layout.values().iter().enumerate() {
            if metadata.source_kind.is_rescan_stable() {
                self.values
                    .push(unsafe { self.evaluate(index, metadata, econtext) });
            } else {
                self.values.push(unsafe {
                    RuntimeValue::from_raw(
                        pg_sys::Datum::from(0usize),
                        true,
                        metadata,
                    )
                });
            }
        }
        self.pending.clone_from(&self.values);
    }

    /// Evaluate the complete layout when every dynamic input is already
    /// available, as required by relation-scan filter startup.
    ///
    /// # Safety
    ///
    /// `econtext` must be live for all initialized expression states and have
    /// every referenced parameter and tuple slot populated.
    pub unsafe fn bind_initial(&mut self, econtext: *mut pg_sys::ExprContext) {
        self.values.clear();
        for (index, &metadata) in self.layout.values().iter().enumerate() {
            self.values
                .push(unsafe { self.evaluate(index, metadata, econtext) });
        }
        self.pending.clone_from(&self.values);
    }

    /// Re-evaluate only values whose parameter dependencies changed.
    ///
    /// # Safety
    ///
    /// [`Self::bind_initial_template`] must have completed, and `econtext` must be live
    /// and valid for the initialized expression states with every referenced
    /// parameter and tuple slot populated for this rescan.
    pub unsafe fn rebind_dynamic(&mut self, econtext: *mut pg_sys::ExprContext) {
        for &index in self.dynamic_slots.iter() {
            let metadata = self.layout.values()[index];
            self.pending[index] = unsafe { self.evaluate(index, metadata, econtext) };
        }
        mem::swap(&mut self.values, &mut self.pending);
    }

    /// Atomically re-evaluate the complete layout for a query-plan rebuild.
    /// PostgreSQL resets the expression context before `ReScanCustomScan`, so
    /// even a statement-stable by-reference Datum must be refreshed before a
    /// newly compiled physical plan reads it.
    ///
    /// # Safety
    ///
    /// [`Self::bind_initial_template`] must have completed and `econtext` must
    /// have every referenced parameter and tuple slot populated.
    pub unsafe fn rebind_complete(&mut self, econtext: *mut pg_sys::ExprContext) {
        for (index, &metadata) in self.layout.values().iter().enumerate() {
            self.pending[index] = unsafe { self.evaluate(index, metadata, econtext) };
        }
        mem::swap(&mut self.values, &mut self.pending);
    }

    unsafe fn evaluate(
        &self,
        index: usize,
        metadata: RuntimeValueSpec,
        econtext: *mut pg_sys::ExprContext,
    ) -> RuntimeValue {
        let state = unsafe { pg_sys::list_nth(self.expr_states, index as i32) }
            as *mut pg_sys::ExprState;
        let mut is_null = false;
        let datum = unsafe {
            pg_sys::ExecEvalExprSwitchContext(state, econtext, &mut is_null)
        };
        unsafe { RuntimeValue::from_raw(datum, is_null, metadata) }
    }

    /// # Safety
    /// `chg_param` is NULL or the current PlanState bitmap.
    pub unsafe fn values_changed(&self, chg_param: *mut pg_sys::Bitmapset) -> bool {
        unsafe { self.param_refs.changed(chg_param) }
    }
}
