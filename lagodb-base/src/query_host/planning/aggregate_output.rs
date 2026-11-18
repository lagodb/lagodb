//! Raw scan slots of an aggregate CustomScan.
//!
//! The CustomScan emits each grouping expression and Aggref the path target
//! reads exactly once, in `custom_scan_tlist`. PostgreSQL then evaluates the
//! path target over those slots: `set_customscan_references()` rewrites every
//! target subexpression equal to a scan-tlist entry into an `INDEX_VAR`
//! reference (`fix_upper_expr`), and `ExecInitCustomScan` projects the result.
//! A target such as `sum(a) / count(*)` or `g + 1` therefore needs no engine
//! expression of its own.

use std::ffi::c_int;

use lagodb_core::query_contract::OutputId;
use pgrx::pg_sys;

#[derive(Default)]
pub(super) struct AggregateScanSlots {
    /// Scan-tlist expression and the aggregate output it carries.
    slots: Vec<(*mut pg_sys::Expr, OutputId)>,
}

impl AggregateScanSlots {
    /// Add the slots one path-target expression reads. `group_output`
    /// resolves a grouping expression; `aggregate_output` resolves or builds
    /// an Aggref. A Var or PlaceHolderVar outside every grouping expression,
    /// or a GroupingFunc, has no slot and declines.
    ///
    /// # Safety
    ///
    /// `expression` must be a live expression of the current planner tree.
    pub(super) unsafe fn add_target(
        &mut self,
        expression: *mut pg_sys::Expr,
        group_output: impl Fn(*mut pg_sys::Expr) -> Option<OutputId>,
        mut aggregate_output: impl FnMut(*mut pg_sys::Expr) -> Option<OutputId>,
    ) -> Option<()> {
        if let Some(output) = group_output(expression) {
            self.push(expression, output);
            return Some(());
        }
        let references = unsafe {
            pg_sys::pull_var_clause(
                expression.cast(),
                (pg_sys::PVC_INCLUDE_AGGREGATES | pg_sys::PVC_INCLUDE_PLACEHOLDERS)
                    as c_int,
            )
        };
        for index in 0..unsafe { pg_sys::list_length(references) } {
            let reference =
                unsafe { pg_sys::list_nth(references, index) }.cast::<pg_sys::Expr>();
            let output = match unsafe { (*reference).type_ } {
                pg_sys::NodeTag::T_Aggref => aggregate_output(reference)?,
                pg_sys::NodeTag::T_Var | pg_sys::NodeTag::T_PlaceHolderVar => {
                    group_output(reference)?
                }
                _ => return None,
            };
            self.push(reference, output);
        }
        Some(())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// The slots in scan-tlist order.
    pub(super) fn iter(&self) -> impl Iterator<Item = (*mut pg_sys::Expr, OutputId)> {
        self.slots.iter().copied()
    }

    fn push(&mut self, expression: *mut pg_sys::Expr, output: OutputId) {
        if !self.slots.iter().any(|&(_, existing)| existing == output) {
            self.slots.push((expression, output));
        }
    }
}
