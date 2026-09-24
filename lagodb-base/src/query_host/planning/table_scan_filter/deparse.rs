//! Planner-phase filter rendering using query-local runtime parameter numbers.

use std::ffi::{CString, c_void};
use std::ptr;

use lagodb_core::expr::explain::deparse_and_join;
use pgrx::{pg_guard, pg_sys};

use crate::query_host::error::QueryHostError;

use super::super::expression::ScopedRuntimeBinding;

pub(super) struct FilterDeparser<'a> {
    bindings: &'a [ScopedRuntimeBinding],
    root: *mut pg_sys::PlannerInfo,
    range_table_index: pg_sys::Index,
    context: *mut pg_sys::List,
    missing_binding: bool,
}

impl<'a> FilterDeparser<'a> {
    pub(super) unsafe fn new(
        bindings: &'a [ScopedRuntimeBinding],
        root: *mut pg_sys::PlannerInfo,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
    ) -> Self {
        let entry = unsafe { &*range_table_entry };
        let context = unsafe {
            pg_sys::deparse_context_for((*entry.eref).aliasname, entry.relid)
        };
        Self {
            bindings,
            root,
            range_table_index,
            context,
            missing_binding: false,
        }
    }

    pub(super) unsafe fn deparse(
        &mut self,
        source: *mut pg_sys::Node,
    ) -> Result<CString, QueryHostError> {
        // ruleutils' PARAM_EXEC deparser needs a completed Plan, unavailable
        // during path construction. Like postgres_fdw's deparseParam, number
        // parameters in the host's binding catalog instead. These copied
        // PARAM_EXTERN nodes are presentation-only: $N denotes Runtime
        // Binding N, and never changes the executable source expression.
        let expression = unsafe { Self::rewrite(source, ptr::from_mut(self).cast()) };
        if self.missing_binding {
            return Err(QueryHostError::invalid_plan(
                "table-scan filter parameter has no query runtime binding",
            ));
        }
        unsafe {
            pg_sys::ChangeVarNodes(expression, self.range_table_index as i32, 1, 0);
        }
        let expressions = if unsafe { (*expression).type_ } == pg_sys::NodeTag::T_List
        {
            let list = expression.cast::<pg_sys::List>();
            let count = unsafe { pg_sys::list_length(list) };
            (0..count)
                .map(|index| unsafe { pg_sys::list_nth(list, index) }.cast())
                .collect::<Vec<*mut pg_sys::Expr>>()
        } else {
            vec![expression.cast()]
        };
        unsafe { deparse_and_join(self.context, expressions) }.ok_or_else(|| {
            QueryHostError::invalid_plan(
                "PostgreSQL could not deparse a table-scan filter",
            )
        })
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn rewrite(
        node: *mut pg_sys::Node,
        context: *mut c_void,
    ) -> *mut pg_sys::Node {
        // expression_tree_mutator calls the callback for absent child nodes.
        if node.is_null() {
            return ptr::null_mut();
        }
        let deparser = unsafe { &mut *context.cast::<FilterDeparser<'_>>() };
        let tag = unsafe { (*node).type_ };
        // Lowering registers the complete runtime expression, including any
        // RelabelType. Match that boundary before descending into its children;
        // comparing the unwrapped Param would lose the binding's identity and
        // result type. The scan root disambiguates lifted SubPlan namespaces;
        // position() still returns the query-global binding number.
        if let Some(index) = deparser.bindings.iter().position(|binding| unsafe {
            binding.matches_runtime_expression(deparser.root, node)
        }) {
            let parameter = unsafe {
                pg_sys::palloc0(size_of::<pg_sys::Param>()).cast::<pg_sys::Param>()
            };
            unsafe {
                (*parameter).xpr.type_ = pg_sys::NodeTag::T_Param;
                (*parameter).paramkind = pg_sys::ParamKind::PARAM_EXTERN;
                (*parameter).paramid = (index + 1) as i32;
                (*parameter).paramtype = pg_sys::exprType(node);
                (*parameter).paramtypmod = pg_sys::exprTypmod(node);
                (*parameter).paramcollid = pg_sys::exprCollation(node);
                (*parameter).location = -1;
            }
            return parameter.cast();
        }
        if tag == pg_sys::NodeTag::T_Param {
            deparser.missing_binding = true;
        }
        unsafe {
            pg_sys::expression_tree_mutator_impl(
                node,
                Some(FilterDeparser::rewrite),
                context,
            )
        }
    }
}
