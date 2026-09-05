//! PostgreSQL executor state owned by one physical UDF expression.

use std::cell::Cell;
use std::ffi::c_void;
use std::ptr;
use std::thread;

use pgrx::{pg_guard, pg_sys};

use crate::plan::{PostgresEvalExpr, PostgresEvalInput};

thread_local! {
    static SUPPRESS_EXPLICIT_CLEANUP: Cell<bool> = const { Cell::new(false) };
}

pub(in crate::datafusion) fn without_pg_cleanup<R>(
    operation: impl FnOnce() -> R,
) -> R {
    struct Restore(bool);

    impl Drop for Restore {
        fn drop(&mut self) {
            SUPPRESS_EXPLICIT_CLEANUP.set(self.0);
        }
    }

    SUPPRESS_EXPLICIT_CLEANUP.with(|suppressed| {
        let restore = Restore(suppressed.replace(true));
        let result = operation();
        drop(restore);
        result
    })
}

fn explicit_cleanup_is_suppressed() -> bool {
    SUPPRESS_EXPLICIT_CLEANUP.get()
}

struct SyntheticExpressionValidator<'a> {
    inputs: &'a [PostgresEvalInput],
    valid: bool,
}

#[pg_guard]
unsafe extern "C-unwind" fn validate_synthetic_expression(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let validator =
        unsafe { &mut *context.cast::<SyntheticExpressionValidator<'_>>() };
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_Var => {
            let var = unsafe { &*node.cast::<pg_sys::Var>() };
            let input = usize::try_from(var.varattno)
                .ok()
                .and_then(|attno| attno.checked_sub(1))
                .and_then(|index| validator.inputs.get(index));
            validator.valid = input.is_some_and(|input| {
                let value_type = input.value_type();
                var.varno == pg_sys::INNER_VAR
                    && var.varlevelsup == 0
                    && var.vartype == value_type.type_oid
                    && var.vartypmod == value_type.typmod
                    && var.varcollid == value_type.collation
            });
            !validator.valid
        }
        pg_sys::NodeTag::T_Param
        | pg_sys::NodeTag::T_Aggref
        | pg_sys::NodeTag::T_SubPlan
        | pg_sys::NodeTag::T_AlternativeSubPlan
        | pg_sys::NodeTag::T_WindowFunc
        | pg_sys::NodeTag::T_GroupingFunc => {
            validator.valid = false;
            true
        }
        pg_sys::NodeTag::T_FuncExpr
            if unsafe { (*node.cast::<pg_sys::FuncExpr>()).funcretset } =>
        {
            validator.valid = false;
            true
        }
        _ => unsafe {
            pg_sys::expression_tree_walker(
                node,
                Some(validate_synthetic_expression),
                context,
            )
        },
    }
}

pub(super) struct PgExprState {
    pub(super) expr: *mut pg_sys::ExprState,
    pub(super) estate: *mut pg_sys::EState,
    pub(super) econtext: *mut pg_sys::ExprContext,
    pub(super) slot: *mut pg_sys::TupleTableSlot,
    tuple_desc: pg_sys::TupleDesc,
}

/// PostgreSQL statement context shared by every fallback expression in one
/// query execution.
///
/// This is runtime-only state: plan data never contains a PostgreSQL pointer.
/// The compiled physical plan owns copies of this handle. Normal executor close
/// releases the physical plan before the outer executor tears down the context;
/// abort cleanup suppresses PostgreSQL calls while dropping already-reclaimed
/// evaluator wrappers.
#[derive(Clone, Copy)]
pub(in crate::datafusion) struct PgExprRuntime {
    statement_context: pg_sys::MemoryContext,
}

impl PgExprRuntime {
    /// Bind fallback evaluation to the executor statement that owns `parent`.
    ///
    /// # Safety
    ///
    /// `parent` must be a live PostgreSQL `PlanState`. The resulting runtime
    /// handle must not be used to initialize an evaluator after the plan
    /// state's executor context begins teardown. Physical expressions dropped
    /// during abort teardown must use [`without_pg_cleanup`].
    pub(in crate::datafusion) unsafe fn from_plan_state(
        parent: *mut pg_sys::PlanState,
    ) -> Self {
        Self {
            statement_context: unsafe { (*(*parent).state).es_query_cxt },
        }
    }

    /// Initialize one expression evaluator below the owning statement context.
    ///
    /// # Safety
    ///
    /// The statement context must still be live, and this call must run on the
    /// PostgreSQL backend thread that created the runtime handle.
    pub(super) unsafe fn initialize(
        self,
        expression: &PostgresEvalExpr,
    ) -> Result<PgExprState, String> {
        let input_count = i32::try_from(expression.inputs().len())
            .ok()
            .filter(|count| *count <= pg_sys::MaxTupleAttributeNumber as i32)
            .ok_or_else(|| {
                "PostgreSQL expression has too many synthetic inputs".to_owned()
            })?;
        let _statement_context =
            unsafe { MemoryContextSwitchGuard::enter(self.statement_context) };
        let estate = unsafe { pg_sys::CreateExecutorState() };
        let initialized = {
            let _query_context =
                unsafe { MemoryContextSwitchGuard::enter((*estate).es_query_cxt) };
            unsafe {
                PgExprState::initialize_in_query_context(
                    expression,
                    input_count,
                    estate,
                )
            }
        };
        if initialized.is_err() {
            unsafe { pg_sys::FreeExecutorState(estate) };
        }
        initialized
    }
}

// SAFETY: the handle is copied into DataFusion's Send/Sync expression graph,
// but the query uses a current-thread runtime with one partition. Every access
// and Drop therefore stays on the backend thread. Normal close releases the
// physical plan before the referenced statement context; abort teardown drops
// its Rust wrappers under `without_pg_cleanup` and does not dereference their
// PostgreSQL pointers. Increasing partitions or moving execution to another
// thread invalidates this proof.
unsafe impl Send for PgExprRuntime {}
unsafe impl Sync for PgExprRuntime {}

/// Restores PostgreSQL's prior current memory context during both normal
/// return and pgrx's ERROR unwind.
pub(super) struct MemoryContextSwitchGuard {
    previous: pg_sys::MemoryContext,
}

impl MemoryContextSwitchGuard {
    unsafe fn enter(context: pg_sys::MemoryContext) -> Self {
        Self {
            previous: unsafe { pg_sys::MemoryContextSwitchTo(context) },
        }
    }
}

impl Drop for MemoryContextSwitchGuard {
    fn drop(&mut self) {
        unsafe {
            pg_sys::MemoryContextSwitchTo(self.previous);
        }
    }
}

// SAFETY: query execution uses a current-thread Tokio runtime and exactly one
// DataFusion partition inside the PostgreSQL backend. Increasing partitions or
// moving query tasks to another thread invalidates this proof and requires
// isolated PostgreSQL executor state.
unsafe impl Send for PgExprState {}
unsafe impl Sync for PgExprState {}

impl PgExprState {
    unsafe fn initialize_in_query_context(
        expression: &PostgresEvalExpr,
        input_count: i32,
        estate: *mut pg_sys::EState,
    ) -> Result<Self, String> {
        let expression_node = unsafe {
            pg_sys::stringToNode(expression.serialized().as_ptr().cast_mut())
                .cast::<pg_sys::Expr>()
        };
        if expression_node.is_null() {
            return Err(
                "PostgreSQL could not restore the serialized expression".to_owned()
            );
        }
        let result_type = expression.result_type();
        let mut validator = SyntheticExpressionValidator {
            inputs: expression.inputs(),
            valid: !unsafe { pg_sys::expression_returns_set(expression_node.cast()) }
                && unsafe { pg_sys::exprType(expression_node.cast()) }
                    == result_type.type_oid
                && unsafe { pg_sys::exprTypmod(expression_node.cast()) }
                    == result_type.typmod
                && unsafe { pg_sys::exprCollation(expression_node.cast()) }
                    == result_type.collation,
        };
        if validator.valid {
            unsafe {
                validate_synthetic_expression(
                    expression_node.cast(),
                    ptr::from_mut(&mut validator).cast(),
                );
            }
        }
        if !validator.valid {
            return Err(
                "serialized PostgreSQL expression contains invalid synthetic inputs"
                    .to_owned(),
            );
        }
        let tuple_desc = unsafe { pg_sys::CreateTemplateTupleDesc(input_count) };
        for (index, input) in expression.inputs().iter().enumerate() {
            let value_type = input.value_type();
            let attno = pg_sys::AttrNumber::try_from(index + 1).map_err(|_| {
                "PostgreSQL expression synthetic input index is out of range"
                    .to_owned()
            })?;
            unsafe {
                pg_sys::TupleDescInitEntry(
                    tuple_desc,
                    attno,
                    ptr::null(),
                    value_type.type_oid,
                    value_type.typmod,
                    0,
                );
                pg_sys::TupleDescInitEntryCollation(
                    tuple_desc,
                    attno,
                    value_type.collation,
                );
            }
        }
        let slot = unsafe {
            pg_sys::MakeSingleTupleTableSlot(tuple_desc, &pg_sys::TTSOpsVirtual)
        };
        let econtext = unsafe { pg_sys::CreateExprContext(estate) };
        unsafe { (*econtext).ecxt_innertuple = slot };
        let expr = unsafe { pg_sys::ExecInitExpr(expression_node, ptr::null_mut()) };
        Ok(Self {
            expr,
            estate,
            econtext,
            slot,
            tuple_desc,
        })
    }

    pub(super) unsafe fn enter_tuple_context(&self) -> MemoryContextSwitchGuard {
        unsafe {
            MemoryContextSwitchGuard::enter((*self.econtext).ecxt_per_tuple_memory)
        }
    }
}

impl Drop for PgExprState {
    fn drop(&mut self) {
        // PostgreSQL cleanup functions are not safe during Rust unwinding. The
        // ResourceOwner abort path also suppresses this normal-close sequence
        // after the unwind has ended. The outer executor's statement context
        // remains the abort-cleanup owner in either case.
        if thread::panicking() || explicit_cleanup_is_suppressed() {
            return;
        }
        unsafe {
            pg_sys::ExecDropSingleTupleTableSlot(self.slot);
            pg_sys::FreeTupleDesc(self.tuple_desc);
            pg_sys::FreeExprContext(self.econtext, true);
            pg_sys::FreeExecutorState(self.estate);
        }
    }
}
