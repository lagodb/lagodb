//! Planning-time construction of exact PostgreSQL expression fallback nodes.

use std::ffi::{CStr, c_void};
use std::ptr;
use std::sync::Arc;

use lagodb_core::expr::ExprType;
use lagodb_query::plan::{
    ExecutionExpr, PostgresEvalExpr, PostgresEvalInput, PostgresExprVolatility,
};
use pgrx::{pg_guard, pg_sys};

struct DependencyBinding {
    node: *mut pg_sys::Node,
    slot_attno: pg_sys::AttrNumber,
    value_type: ExprType,
}

struct DependencyCollector {
    nodes: Vec<*mut pg_sys::Node>,
    invalid: bool,
}

impl DependencyCollector {
    unsafe fn collect(
        expression: *mut pg_sys::Node,
    ) -> Option<Vec<*mut pg_sys::Node>> {
        let mut collector = Self {
            nodes: Vec::new(),
            invalid: false,
        };
        unsafe {
            dependency_walker(expression, ptr::from_mut(&mut collector).cast());
        }
        (!collector.invalid).then_some(collector.nodes)
    }

    unsafe fn push_non_var_unique(&mut self, node: *mut pg_sys::Node) {
        if !self.nodes.iter().any(|candidate| unsafe {
            pg_sys::equal((*candidate).cast::<c_void>(), node.cast::<c_void>())
        }) {
            self.nodes.push(node);
        }
    }

    unsafe fn push_var(&mut self, node: *mut pg_sys::Node) {
        let var = node.cast::<pg_sys::Var>();
        let varno = unsafe { (*var).varno as usize };
        let Ok(attno) = usize::try_from(unsafe { (*var).varattno }) else {
            self.invalid = true;
            return;
        };
        if varno >= pg_sys::INNER_VAR as usize || attno == 0 {
            self.invalid = true;
            return;
        }
        // Source identity is resolved by QueryExpressionPlanner. Keep every
        // raw occurrence here; DependencyInputs performs the real dense
        // ScanId/attno deduplication after lowering.
        self.nodes.push(node);
    }
}

struct DependencyInputs {
    inputs: Vec<PostgresEvalInput>,
    columns_by_scan: Vec<Vec<Option<usize>>>,
    runtime_values: Vec<Option<usize>>,
    outputs: Vec<Option<usize>>,
}

#[derive(Clone, Copy)]
enum DependencyIdentity {
    Column { scan: usize, attno: usize },
    RuntimeValue(usize),
    Output(usize),
}

impl DependencyInputs {
    fn new() -> Self {
        Self {
            inputs: Vec::new(),
            columns_by_scan: Vec::new(),
            runtime_values: Vec::new(),
            outputs: Vec::new(),
        }
    }

    fn intern(
        &mut self,
        expression: ExecutionExpr,
        value_type: ExprType,
    ) -> Option<pg_sys::AttrNumber> {
        let identity = match &expression {
            ExecutionExpr::Column(column) => DependencyIdentity::Column {
                scan: column.scan.index(),
                attno: usize::try_from(column.attno).ok()?.checked_sub(1)?,
            },
            ExecutionExpr::Value(value) => {
                DependencyIdentity::RuntimeValue(value.index())
            }
            ExecutionExpr::DecimalValue { .. } => return None,
            ExecutionExpr::Output(output) => {
                DependencyIdentity::Output(output.index())
            }
            _ => return None,
        };
        let existing = match identity {
            DependencyIdentity::Column { scan, attno } => self
                .columns_by_scan
                .get(scan)
                .and_then(|columns| columns.get(attno))
                .copied()
                .flatten(),
            DependencyIdentity::RuntimeValue(value) => {
                self.runtime_values.get(value).copied().flatten()
            }
            DependencyIdentity::Output(output) => {
                self.outputs.get(output).copied().flatten()
            }
        };
        if let Some(index) = existing {
            if self.inputs[index].value_type() != value_type {
                return None;
            }
            return pg_sys::AttrNumber::try_from(index + 1).ok();
        }
        if self.inputs.len() >= pg_sys::MaxTupleAttributeNumber as usize {
            return None;
        }
        let index = self.inputs.len();
        self.inputs
            .push(PostgresEvalInput::new(expression, value_type));
        match identity {
            DependencyIdentity::Column { scan, attno } => {
                if self.columns_by_scan.len() <= scan {
                    self.columns_by_scan.resize_with(scan + 1, Vec::new);
                }
                let columns = &mut self.columns_by_scan[scan];
                if columns.len() <= attno {
                    columns.resize(attno + 1, None);
                }
                columns[attno] = Some(index);
            }
            DependencyIdentity::RuntimeValue(value) => {
                if self.runtime_values.len() <= value {
                    self.runtime_values.resize(value + 1, None);
                }
                self.runtime_values[value] = Some(index);
            }
            DependencyIdentity::Output(output) => {
                if self.outputs.len() <= output {
                    self.outputs.resize(output + 1, None);
                }
                self.outputs[output] = Some(index);
            }
        }
        pg_sys::AttrNumber::try_from(index + 1).ok()
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn dependency_walker(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let collector = unsafe { &mut *context.cast::<DependencyCollector>() };
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_Var => {
            unsafe { collector.push_var(node) };
            return false;
        }
        pg_sys::NodeTag::T_Param | pg_sys::NodeTag::T_Aggref => {
            unsafe { collector.push_non_var_unique(node) };
            return false;
        }
        pg_sys::NodeTag::T_SubPlan
        | pg_sys::NodeTag::T_AlternativeSubPlan
        | pg_sys::NodeTag::T_WindowFunc
        | pg_sys::NodeTag::T_GroupingFunc => {
            collector.invalid = true;
            return true;
        }
        pg_sys::NodeTag::T_FuncExpr
            if unsafe { (*node.cast::<pg_sys::FuncExpr>()).funcretset } =>
        {
            collector.invalid = true;
            return true;
        }
        _ => {}
    }
    unsafe { pg_sys::expression_tree_walker(node, Some(dependency_walker), context) }
}

struct RewriteContext<'a> {
    dependencies: &'a [DependencyBinding],
}

#[pg_guard]
unsafe extern "C-unwind" fn dependency_mutator(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> *mut pg_sys::Node {
    if node.is_null() {
        return ptr::null_mut();
    }
    let context = unsafe { &*context.cast::<RewriteContext<'_>>() };
    if let Some(dependency) = context.dependencies.iter().find(|dependency| unsafe {
        pg_sys::equal(dependency.node.cast::<c_void>(), node.cast::<c_void>())
    }) {
        let value_type = dependency.value_type;
        return unsafe {
            pg_sys::makeVar(
                pg_sys::INNER_VAR,
                dependency.slot_attno,
                value_type.type_oid,
                value_type.typmod,
                value_type.collation,
                0,
            )
            .cast()
        };
    }
    unsafe {
        pg_sys::expression_tree_mutator_impl(
            node,
            Some(dependency_mutator),
            ptr::from_ref(context).cast_mut().cast(),
        )
    }
}

struct RewrittenExpressionValidator<'a> {
    inputs: &'a [PostgresEvalInput],
    valid: bool,
}

#[pg_guard]
unsafe extern "C-unwind" fn rewritten_expression_walker(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let validator =
        unsafe { &mut *context.cast::<RewrittenExpressionValidator<'_>>() };
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_Var => {
            let var = unsafe { &*node.cast::<pg_sys::Var>() };
            let index = usize::try_from(var.varattno)
                .ok()
                .and_then(|attno| attno.checked_sub(1));
            let input = index.and_then(|index| validator.inputs.get(index));
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
        _ => unsafe {
            pg_sys::expression_tree_walker(
                node,
                Some(rewritten_expression_walker),
                context,
            )
        },
    }
}

pub(super) struct PostgresExpressionBuilder;

impl PostgresExpressionBuilder {
    pub(super) unsafe fn lower(
        expression: *mut pg_sys::Node,
        mut lower_dependency: impl FnMut(
            *mut pg_sys::Node,
        ) -> Option<(ExecutionExpr, ExprType)>,
    ) -> Option<ExecutionExpr> {
        if unsafe { pg_sys::expression_returns_set(expression) } {
            return None;
        }
        let dependency_nodes = unsafe { DependencyCollector::collect(expression) }?;
        let mut inputs = DependencyInputs::new();
        let dependencies = dependency_nodes
            .into_iter()
            .map(|node| {
                let (expression, value_type) = lower_dependency(node)?;
                let slot_attno = inputs.intern(expression, value_type)?;
                Some(DependencyBinding {
                    node,
                    slot_attno,
                    value_type,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        let mut rewrite = RewriteContext {
            dependencies: &dependencies,
        };
        let rewritten = unsafe {
            dependency_mutator(expression, ptr::from_mut(&mut rewrite).cast())
        };
        if rewritten.is_null() {
            return None;
        }
        let mut validator = RewrittenExpressionValidator {
            inputs: &inputs.inputs,
            valid: true,
        };
        unsafe {
            rewritten_expression_walker(
                rewritten,
                ptr::from_mut(&mut validator).cast(),
            );
        }
        if !validator.valid {
            return None;
        }
        let serialized_raw = unsafe { pg_sys::nodeToString(rewritten.cast()) };
        if serialized_raw.is_null() {
            return None;
        }
        let serialized = Arc::<CStr>::from(unsafe { CStr::from_ptr(serialized_raw) });
        unsafe { pg_sys::pfree(serialized_raw.cast()) };
        let volatility = if unsafe { pg_sys::contain_volatile_functions(expression) }
        {
            PostgresExprVolatility::Volatile
        } else if unsafe { pg_sys::contain_mutable_functions(expression) } {
            PostgresExprVolatility::Stable
        } else {
            PostgresExprVolatility::Immutable
        };
        Some(ExecutionExpr::Postgres(PostgresEvalExpr::new(
            serialized,
            inputs.inputs.into_boxed_slice(),
            ExprType {
                type_oid: unsafe { pg_sys::exprType(expression) },
                typmod: unsafe { pg_sys::exprTypmod(expression) },
                collation: unsafe { pg_sys::exprCollation(expression) },
            },
            volatility,
        )))
    }
}
