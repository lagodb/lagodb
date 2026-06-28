//! Plan-stage copy-on-write boolean expression normalization.

use core::ptr;

use pgrx::pg_sys;

use crate::expr::nodes::{
    PgBoolExpr, PgExprRef, PgNullTest, PgOpExpr, PgRelabelType,
};

/// Copy-on-write NOT rewrite (mirrors PG `negate_clause`); null in -> null out.
///
/// # Safety
///
/// `expr` must be NULL or a live PostgreSQL `Expr` tree. Any copied nodes are
/// allocated in the current PostgreSQL memory context and keep pointers to
/// unchanged children owned by that same context.
pub unsafe fn rewrite_not(expr: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
    unsafe { negate_clause(expr) }
}

unsafe fn negate_clause(expr: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
    if expr.is_null() {
        return expr;
    }
    let r = unsafe { PgExprRef::from_raw(expr) };
    let tag = unsafe { r.node_tag() };

    match tag {
        pg_sys::NodeTag::T_BoolExpr => {
            let be = unsafe { PgBoolExpr::try_from_expr(r) }
                .expect("PgBoolExpr tag matched but downcast failed");
            let boolop = unsafe { be.boolop() };
            let args = unsafe { be.args_list() };

            if boolop == pg_sys::BoolExprType::NOT_EXPR {
                let arg_ptr = unsafe { first_list_node(args) } as *mut pg_sys::Expr;
                if arg_ptr.is_null() {
                    return expr;
                }
                let normalized = unsafe { negate_clause(arg_ptr) };
                return unsafe { negate(normalized) };
            }

            let new_args = unsafe { normalize_args(args) };
            if new_args == args {
                expr
            } else {
                let location = unsafe { (*be.as_ptr()).location };
                unsafe { pg_sys::makeBoolExpr(boolop, new_args, location) }
            }
        }
        pg_sys::NodeTag::T_OpExpr => {
            let op = unsafe { PgOpExpr::try_from_expr(r) }
                .expect("PgOpExpr tag matched but downcast failed");
            let args = unsafe { op.args_list() };
            let new_args = unsafe { normalize_args(args) };
            if new_args == args {
                expr
            } else {
                let src = unsafe { &*op.as_ptr() };
                unsafe { clone_op_expr(src, src.opno, src.opfuncid, new_args) }
            }
        }
        pg_sys::NodeTag::T_NullTest => {
            let nt = unsafe { PgNullTest::try_from_expr(r) }
                .expect("PgNullTest tag matched but downcast failed");
            let arg = unsafe { (*nt.as_ptr()).arg };
            if arg.is_null() {
                return expr;
            }
            let new_arg = unsafe { negate_clause(arg) };
            if new_arg == arg {
                expr
            } else {
                let src = unsafe { &*nt.as_ptr() };
                unsafe { clone_null_test(src, new_arg, src.nulltesttype) }
            }
        }
        pg_sys::NodeTag::T_RelabelType => {
            let rl = unsafe { PgRelabelType::try_from_expr(r) }
                .expect("PgRelabelType tag matched but downcast failed");
            let arg = unsafe { (*rl.as_ptr()).arg };
            if arg.is_null() {
                return expr;
            }
            let new_arg = unsafe { negate_clause(arg) };
            if new_arg == arg {
                expr
            } else {
                let src = unsafe { &*rl.as_ptr() };
                unsafe { clone_relabel_type(src, new_arg) }
            }
        }
        _ => expr,
    }
}

unsafe fn normalize_args(list: *mut pg_sys::List) -> *mut pg_sys::List {
    if list.is_null() {
        return list;
    }
    let len = unsafe { pg_sys::list_length(list) };
    let mut normalized: Vec<*mut pg_sys::Expr> = Vec::with_capacity(len as usize);
    let mut changed = false;
    for i in 0..len {
        let cell = unsafe { pg_sys::list_nth(list, i) } as *mut pg_sys::Expr;
        let new_cell = if cell.is_null() {
            cell
        } else {
            unsafe { negate_clause(cell) }
        };
        if new_cell != cell {
            changed = true;
        }
        normalized.push(new_cell);
    }
    if !changed {
        return list;
    }
    let mut out: *mut pg_sys::List = ptr::null_mut();
    for &cell in &normalized {
        out = unsafe { pg_sys::lappend(out, cell as *mut core::ffi::c_void) };
    }
    out
}

unsafe fn negate(child: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
    if child.is_null() {
        return child;
    }
    let r = unsafe { PgExprRef::from_raw(child) };
    let tag = unsafe { r.node_tag() };

    match tag {
        pg_sys::NodeTag::T_OpExpr => {
            let op = unsafe { PgOpExpr::try_from_expr(r) }
                .expect("PgOpExpr tag matched but downcast failed");
            let opno = unsafe { (*op.as_ptr()).opno };
            let negator = unsafe { pg_sys::get_negator(opno) };
            if pg_sys::Oid::INVALID == negator {
                return unsafe { wrap_in_not(child) };
            }
            if !unsafe { negator_preserves_signature(opno, negator) } {
                return unsafe { wrap_in_not(child) };
            }
            unsafe { make_op_expr_with_opno(op, negator) }
        }
        pg_sys::NodeTag::T_NullTest => {
            let nt = unsafe { PgNullTest::try_from_expr(r) }
                .expect("PgNullTest tag matched but downcast failed");
            unsafe { make_flipped_null_test(nt) }
        }
        pg_sys::NodeTag::T_BoolExpr => {
            let be = unsafe { PgBoolExpr::try_from_expr(r) }
                .expect("PgBoolExpr tag matched but downcast failed");
            let boolop = unsafe { be.boolop() };
            match boolop {
                pg_sys::BoolExprType::NOT_EXPR => {
                    let args = unsafe { be.args_list() };
                    let inner = unsafe { first_list_node(args) } as *mut pg_sys::Expr;
                    if inner.is_null() {
                        unsafe { wrap_in_not(child) }
                    } else {
                        inner
                    }
                }
                pg_sys::BoolExprType::AND_EXPR => {
                    let new_args = unsafe { negate_each(be.args_list()) };
                    unsafe {
                        pg_sys::makeBoolExpr(
                            pg_sys::BoolExprType::OR_EXPR,
                            new_args,
                            (*be.as_ptr()).location,
                        )
                    }
                }
                pg_sys::BoolExprType::OR_EXPR => {
                    let new_args = unsafe { negate_each(be.args_list()) };
                    unsafe {
                        pg_sys::makeBoolExpr(
                            pg_sys::BoolExprType::AND_EXPR,
                            new_args,
                            (*be.as_ptr()).location,
                        )
                    }
                }
                _ => unsafe { wrap_in_not(child) },
            }
        }
        _ => unsafe { wrap_in_not(child) },
    }
}

unsafe fn negate_each(args: *mut pg_sys::List) -> *mut pg_sys::List {
    let mut out: *mut pg_sys::List = ptr::null_mut();
    if args.is_null() {
        return out;
    }
    let len = unsafe { pg_sys::list_length(args) };
    for i in 0..len {
        let cell = unsafe { pg_sys::list_nth(args, i) } as *mut pg_sys::Expr;
        let negated = unsafe { negate(cell) };
        out = unsafe { pg_sys::lappend(out, negated as *mut core::ffi::c_void) };
    }
    out
}

unsafe fn wrap_in_not(child: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
    let mut args: *mut pg_sys::List = ptr::null_mut();
    args = unsafe { pg_sys::lappend(args, child as *mut core::ffi::c_void) };
    let location = unsafe { read_expr_location(child) };
    unsafe { pg_sys::makeBoolExpr(pg_sys::BoolExprType::NOT_EXPR, args, location) }
}

unsafe fn read_expr_location(expr: *mut pg_sys::Expr) -> pg_sys::ParseLoc {
    if expr.is_null() {
        return -1;
    }
    let r = unsafe { PgExprRef::from_raw(expr) };
    let tag = unsafe { r.node_tag() };
    match tag {
        pg_sys::NodeTag::T_OpExpr => unsafe {
            (*(expr as *mut pg_sys::OpExpr)).location
        },
        pg_sys::NodeTag::T_Var => unsafe { (*(expr as *mut pg_sys::Var)).location },
        pg_sys::NodeTag::T_Const => unsafe {
            (*(expr as *mut pg_sys::Const)).location
        },
        pg_sys::NodeTag::T_Param => unsafe {
            (*(expr as *mut pg_sys::Param)).location
        },
        pg_sys::NodeTag::T_BoolExpr => unsafe {
            (*(expr as *mut pg_sys::BoolExpr)).location
        },
        pg_sys::NodeTag::T_NullTest => unsafe {
            (*(expr as *mut pg_sys::NullTest)).location
        },
        pg_sys::NodeTag::T_RelabelType => unsafe {
            (*(expr as *mut pg_sys::RelabelType)).location
        },
        _ => -1,
    }
}

unsafe fn make_op_expr_with_opno(
    op: PgOpExpr<'_>,
    negator_opno: pg_sys::Oid,
) -> *mut pg_sys::Expr {
    let src = unsafe { &*op.as_ptr() };
    unsafe { clone_op_expr(src, negator_opno, pg_sys::Oid::INVALID, src.args) }
}

unsafe fn clone_op_expr(
    src: &pg_sys::OpExpr,
    opno: pg_sys::Oid,
    opfuncid: pg_sys::Oid,
    args: *mut pg_sys::List,
) -> *mut pg_sys::Expr {
    let new = unsafe {
        pg_sys::palloc0(core::mem::size_of::<pg_sys::OpExpr>()) as *mut pg_sys::OpExpr
    };
    unsafe {
        (*new).xpr.type_ = pg_sys::NodeTag::T_OpExpr;
        (*new).opno = opno;
        (*new).opfuncid = opfuncid;
        (*new).opresulttype = src.opresulttype;
        (*new).opretset = src.opretset;
        (*new).opcollid = src.opcollid;
        (*new).inputcollid = src.inputcollid;
        (*new).args = args;
        (*new).location = src.location;
    }
    new as *mut pg_sys::Expr
}

unsafe fn make_flipped_null_test(nt: PgNullTest<'_>) -> *mut pg_sys::Expr {
    let src = unsafe { &*nt.as_ptr() };
    let flipped = if src.nulltesttype == pg_sys::NullTestType::IS_NULL {
        pg_sys::NullTestType::IS_NOT_NULL
    } else {
        pg_sys::NullTestType::IS_NULL
    };
    unsafe { clone_null_test(src, src.arg, flipped) }
}

unsafe fn clone_null_test(
    src: &pg_sys::NullTest,
    arg: *mut pg_sys::Expr,
    nulltesttype: pg_sys::NullTestType::Type,
) -> *mut pg_sys::Expr {
    let new = unsafe {
        pg_sys::palloc0(core::mem::size_of::<pg_sys::NullTest>())
            as *mut pg_sys::NullTest
    };
    unsafe {
        (*new).xpr.type_ = pg_sys::NodeTag::T_NullTest;
        (*new).arg = arg;
        (*new).nulltesttype = nulltesttype;
        (*new).argisrow = src.argisrow;
        (*new).location = src.location;
    }
    new as *mut pg_sys::Expr
}

unsafe fn clone_relabel_type(
    src: &pg_sys::RelabelType,
    arg: *mut pg_sys::Expr,
) -> *mut pg_sys::Expr {
    let new = unsafe {
        pg_sys::palloc0(core::mem::size_of::<pg_sys::RelabelType>())
            as *mut pg_sys::RelabelType
    };
    unsafe {
        (*new).xpr.type_ = pg_sys::NodeTag::T_RelabelType;
        (*new).arg = arg;
        (*new).resulttype = src.resulttype;
        (*new).resulttypmod = src.resulttypmod;
        (*new).resultcollid = src.resultcollid;
        (*new).relabelformat = src.relabelformat;
        (*new).location = src.location;
    }
    new as *mut pg_sys::Expr
}

/// Refuse negators whose operand/result types differ from `original`.
unsafe fn negator_preserves_signature(
    original: pg_sys::Oid,
    negator: pg_sys::Oid,
) -> bool {
    let orig_tup = unsafe {
        pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::OPEROID as core::ffi::c_int,
            pg_sys::Datum::from(original),
        )
    };
    if orig_tup.is_null() {
        return false;
    }
    let neg_tup = unsafe {
        pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::OPEROID as core::ffi::c_int,
            pg_sys::Datum::from(negator),
        )
    };
    if neg_tup.is_null() {
        unsafe { pg_sys::ReleaseSysCache(orig_tup) };
        return false;
    }
    let orig_form =
        unsafe { heap_tuple_get_struct::<pg_sys::FormData_pg_operator>(orig_tup) };
    let neg_form =
        unsafe { heap_tuple_get_struct::<pg_sys::FormData_pg_operator>(neg_tup) };

    let preserved = unsafe {
        (*orig_form).oprleft == (*neg_form).oprleft
            && (*orig_form).oprright == (*neg_form).oprright
            && (*orig_form).oprresult == (*neg_form).oprresult
    };

    unsafe {
        pg_sys::ReleaseSysCache(neg_tup);
        pg_sys::ReleaseSysCache(orig_tup);
    }

    preserved
}

/// PG `GETSTRUCT(tup)` - user struct follows `HeapTupleHeaderData`.
unsafe fn heap_tuple_get_struct<T>(tuple: pg_sys::HeapTuple) -> *const T {
    let header = unsafe { (*tuple).t_data };
    let hoff = unsafe { (*header).t_hoff } as usize;
    unsafe { (header as *const u8).add(hoff) as *const T }
}

unsafe fn first_list_node(list: *mut pg_sys::List) -> *mut pg_sys::Node {
    if list.is_null() {
        return ptr::null_mut();
    }
    if unsafe { pg_sys::list_length(list) } == 0 {
        return ptr::null_mut();
    }
    unsafe { pg_sys::list_nth(list, 0) as *mut pg_sys::Node }
}
