//! Shared raw PG-node fixtures and stable test constants for Iceberg `#[pg_test]`.
//!
//! This module intentionally stops at node construction. Classifier/translator
//! execution lives in the dedicated harness modules.

use core::ffi::c_int;
use core::ptr;

use pgrx::pg_sys;

// Scoped non-integer comparison operator OIDs (from `pg_operator.dat`).
pub(crate) const TEXTEQ_OPNO_SCOPED: u32 = 98;
pub(crate) const NUMERIC_LT_OPNO: u32 = 1754;
pub(crate) const DATE_GE_OPNO: u32 = 1098;
pub(crate) const TIMESTAMP_GT_OPNO: u32 = 2064;

// Built-in `text` comparison operator OIDs (from `pg_operator.dat`).
pub(crate) const TEXT_LT_OPNO: u32 = 664;
pub(crate) const TEXT_LE_OPNO: u32 = 665;
pub(crate) const TEXT_GT_OPNO: u32 = 666;
pub(crate) const TEXT_GE_OPNO: u32 = 667;
pub(crate) const TEXTNE_OPNO: u32 = 531;

/// The four ordered `text` comparison operators (`<`, `<=`, `>`, `>=`).
pub(crate) const TEXT_ORDERED_OPNOS: [u32; 4] =
    [TEXT_LT_OPNO, TEXT_LE_OPNO, TEXT_GT_OPNO, TEXT_GE_OPNO];

/// Synthetic 1-based RTI assigned to the scan relation under test.
pub(crate) const SCAN_RELID: u32 = 1;

/// 1-based RTI for an outer relation (join-parameterized pushdown).
pub(crate) const OUTER_RELID: u32 = 2;

// Stable PG built-in operator OIDs from `pg_operator.dat`.
pub(crate) const INT4EQ_OPNO: u32 = 96;
pub(crate) const INT4LT_OPNO: u32 = 97;
pub(crate) const TEXTEQ_OPNO: u32 = 98;
pub(crate) const FLOAT8EQ_OPNO: u32 = 670;
pub(crate) const NUMERIC_EQ_OPNO: u32 = 1752;
pub(crate) const DATE_EQ_OPNO: u32 = 1093; // date_eq
pub(crate) const TIMESTAMP_EQ_OPNO: u32 = 2060; // timestamp_eq

/// Non-default collation OID (above built-in range).
pub(crate) const NON_DEFAULT_COLLATION_OID: u32 = 50_000;

/// int4 `Var` at `(varno, varattno)`.
pub(crate) unsafe fn make_int4_var(varno: u32, varattno: i16) -> *mut pg_sys::Expr {
    unsafe {
        let v = pg_sys::makeVar(
            varno as c_int,
            varattno as pg_sys::AttrNumber,
            pg_sys::INT4OID,
            -1,
            pg_sys::Oid::INVALID,
            0,
        );
        v.cast()
    }
}

/// int4 `Const` (pass-by-value).
pub(crate) unsafe fn make_int4_const(value: i32) -> *mut pg_sys::Expr {
    unsafe {
        let c = pg_sys::makeConst(
            pg_sys::INT4OID,
            -1,
            pg_sys::Oid::INVALID,
            core::mem::size_of::<i32>() as c_int,
            pg_sys::Datum::from(value),
            false,
            true,
        );
        c.cast()
    }
}

/// `Param` of arbitrary type / collation.
pub(crate) unsafe fn make_typed_param(
    paramkind: pg_sys::ParamKind::Type,
    paramid: c_int,
    type_oid: pg_sys::Oid,
    collation: u32,
) -> *mut pg_sys::Expr {
    unsafe {
        let p = pg_sys::palloc0(core::mem::size_of::<pg_sys::Param>())
            as *mut pg_sys::Param;
        (*p).xpr.type_ = pg_sys::NodeTag::T_Param;
        (*p).paramkind = paramkind;
        (*p).paramid = paramid;
        (*p).paramtype = type_oid;
        (*p).paramtypmod = -1;
        (*p).paramcollid = pg_sys::Oid::from(collation);
        (*p).location = -1;
        p.cast()
    }
}

/// Synthetic `OpExpr` with explicit `(opno, opresulttype, opcollid, inputcollid)`.
pub(crate) unsafe fn make_opexpr(
    opno: u32,
    opresulttype: pg_sys::Oid,
    opcollid: u32,
    inputcollid: u32,
    args: &[*mut pg_sys::Expr],
) -> *mut pg_sys::Expr {
    unsafe {
        let mut arg_list: *mut pg_sys::List = ptr::null_mut();
        for &a in args {
            arg_list = pg_sys::lappend(arg_list, a.cast());
        }

        let op = pg_sys::palloc0(core::mem::size_of::<pg_sys::OpExpr>())
            as *mut pg_sys::OpExpr;
        (*op).xpr.type_ = pg_sys::NodeTag::T_OpExpr;
        (*op).opno = pg_sys::Oid::from(opno);
        (*op).opfuncid = pg_sys::Oid::INVALID;
        (*op).opresulttype = opresulttype;
        (*op).opretset = false;
        (*op).opcollid = pg_sys::Oid::from(opcollid);
        (*op).inputcollid = pg_sys::Oid::from(inputcollid);
        (*op).args = arg_list;
        (*op).location = -1;
        op.cast()
    }
}

/// `Var` of arbitrary type / collation at `(varno, varattno)`.
pub(crate) unsafe fn make_typed_var(
    varno: u32,
    varattno: i16,
    type_oid: pg_sys::Oid,
    collation: u32,
) -> *mut pg_sys::Expr {
    unsafe {
        let v = pg_sys::makeVar(
            varno as c_int,
            varattno as pg_sys::AttrNumber,
            type_oid,
            -1,
            pg_sys::Oid::from(collation),
            0,
        );
        v.cast()
    }
}

/// `Const` of arbitrary type from a prepared (non-null) `Datum`.
pub(crate) unsafe fn make_typed_const(
    type_oid: pg_sys::Oid,
    collation: u32,
    len: c_int,
    value: pg_sys::Datum,
    byval: bool,
) -> *mut pg_sys::Expr {
    unsafe {
        let c = pg_sys::makeConst(
            type_oid,
            -1,
            pg_sys::Oid::from(collation),
            len,
            value,
            false, // constisnull
            byval,
        );
        c.cast()
    }
}

/// Wrap `expr` in `depth` `RelabelType` nodes.
pub(crate) unsafe fn wrap_relabel(
    mut expr: *mut pg_sys::Expr,
    depth: u8,
) -> *mut pg_sys::Expr {
    unsafe {
        for _ in 0..depth {
            let resulttype = pg_sys::exprType(expr.cast());
            let resultcollid = pg_sys::exprCollation(expr.cast());
            let rl = pg_sys::palloc0(core::mem::size_of::<pg_sys::RelabelType>())
                as *mut pg_sys::RelabelType;
            (*rl).xpr.type_ = pg_sys::NodeTag::T_RelabelType;
            (*rl).arg = expr;
            (*rl).resulttype = resulttype;
            (*rl).resulttypmod = -1;
            (*rl).resultcollid = resultcollid;
            (*rl).relabelformat = pg_sys::CoercionForm::COERCE_IMPLICIT_CAST;
            (*rl).location = -1;
            expr = rl.cast();
        }
        expr
    }
}
