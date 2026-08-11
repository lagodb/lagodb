#[cfg(any(test, feature = "pg_test"))]
pub(crate) mod pg {
    use core::ffi::{c_int, c_void};
    use std::ptr;

    use pgrx::pg_sys;

    pub(crate) const INT4_EQ_OPNO: u32 = 96;

    #[derive(Clone, Copy)]
    pub(crate) struct PgNodeBuilder {
        scan_relid: c_int,
    }

    impl PgNodeBuilder {
        pub(crate) const fn new(scan_relid: c_int) -> Self {
            Self { scan_relid }
        }

        pub(crate) unsafe fn int4_var(
            self,
            attno: pg_sys::AttrNumber,
        ) -> *mut pg_sys::Expr {
            unsafe { self.int4_var_at(self.scan_relid, attno) }
        }

        pub(crate) unsafe fn int4_var_at(
            self,
            varno: c_int,
            attno: pg_sys::AttrNumber,
        ) -> *mut pg_sys::Expr {
            unsafe {
                pg_sys::makeVar(
                    varno,
                    attno,
                    pg_sys::INT4OID,
                    -1,
                    pg_sys::Oid::INVALID,
                    0,
                )
                .cast()
            }
        }

        pub(crate) unsafe fn int4_const(self, value: i32) -> *mut pg_sys::Expr {
            unsafe {
                pg_sys::makeConst(
                    pg_sys::INT4OID,
                    -1,
                    pg_sys::Oid::INVALID,
                    core::mem::size_of::<i32>() as c_int,
                    pg_sys::Datum::from(value),
                    false,
                    true,
                )
                .cast()
            }
        }

        pub(crate) unsafe fn int4_op_clause(
            self,
            opno: u32,
            left: *mut pg_sys::Expr,
            right: *mut pg_sys::Expr,
        ) -> *mut pg_sys::Expr {
            unsafe {
                pg_sys::make_opclause(
                    pg_sys::Oid::from(opno),
                    pg_sys::BOOLOID,
                    false,
                    left,
                    right,
                    pg_sys::Oid::INVALID,
                    pg_sys::Oid::INVALID,
                )
            }
        }

        pub(crate) unsafe fn int4_var_op_const(
            self,
            opno: u32,
            attno: pg_sys::AttrNumber,
            value: i32,
        ) -> *mut pg_sys::Expr {
            let left = unsafe { self.int4_var(attno) };
            let right = unsafe { self.int4_const(value) };
            unsafe { self.int4_op_clause(opno, left, right) }
        }

        pub(crate) unsafe fn op_expr(
            self,
            spec: OpExprSpec,
            args: &[*mut pg_sys::Expr],
        ) -> *mut pg_sys::Expr {
            unsafe {
                let mut arg_list: *mut pg_sys::List = ptr::null_mut();
                for &arg in args {
                    arg_list = pg_sys::lappend(arg_list, arg.cast::<c_void>());
                }

                let op = pg_sys::palloc0(core::mem::size_of::<pg_sys::OpExpr>())
                    as *mut pg_sys::OpExpr;
                (*op).xpr.type_ = pg_sys::NodeTag::T_OpExpr;
                (*op).opno = spec.opno;
                (*op).opfuncid = spec.opfuncid;
                (*op).opresulttype = spec.opresulttype;
                (*op).opretset = spec.opretset;
                (*op).opcollid = spec.opcollid;
                (*op).inputcollid = spec.inputcollid;
                (*op).args = arg_list;
                (*op).location = spec.location;
                op.cast()
            }
        }

        pub(crate) unsafe fn expr_list(
            self,
            cells: &[*mut pg_sys::Expr],
        ) -> *mut pg_sys::List {
            unsafe {
                let mut out: *mut pg_sys::List = ptr::null_mut();
                for &cell in cells {
                    out = pg_sys::lappend(out, cell.cast::<c_void>());
                }
                out
            }
        }
    }

    #[derive(Clone, Copy)]
    pub(crate) struct OpExprSpec {
        pub(crate) opno: pg_sys::Oid,
        pub(crate) opfuncid: pg_sys::Oid,
        pub(crate) opresulttype: pg_sys::Oid,
        pub(crate) opretset: bool,
        pub(crate) opcollid: pg_sys::Oid,
        pub(crate) inputcollid: pg_sys::Oid,
        pub(crate) location: i32,
    }

    impl OpExprSpec {
        pub(crate) fn int4_comparison(opno: u32) -> Self {
            Self {
                opno: pg_sys::Oid::from(opno),
                opfuncid: pg_sys::Oid::INVALID,
                opresulttype: pg_sys::BOOLOID,
                opretset: false,
                opcollid: pg_sys::Oid::INVALID,
                inputcollid: pg_sys::Oid::INVALID,
                location: -1,
            }
        }
    }
}
