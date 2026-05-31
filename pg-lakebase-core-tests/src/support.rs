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

        pub(crate) unsafe fn int4_param(
            self,
            kind: pg_sys::ParamKind::Type,
            param_id: c_int,
        ) -> *mut pg_sys::Expr {
            unsafe {
                let p = pg_sys::palloc0(core::mem::size_of::<pg_sys::Param>())
                    as *mut pg_sys::Param;
                (*p).xpr.type_ = pg_sys::NodeTag::T_Param;
                (*p).paramkind = kind;
                (*p).paramid = param_id;
                (*p).paramtype = pg_sys::INT4OID;
                (*p).paramcollid = pg_sys::Oid::INVALID;
                (*p).paramtypmod = -1;
                (*p).location = -1;
                p.cast()
            }
        }

        pub(crate) unsafe fn int4_exec_param(
            self,
            param_id: c_int,
        ) -> *mut pg_sys::Expr {
            unsafe { self.int4_param(pg_sys::ParamKind::PARAM_EXEC, param_id) }
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

        pub(crate) unsafe fn bool_expr(
            self,
            boolop: pg_sys::BoolExprType::Type,
            args: &[*mut pg_sys::Expr],
        ) -> *mut pg_sys::Expr {
            unsafe { pg_sys::makeBoolExpr(boolop, self.expr_list(args), -1) }
        }

        pub(crate) unsafe fn null_test(
            self,
            arg: *mut pg_sys::Expr,
            nulltesttype: pg_sys::NullTestType::Type,
        ) -> *mut pg_sys::Expr {
            unsafe {
                let nt = pg_sys::palloc0(core::mem::size_of::<pg_sys::NullTest>())
                    as *mut pg_sys::NullTest;
                (*nt).xpr.type_ = pg_sys::NodeTag::T_NullTest;
                (*nt).arg = arg;
                (*nt).nulltesttype = nulltesttype;
                (*nt).argisrow = false;
                (*nt).location = -1;
                nt.cast()
            }
        }

        pub(crate) unsafe fn relabel_int4(
            self,
            arg: *mut pg_sys::Expr,
        ) -> *mut pg_sys::Expr {
            unsafe {
                let relabel =
                    pg_sys::palloc0(core::mem::size_of::<pg_sys::RelabelType>())
                        as *mut pg_sys::RelabelType;
                (*relabel).xpr.type_ = pg_sys::NodeTag::T_RelabelType;
                (*relabel).arg = arg;
                (*relabel).resulttype = pg_sys::INT4OID;
                (*relabel).resulttypmod = -1;
                (*relabel).resultcollid = pg_sys::Oid::INVALID;
                (*relabel).relabelformat = pg_sys::CoercionForm::COERCE_IMPLICIT_CAST;
                (*relabel).location = -1;
                relabel.cast()
            }
        }

        pub(crate) unsafe fn int4_func_expr(
            self,
            arg: *mut pg_sys::Expr,
        ) -> *mut pg_sys::Expr {
            unsafe {
                pg_sys::makeFuncExpr(
                    pg_sys::Oid::INVALID,
                    pg_sys::INT4OID,
                    self.expr_list(&[arg]),
                    pg_sys::Oid::INVALID,
                    pg_sys::Oid::INVALID,
                    pg_sys::CoercionForm::COERCE_EXPLICIT_CALL,
                )
                .cast()
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

        pub(crate) unsafe fn restrictinfo(
            self,
            clause: *mut pg_sys::Expr,
            pseudoconstant: bool,
            leakproof: bool,
            security_level: u32,
        ) -> *mut pg_sys::RestrictInfo {
            unsafe {
                let ri = pg_sys::palloc0(core::mem::size_of::<pg_sys::RestrictInfo>())
                    as *mut pg_sys::RestrictInfo;
                (*ri).type_ = pg_sys::NodeTag::T_RestrictInfo;
                (*ri).clause = clause;
                (*ri).pseudoconstant = pseudoconstant;
                (*ri).leakproof = leakproof;
                (*ri).security_level = security_level;
                ri
            }
        }

        pub(crate) unsafe fn restrictinfo_list(
            self,
            rinfos: &[*mut pg_sys::RestrictInfo],
        ) -> *mut pg_sys::List {
            unsafe {
                let mut out: *mut pg_sys::List = ptr::null_mut();
                for &rinfo in rinfos {
                    out = pg_sys::lappend(out, rinfo.cast::<c_void>());
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

        pub(crate) fn int4_eq_deparse() -> Self {
            Self {
                opfuncid: pg_sys::Oid::from(65u32),
                ..Self::int4_comparison(INT4_EQ_OPNO)
            }
        }

        pub(crate) fn with_collations(
            mut self,
            opcollid: pg_sys::Oid,
            inputcollid: pg_sys::Oid,
        ) -> Self {
            self.opcollid = opcollid;
            self.inputcollid = inputcollid;
            self
        }
    }

    pub(crate) struct PlannerRelFixture {
        pub(crate) root: *mut pg_sys::PlannerInfo,
        pub(crate) baserel: *mut pg_sys::RelOptInfo,
        pub(crate) rte: *mut pg_sys::RangeTblEntry,
    }

    impl PlannerRelFixture {
        pub(crate) unsafe fn relation(relid: u32, rel_oid: u32) -> Self {
            unsafe {
                let rte =
                    pg_sys::palloc0(core::mem::size_of::<pg_sys::RangeTblEntry>())
                        as *mut pg_sys::RangeTblEntry;
                (*rte).type_ = pg_sys::NodeTag::T_RangeTblEntry;
                (*rte).rtekind = pg_sys::RTEKind::RTE_RELATION;
                (*rte).relkind = pg_sys::RELKIND_RELATION as core::ffi::c_char;
                (*rte).relid = pg_sys::Oid::from(rel_oid);

                let mut rtable: *mut pg_sys::List = ptr::null_mut();
                rtable = pg_sys::lappend(rtable, rte.cast::<c_void>());

                let parse = pg_sys::palloc0(core::mem::size_of::<pg_sys::Query>())
                    as *mut pg_sys::Query;
                (*parse).type_ = pg_sys::NodeTag::T_Query;
                (*parse).commandType = pg_sys::CmdType::CMD_SELECT;
                (*parse).rtable = rtable;

                let root = pg_sys::palloc0(core::mem::size_of::<pg_sys::PlannerInfo>())
                    as *mut pg_sys::PlannerInfo;
                (*root).type_ = pg_sys::NodeTag::T_PlannerInfo;
                (*root).parse = parse;

                let baserel =
                    pg_sys::palloc0(core::mem::size_of::<pg_sys::RelOptInfo>())
                        as *mut pg_sys::RelOptInfo;
                (*baserel).type_ = pg_sys::NodeTag::T_RelOptInfo;
                (*baserel).relid = relid as pg_sys::Index;
                (*baserel).relids = pg_sys::bms_make_singleton(relid as c_int);
                (*baserel).baserestrict_min_security = 0;

                let reltarget = pg_sys::palloc0(core::mem::size_of::<
                    pg_sys::PathTarget,
                >()) as *mut pg_sys::PathTarget;
                (*reltarget).type_ = pg_sys::NodeTag::T_PathTarget;
                (*reltarget).exprs = ptr::null_mut();
                (*baserel).reltarget = reltarget;

                Self { root, baserel, rte }
            }
        }
    }
}
