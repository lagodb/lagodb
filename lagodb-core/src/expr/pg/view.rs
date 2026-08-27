//! Zero-copy borrowed views over PostgreSQL expression nodes.

use core::marker::PhantomData;
use core::ptr::NonNull;

use pgrx::pg_sys;

use crate::expr::contract::{ParamKey, PgComparisonOp};

#[derive(Clone, Copy)]
pub struct PgExprRef<'a> {
    ptr: NonNull<pg_sys::Expr>,
    _lifetime: PhantomData<&'a pg_sys::Expr>,
}

impl<'a> PgExprRef<'a> {
    /// # Safety
    ///
    /// `ptr` must identify a live PostgreSQL expression for all of `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::Expr) -> Self {
        Self {
            ptr: NonNull::new(ptr)
                .expect("PgExprRef::from_raw requires a non-null expression"),
            _lifetime: PhantomData,
        }
    }

    /// # Safety
    ///
    /// A non-null `ptr` must satisfy [`Self::from_raw`].
    #[inline]
    pub unsafe fn from_raw_opt(ptr: *mut pg_sys::Expr) -> Option<Self> {
        NonNull::new(ptr).map(|ptr| Self {
            ptr,
            _lifetime: PhantomData,
        })
    }

    #[inline]
    pub fn as_ptr(self) -> *mut pg_sys::Expr {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn node_tag(self) -> pg_sys::NodeTag {
        // SAFETY: construction established a live Expr for `'a`.
        unsafe { (*self.ptr.as_ptr()).type_ }
    }

    #[inline]
    pub fn type_oid(self) -> pg_sys::Oid {
        // SAFETY: construction established a live expression node for `'a`.
        unsafe { pg_sys::exprType(self.as_ptr().cast()) }
    }

    #[inline]
    pub fn typmod(self) -> i32 {
        // SAFETY: construction established a live expression node for `'a`.
        unsafe { pg_sys::exprTypmod(self.as_ptr().cast()) }
    }

    #[inline]
    pub fn collation(self) -> pg_sys::Oid {
        // SAFETY: construction established a live expression node for `'a`.
        unsafe { pg_sys::exprCollation(self.as_ptr().cast()) }
    }

    #[inline]
    pub fn without_relabels(mut self) -> Self {
        while let Some(relabel) = PgRelabelType::try_from_expr(self) {
            let Some(inner) = relabel.arg() else {
                break;
            };
            self = inner;
        }
        self
    }
}

impl core::fmt::Debug for PgExprRef<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("PgExprRef").field(&self.ptr).finish()
    }
}

macro_rules! pg_node_view {
    ($wrapper:ident, $node:ident, $tag:ident) => {
        #[derive(Clone, Copy, Debug)]
        pub struct $wrapper<'a> {
            ptr: NonNull<pg_sys::$node>,
            _lifetime: PhantomData<&'a pg_sys::$node>,
        }

        impl<'a> $wrapper<'a> {
            #[inline]
            pub fn try_from_expr(expr: PgExprRef<'a>) -> Option<Self> {
                (expr.node_tag() == pg_sys::NodeTag::$tag).then(|| Self {
                    ptr: expr.ptr.cast(),
                    _lifetime: PhantomData,
                })
            }
        }
    };
}

pg_node_view!(PgVar, Var, T_Var);
pg_node_view!(PgConst, Const, T_Const);
pg_node_view!(PgParam, Param, T_Param);
pg_node_view!(PgOpExpr, OpExpr, T_OpExpr);
pg_node_view!(PgBoolExpr, BoolExpr, T_BoolExpr);
pg_node_view!(PgNullTest, NullTest, T_NullTest);
pg_node_view!(PgRelabelType, RelabelType, T_RelabelType);

impl PgVar<'_> {
    #[inline]
    pub fn varno(self) -> core::ffi::c_int {
        unsafe { (*self.ptr.as_ptr()).varno }
    }

    #[inline]
    pub fn varattno(self) -> pg_sys::AttrNumber {
        unsafe { (*self.ptr.as_ptr()).varattno }
    }

    #[inline]
    pub fn vartype(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).vartype }
    }

    #[inline]
    pub fn vartypmod(self) -> i32 {
        unsafe { (*self.ptr.as_ptr()).vartypmod }
    }

    #[inline]
    pub fn varcollid(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).varcollid }
    }
}

impl PgConst<'_> {
    #[inline]
    pub fn parts(self) -> (pg_sys::Oid, pg_sys::Oid, pg_sys::Datum, bool) {
        let node = unsafe { self.ptr.as_ref() };
        (
            node.consttype,
            node.constcollid,
            node.constvalue,
            node.constisnull,
        )
    }

    #[inline]
    pub fn typmod(self) -> i32 {
        unsafe { (*self.ptr.as_ptr()).consttypmod }
    }
}

impl PgParam<'_> {
    #[inline]
    pub fn paramkind(self) -> pg_sys::ParamKind::Type {
        unsafe { (*self.ptr.as_ptr()).paramkind }
    }

    #[inline]
    pub fn paramid(self) -> core::ffi::c_int {
        unsafe { (*self.ptr.as_ptr()).paramid }
    }

    #[inline]
    pub fn paramtype(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).paramtype }
    }

    #[inline]
    pub fn paramtypmod(self) -> i32 {
        unsafe { (*self.ptr.as_ptr()).paramtypmod }
    }

    #[inline]
    pub fn paramcollid(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).paramcollid }
    }

    #[inline]
    pub fn key(self) -> ParamKey {
        ParamKey {
            paramkind: self.paramkind(),
            param_id: self.paramid(),
        }
    }
}

impl<'a> PgOpExpr<'a> {
    #[inline]
    pub fn comparison_op(self) -> PgComparisonOp {
        let node = unsafe { self.ptr.as_ref() };
        PgComparisonOp {
            opno: node.opno,
            opfuncid: node.opfuncid,
            opresulttype: node.opresulttype,
            opcollid: node.opcollid,
            inputcollid: node.inputcollid,
        }
    }

    #[inline]
    pub(crate) fn args_list(self) -> *mut pg_sys::List {
        unsafe { (*self.ptr.as_ptr()).args }
    }

    #[inline]
    pub fn arity(self) -> usize {
        let args = self.args_list();
        if args.is_null() {
            0
        } else {
            unsafe { (*args).length as usize }
        }
    }

    #[inline]
    pub fn binary_operands(self) -> Option<(PgExprRef<'a>, PgExprRef<'a>)> {
        if self.arity() != 2 {
            return None;
        }
        let args = self.args_list();
        let left = unsafe { pg_sys::list_nth(args, 0) } as *mut pg_sys::Expr;
        let right = unsafe { pg_sys::list_nth(args, 1) } as *mut pg_sys::Expr;
        Some((unsafe { PgExprRef::from_raw_opt(left) }?, unsafe {
            PgExprRef::from_raw_opt(right)
        }?))
    }
}

impl PgBoolExpr<'_> {
    #[inline]
    pub fn boolop(self) -> pg_sys::BoolExprType::Type {
        unsafe { (*self.ptr.as_ptr()).boolop }
    }

    #[inline]
    pub(crate) fn args_list(self) -> *mut pg_sys::List {
        unsafe { (*self.ptr.as_ptr()).args }
    }
}

impl<'a> PgNullTest<'a> {
    #[inline]
    pub fn nulltesttype(self) -> pg_sys::NullTestType::Type {
        unsafe { (*self.ptr.as_ptr()).nulltesttype }
    }

    #[inline]
    pub fn argisrow(self) -> bool {
        unsafe { (*self.ptr.as_ptr()).argisrow }
    }

    #[inline]
    pub fn arg(self) -> Option<PgExprRef<'a>> {
        unsafe { PgExprRef::from_raw_opt((*self.ptr.as_ptr()).arg) }
    }
}

impl<'a> PgRelabelType<'a> {
    #[inline]
    pub fn arg(self) -> Option<PgExprRef<'a>> {
        unsafe { PgExprRef::from_raw_opt((*self.ptr.as_ptr()).arg) }
    }
}
