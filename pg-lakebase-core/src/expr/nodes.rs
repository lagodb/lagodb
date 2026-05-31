//! Thin borrowed views over PG `Expr` nodes (no `copyObject`). All field access is `unsafe`
//! while the owning PG memory context is live.

use core::marker::PhantomData;
use core::ptr::NonNull;

use pgrx::pg_sys;

// =============================================================================
// PgExprRef / PgExprOwned
// =============================================================================

/// Borrowed, non-null reference to a PG [`pg_sys::Expr`] node.
///
/// A [`PgExprRef`] is the entry point into every typed view in this module.
/// The lifetime `'a` is the lifetime of the PG memory context that owns the
/// underlying node — typically the planner's per-query context for plan-stage
/// walks, or the executor's per-query context for runtime walks.
///
/// `PgExprRef` does not distinguish between concrete node kinds; use
/// [`PgExprRef::node_tag`] together with the `PgX::try_from_expr` constructors
/// (e.g. [`PgVar::try_from_expr`]) to discriminate.
#[derive(Clone, Copy)]
pub struct PgExprRef<'a> {
    ptr: NonNull<pg_sys::Expr>,
    _marker: PhantomData<&'a pg_sys::Expr>,
}

impl<'a> PgExprRef<'a> {
    /// Wrap a non-null `*mut pg_sys::Expr` as a [`PgExprRef`].
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid `*mut pg_sys::Expr` for the entire lifetime `'a`.
    /// In particular, the owning PostgreSQL memory context must still be live
    /// and the node must not have been freed.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::Expr) -> Self {
        Self {
            ptr: NonNull::new(ptr).expect(
                "pg-lakebase-core: PgExprRef::from_raw called with a null pointer",
            ),
            _marker: PhantomData,
        }
    }

    /// Like [`PgExprRef::from_raw`] but returns `None` for null pointers.
    ///
    /// # Safety
    ///
    /// If `ptr` is non-null, it must be valid for `'a` per [`PgExprRef::from_raw`].
    #[inline]
    pub unsafe fn from_raw_opt(ptr: *mut pg_sys::Expr) -> Option<Self> {
        NonNull::new(ptr).map(|ptr| Self {
            ptr,
            _marker: PhantomData,
        })
    }

    /// Returns the underlying raw pointer.
    #[inline]
    pub fn as_ptr(self) -> *mut pg_sys::Expr {
        self.ptr.as_ptr()
    }

    /// Returns the node's `NodeTag`.
    ///
    /// # Safety
    ///
    /// The node must still be live. See struct-level docs.
    #[inline]
    pub unsafe fn node_tag(self) -> pg_sys::NodeTag {
        unsafe { (*self.ptr.as_ptr()).type_ }
    }

    /// Recursively unwrap any number of `RelabelType` nodes from this expression.
    ///
    /// `RelabelType` only attaches type / collation labels; it does not change the
    /// wrapped value's representation. Classifiers and translators usually want to
    /// inspect the underlying expression. Returns `self` unchanged if it is not a
    /// `RelabelType`.
    ///
    /// # Safety
    ///
    /// The chain of `RelabelType` nodes and the underlying expression must remain
    /// live for `'a`.
    #[inline]
    pub unsafe fn without_relabels(mut self) -> Self {
        loop {
            match unsafe { PgRelabelType::try_from_expr(self) } {
                Some(rl) => match unsafe { rl.arg() } {
                    Some(inner) => self = inner,
                    None => return self,
                },
                None => return self,
            }
        }
    }
}

impl core::fmt::Debug for PgExprRef<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PgExprRef")
            .field("ptr", &self.ptr.as_ptr())
            .finish()
    }
}

/// Non-borrowing handle to a PG-allocated `Expr` node.
///
/// Used by [`PlanPushdownSplit`](super::split) to describe nodes that will be
/// re-rooted into the plan tree (`scan.plan.qual` / `custom_exprs`).
/// PostgreSQL still owns the memory. The handle is `Copy` because it is
/// *not* a Rust-owning resource; dropping it does nothing.
///
/// Convert to a borrowed view with [`PgExprOwned::as_ref`] when you have a
/// suitable lifetime witness for the owning memory context.
#[derive(Clone, Copy)]
pub struct PgExprOwned {
    ptr: NonNull<pg_sys::Expr>,
}

impl PgExprOwned {
    /// Wrap a non-null `*mut pg_sys::Expr` allocated in a PG memory context.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a valid `pg_sys::Expr` allocated by PostgreSQL. The
    /// caller is responsible for ensuring that the owning memory context lives
    /// at least as long as any subsequent use of this handle.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::Expr) -> Self {
        Self {
            ptr: NonNull::new(ptr).expect(
                "pg-lakebase-core: PgExprOwned::from_raw called with a null pointer",
            ),
        }
    }

    /// Returns the underlying raw pointer.
    #[inline]
    pub fn as_ptr(self) -> *mut pg_sys::Expr {
        self.ptr.as_ptr()
    }

    /// Reborrow as a [`PgExprRef`] under lifetime `'a`.
    ///
    /// # Safety
    ///
    /// The owning memory context must still be live for the entire `'a`.
    #[inline]
    pub unsafe fn as_ref<'a>(self) -> PgExprRef<'a> {
        PgExprRef {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl core::fmt::Debug for PgExprOwned {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PgExprOwned")
            .field("ptr", &self.ptr.as_ptr())
            .finish()
    }
}

// =============================================================================
// Typed node views
// =============================================================================

/// Generates a typed-view wrapper around a concrete PG `Expr` subtype.
///
/// Each generated view holds a `NonNull<pg_sys::$NodeTy>` plus a borrow
/// lifetime, exposes `from_raw` / `try_from_expr` constructors, an `as_ptr`
/// accessor, and an unsafe `as_node` reference accessor.
macro_rules! pg_node_view {
    ($Wrapper:ident, $NodeTy:ident, $Tag:ident) => {
        #[doc = concat!(
                            "Borrowed view over a PG [`pg_sys::",
                            stringify!($NodeTy),
                            "`] node.\n\n",
                            "Construct via [`",
                            stringify!($Wrapper),
                            "::try_from_expr`] which discriminates by `NodeTag`.",
                        )]
        #[derive(Clone, Copy)]
        pub struct $Wrapper<'a> {
            ptr: NonNull<pg_sys::$NodeTy>,
            _marker: PhantomData<&'a pg_sys::$NodeTy>,
        }

        impl<'a> $Wrapper<'a> {
            /// Wrap a non-null `*mut pg_sys::$NodeTy`.
            ///
            /// # Safety
            ///
            /// `ptr` must point to a live PG-allocated node of the correct
            /// type for the entire lifetime `'a`.
            #[inline]
            pub unsafe fn from_raw(ptr: *mut pg_sys::$NodeTy) -> Self {
                Self {
                    ptr: NonNull::new(ptr).expect(concat!(
                        "pg-lakebase-core: ",
                        stringify!($Wrapper),
                        "::from_raw called with a null pointer",
                    )),
                    _marker: PhantomData,
                }
            }

            /// Discriminate a [`PgExprRef`] into this typed view.
            ///
            /// Returns `Some(self)` when the node tag matches `T_$NodeTy`,
            /// `None` otherwise.
            ///
            /// # Safety
            ///
            /// The underlying node must still be live.
            #[inline]
            pub unsafe fn try_from_expr(expr: PgExprRef<'a>) -> Option<Self> {
                // SAFETY: caller upholds liveness; we read only the `type_`
                // tag at the start of the node, which is shared by every
                // `Expr` subtype.
                let tag = unsafe { expr.node_tag() };
                if tag == pg_sys::NodeTag::$Tag {
                    Some(Self {
                        ptr: expr.ptr.cast(),
                        _marker: PhantomData,
                    })
                } else {
                    None
                }
            }

            /// Returns the underlying raw pointer.
            #[inline]
            pub fn as_ptr(self) -> *mut pg_sys::$NodeTy {
                self.ptr.as_ptr()
            }

            /// Returns the node as a [`PgExprRef`] (upcast).
            #[inline]
            pub fn as_expr(self) -> PgExprRef<'a> {
                PgExprRef {
                    ptr: self.ptr.cast(),
                    _marker: PhantomData,
                }
            }

            /// Borrow the wrapped node as a `&pg_sys::$NodeTy`.
            ///
            /// # Safety
            ///
            /// The node must still be live for the borrow.
            #[inline]
            pub unsafe fn as_node(&self) -> &'a pg_sys::$NodeTy {
                unsafe { &*self.ptr.as_ptr() }
            }
        }

        impl core::fmt::Debug for $Wrapper<'_> {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.debug_struct(stringify!($Wrapper))
                    .field("ptr", &self.ptr.as_ptr())
                    .finish()
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

// -----------------------------------------------------------------------------
// Field accessors
//
// We provide a small set of accessors that the walker / classifier / translator
// actually need. Anything not exposed here can be reached via `as_node()`.
// -----------------------------------------------------------------------------

impl<'a> PgVar<'a> {
    /// `Var.varno` — the 1-based range table index of the source relation.
    ///
    /// At plan time this is `RelOptInfo.relid`; at runtime (after
    /// `set_customscan_references`) it is `cscan->scan.scanrelid`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn varno(self) -> core::ffi::c_int {
        unsafe { (*self.ptr.as_ptr()).varno }
    }

    /// `Var.varattno` — the 1-based attribute number on the source relation,
    /// or `0` for a whole-row reference, or a negative value for a system
    /// column.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn varattno(self) -> pg_sys::AttrNumber {
        unsafe { (*self.ptr.as_ptr()).varattno }
    }

    /// `Var.vartype` — the column's type OID.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn vartype(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).vartype }
    }

    /// `Var.varcollid` — the column's collation OID.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn varcollid(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).varcollid }
    }
}

impl<'a> PgConst<'a> {
    /// Returns `(consttype, constcollid, constvalue, constisnull)`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn parts(self) -> (pg_sys::Oid, pg_sys::Oid, pg_sys::Datum, bool) {
        let n = unsafe { &*self.ptr.as_ptr() };
        (n.consttype, n.constcollid, n.constvalue, n.constisnull)
    }
}

impl<'a> PgParam<'a> {
    /// `Param.paramkind`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn paramkind(self) -> pg_sys::ParamKind::Type {
        unsafe { (*self.ptr.as_ptr()).paramkind }
    }

    /// `Param.paramid` — the 1-based parameter id.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn paramid(self) -> core::ffi::c_int {
        unsafe { (*self.ptr.as_ptr()).paramid }
    }

    /// `Param.paramtype` — the resolved type OID at plan time.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn paramtype(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).paramtype }
    }

    /// `Param.paramcollid`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn paramcollid(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).paramcollid }
    }

    /// The `(ParamKind, param_id)` identity of this plan-tree `Param` node.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn key(self) -> ParamKey {
        ParamKey {
            paramkind: unsafe { self.paramkind() },
            param_id: unsafe { self.paramid() },
        }
    }
}

/// Returns the raw `*mut pg_sys::List` of an `OpExpr`'s `args` field.
///
/// # Safety
///
/// The node must still be live.
#[inline]
unsafe fn op_args(op: PgOpExpr<'_>) -> *mut pg_sys::List {
    unsafe { (*op.as_ptr()).args }
}

impl<'a> PgOpExpr<'a> {
    /// Extract the operator identity as a [`PgComparisonOp`].
    ///
    /// The classification key for pushdown is `(opno, opcollid, inputcollid)`;
    /// `opfuncid` and `opresulttype` are exposed for diagnostics only (see
    /// [`PgComparisonOp`]).
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn comparison_op(self) -> PgComparisonOp {
        let n = unsafe { &*self.ptr.as_ptr() };
        PgComparisonOp {
            opno: n.opno,
            opfuncid: n.opfuncid,
            opresulttype: n.opresulttype,
            opcollid: n.opcollid,
            inputcollid: n.inputcollid,
        }
    }

    /// Returns the raw `*mut pg_sys::List` of `OpExpr.args`.
    ///
    /// # Safety
    ///
    /// The node must still be live. The caller is responsible for traversing
    /// the list with `pg_sys::list_*` helpers.
    #[inline]
    pub unsafe fn args_list(self) -> *mut pg_sys::List {
        unsafe { (*self.ptr.as_ptr()).args }
    }

    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn arity(self) -> usize {
        let args = unsafe { op_args(self) };
        if args.is_null() {
            0
        } else {
            unsafe { (*args).length as usize }
        }
    }

    /// Binary operands; `None` if arity != 2 or either cell is NULL.
    ///
    /// # Safety
    ///
    /// The node must still be live for `'a`.
    #[inline]
    pub unsafe fn binary_operands(self) -> Option<(PgExprRef<'a>, PgExprRef<'a>)> {
        if unsafe { self.arity() } != 2 {
            return None;
        }
        let args = unsafe { op_args(self) };
        // SAFETY: `arity() == 2` guarantees `args` is non-null with cells 0 and
        // 1 present, so the indices are in range.
        let lhs_raw = unsafe { pg_sys::list_nth(args, 0) } as *mut pg_sys::Expr;
        let rhs_raw = unsafe { pg_sys::list_nth(args, 1) } as *mut pg_sys::Expr;
        let lhs = unsafe { PgExprRef::from_raw_opt(lhs_raw) }?;
        let rhs = unsafe { PgExprRef::from_raw_opt(rhs_raw) }?;
        Some((lhs, rhs))
    }
}

impl<'a> PgBoolExpr<'a> {
    /// `BoolExpr.boolop`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn boolop(self) -> pg_sys::BoolExprType::Type {
        unsafe { (*self.ptr.as_ptr()).boolop }
    }

    /// Returns the raw `*mut pg_sys::List` of `BoolExpr.args`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn args_list(self) -> *mut pg_sys::List {
        unsafe { (*self.ptr.as_ptr()).args }
    }
}

impl<'a> PgNullTest<'a> {
    /// `NullTest.nulltesttype` — `IS_NULL` or `IS_NOT_NULL`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn nulltesttype(self) -> pg_sys::NullTestType::Type {
        unsafe { (*self.ptr.as_ptr()).nulltesttype }
    }

    /// `NullTest.argisrow` — true for row-level `IS NULL`/`IS NOT NULL`.
    ///
    /// Row-level null tests are not supported by the v1 pushdown classifier;
    /// the caller is expected to force them to residual.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn argisrow(self) -> bool {
        unsafe { (*self.ptr.as_ptr()).argisrow }
    }

    /// Returns the wrapped argument as a [`PgExprRef`].
    ///
    /// # Safety
    ///
    /// The node must still be live. Returns `None` if the argument is null,
    /// which should not happen for a well-formed `NullTest`.
    #[inline]
    pub unsafe fn arg(self) -> Option<PgExprRef<'a>> {
        let arg = unsafe { (*self.ptr.as_ptr()).arg };
        unsafe { PgExprRef::from_raw_opt(arg) }
    }
}

impl<'a> PgRelabelType<'a> {
    /// `RelabelType.resulttype`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn resulttype(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).resulttype }
    }

    /// `RelabelType.resultcollid`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn resultcollid(self) -> pg_sys::Oid {
        unsafe { (*self.ptr.as_ptr()).resultcollid }
    }

    /// Returns the wrapped argument as a [`PgExprRef`].
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn arg(self) -> Option<PgExprRef<'a>> {
        let arg = unsafe { (*self.ptr.as_ptr()).arg };
        unsafe { PgExprRef::from_raw_opt(arg) }
    }
}

// =============================================================================
// PgComparisonOp
// =============================================================================

/// Classification key is `(opno, opcollid, inputcollid)` — not operator name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PgComparisonOp {
    pub opno: pg_sys::Oid,
    pub opfuncid: pg_sys::Oid,
    pub opresulttype: pg_sys::Oid,
    /// Output collation OID (`OpExpr.opcollid`).
    pub opcollid: pg_sys::Oid,
    /// Input collation OID (`OpExpr.inputcollid`).
    pub inputcollid: pg_sys::Oid,
}

// =============================================================================
// Param model: PgParamRef (plan time) vs PgParamValue (runtime)
// =============================================================================

/// Plan-time `Param` node reference (no runtime `Datum`).
#[derive(Clone, Copy, Debug)]
pub struct PgParamRef<'a> {
    inner: PgParam<'a>,
}

impl<'a> PgParamRef<'a> {
    /// Wrap a [`PgParam`] node view as a plan-time [`PgParamRef`].
    #[inline]
    pub fn from_param(param: PgParam<'a>) -> Self {
        Self { inner: param }
    }

    /// Borrow the underlying [`PgParam`] node view.
    #[inline]
    pub fn param(self) -> PgParam<'a> {
        self.inner
    }

    /// `Param.paramkind`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn paramkind(self) -> pg_sys::ParamKind::Type {
        unsafe { self.inner.paramkind() }
    }

    /// `Param.paramid`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn paramid(self) -> core::ffi::c_int {
        unsafe { self.inner.paramid() }
    }

    /// `Param.paramtype`.
    ///
    /// # Safety
    ///
    /// The node must still be live.
    #[inline]
    pub unsafe fn paramtype(self) -> pg_sys::Oid {
        unsafe { self.inner.paramtype() }
    }
}

/// Runtime param value from `RuntimeParamResolver::resolve` (do not embed in the plan tree).
#[derive(Clone, Copy, Debug)]
pub struct PgParamValue {
    /// 1-based `Param.paramid`.
    pub param_id: core::ffi::c_int,
    /// `PARAM_EXTERN` / `PARAM_EXEC` (other kinds are rejected upstream).
    pub paramkind: pg_sys::ParamKind::Type,
    /// Resolved type OID (`ParamExternData.ptype` for EXTERN; the plan-time
    /// `Param.paramtype` for EXEC after materialization).
    pub type_oid: pg_sys::Oid,
    /// Collation OID (`Param.paramcollid`).
    pub collid: pg_sys::Oid,
    /// Resolved `Datum`. Only meaningful when `is_null == false`. Lives in the
    /// per-tuple memory context (EXTERN) or `paramExecVals` (EXEC).
    pub datum: pg_sys::Datum,
    /// SQL NULL flag.
    pub is_null: bool,
}

/// `(paramkind, param_id)` — EXTERN `$n` and EXEC slot `n` share numeric ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ParamKey {
    /// `PARAM_EXTERN` / `PARAM_EXEC`.
    pub paramkind: pg_sys::ParamKind::Type,
    /// 1-based `$n` for `PARAM_EXTERN`; 0-based executor slot for `PARAM_EXEC`.
    pub param_id: core::ffi::c_int,
}

impl PgParamValue {
    #[inline]
    pub fn key(&self) -> ParamKey {
        ParamKey {
            paramkind: self.paramkind,
            param_id: self.param_id,
        }
    }
}

// =============================================================================
// Translator inputs: PgScalarRef, PgColumnRef, PgLiteral
// =============================================================================

/// A scalar input to the runtime predicate translator.
///
/// At runtime each leaf of a comparison is one of:
///
/// - [`PgScalarRef::Column`] — a reference to the scan relation's column.
/// - [`PgScalarRef::Literal`] — an inlined `Const`.
/// - [`PgScalarRef::Param`]   — a [`PgParamValue`] resolved from `EState`.
///
/// The translator is responsible for unwrapping `RelabelType`, matching
/// scan-relation `Var`s through `column_refs[]`, and resolving `Param` nodes into
/// `PgParamValue`s before constructing a `PgScalarRef`.
#[derive(Clone, Copy, Debug)]
pub enum PgScalarRef<'a> {
    /// A reference to a scan-relation column. Carries pre-resolved metadata so
    /// the runtime translator does not have to interpret setrefs-rewritten
    /// `Var.varno` / `Var.varattno` shapes.
    Column(PgColumnRef<'a>),
    /// A literal value extracted from a `Const` node.
    Literal(PgLiteral<'a>),
    /// A parameter value resolved at `Begin/ReScan` time.
    Param(PgParamValue),
}

/// Runtime column metadata matched from plan-time `column_refs[]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgColumnRef<'a> {
    /// `pg_class` OID of the scan relation. Pinned by the planner-acquired
    /// lock; safe to use at runtime for catalog / pruning calls.
    pub rel_oid: pg_sys::Oid,
    /// 1-based attribute number on the scan relation. Always `> 0` for v1
    /// (system columns and whole-row Var are gated out at the path stage).
    pub attno: pg_sys::AttrNumber,
    /// Column type OID.
    pub atttypid: pg_sys::Oid,
    /// Column collation OID.
    pub attcollation: pg_sys::Oid,
    pub name: Option<&'a str>,
}

/// A literal value extracted from a [`PgConst`] node.
///
/// `PgLiteral` carries the `Datum` plus enough metadata for the runtime
/// translator to interpret it without revisiting the original `Const` node.
/// The `Datum` is borrowed from the plan tree (or from a per-tuple-context
/// copy made by the walker); the lifetime `'a` ties the borrow to the owning
/// memory context.
#[derive(Clone, Copy, Debug)]
pub struct PgLiteral<'a> {
    /// Type OID (`Const.consttype`).
    pub type_oid: pg_sys::Oid,
    /// Collation OID (`Const.constcollid`).
    pub collid: pg_sys::Oid,
    /// Value `Datum` (`Const.constvalue`). Only meaningful when
    /// `is_null == false`.
    pub datum: pg_sys::Datum,
    /// SQL NULL flag (`Const.constisnull`).
    pub is_null: bool,
    _marker: PhantomData<&'a pg_sys::Const>,
}

impl<'a> PgLiteral<'a> {
    /// Build a [`PgLiteral`] from a [`PgConst`] view.
    ///
    /// # Safety
    ///
    /// The underlying `Const` node must still be live for `'a`.
    #[inline]
    pub unsafe fn from_const(c: PgConst<'a>) -> Self {
        let (type_oid, collid, datum, is_null) = unsafe { c.parts() };
        Self {
            type_oid,
            collid,
            datum,
            is_null,
            _marker: PhantomData,
        }
    }
}
