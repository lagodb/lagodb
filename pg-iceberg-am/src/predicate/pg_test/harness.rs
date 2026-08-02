//! Shared backend harness for synthetic predicate expressions.

use core::ffi::c_int;
use core::ptr;

use iceberg_lite::expr::Predicate;
use pg_lakebase_core::expr::pg::PgExprRef;
use pg_lakebase_core::expr::predicate::PlanPredicateContext;
use pg_lakebase_core::expr::translator::BuildPredicateError;
use pg_lakebase_core::expr::{
    ColumnRef, PredicateBuilder, QualPushdownDecision, ResolvedParam,
};
use pgrx::pg_sys;

use crate::predicate::IcebergPredicateClassifier;
use crate::predicate::policy::test_opno_table as op;
use crate::predicate::translator::{
    IcebergPredicateTranslator, IcebergTranslationError,
};

// -----------------------------------------------------------------------------
// Stable operator and relation fixtures
// -----------------------------------------------------------------------------

pub(super) const NUMERIC_LT_OPNO: u32 = 1754;
pub(super) const DATE_GE_OPNO: u32 = op::DATE[5];
pub(super) const TIMESTAMP_GT_OPNO: u32 = op::TIMESTAMP[4];

pub(super) const TEXT_LT_OPNO: u32 = op::TEXT[2];
const TEXT_LE_OPNO: u32 = op::TEXT[3];
const TEXT_GT_OPNO: u32 = op::TEXT[4];
const TEXT_GE_OPNO: u32 = op::TEXT[5];
pub(super) const TEXTNE_OPNO: u32 = op::TEXT[1];

pub(super) const TEXT_ORDERED_OPNOS: [u32; 4] =
    [TEXT_LT_OPNO, TEXT_LE_OPNO, TEXT_GT_OPNO, TEXT_GE_OPNO];

pub(super) const SCAN_RELID: u32 = 1;
const SCAN_REL_OID: u32 = 16_384;
const COLUMN_NAME: &str = "col";

pub(super) const INT4EQ_OPNO: u32 = op::INT4[0];
pub(super) const INT4LT_OPNO: u32 = op::INT4[2];
pub(super) const TEXTEQ_OPNO: u32 = op::TEXT[0];

// -----------------------------------------------------------------------------
// Raw PostgreSQL node construction
// -----------------------------------------------------------------------------

/// int4 `Var` at `(varno, varattno)`.
///
/// # Safety
///
/// Must run inside a PostgreSQL backend with an active memory context.
pub(super) unsafe fn make_int4_var(varno: u32, varattno: i16) -> *mut pg_sys::Expr {
    // SAFETY: the caller runs inside a PostgreSQL backend memory context.
    unsafe {
        pg_sys::makeVar(
            varno as c_int,
            varattno as pg_sys::AttrNumber,
            pg_sys::INT4OID,
            -1,
            pg_sys::Oid::INVALID,
            0,
        )
        .cast()
    }
}

/// int4 `Const` (pass-by-value).
///
/// # Safety
///
/// Must run inside a PostgreSQL backend with an active memory context.
pub(super) unsafe fn make_int4_const(value: i32) -> *mut pg_sys::Expr {
    // SAFETY: the caller runs inside a PostgreSQL backend memory context.
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

/// Synthetic `OpExpr` with explicit comparison metadata.
///
/// # Safety
///
/// Every argument must point to a live expression in the active PostgreSQL
/// memory context.
pub(super) unsafe fn make_opexpr(
    opno: u32,
    opresulttype: pg_sys::Oid,
    opcollid: u32,
    inputcollid: u32,
    args: &[*mut pg_sys::Expr],
) -> *mut pg_sys::Expr {
    // SAFETY: every argument is a live backend-owned expression and all
    // allocations are made in the caller's active PostgreSQL memory context.
    unsafe {
        let mut arg_list: *mut pg_sys::List = ptr::null_mut();
        for &arg in args {
            arg_list = pg_sys::lappend(arg_list, arg.cast());
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

/// # Safety
///
/// Must run inside a PostgreSQL backend with an active memory context.
unsafe fn make_typed_var(
    varno: u32,
    varattno: i16,
    type_oid: pg_sys::Oid,
    collation: u32,
) -> *mut pg_sys::Expr {
    // SAFETY: the caller runs inside a PostgreSQL backend memory context.
    unsafe {
        pg_sys::makeVar(
            varno as c_int,
            varattno as pg_sys::AttrNumber,
            type_oid,
            -1,
            pg_sys::Oid::from(collation),
            0,
        )
        .cast()
    }
}

/// # Safety
///
/// `value`, `len`, and `byval` must describe the PostgreSQL type identified by
/// `type_oid`, and the referenced Datum storage must remain live for the call.
unsafe fn make_typed_const(
    type_oid: pg_sys::Oid,
    collation: u32,
    len: c_int,
    value: pg_sys::Datum,
    byval: bool,
) -> *mut pg_sys::Expr {
    // SAFETY: `value`, `len`, and `byval` are supplied by a matching
    // `ConstSpec`, and the caller runs inside a PostgreSQL backend.
    unsafe {
        pg_sys::makeConst(
            type_oid,
            -1,
            pg_sys::Oid::from(collation),
            len,
            value,
            false,
            byval,
        )
        .cast()
    }
}

/// # Safety
///
/// `expr` must point to a live PostgreSQL expression in the active memory
/// context.
unsafe fn wrap_relabel(mut expr: *mut pg_sys::Expr, depth: u8) -> *mut pg_sys::Expr {
    // SAFETY: `expr` is a live backend-owned expression; every wrapper is
    // allocated in the active PostgreSQL memory context.
    unsafe {
        for _ in 0..depth {
            let resulttype = pg_sys::exprType(expr.cast());
            let resultcollid = pg_sys::exprCollation(expr.cast());
            let relabel = pg_sys::palloc0(core::mem::size_of::<pg_sys::RelabelType>())
                as *mut pg_sys::RelabelType;
            (*relabel).xpr.type_ = pg_sys::NodeTag::T_RelabelType;
            (*relabel).arg = expr;
            (*relabel).resulttype = resulttype;
            (*relabel).resulttypmod = -1;
            (*relabel).resultcollid = resultcollid;
            (*relabel).relabelformat = pg_sys::CoercionForm::COERCE_IMPLICIT_CAST;
            (*relabel).location = -1;
            expr = relabel.cast();
        }
        expr
    }
}

// -----------------------------------------------------------------------------
// Classifier harness
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub(super) struct ClassifierHarness;

pub(super) const CLASSIFIER: ClassifierHarness = ClassifierHarness;

impl ClassifierHarness {
    /// Classify a synthetic expression via core parsing and AM policy.
    ///
    /// # Safety
    ///
    /// `expr` must point to a live PostgreSQL expression tree.
    pub(super) unsafe fn classify(
        &self,
        expr: *mut pg_sys::Expr,
    ) -> QualPushdownDecision {
        // SAFETY: guaranteed by this method's contract.
        unsafe {
            let predicate_ctx = PlanPredicateContext {
                rel_oid: pg_sys::Oid::INVALID,
                scan_relid: SCAN_RELID as c_int,
            };
            let leaf = PgExprRef::from_raw(expr);
            let predicate = match predicate_ctx.parse_leaf(leaf) {
                Ok(predicate) => predicate,
                Err(_) => return QualPushdownDecision::Unsupported,
            };
            IcebergPredicateClassifier.classify(&predicate)
        }
    }
}

// -----------------------------------------------------------------------------
// Predicate specifications and observations
// -----------------------------------------------------------------------------

pub(super) type TranslationResult =
    Result<Predicate, BuildPredicateError<IcebergTranslationError>>;

pub(super) const PREDICATE_HARNESS: PredicateTestHarness = PredicateTestHarness {
    scan_relid: SCAN_RELID as c_int,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct PredicateTestHarness {
    scan_relid: c_int,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ScanColumnSpec<'a> {
    pub(super) rel_oid: pg_sys::Oid,
    pub(super) attno: i16,
    pub(super) type_oid: pg_sys::Oid,
    pub(super) collation: u32,
    pub(super) name: &'a str,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ConstSpec {
    pub(super) type_oid: pg_sys::Oid,
    pub(super) collation: u32,
    pub(super) len: c_int,
    pub(super) datum: pg_sys::Datum,
    pub(super) byval: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ComparisonOpSpec {
    pub(super) opno: u32,
    pub(super) opcollid: u32,
    pub(super) inputcollid: u32,
    pub(super) opfuncid: u32,
    pub(super) opresulttype: pg_sys::Oid,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct RelabelSpec {
    pub(super) lhs_depth: u8,
    pub(super) rhs_depth: u8,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum OperandSpec {
    ScanCol,
    Const(ConstSpec),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ComparisonSpec<'a> {
    pub(super) column: ScanColumnSpec<'a>,
    pub(super) lhs: OperandSpec,
    pub(super) rhs: OperandSpec,
    pub(super) op: ComparisonOpSpec,
    pub(super) relabel: RelabelSpec,
}

pub(super) struct ClauseObservation {
    pub(super) decision: QualPushdownDecision,
    pub(super) translated: TranslationResult,
}

impl ClauseObservation {
    pub(super) fn translator_builds(&self) -> bool {
        self.translated.is_ok()
    }

    pub(super) fn translate_debug(&self) -> String {
        format!("{:?}", self.translated)
    }
}

impl PredicateTestHarness {
    /// # Safety
    ///
    /// Every pointer constructed from `spec` is valid only in the active
    /// PostgreSQL backend memory context.
    pub(super) unsafe fn classify(
        &self,
        spec: &ComparisonSpec<'_>,
    ) -> QualPushdownDecision {
        // SAFETY: guaranteed by this method's contract.
        unsafe { CLASSIFIER.classify(self.build_expr(spec)) }
    }

    /// # Safety
    ///
    /// Every pointer constructed from `spec` is valid only in the active
    /// PostgreSQL backend memory context.
    pub(super) unsafe fn observe(
        &self,
        spec: &ComparisonSpec<'_>,
    ) -> ClauseObservation {
        // SAFETY: guaranteed by this method's contract.
        unsafe {
            let expr = self.build_expr(spec);
            ClauseObservation {
                decision: CLASSIFIER.classify(expr),
                translated: self.translate_expr(expr, spec),
            }
        }
    }

    /// # Safety
    ///
    /// `expr` must be the live expression built from `spec` in the active
    /// PostgreSQL memory context.
    unsafe fn translate_expr(
        &self,
        expr: *mut pg_sys::Expr,
        spec: &ComparisonSpec<'_>,
    ) -> TranslationResult {
        // SAFETY: `expr` and its metadata were built together from `spec` and
        // remain live in the active PostgreSQL memory context.
        unsafe {
            let column_refs = [ColumnRef {
                expr_index: 0,
                rel_oid: spec.column.rel_oid,
                attno: spec.column.attno,
                atttypid: spec.column.type_oid,
                attcollation: pg_sys::Oid::from(spec.column.collation),
                name: Some(String::from(spec.column.name)),
            }];
            let exprs = [expr];
            let resolved_params: [ResolvedParam; 0] = [];
            let mut translator = IcebergPredicateTranslator::new_unbound_for_tests();
            let mut builder = PredicateBuilder::new(
                &mut translator,
                &exprs,
                &column_refs,
                &resolved_params,
                self.scan_relid,
            );
            builder.build_one(0)
        }
    }

    /// # Safety
    ///
    /// Must run inside a PostgreSQL backend with an active memory context.
    unsafe fn build_expr(&self, spec: &ComparisonSpec<'_>) -> *mut pg_sys::Expr {
        // SAFETY: all nodes are allocated in the active PostgreSQL memory
        // context and remain live for the complete observation.
        unsafe {
            let lhs = wrap_relabel(
                self.build_operand(spec.lhs, spec.column),
                spec.relabel.lhs_depth,
            );
            let rhs = wrap_relabel(
                self.build_operand(spec.rhs, spec.column),
                spec.relabel.rhs_depth,
            );
            let expr = make_opexpr(
                spec.op.opno,
                spec.op.opresulttype,
                spec.op.opcollid,
                spec.op.inputcollid,
                &[lhs, rhs],
            );
            (*(expr as *mut pg_sys::OpExpr)).opfuncid =
                pg_sys::Oid::from(spec.op.opfuncid);
            expr
        }
    }

    /// # Safety
    ///
    /// Must run inside a PostgreSQL backend with an active memory context;
    /// any Datum in `operand` must match its `ConstSpec` metadata.
    unsafe fn build_operand(
        &self,
        operand: OperandSpec,
        column: ScanColumnSpec<'_>,
    ) -> *mut pg_sys::Expr {
        // SAFETY: each spec variant carries metadata matching the raw Datum or
        // Var representation passed to PostgreSQL's constructors.
        unsafe {
            match operand {
                OperandSpec::ScanCol => make_typed_var(
                    self.scan_relid as u32,
                    column.attno,
                    column.type_oid,
                    column.collation,
                ),
                OperandSpec::Const(spec) => make_typed_const(
                    spec.type_oid,
                    spec.collation,
                    spec.len,
                    spec.datum,
                    spec.byval,
                ),
            }
        }
    }
}

impl ScanColumnSpec<'static> {
    pub(super) fn synthetic(type_oid: pg_sys::Oid, collation: u32) -> Self {
        Self {
            rel_oid: pg_sys::Oid::from(SCAN_REL_OID),
            attno: 1,
            type_oid,
            collation,
            name: COLUMN_NAME,
        }
    }
}

impl RelabelSpec {
    pub(super) const NONE: Self = Self {
        lhs_depth: 0,
        rhs_depth: 0,
    };
}

impl<'a> ComparisonSpec<'a> {
    pub(super) fn new(
        column: ScanColumnSpec<'a>,
        lhs: OperandSpec,
        rhs: OperandSpec,
        op: ComparisonOpSpec,
        relabel: RelabelSpec,
    ) -> Self {
        Self {
            column,
            lhs,
            rhs,
            op,
            relabel,
        }
    }

    pub(super) fn scan_col_op_const(
        column: ScanColumnSpec<'a>,
        op: ComparisonOpSpec,
        constant: ConstSpec,
    ) -> Self {
        Self {
            column,
            lhs: OperandSpec::ScanCol,
            rhs: OperandSpec::Const(constant),
            op,
            relabel: RelabelSpec::NONE,
        }
    }
}
