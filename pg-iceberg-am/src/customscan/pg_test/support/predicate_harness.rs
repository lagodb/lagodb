//! Shared observation harness for comparison-clause predicate tests.

use core::ffi::c_int;

use iceberg_lite::expr::Predicate;
use pg_lakebase_core::expr::nodes::PgParamValue;
use pg_lakebase_core::expr::split::{ColumnRef, QualPushdownDecision};
use pg_lakebase_core::expr::translator::{BuildPredicateError, PredicateBuilder};
use pgrx::pg_sys;

use crate::customscan::pg_test::support::classifier_harness::CLASSIFIER;
use crate::customscan::pg_test::support::fixtures::{
    OUTER_RELID, SCAN_RELID, make_opexpr, make_typed_const, make_typed_param,
    make_typed_var, wrap_relabel,
};
use crate::predicate::translator::{
    IcebergPredicateTranslator, IcebergTranslationError,
};

const SCAN_REL_OID: u32 = 16_384;
const COLUMN_NAME: &str = "col";

pub(crate) type TranslationResult =
    Result<Predicate, BuildPredicateError<IcebergTranslationError>>;

pub(crate) const PREDICATE_HARNESS: PredicateTestHarness = PredicateTestHarness {
    scan_relid: SCAN_RELID as c_int,
};

/// Concrete test-side harness for synthetic comparison clauses.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PredicateTestHarness {
    scan_relid: c_int,
}

/// Scan-column metadata for translator `ColumnRef` assembly.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScanColumnSpec<'a> {
    pub(crate) rel_oid: pg_sys::Oid,
    pub(crate) attno: i16,
    pub(crate) type_oid: pg_sys::Oid,
    pub(crate) collation: u32,
    pub(crate) name: &'a str,
}

/// Concrete literal operand metadata.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConstSpec {
    pub(crate) type_oid: pg_sys::Oid,
    pub(crate) collation: u32,
    pub(crate) len: c_int,
    pub(crate) datum: pg_sys::Datum,
    pub(crate) byval: bool,
}

/// PG comparison operator metadata for a synthetic `OpExpr`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ComparisonOpSpec {
    pub(crate) opno: u32,
    pub(crate) opcollid: u32,
    pub(crate) inputcollid: u32,
    pub(crate) opfuncid: u32,
    pub(crate) opresulttype: pg_sys::Oid,
}

/// Optional `RelabelType` wrapping for both comparison operands.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RelabelSpec {
    pub(crate) lhs_depth: u8,
    pub(crate) rhs_depth: u8,
}

/// Comparison-clause operand shape.
#[derive(Clone, Copy, Debug)]
pub(crate) enum OperandSpec {
    ScanCol,
    OuterCol,
    Const(ConstSpec),
    Param {
        kind: pg_sys::ParamKind::Type,
        id: c_int,
        type_oid: pg_sys::Oid,
        collation: u32,
    },
    SystemCol {
        relid: u32,
    },
    WholeRow {
        relid: u32,
    },
    ParamSublink,
}

/// Canonical comparison-clause inputs for AM predicate tests.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ComparisonSpec<'a> {
    pub(crate) column: ScanColumnSpec<'a>,
    pub(crate) lhs: OperandSpec,
    pub(crate) rhs: OperandSpec,
    pub(crate) op: ComparisonOpSpec,
    pub(crate) relabel: RelabelSpec,
}

/// Classifier and translator observations for one clause.
pub(crate) struct ClauseObservation {
    pub(crate) decision: QualPushdownDecision,
    pub(crate) translated: TranslationResult,
}

impl ClauseObservation {
    pub(crate) fn translator_builds(&self) -> bool {
        self.translated.is_ok()
    }

    pub(crate) fn translate_debug(&self) -> String {
        format!("{:?}", self.translated)
    }
}

impl PredicateTestHarness {
    pub(crate) unsafe fn classify(
        &self,
        spec: &ComparisonSpec<'_>,
    ) -> QualPushdownDecision {
        unsafe { CLASSIFIER.classify(self.build_expr(spec)) }
    }

    pub(crate) unsafe fn observe(
        &self,
        spec: &ComparisonSpec<'_>,
    ) -> ClauseObservation {
        unsafe {
            let expr = self.build_expr(spec);
            ClauseObservation {
                decision: CLASSIFIER.classify(expr),
                translated: self.translate_expr(expr, spec),
            }
        }
    }

    unsafe fn translate_expr(
        &self,
        expr: *mut pg_sys::Expr,
        spec: &ComparisonSpec<'_>,
    ) -> TranslationResult {
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
            let resolved_params: [PgParamValue; 0] = [];
            let mut translator = IcebergPredicateTranslator::new();
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

    unsafe fn build_expr(&self, spec: &ComparisonSpec<'_>) -> *mut pg_sys::Expr {
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

    unsafe fn build_operand(
        &self,
        operand: OperandSpec,
        column: ScanColumnSpec<'_>,
    ) -> *mut pg_sys::Expr {
        unsafe {
            match operand {
                OperandSpec::ScanCol => make_typed_var(
                    self.scan_relid as u32,
                    column.attno,
                    column.type_oid,
                    column.collation,
                ),
                OperandSpec::OuterCol => make_typed_var(
                    OUTER_RELID,
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
                OperandSpec::Param {
                    kind,
                    id,
                    type_oid,
                    collation,
                } => make_typed_param(kind, id, type_oid, collation),
                OperandSpec::SystemCol { relid } => {
                    make_typed_var(relid, -1, column.type_oid, column.collation)
                }
                OperandSpec::WholeRow { relid } => {
                    make_typed_var(relid, 0, column.type_oid, column.collation)
                }
                OperandSpec::ParamSublink => make_typed_param(
                    pg_sys::ParamKind::PARAM_SUBLINK,
                    1,
                    column.type_oid,
                    column.collation,
                ),
            }
        }
    }
}

impl ScanColumnSpec<'static> {
    pub(crate) fn synthetic(type_oid: pg_sys::Oid, collation: u32) -> Self {
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
    pub(crate) const NONE: Self = Self {
        lhs_depth: 0,
        rhs_depth: 0,
    };
}

impl<'a> ComparisonSpec<'a> {
    pub(crate) fn new(
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

    pub(crate) fn scan_col_op_const(
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
