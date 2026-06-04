//! Shared classifier observation harness for arbitrary test expressions.

use pg_lakebase_core::customscan::provider::PlanTranslateContext;
use pg_lakebase_core::expr::nodes::PgExprRef;
use pg_lakebase_core::expr::predicate::PlanPredicateContext;
use pg_lakebase_core::expr::split::QualPushdownDecision;
use pgrx::pg_sys;

use crate::customscan::IcebergPredicateClassifier;
use crate::customscan::pg_test::support::fixtures::SCAN_RELID;

/// Stateless backend harness for running the AM classifier over synthetic PG nodes.
pub(crate) struct ClassifierHarness;

pub(crate) const CLASSIFIER: ClassifierHarness = ClassifierHarness;

impl ClassifierHarness {
    /// Classify `expr` via core `parse_leaf` + Iceberg classifier dispatch.
    ///
    /// # Safety
    /// Allocates a synthetic `RelOptInfo` in the `#[pg_test]` per-query memory
    /// context and traverses the raw PG node pointed to by `expr`.
    pub(crate) unsafe fn classify(
        &self,
        expr: *mut pg_sys::Expr,
    ) -> QualPushdownDecision {
        unsafe {
            let ctx = self.make_ctx();
            let predicate_ctx = PlanPredicateContext {
                rel_oid: pg_sys::Oid::INVALID,
                scan_relid: ctx.scan_relid(),
            };
            let leaf = PgExprRef::from_raw(expr);
            let predicate = match predicate_ctx.parse_leaf(leaf) {
                Ok(p) => p,
                Err(_) => return QualPushdownDecision::Unsupported,
            };
            IcebergPredicateClassifier::default().classify_predicate(&ctx, &predicate)
        }
    }

    unsafe fn make_ctx(&self) -> PlanTranslateContext {
        unsafe { PlanTranslateContext::new(self.make_baserel()) }
    }

    unsafe fn make_baserel(&self) -> *mut pg_sys::RelOptInfo {
        unsafe {
            let rel = pg_sys::palloc0(core::mem::size_of::<pg_sys::RelOptInfo>())
                as *mut pg_sys::RelOptInfo;
            (*rel).type_ = pg_sys::NodeTag::T_RelOptInfo;
            (*rel).relid = SCAN_RELID as pg_sys::Index;
            rel
        }
    }
}
