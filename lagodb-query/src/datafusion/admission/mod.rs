//! Planning-time execution admission against provider output representations.
//!
//! PostgreSQL expression semantics have already been checked by QueryPlanData.
//! This stage uses the execution compiler with typed leaves, then DataFusion's
//! own coercion rules. No runtime values, scans, or UDF state are evaluated.
//! It supplements the IR's operator capabilities with physical type checks and
//! explicit representation restrictions; success is not a separate proof of
//! full PG equivalence. Provider mapping contracts and existing IR limitations
//! still apply. Declines retain their causes for the host's planning diagnostics.

mod semantics;

use arrow_schema::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::common::config::ConfigOptions;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;

use crate::plan::QueryPlanData;

use super::compiler::{DataFusionLogicalPlanCompiler, DataFusionPlanError};
use super::scan_binding::ScanBindings;
use semantics::RepresentationSemantics;

/// A planning decline, not a PostgreSQL ERROR. The host retains its existing
/// fallback policy while the cause remains available through Display/source.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct DataFusionAdmissionDecline(AdmissionDeclineReason);

#[derive(Debug, thiserror::Error)]
enum AdmissionDeclineReason {
    #[error("expression compilation failed: {0}")]
    Compilation(#[from] DataFusionPlanError),
    #[error("physical type coercion failed: {0}")]
    TypeCoercion(#[source] DataFusionError),
    #[error("expression type resolution failed: {0}")]
    ExpressionType(#[source] DataFusionError),
    #[error(
        "nanosecond equality or ordering does not preserve PG microsecond semantics"
    )]
    NanosecondComparison,
    #[error("timestamp promotion to nanoseconds narrows the PG value range")]
    NanosecondPromotion,
}

/// Admits a validated query using the projected schemas returned by scan
/// planning. All state is local to one candidate and is discarded before path
/// installation; neither Arrow objects nor execution handles enter PG plan data.
pub struct DataFusionPlanAdmission<'a> {
    plan: &'a QueryPlanData,
    scans: ScanBindings,
}

impl<'a> DataFusionPlanAdmission<'a> {
    /// Schemas follow dense ScanId order and each scan's requested projection.
    pub fn new<'schema>(
        plan: &'a QueryPlanData,
        schemas: impl ExactSizeIterator<Item = &'schema SchemaRef>,
    ) -> Self {
        Self {
            plan,
            scans: ScanBindings::for_planning(plan.fragment(), schemas),
        }
    }

    /// Build the execution logical plan with typed leaves, then use DataFusion's
    /// plan-level coercion so intermediate output schemas and UDF signatures
    /// follow the same rules as execution. No optimizer or evaluator is invoked.
    pub fn check(&self) -> Result<(), DataFusionAdmissionDecline> {
        self.check_plan().map_err(DataFusionAdmissionDecline)
    }

    fn check_plan(&self) -> Result<(), AdmissionDeclineReason> {
        let compiler = DataFusionLogicalPlanCompiler::for_planning(
            &self.scans,
            self.plan.runtime_values(),
        );
        let plan = compiler.compile(self.plan.fragment().root())?;
        let plan = TypeCoercion::new()
            .analyze(plan, &ConfigOptions::new())
            .map_err(AdmissionDeclineReason::TypeCoercion)?;
        RepresentationSemantics::check_plan(&plan)
    }
}
