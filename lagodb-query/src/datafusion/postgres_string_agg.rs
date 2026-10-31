//! PostgreSQL-compatible ordered STRING_AGG lowering.
//!
//! DataFusion's ordered `StringAgg` delegates accumulation to `ArrayAgg`,
//! which retains the ordering columns and sorts them when the input does not
//! satisfy the aggregate ordering. `ArrayAgg` therefore declares a soft
//! ordering requirement. `StringAgg` has the same execution behavior but
//! inherits the aggregate trait's hard-requirement default, which prevents one
//! AggregateExec from containing STRING_AGGs with independent orderings.

use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::functions_aggregate::string_agg::StringAgg;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, Expr,
    GroupsAccumulator, ReversedUDAF, Signature,
};

pub(super) const NAME: &str = "lagodb_string_agg";

static STRING_AGG: LazyLock<Arc<AggregateUDF>> = LazyLock::new(|| {
    Arc::new(AggregateUDF::from(PostgresStringAgg {
        inner: StringAgg::new(),
    }))
});

pub(super) fn string_agg(argument: Expr, delimiter: Expr) -> Expr {
    STRING_AGG.call(vec![argument, delimiter])
}

pub(super) fn udaf() -> Arc<AggregateUDF> {
    Arc::clone(&STRING_AGG)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PostgresStringAgg {
    inner: StringAgg,
}

impl AggregateUDFImpl for PostgresStringAgg {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        self.inner.create_groups_accumulator(args)
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::SoftRequirement
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        _beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        // The delegated StringAgg always constructs ArrayAgg's sorting
        // accumulator for an ordered call. It is correct for either input
        // shape; DataFusion 55 does not expose StringAgg's inner ArrayAgg state
        // to select its pre-ordered fast path here.
        Ok(Some(self))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Reversed(Arc::clone(&STRING_AGG))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}
