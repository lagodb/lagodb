//! Exact bounded-NUMERIC aggregates owned by LagoDB.
//!
//! Integer-input SUM/AVG deliberately use DataFusion's native aggregates.
//! These UDAFs cover only PostgreSQL NUMERIC values represented by the source
//! ABI as Decimal128, retaining a fixed i256 coefficient through aggregation
//! and materializing PostgreSQL NUMERIC once per output group.

mod accumulator;
mod output;

use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

use crate::plan::AggregateKind;

use self::accumulator::NumericAccumulator;

pub(super) const SUM_NAME: &str = "lagodb_numeric_sum";
pub(super) const AVG_NAME: &str = "lagodb_numeric_avg";

static SUM: LazyLock<Arc<AggregateUDF>> = LazyLock::new(|| {
    Arc::new(AggregateUDF::from(NumericAggregate::new(
        SUM_NAME,
        AggregateKind::NumericSum,
    )))
});
static AVG: LazyLock<Arc<AggregateUDF>> = LazyLock::new(|| {
    Arc::new(AggregateUDF::from(NumericAggregate::new(
        AVG_NAME,
        AggregateKind::NumericAverage,
    )))
});

pub(super) fn sum() -> Arc<AggregateUDF> {
    Arc::clone(&SUM)
}

pub(super) fn avg() -> Arc<AggregateUDF> {
    Arc::clone(&AVG)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NumericAggregate {
    name: &'static str,
    kind: AggregateKind,
    signature: Signature,
}

impl NumericAggregate {
    fn new(name: &'static str, kind: AggregateKind) -> Self {
        Self {
            name,
            kind,
            // The PostgreSQL semantic gate owns the bounded Decimal128
            // allowlist. Any(1) prevents DataFusion from inserting a lossy
            // cast before this UDAF validates the physical input.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    fn decimal_scale(name: &str, data_type: &DataType) -> Result<u32> {
        match data_type {
            DataType::Decimal128(precision, scale)
                if (1..=38).contains(precision)
                    && *scale >= 0
                    && *scale <= *precision as i8 =>
            {
                Ok(*scale as u32)
            }
            data_type => {
                exec_err!("{name} requires bounded Decimal128 input, got {data_type}")
            }
        }
    }
}

impl AggregateUDFImpl for NumericAggregate {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = arg_types else {
            return exec_err!("{} requires one input", self.name);
        };
        Self::decimal_scale(self.name, data_type).map(|_| DataType::Binary)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let [field] = args.expr_fields else {
            return exec_err!("{} requires one input", args.name);
        };
        Ok(Box::new(NumericAccumulator::new(
            self.kind,
            Self::decimal_scale(args.name, field.data_type())?,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}_state", args.name),
            DataType::Binary,
            false,
        ))])
    }
}
