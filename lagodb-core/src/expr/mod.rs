//! PostgreSQL expression views, relation analysis, and planned filter pushdown.

mod bindings;
mod codec;
mod comparison;
pub(crate) mod contract;
pub(crate) mod execution;
pub mod explain;
mod integer_widening;
pub mod pg;
pub mod planning;
pub mod pushdown;
mod runtime_values;
mod scalar_semantics;
mod value;

pub(crate) use planning::{inspect, relation};

pub use bindings::{RuntimeValue, RuntimeValueBindings, RuntimeValueExpr};
pub use codec::{
    ExpressionCodecError, ExpressionPlanDataDecode, ExpressionPlanDataEncode,
};
pub use comparison::{PgComparisonKind, PgComparisonSignature, PgNanComparison};
pub use contract::{
    ParamKey, PgComparisonIdentity, PgComparisonOp, PushdownContract, PushdownCosting,
};
pub use integer_widening::PgIntegerWidening;
pub use runtime_values::{RuntimeValueState, RuntimeValueStateError};
pub use scalar_semantics::PgTextComparisonSemantics;
pub use value::{
    ColumnRef, ExprType, RuntimeValueId, RuntimeValueLayout, RuntimeValueSource,
    RuntimeValueSpec,
};
