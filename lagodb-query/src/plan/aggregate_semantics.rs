//! PostgreSQL aggregate OID and type allowlist.

use lagodb_core::expr::ExprType;
use lagodb_core::tuple::numeric_precision_scale;
use pgrx::pg_sys;

use super::aggregate::{AggCall, AggregateArguments, AggregateKind};
use super::ir::QueryPlanError;
use super::semantics::ScalarSemantics;

impl AggregateKind {
    pub(super) fn classify(
        function_oid: pg_sys::Oid,
        arguments: &AggregateArguments,
    ) -> Result<AggregateKind, QueryPlanError> {
        match u32::from(function_oid) {
            pg_sys::F_COUNT_ if matches!(arguments, AggregateArguments::None) => {
                Ok(AggregateKind::Count)
            }
            pg_sys::F_COUNT_ANY
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::Count)
            }
            pg_sys::F_MIN_INT2
            | pg_sys::F_MIN_INT4
            | pg_sys::F_MIN_INT8
            | pg_sys::F_MIN_FLOAT4
            | pg_sys::F_MIN_FLOAT8
            | pg_sys::F_MIN_NUMERIC
            | pg_sys::F_MIN_DATE
            | pg_sys::F_MIN_TIME
            | pg_sys::F_MIN_TIMESTAMP
            | pg_sys::F_MIN_TIMESTAMPTZ
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::Min)
            }
            pg_sys::F_MAX_INT2
            | pg_sys::F_MAX_INT4
            | pg_sys::F_MAX_INT8
            | pg_sys::F_MAX_FLOAT4
            | pg_sys::F_MAX_FLOAT8
            | pg_sys::F_MAX_NUMERIC
            | pg_sys::F_MAX_DATE
            | pg_sys::F_MAX_TIME
            | pg_sys::F_MAX_TIMESTAMP
            | pg_sys::F_MAX_TIMESTAMPTZ
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::Max)
            }
            pg_sys::F_SUM_INT2
            | pg_sys::F_SUM_INT4
            | pg_sys::F_SUM_INT8
            | pg_sys::F_SUM_FLOAT4
            | pg_sys::F_SUM_FLOAT8
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::Sum)
            }
            pg_sys::F_AVG_INT2
            | pg_sys::F_AVG_INT4
            | pg_sys::F_AVG_INT8
            | pg_sys::F_AVG_FLOAT4
            | pg_sys::F_AVG_FLOAT8
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::Average)
            }
            pg_sys::F_SUM_NUMERIC
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::NumericSum)
            }
            pg_sys::F_AVG_NUMERIC
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::NumericAverage)
            }
            pg_sys::F_VARIANCE_INT2
            | pg_sys::F_VARIANCE_INT4
            | pg_sys::F_VARIANCE_INT8
            | pg_sys::F_VARIANCE_FLOAT4
            | pg_sys::F_VARIANCE_FLOAT8
            | pg_sys::F_VAR_SAMP_INT2
            | pg_sys::F_VAR_SAMP_INT4
            | pg_sys::F_VAR_SAMP_INT8
            | pg_sys::F_VAR_SAMP_FLOAT4
            | pg_sys::F_VAR_SAMP_FLOAT8
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::VarianceSample)
            }
            pg_sys::F_VAR_POP_INT2
            | pg_sys::F_VAR_POP_INT4
            | pg_sys::F_VAR_POP_INT8
            | pg_sys::F_VAR_POP_FLOAT4
            | pg_sys::F_VAR_POP_FLOAT8
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::VariancePopulation)
            }
            pg_sys::F_STDDEV_INT2
            | pg_sys::F_STDDEV_INT4
            | pg_sys::F_STDDEV_INT8
            | pg_sys::F_STDDEV_FLOAT4
            | pg_sys::F_STDDEV_FLOAT8
            | pg_sys::F_STDDEV_SAMP_INT2
            | pg_sys::F_STDDEV_SAMP_INT4
            | pg_sys::F_STDDEV_SAMP_INT8
            | pg_sys::F_STDDEV_SAMP_FLOAT4
            | pg_sys::F_STDDEV_SAMP_FLOAT8
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::StddevSample)
            }
            pg_sys::F_STDDEV_POP_INT2
            | pg_sys::F_STDDEV_POP_INT4
            | pg_sys::F_STDDEV_POP_INT8
            | pg_sys::F_STDDEV_POP_FLOAT4
            | pg_sys::F_STDDEV_POP_FLOAT8
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::StddevPopulation)
            }
            pg_sys::F_BOOL_AND | pg_sys::F_EVERY
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::BoolAnd)
            }
            pg_sys::F_BOOL_OR
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::BoolOr)
            }
            pg_sys::F_ARRAY_AGG_ANYNONARRAY
                if matches!(arguments, AggregateArguments::Unary(_)) =>
            {
                Ok(AggregateKind::ArrayAgg)
            }
            pg_sys::F_STRING_AGG_TEXT_TEXT
                if matches!(arguments, AggregateArguments::StringAgg { .. }) =>
            {
                Ok(AggregateKind::StringAgg)
            }
            _ => Err(QueryPlanError::UnsupportedAggregate),
        }
    }
}

impl AggCall {
    pub(super) fn validate_argument_type(
        &self,
        argument_type: Option<ExprType>,
    ) -> Result<(), QueryPlanError> {
        let exact = |type_oid| ExprType {
            type_oid,
            typmod: -1,
            collation: pg_sys::InvalidOid,
        };
        let valid = match u32::from(self.function_oid()) {
            pg_sys::F_COUNT_ => {
                argument_type.is_none()
                    && self.result_type() == exact(pg_sys::INT8OID)
            }
            pg_sys::F_COUNT_ANY => {
                argument_type.is_some()
                    && self.result_type() == exact(pg_sys::INT8OID)
            }
            pg_sys::F_MIN_INT2 | pg_sys::F_MAX_INT2 => {
                argument_type == Some(exact(pg_sys::INT2OID))
                    && self.result_type() == exact(pg_sys::INT2OID)
            }
            pg_sys::F_MIN_INT4 | pg_sys::F_MAX_INT4 => {
                argument_type == Some(exact(pg_sys::INT4OID))
                    && self.result_type() == exact(pg_sys::INT4OID)
            }
            pg_sys::F_MIN_INT8 | pg_sys::F_MAX_INT8 => {
                argument_type == Some(exact(pg_sys::INT8OID))
                    && self.result_type() == exact(pg_sys::INT8OID)
            }
            pg_sys::F_SUM_INT2 => {
                argument_type == Some(exact(pg_sys::INT2OID))
                    && self.result_type() == exact(pg_sys::INT8OID)
            }
            pg_sys::F_SUM_INT4 => {
                argument_type == Some(exact(pg_sys::INT4OID))
                    && self.result_type() == exact(pg_sys::INT8OID)
            }
            pg_sys::F_SUM_INT8 => {
                argument_type == Some(exact(pg_sys::INT8OID))
                    && self.result_type() == exact(pg_sys::NUMERICOID)
            }
            pg_sys::F_AVG_INT2 => {
                argument_type == Some(exact(pg_sys::INT2OID))
                    && self.result_type() == exact(pg_sys::NUMERICOID)
            }
            pg_sys::F_AVG_INT4 => {
                argument_type == Some(exact(pg_sys::INT4OID))
                    && self.result_type() == exact(pg_sys::NUMERICOID)
            }
            pg_sys::F_AVG_INT8 => {
                argument_type == Some(exact(pg_sys::INT8OID))
                    && self.result_type() == exact(pg_sys::NUMERICOID)
            }
            pg_sys::F_SUM_FLOAT4 => {
                argument_type == Some(exact(pg_sys::FLOAT4OID))
                    && self.result_type() == exact(pg_sys::FLOAT4OID)
            }
            pg_sys::F_SUM_FLOAT8 => {
                argument_type == Some(exact(pg_sys::FLOAT8OID))
                    && self.result_type() == exact(pg_sys::FLOAT8OID)
            }
            pg_sys::F_AVG_FLOAT4 => {
                argument_type == Some(exact(pg_sys::FLOAT4OID))
                    && self.result_type() == exact(pg_sys::FLOAT8OID)
            }
            pg_sys::F_AVG_FLOAT8 => {
                argument_type == Some(exact(pg_sys::FLOAT8OID))
                    && self.result_type() == exact(pg_sys::FLOAT8OID)
            }
            pg_sys::F_SUM_NUMERIC | pg_sys::F_AVG_NUMERIC => {
                argument_type.is_some_and(Self::bounded_numeric_input)
                    && self.result_type() == exact(pg_sys::NUMERICOID)
            }
            pg_sys::F_MIN_FLOAT4 | pg_sys::F_MAX_FLOAT4 => {
                argument_type == Some(exact(pg_sys::FLOAT4OID))
                    && self.result_type() == exact(pg_sys::FLOAT4OID)
            }
            pg_sys::F_MIN_FLOAT8 | pg_sys::F_MAX_FLOAT8 => {
                argument_type == Some(exact(pg_sys::FLOAT8OID))
                    && self.result_type() == exact(pg_sys::FLOAT8OID)
            }
            pg_sys::F_MIN_NUMERIC | pg_sys::F_MAX_NUMERIC => {
                argument_type.is_some_and(Self::bounded_numeric_input)
                    && self.result_type() == exact(pg_sys::NUMERICOID)
            }
            pg_sys::F_MIN_DATE | pg_sys::F_MAX_DATE => {
                argument_type == Some(exact(pg_sys::DATEOID))
                    && self.result_type() == exact(pg_sys::DATEOID)
            }
            pg_sys::F_MIN_TIME | pg_sys::F_MAX_TIME => {
                argument_type == Some(exact(pg_sys::TIMEOID))
                    && self.result_type() == exact(pg_sys::TIMEOID)
            }
            pg_sys::F_MIN_TIMESTAMP | pg_sys::F_MAX_TIMESTAMP => {
                argument_type == Some(exact(pg_sys::TIMESTAMPOID))
                    && self.result_type() == exact(pg_sys::TIMESTAMPOID)
            }
            pg_sys::F_MIN_TIMESTAMPTZ | pg_sys::F_MAX_TIMESTAMPTZ => {
                argument_type == Some(exact(pg_sys::TIMESTAMPTZOID))
                    && self.result_type() == exact(pg_sys::TIMESTAMPTZOID)
            }
            pg_sys::F_VARIANCE_INT2
            | pg_sys::F_VAR_SAMP_INT2
            | pg_sys::F_VAR_POP_INT2
            | pg_sys::F_STDDEV_INT2
            | pg_sys::F_STDDEV_SAMP_INT2
            | pg_sys::F_STDDEV_POP_INT2 => {
                argument_type == Some(exact(pg_sys::INT2OID))
                    && self.result_type() == exact(pg_sys::NUMERICOID)
            }
            pg_sys::F_VARIANCE_INT4
            | pg_sys::F_VAR_SAMP_INT4
            | pg_sys::F_VAR_POP_INT4
            | pg_sys::F_STDDEV_INT4
            | pg_sys::F_STDDEV_SAMP_INT4
            | pg_sys::F_STDDEV_POP_INT4 => {
                argument_type == Some(exact(pg_sys::INT4OID))
                    && self.result_type() == exact(pg_sys::NUMERICOID)
            }
            pg_sys::F_VARIANCE_INT8
            | pg_sys::F_VAR_SAMP_INT8
            | pg_sys::F_VAR_POP_INT8
            | pg_sys::F_STDDEV_INT8
            | pg_sys::F_STDDEV_SAMP_INT8
            | pg_sys::F_STDDEV_POP_INT8 => {
                argument_type == Some(exact(pg_sys::INT8OID))
                    && self.result_type() == exact(pg_sys::NUMERICOID)
            }
            pg_sys::F_VARIANCE_FLOAT4
            | pg_sys::F_VAR_SAMP_FLOAT4
            | pg_sys::F_VAR_POP_FLOAT4
            | pg_sys::F_STDDEV_FLOAT4
            | pg_sys::F_STDDEV_SAMP_FLOAT4
            | pg_sys::F_STDDEV_POP_FLOAT4 => {
                argument_type == Some(exact(pg_sys::FLOAT4OID))
                    && self.result_type() == exact(pg_sys::FLOAT8OID)
            }
            pg_sys::F_VARIANCE_FLOAT8
            | pg_sys::F_VAR_SAMP_FLOAT8
            | pg_sys::F_VAR_POP_FLOAT8
            | pg_sys::F_STDDEV_FLOAT8
            | pg_sys::F_STDDEV_SAMP_FLOAT8
            | pg_sys::F_STDDEV_POP_FLOAT8 => {
                argument_type == Some(exact(pg_sys::FLOAT8OID))
                    && self.result_type() == exact(pg_sys::FLOAT8OID)
            }
            pg_sys::F_BOOL_AND | pg_sys::F_BOOL_OR | pg_sys::F_EVERY => {
                argument_type == Some(exact(pg_sys::BOOLOID))
                    && self.result_type() == exact(pg_sys::BOOLOID)
            }
            pg_sys::F_ARRAY_AGG_ANYNONARRAY => argument_type.is_some_and(|value| {
                Self::supports_array_agg_type(value)
                    && unsafe {
                        pg_sys::get_element_type(self.result_type().type_oid)
                    } == value.type_oid
            }),
            pg_sys::F_STRING_AGG_TEXT_TEXT => {
                argument_type.is_some_and(|value| value.type_oid == pg_sys::TEXTOID)
                    && self.result_type().type_oid == pg_sys::TEXTOID
                    && self.result_type().typmod == -1
            }
            _ => false,
        };
        (valid
            && (!self.is_distinct()
                || Self::supports_distinct(self.kind(), argument_type)))
        .then_some(())
        .ok_or(QueryPlanError::UnsupportedAggregate)
    }

    fn supports_distinct(
        kind: AggregateKind,
        argument_type: Option<ExprType>,
    ) -> bool {
        let Some(argument_type) = argument_type else {
            return false;
        };
        match kind {
            AggregateKind::Count => Self::supports_count_distinct(argument_type),
            AggregateKind::Min | AggregateKind::Max => {
                ScalarSemantics::Integer.supports_type(argument_type)
                    || Self::supports_float(argument_type)
                    || matches!(
                        argument_type.type_oid,
                        pg_sys::DATEOID
                            | pg_sys::TIMEOID
                            | pg_sys::TIMESTAMPOID
                            | pg_sys::TIMESTAMPTZOID
                    )
            }
            AggregateKind::Sum | AggregateKind::Average => {
                ScalarSemantics::Integer.supports_type(argument_type)
                    || Self::supports_float(argument_type)
            }
            AggregateKind::NumericSum | AggregateKind::NumericAverage => false,
            AggregateKind::VarianceSample | AggregateKind::VariancePopulation => {
                ScalarSemantics::Integer.supports_type(argument_type)
                    || Self::supports_float(argument_type)
            }
            // DataFusion 55 rejects DISTINCT for both STDDEV UDAFs during
            // physical planning, so admitting it would produce a runtime
            // planning failure instead of an executable offload plan.
            AggregateKind::StddevSample | AggregateKind::StddevPopulation => false,
            AggregateKind::BoolAnd | AggregateKind::BoolOr => {
                argument_type
                    == ExprType {
                        type_oid: pg_sys::BOOLOID,
                        typmod: -1,
                        collation: pg_sys::InvalidOid,
                    }
            }
            AggregateKind::ArrayAgg => Self::supports_array_agg_type(argument_type),
            AggregateKind::StringAgg => argument_type.type_oid == pg_sys::TEXTOID,
        }
    }

    fn supports_count_distinct(value_type: ExprType) -> bool {
        ScalarSemantics::Integer.supports_type(value_type)
            || Self::supports_float(value_type)
            || Self::supports_text_distinct(value_type)
            || Self::bounded_numeric_input(value_type)
            || matches!(
                value_type.type_oid,
                pg_sys::BOOLOID
                    | pg_sys::DATEOID
                    | pg_sys::TIMEOID
                    | pg_sys::TIMESTAMPOID
                    | pg_sys::TIMESTAMPTZOID
                    | pg_sys::UUIDOID
                    | pg_sys::BYTEAOID
            )
    }

    fn supports_float(value_type: ExprType) -> bool {
        matches!(value_type.type_oid, pg_sys::FLOAT4OID | pg_sys::FLOAT8OID)
            && value_type.typmod == -1
            && value_type.collation == pg_sys::InvalidOid
    }

    fn supports_array_agg_type(value_type: ExprType) -> bool {
        ScalarSemantics::Integer.supports_type(value_type)
            || Self::supports_float(value_type)
            || Self::supports_text_distinct(value_type)
            || value_type
                == ExprType {
                    type_oid: pg_sys::BOOLOID,
                    typmod: -1,
                    collation: pg_sys::InvalidOid,
                }
    }

    fn supports_text_distinct(value_type: ExprType) -> bool {
        matches!(
            value_type.type_oid,
            pg_sys::TEXTOID
                | pg_sys::VARCHAROID
                | pg_sys::BPCHAROID
                | pg_sys::NAMEOID
        ) && value_type.collation != pg_sys::InvalidOid
    }

    /// Current scan providers expose PostgreSQL NUMERIC as Iceberg's fixed
    /// Decimal128 domain. A declared typmod proves the coefficient and scale
    /// bounds used by the fixed i256 aggregate state; unbounded NUMERIC remains
    /// outside the source ABI and must decline at the semantic gate.
    fn bounded_numeric_input(value_type: ExprType) -> bool {
        value_type.type_oid == pg_sys::NUMERICOID
            && value_type.collation == pg_sys::InvalidOid
            && numeric_precision_scale(value_type.typmod).is_some_and(|typmod| {
                (1..=38).contains(&typmod.precision)
                    && typmod.scale >= 0
                    && typmod.scale <= typmod.precision as i32
            })
    }
}
