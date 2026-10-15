//! Timestamp pruning with the same nanosecond-to-microsecond floor as reads.

use iceberg_lite::expr::{Predicate, PredicateOperator};
use iceberg_lite::spec::Datum;
use lagodb_arrow::pg_epoch_micros_to_unix_micros;

use super::builder::IcebergPredicateBuilder;
use super::plan::PlannedComparisonOperator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimestampPrecision {
    Microseconds,
    Nanoseconds,
}

pub(crate) struct TimestampComparison {
    unix_micros: i64,
    precision: TimestampPrecision,
    with_timezone: bool,
}

impl TimestampComparison {
    pub(crate) fn from_pg_micros(
        pg_micros: i64,
        precision: TimestampPrecision,
        with_timezone: bool,
    ) -> Option<Self> {
        if matches!(pg_micros, i64::MIN | i64::MAX) {
            return None;
        }
        Some(Self {
            unix_micros: pg_epoch_micros_to_unix_micros(pg_micros)?,
            precision,
            with_timezone,
        })
    }

    /// NULL and operator mirroring/negation are handled by the caller. An
    /// unrepresentable boundary abandons pruning; the PG residual stays active.
    pub(crate) fn bind(
        self,
        column: IcebergPredicateBuilder,
        operator: PlannedComparisonOperator,
    ) -> Option<Predicate> {
        if self.precision == TimestampPrecision::Microseconds {
            let datum = if self.with_timezone {
                Datum::timestamptz_micros(self.unix_micros)
            } else {
                Datum::timestamp_micros(self.unix_micros)
            };
            return Some(column.comparison(operator.into(), datum));
        }

        // A PG value u represents the entire half-open nanosecond interval
        // [u*1000, (u+1)*1000), including for negative epochs. Comparing only
        // against u*1000 would change the truth sets of =, <= and >.
        Some(match operator {
            PlannedComparisonOperator::Eq | PlannedComparisonOperator::NotEq => {
                let lower = self.nanos_boundary(false)?;
                let upper = self.nanos_boundary(true)?;
                if matches!(operator, PlannedComparisonOperator::Eq) {
                    column
                        .clone()
                        .comparison(PredicateOperator::GreaterThanOrEq, lower)
                        .and(column.comparison(PredicateOperator::LessThan, upper))
                } else {
                    column
                        .clone()
                        .comparison(PredicateOperator::LessThan, lower)
                        .or(column
                            .comparison(PredicateOperator::GreaterThanOrEq, upper))
                }
            }
            PlannedComparisonOperator::Lt => column
                .comparison(PredicateOperator::LessThan, self.nanos_boundary(false)?),
            PlannedComparisonOperator::Le => column
                .comparison(PredicateOperator::LessThan, self.nanos_boundary(true)?),
            PlannedComparisonOperator::Gt => column.comparison(
                PredicateOperator::GreaterThanOrEq,
                self.nanos_boundary(true)?,
            ),
            PlannedComparisonOperator::Ge => column.comparison(
                PredicateOperator::GreaterThanOrEq,
                self.nanos_boundary(false)?,
            ),
        })
    }

    fn nanos_boundary(&self, upper: bool) -> Option<Datum> {
        let micros = if upper {
            self.unix_micros.checked_add(1)?
        } else {
            self.unix_micros
        };
        let nanos = micros.checked_mul(1_000)?;
        Some(if self.with_timezone {
            Datum::timestamptz_nanos(nanos)
        } else {
            Datum::timestamp_nanos(nanos)
        })
    }
}
