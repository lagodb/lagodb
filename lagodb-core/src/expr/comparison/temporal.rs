//! Exact planning-time normalization for PostgreSQL date/timestamp comparisons.

use pgrx::datum::{Date, Timestamp};
use pgrx::pg_sys;

use crate::expr::contract::PgComparisonOp;
use crate::expr::pg::{PgConst, PgExprRef, PgVar};

use super::{PgComparisonKind, PgComparisonSignature};

/// A `date` column compared with a folded `timestamp` constant, restated
/// without the timestamp.
///
/// PostgreSQL compares the date at midnight. Restating the comparison over
/// dates avoids an execution engine's narrower timestamp coercion and keeps
/// SQL NULL semantics for an equality that no date can satisfy.
#[derive(Clone, Copy, Debug)]
pub struct PgDateTimestampComparison<'a> {
    column: PgExprRef<'a>,
    outcome: PgDateComparisonOutcome<'a>,
}

/// The date predicate equivalent to the original comparison.
#[derive(Clone, Copy, Debug)]
pub enum PgDateComparisonOutcome<'a> {
    /// `column operator bound`, where `operator` is a built-in date comparison.
    Compare {
        operator: PgComparisonOp,
        bound: PgExprRef<'a>,
    },
    /// True for every non-NULL date and NULL for NULL.
    StrictTrue,
    /// False for every non-NULL date and NULL for NULL.
    StrictFalse,
}

impl<'a> PgDateTimestampComparison<'a> {
    /// Normalize a built-in comparison between a direct date column and a
    /// non-NULL timestamp-without-time-zone constant.
    ///
    /// `Param` values and timezone-dependent timestamptz comparisons are
    /// deliberately outside this planning-time transformation.
    ///
    /// # Safety
    ///
    /// Both expressions and the timestamp Datum must remain live in the
    /// current PostgreSQL planner memory context. The normalized date Const is
    /// allocated in that context.
    pub unsafe fn normalize(
        operator: PgComparisonOp,
        left: PgExprRef<'a>,
        right: PgExprRef<'a>,
    ) -> Option<Self> {
        let signature = operator.builtin_signature()?;
        let (column, constant, kind) =
            match (signature.left_type(), signature.right_type()) {
                (pg_sys::DATEOID, pg_sys::TIMESTAMPOID) => {
                    (left, right, signature.kind())
                }
                (pg_sys::TIMESTAMPOID, pg_sys::DATEOID) => {
                    (right, left, signature.kind().commuted())
                }
                _ => return None,
            };
        PgVar::try_from_expr(column.without_relabels())?;
        let constant = PgConst::try_from_expr(constant.without_relabels())?;
        // SAFETY: the caller keeps the Const and its Datum live.
        let timestamp = unsafe { constant.timestamp() }?;
        let outcome = match TimestampDay::from(timestamp).date_comparison(kind) {
            DateComparison::Compare { kind, date } => {
                PgDateComparisonOutcome::Compare {
                    operator: PgComparisonSignature::for_types(
                        pg_sys::DATEOID,
                        pg_sys::DATEOID,
                        kind,
                    )
                    .expect("built-in date comparison is registered")
                    .collationless_operator(),
                    // SAFETY: the caller's planner context owns the Const.
                    bound: unsafe { constant.derived_date(date) },
                }
            }
            DateComparison::StrictTrue => PgDateComparisonOutcome::StrictTrue,
            DateComparison::StrictFalse => PgDateComparisonOutcome::StrictFalse,
        };
        Some(Self { column, outcome })
    }

    /// The compared date column, possibly under binary-compatible relabels.
    #[inline]
    pub const fn column(self) -> PgExprRef<'a> {
        self.column
    }

    #[inline]
    pub const fn outcome(self) -> PgDateComparisonOutcome<'a> {
        self.outcome
    }
}

/// The date part of a timestamp and whether the timestamp is that date's
/// midnight, as PostgreSQL defines both.
#[derive(Clone, Copy, Debug)]
struct TimestampDay {
    date: pg_sys::DateADT,
    at_midnight: bool,
}

/// `date <kind> timestamp` restated over dates.
#[derive(Clone, Copy, Debug)]
enum DateComparison {
    Compare {
        kind: PgComparisonKind,
        date: pg_sys::DateADT,
    },
    StrictTrue,
    StrictFalse,
}

impl From<Timestamp> for TimestampDay {
    /// `timestamp_date` maps both infinities to the matching date infinity,
    /// and `date_timestamp` maps them back, so infinities count as midnight.
    fn from(timestamp: Timestamp) -> Self {
        let date = Date::from(timestamp);
        Self {
            date: date.into_inner(),
            at_midnight: Timestamp::from(date).into_inner() == timestamp.into_inner(),
        }
    }
}

impl TimestampDay {
    /// PostgreSQL's `date_cmp_timestamp_internal` compares the date's midnight
    /// with the timestamp, so a bound inside a day moves up to the next date
    /// for `<` and `>=`. No date equals such a bound.
    fn date_comparison(self, kind: PgComparisonKind) -> DateComparison {
        if self.at_midnight {
            return DateComparison::Compare {
                kind,
                date: self.date,
            };
        }
        match kind {
            PgComparisonKind::Equal => DateComparison::StrictFalse,
            PgComparisonKind::NotEqual => DateComparison::StrictTrue,
            PgComparisonKind::LessEqual | PgComparisonKind::Greater => {
                DateComparison::Compare {
                    kind,
                    date: self.date,
                }
            }
            // A non-midnight timestamp is finite, and its date is at most
            // 294276-12-31, far below the date range end, so the next date is
            // a finite date.
            PgComparisonKind::Less | PgComparisonKind::GreaterEqual => {
                DateComparison::Compare {
                    kind,
                    date: self.date + 1,
                }
            }
        }
    }
}
