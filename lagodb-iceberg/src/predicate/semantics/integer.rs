//! Integer-domain boundary classification for Iceberg predicates.

use lagodb_core::expr::PgComparisonKind;

/// Position of an `int8` comparison value outside Iceberg's `int` domain.
/// Classification happens once while a planned or runtime predicate is bound,
/// never while rows are evaluated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::predicate) enum Int32OutOfRange {
    Below,
    Above,
}

impl Int32OutOfRange {
    pub(in crate::predicate) fn narrow(value: i64) -> Result<i32, Self> {
        i32::try_from(value)
            .map_err(|_| if value < 0 { Self::Below } else { Self::Above })
    }

    pub(in crate::predicate) const fn comparison_matches_non_null(
        self,
        operator: PgComparisonKind,
    ) -> bool {
        match (self, operator) {
            (_, PgComparisonKind::NotEqual)
            | (Self::Above, PgComparisonKind::Less | PgComparisonKind::LessEqual)
            | (
                Self::Below,
                PgComparisonKind::Greater | PgComparisonKind::GreaterEqual,
            ) => true,
            (_, PgComparisonKind::Equal)
            | (
                Self::Above,
                PgComparisonKind::Greater | PgComparisonKind::GreaterEqual,
            )
            | (Self::Below, PgComparisonKind::Less | PgComparisonKind::LessEqual) => {
                false
            }
        }
    }
}
