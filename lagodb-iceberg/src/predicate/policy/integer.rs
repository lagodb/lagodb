//! Integer-domain boundary classification for Iceberg predicates.

use super::ComparisonOpClass;

/// Position of an `int8` comparison value outside Iceberg's `int` domain.
/// Classification happens once while a planned or runtime predicate is bound,
/// never while rows are evaluated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Int32OutOfRange {
    Below,
    Above,
}

impl Int32OutOfRange {
    pub(crate) fn narrow(value: i64) -> Result<i32, Self> {
        i32::try_from(value)
            .map_err(|_| if value < 0 { Self::Below } else { Self::Above })
    }

    pub(crate) const fn comparison_matches_non_null(
        self,
        operator: ComparisonOpClass,
    ) -> bool {
        match (self, operator) {
            (_, ComparisonOpClass::NotEqual)
            | (Self::Above, ComparisonOpClass::Less | ComparisonOpClass::LessEqual)
            | (
                Self::Below,
                ComparisonOpClass::Greater | ComparisonOpClass::GreaterEqual,
            ) => true,
            (_, ComparisonOpClass::Equal)
            | (
                Self::Above,
                ComparisonOpClass::Greater | ComparisonOpClass::GreaterEqual,
            )
            | (Self::Below, ComparisonOpClass::Less | ComparisonOpClass::LessEqual) => {
                false
            }
        }
    }
}
