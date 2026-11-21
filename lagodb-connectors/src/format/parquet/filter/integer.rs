//! Integer comparison values narrowed once to the column's physical width.

use super::operator::ComparisonOperator;
use super::value::BoundValue;

/// Physical width of an integer column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IntegerWidth {
    I16,
    I32,
    I64,
}

/// Position of a comparison value outside the column's integer domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IntegerOutOfRange {
    Below,
    Above,
}

impl IntegerWidth {
    const WIDTH_I16: i32 = 0;
    const WIDTH_I32: i32 = 1;
    const WIDTH_I64: i32 = 2;

    pub(super) const fn tag(self) -> i32 {
        match self {
            Self::I16 => Self::WIDTH_I16,
            Self::I32 => Self::WIDTH_I32,
            Self::I64 => Self::WIDTH_I64,
        }
    }

    pub(super) const fn from_tag(tag: i32) -> Option<Self> {
        match tag {
            Self::WIDTH_I16 => Some(Self::I16),
            Self::WIDTH_I32 => Some(Self::I32),
            Self::WIDTH_I64 => Some(Self::I64),
            _ => None,
        }
    }

    /// Narrow a value compared by PostgreSQL in the integer domain. Ordering is
    /// unchanged for in-range values; out-of-range values decide the
    /// comparison for every non-NULL row.
    pub(super) fn narrow(self, value: i64) -> Result<BoundValue, IntegerOutOfRange> {
        let outside = || {
            if value < 0 {
                IntegerOutOfRange::Below
            } else {
                IntegerOutOfRange::Above
            }
        };
        match self {
            Self::I16 => i16::try_from(value)
                .map(BoundValue::I16)
                .map_err(|_| outside()),
            Self::I32 => i32::try_from(value)
                .map(BoundValue::I32)
                .map_err(|_| outside()),
            Self::I64 => Ok(BoundValue::I64(value)),
        }
    }
}

impl IntegerOutOfRange {
    pub(super) const fn comparison_matches_non_null(
        self,
        operator: ComparisonOperator,
    ) -> bool {
        match (self, operator) {
            (_, ComparisonOperator::NotEq)
            | (Self::Above, ComparisonOperator::Lt | ComparisonOperator::Le)
            | (Self::Below, ComparisonOperator::Gt | ComparisonOperator::Ge) => true,
            (_, ComparisonOperator::Eq)
            | (Self::Above, ComparisonOperator::Gt | ComparisonOperator::Ge)
            | (Self::Below, ComparisonOperator::Lt | ComparisonOperator::Le) => false,
        }
    }
}
