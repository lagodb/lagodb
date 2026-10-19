//! Expression semantics for finite Decimal128-backed NUMERIC values.

use pgrx::pg_sys;

use crate::datum::{
    Decimal128ComparisonValue, Decimal128Semantics, numeric_precision_scale,
};

use super::{ExprType, PgComparisonKind};

impl Decimal128Semantics {
    #[must_use]
    pub fn for_type(value_type: ExprType) -> Option<Self> {
        if value_type.type_oid != pg_sys::NUMERICOID
            || value_type.collation != pg_sys::InvalidOid
        {
            return None;
        }
        let typmod = numeric_precision_scale(value_type.typmod)?;
        Self::new(
            u8::try_from(typmod.precision).ok()?,
            i8::try_from(typmod.scale).ok()?,
        )
    }

    /// Prove one storage comparison has the same bounded Decimal128 shape on
    /// the declared column, its binary-compatible effective type, and the
    /// PostgreSQL-evaluated comparison value.
    #[must_use]
    pub fn for_storage_comparison(
        declared: ExprType,
        effective: ExprType,
        value: ExprType,
    ) -> Option<Self> {
        let declared = Self::for_type(declared)?;
        let effective = Self::for_type(effective)?;
        let value = Self::for_type(value)?;
        (declared == effective && effective == value).then_some(effective)
    }

    #[must_use]
    pub fn value_type(self) -> ExprType {
        ExprType {
            type_oid: pg_sys::NUMERICOID,
            typmod: self.codec().typmod(),
            collation: pg_sys::InvalidOid,
        }
    }
}

impl Decimal128ComparisonValue {
    /// Evaluate a comparison against a non-NULL value from a finite Decimal128
    /// column. A finite literal returns `None` because it still needs the Arrow
    /// or Iceberg scalar comparison kernel.
    #[must_use]
    pub const fn matches_finite_column(
        self,
        operator: PgComparisonKind,
    ) -> Option<bool> {
        let value_is_above = match self {
            Self::Finite(_) => return None,
            Self::NegativeInfinity => false,
            Self::PositiveInfinity | Self::NaN => true,
        };
        Some(match operator {
            PgComparisonKind::NotEqual => true,
            PgComparisonKind::Less | PgComparisonKind::LessEqual => value_is_above,
            PgComparisonKind::Greater | PgComparisonKind::GreaterEqual => {
                !value_is_above
            }
            PgComparisonKind::Equal => false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use PgComparisonKind::{Equal, Greater, GreaterEqual, Less, LessEqual, NotEqual};

    #[test]
    fn special_values_have_total_finite_domain_comparisons() {
        let below = Decimal128ComparisonValue::NegativeInfinity;
        let above = Decimal128ComparisonValue::PositiveInfinity;
        let nan = Decimal128ComparisonValue::NaN;

        for special in [below, above, nan] {
            assert_eq!(special.matches_finite_column(Equal), Some(false));
            assert_eq!(special.matches_finite_column(NotEqual), Some(true));
        }
        for special in [above, nan] {
            assert_eq!(special.matches_finite_column(Less), Some(true));
            assert_eq!(special.matches_finite_column(LessEqual), Some(true));
            assert_eq!(special.matches_finite_column(Greater), Some(false));
            assert_eq!(special.matches_finite_column(GreaterEqual), Some(false));
        }
        assert_eq!(below.matches_finite_column(Less), Some(false));
        assert_eq!(below.matches_finite_column(LessEqual), Some(false));
        assert_eq!(below.matches_finite_column(Greater), Some(true));
        assert_eq!(below.matches_finite_column(GreaterEqual), Some(true));
        assert_eq!(
            Decimal128ComparisonValue::Finite(0).matches_finite_column(Equal),
            None,
        );
    }
}
