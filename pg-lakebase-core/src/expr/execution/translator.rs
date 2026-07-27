//! Provider-facing predicate translation contract.

pub use super::error::BuildPredicateError;
pub use super::params::PgParamValue;
pub use super::value::{PgColumnRef, PgDatumRef, PgLiteral};
pub use crate::expr::contract::PgComparisonOp;

pub trait PgPredicateTranslator {
    type Scalar;
    type Predicate;
    type Error: std::error::Error + 'static;

    fn column(&mut self, col: PgColumnRef<'_>) -> Result<Self::Scalar, Self::Error>;
    fn literal(&mut self, lit: PgLiteral<'_>) -> Result<Self::Scalar, Self::Error>;
    fn param_value(
        &mut self,
        param: PgParamValue<'_>,
    ) -> Result<Self::Scalar, Self::Error>;
    fn comparison(
        &mut self,
        op: PgComparisonOp,
        left: Self::Scalar,
        right: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error>;
    fn is_null(
        &mut self,
        value: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error>;
    fn is_not_null(
        &mut self,
        value: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error>;
    fn and(
        &mut self,
        items: Vec<Self::Predicate>,
    ) -> Result<Self::Predicate, Self::Error>;
    fn or(
        &mut self,
        items: Vec<Self::Predicate>,
    ) -> Result<Self::Predicate, Self::Error>;
    fn not(&mut self, item: Self::Predicate) -> Result<Self::Predicate, Self::Error>;
}
