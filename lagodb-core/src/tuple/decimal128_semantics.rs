//! Validated PostgreSQL `NUMERIC(p, s)` representation for Arrow Decimal128.

use pgrx::pg_sys;

use crate::expr::ExprType;

use super::{Decimal128NumericCodec, numeric_precision_scale};

/// One exact finite decimal shape shared by PostgreSQL, Arrow, and providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal128Semantics {
    precision: u8,
    scale: i8,
}

impl Decimal128Semantics {
    #[must_use]
    pub fn new(precision: u8, scale: i8) -> Option<Self> {
        Decimal128NumericCodec::new(u32::from(precision), u32::try_from(scale).ok()?)
            .ok()
            .map(|_| Self { precision, scale })
    }

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

    #[inline]
    pub const fn precision(self) -> u8 {
        self.precision
    }

    #[inline]
    pub const fn scale(self) -> i8 {
        self.scale
    }

    #[must_use]
    pub fn codec(self) -> Decimal128NumericCodec {
        Decimal128NumericCodec::new(
            u32::from(self.precision),
            u32::try_from(self.scale)
                .expect("validated Decimal128 scale is non-negative"),
        )
        .expect("Decimal128 semantics retain a validated codec shape")
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
