//! Validated PostgreSQL `NUMERIC(p, s)` representation for Decimal128.

use super::numeric::Decimal128NumericCodec;

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
}
