//! One numeric policy shared by managed DDL and storage schema construction.

use iceberg_lite::spec::PrimitiveType;
use lagodb_core::datum::NumericScaledDomain;

pub(crate) struct ManagedNumeric {
    decimal: Option<(u32, u32)>,
}

impl ManagedNumeric {
    pub(crate) fn new(typmod: i32) -> Self {
        // Iceberg decimals hold at most 38 digits; wider NUMERIC is stored
        // as double.
        let decimal = NumericScaledDomain::for_typmod(typmod)
            .filter(|domain| domain.precision() <= 38)
            .map(|domain| (u32::from(domain.precision()), u32::from(domain.scale())));
        Self { decimal }
    }

    pub(crate) fn uses_double(&self) -> bool {
        self.decimal.is_none()
    }

    pub(crate) fn storage_type(&self) -> PrimitiveType {
        match self.decimal {
            Some((precision, scale)) => PrimitiveType::Decimal { precision, scale },
            None => PrimitiveType::Double,
        }
    }
}
