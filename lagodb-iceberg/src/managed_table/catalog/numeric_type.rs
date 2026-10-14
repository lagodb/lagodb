//! One numeric policy shared by managed DDL and storage schema construction.

use iceberg_lite::spec::PrimitiveType;
use lagodb_core::tuple::numeric_precision_scale;

pub(crate) struct ManagedNumeric {
    decimal: Option<(u32, u32)>,
}

impl ManagedNumeric {
    pub(crate) fn new(typmod: i32) -> Self {
        let decimal = numeric_precision_scale(typmod).and_then(|value| {
            let (precision, scale) = if value.scale < 0 {
                (value.precision + value.scale.unsigned_abs(), 0)
            } else {
                (value.precision.max(value.scale as u32), value.scale as u32)
            };
            (precision <= 38 && scale <= 38).then_some((precision, scale))
        });
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
