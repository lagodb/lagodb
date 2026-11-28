//! Fixed-point decimal execution types and the Arrow arithmetic rules over them.
//!
//! PostgreSQL NUMERIC arithmetic has an unbounded result type, while Arrow
//! decimals carry a static precision and scale. A [`FixedDecimalType`] is the
//! static `Decimal128` execution type of one engine value, and arithmetic and
//! CASE take DataFusion's own result types for it. Arrow derives `+ - * /`
//! with the Hive rules capped at 38 digits (`arrow-arith` `decimal_op`),
//! computes in checked i128 and never validates the declared precision. A
//! type below 38 digits therefore bounds its values, while a 38-digit type
//! may carry any i128 coefficient ([`FixedDecimalType::value_domain`]). A
//! result or scaled dividend outside i128 raises an error instead of
//! wrapping, and division by zero raises.
//!
//! SUM and AVG keep DataFusion's native aggregates where their wrapping
//! accumulator provably holds every sum of fewer than 2^64 values; other SUMs
//! accumulate in checked i128 and raise PostgreSQL's NUMERIC overflow error.
//!
//! The result scale of `+ - *` equals the display scale PostgreSQL derives
//! from typmod-constrained columns and constants (`add_var`, `mul_var`), so
//! the fixed scale is also the PostgreSQL result scale. Division and AVG are
//! the exceptions: they keep DataFusion's scale, the dividend's scale plus 4,
//! and truncate, where PostgreSQL chooses a value-dependent scale
//! (`select_div_scale`) and rounds.

use arrow_schema::{DECIMAL128_MAX_PRECISION, DataType};
use lagodb_core::datum::{NumericScaledDomain, numeric_typmod};
use lagodb_core::expr::ExprType;
use pgrx::pg_sys;

use super::Decimal128Semantics;

const _: () = assert!(NumericScaledDomain::MAX_PRECISION > DECIMAL128_MAX_PRECISION);
const _: () = assert!(
    FixedDecimalType::NATIVE_SUM_MAX_PRECISION + FixedDecimalType::SUM_COUNT_DIGITS
        == DECIMAL128_MAX_PRECISION
);

/// The static `Decimal128` type of one NUMERIC engine value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedDecimalType {
    domain: NumericScaledDomain,
}

/// PostgreSQL NUMERIC operators with a native fixed-point implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecimalOperator {
    Add,
    Subtract,
    Multiply,
    /// Arrow's division, truncated to `s1 + 4` fractional digits; PostgreSQL
    /// rounds to a value-dependent scale.
    Divide,
}

impl DecimalOperator {
    /// The operator implemented by a built-in NUMERIC function.
    pub const fn for_function(function_oid: u32) -> Option<Self> {
        match function_oid {
            pg_sys::F_NUMERIC_ADD => Some(Self::Add),
            pg_sys::F_NUMERIC_SUB => Some(Self::Subtract),
            pg_sys::F_NUMERIC_MUL => Some(Self::Multiply),
            pg_sys::F_NUMERIC_DIV => Some(Self::Divide),
            _ => None,
        }
    }

    pub(crate) const fn wire_id(self) -> i32 {
        match self {
            Self::Add => 1,
            Self::Subtract => 2,
            Self::Multiply => 3,
            Self::Divide => 4,
        }
    }

    pub(crate) const fn from_wire_id(id: i32) -> Option<Self> {
        match id {
            1 => Some(Self::Add),
            2 => Some(Self::Subtract),
            3 => Some(Self::Multiply),
            4 => Some(Self::Divide),
            _ => None,
        }
    }

    pub const fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
        }
    }
}

impl FixedDecimalType {
    /// Digits a SUM can add to its input precision: a u64 row count is below
    /// 10^20.
    const SUM_COUNT_DIGITS: u8 = 20;
    /// The largest input precision whose sum of fewer than 2^64 values stays
    /// below 10^38, so a native wrapping Decimal128 accumulator holds it.
    const NATIVE_SUM_MAX_PRECISION: u8 = 18;
    /// Digits DataFusion's native SUM adds to its decimal input precision
    /// (`datafusion-functions-aggregate` `sum.rs`, Spark's rule).
    const NATIVE_SUM_DIGITS: u8 = 10;
    /// Digits DataFusion's native AVG adds to both the precision and the scale
    /// of its decimal input (`average.rs` `return_type`, Spark's rule).
    const NATIVE_AVERAGE_DIGITS: u8 = 4;
    /// Fractional digits Arrow's decimal division adds to the dividend's
    /// scale (`arrow-arith` `decimal_op`).
    const DIVISION_SCALE_DIGITS: u8 = 4;
    /// The smallest Decimal128 input precision for which native AVG keeps its
    /// running sum in `Decimal256(76, s)`: `avg_sum_data_type` reserves 13
    /// digits above the input and leaves Decimal128 beyond 38.
    const NATIVE_AVERAGE_WIDE_SUM_PRECISION: u8 = 26;

    #[must_use]
    pub const fn new(precision: u8, scale: u8) -> Option<Self> {
        if precision > DECIMAL128_MAX_PRECISION {
            return None;
        }
        match NumericScaledDomain::new(precision, scale) {
            Some(domain) => Some(Self { domain }),
            None => None,
        }
    }

    /// The fixed type holding every value of a `numeric(p, s)` type
    /// ([`NumericScaledDomain::for_typmod`]), or `None` beyond Decimal128.
    #[must_use]
    pub fn for_type(value_type: ExprType) -> Option<Self> {
        if value_type.type_oid != pg_sys::NUMERICOID
            || value_type.collation != pg_sys::InvalidOid
        {
            return None;
        }
        let domain = NumericScaledDomain::for_typmod(value_type.typmod)?;
        Self::new(domain.precision(), domain.scale())
    }

    /// The exact decimal image of a PostgreSQL integer type.
    #[must_use]
    pub const fn for_integer(type_oid: pg_sys::Oid) -> Option<Self> {
        match type_oid {
            pg_sys::INT2OID => Self::new(5, 0),
            pg_sys::INT4OID => Self::new(10, 0),
            pg_sys::INT8OID => Self::new(19, 0),
            _ => None,
        }
    }

    #[inline]
    pub const fn precision(self) -> u8 {
        self.domain.precision()
    }

    #[inline]
    pub const fn scale(self) -> u8 {
        self.domain.scale()
    }

    /// The domain of this type's declared values, in which constants are
    /// bound exactly.
    #[inline]
    pub const fn domain(self) -> NumericScaledDomain {
        self.domain
    }

    /// The domain holding every value an engine value of this type can carry.
    /// Arithmetic and SUM cap their result types at 38 digits while their
    /// values may use any i128 coefficient, one digit more; Arrow never
    /// validates a declared decimal precision.
    pub fn value_domain(self) -> NumericScaledDomain {
        if self.precision() < DECIMAL128_MAX_PRECISION {
            return self.domain;
        }
        NumericScaledDomain::new(DECIMAL128_MAX_PRECISION + 1, self.scale())
            .expect("39 digits are within the 76-digit scaled domain")
    }

    pub fn decimal128(self) -> Option<Decimal128Semantics> {
        Decimal128Semantics::new(self.precision(), self.arrow_scale())
    }

    pub fn data_type(self) -> DataType {
        DataType::Decimal128(self.precision(), self.arrow_scale())
    }

    /// The execution type in PostgreSQL typmod form, as carried by plan facts.
    pub fn value_type(self) -> ExprType {
        ExprType {
            type_oid: pg_sys::NUMERICOID,
            typmod: numeric_typmod(
                u32::from(self.precision()),
                i32::from(self.scale()),
            ),
            collation: pg_sys::InvalidOid,
        }
    }

    /// Arrow's result type for `self <operator> right`: the Hive rules with
    /// the precision capped at 38 digits. `None` for a product scale beyond
    /// 38, which Arrow rejects, and for a division whose dividend scaling
    /// factor has no i128 value.
    #[must_use]
    pub fn arithmetic(self, operator: DecimalOperator, right: Self) -> Option<Self> {
        let (precision, scale) = match operator {
            DecimalOperator::Add | DecimalOperator::Subtract => {
                let scale = self.scale().max(right.scale());
                (
                    self.integer_digits().max(right.integer_digits()) + scale + 1,
                    scale,
                )
            }
            DecimalOperator::Multiply => (
                self.precision() + right.precision() + 1,
                self.scale() + right.scale(),
            ),
            // `arrow-arith` `decimal_op`: the dividend is scaled by
            // `10^(scale - s1 + s2)` and divided in checked i128, truncating.
            // The quotient needs at most `p1 - s1 + s2` integer digits.
            DecimalOperator::Divide => {
                let scale = (self.scale() + Self::DIVISION_SCALE_DIGITS)
                    .min(DECIMAL128_MAX_PRECISION);
                let factor_digits = scale - self.scale() + right.scale();
                if factor_digits > DECIMAL128_MAX_PRECISION {
                    return None;
                }
                (self.precision() + factor_digits, scale)
            }
        };
        Self::new(precision.min(DECIMAL128_MAX_PRECISION), scale)
    }

    /// The lossless common type of values of both types, used by CASE,
    /// COALESCE and comparisons. Unlike DataFusion's `type_union_coercion`,
    /// it is not capped at 38 digits: a capped common type would make the
    /// branch or operand casts fail at run time, so `None` declines instead.
    #[must_use]
    pub fn common(self, other: Self) -> Option<Self> {
        let scale = self.scale().max(other.scale());
        Self::new(
            self.integer_digits().max(other.integer_digits()) + scale,
            scale,
        )
    }

    /// The result type of SUM over this type: the exact type of a sum of
    /// fewer than 2^64 values when native SUM computes it
    /// ([`Self::native_sum_input`]), otherwise `(38, s)`, which the checked
    /// i128 SUM fills with any i128 coefficient.
    #[must_use]
    pub const fn sum(self) -> Option<Self> {
        if self.precision() <= Self::NATIVE_SUM_MAX_PRECISION {
            Self::new(self.precision() + Self::SUM_COUNT_DIGITS, self.scale())
        } else {
            Self::new(DECIMAL128_MAX_PRECISION, self.scale())
        }
    }

    /// The operand type for which DataFusion's native SUM returns exactly
    /// [`Self::sum`], or `None` when the sum needs the checked i128 SUM. The
    /// cast raises only the precision, which keeps every value and moves no
    /// data. A compiler-only type for the SUM call, never an execution type.
    #[must_use]
    pub fn native_sum_input(self) -> Option<DataType> {
        (self.precision() <= Self::NATIVE_SUM_MAX_PRECISION).then(|| {
            DataType::Decimal128(
                self.precision() + Self::SUM_COUNT_DIGITS - Self::NATIVE_SUM_DIGITS,
                self.arrow_scale(),
            )
        })
    }

    /// The type native AVG returns for [`Self::native_average_input`]:
    /// `(min(38, p' + 4), min(38, s + 4))`.
    ///
    /// Native AVG sums an input of at most 25 digits with a wrapping add in
    /// `Decimal128(38, s)`, which holds fewer than 2^64 values of at most 18
    /// digits, and wider inputs in `Decimal256(76, s)`, which cannot wrap.
    /// Its final scaling and division are checked (`DecimalAverager::avg`),
    /// so an average beyond the result type raises an error.
    #[must_use]
    pub fn average(self) -> Option<Self> {
        let input = self.native_average_precision();
        Self::new(
            (input + Self::NATIVE_AVERAGE_DIGITS).min(DECIMAL128_MAX_PRECISION),
            (self.scale() + Self::NATIVE_AVERAGE_DIGITS)
                .min(DECIMAL128_MAX_PRECISION),
        )
    }

    /// The operand type for DataFusion's native AVG. An input of 19 to 25
    /// digits has its precision raised to 26, a cast that moves no data, so
    /// native AVG keeps a Decimal256 sum. A compiler-only type for the AVG
    /// call, never an execution type.
    #[must_use]
    pub fn native_average_input(self) -> DataType {
        DataType::Decimal128(self.native_average_precision(), self.arrow_scale())
    }

    const fn native_average_precision(self) -> u8 {
        if self.precision() <= Self::NATIVE_SUM_MAX_PRECISION
            || self.precision() >= Self::NATIVE_AVERAGE_WIDE_SUM_PRECISION
        {
            self.precision()
        } else {
            Self::NATIVE_AVERAGE_WIDE_SUM_PRECISION
        }
    }

    const fn integer_digits(self) -> u8 {
        self.precision() - self.scale()
    }

    pub fn arrow_scale(self) -> i8 {
        i8::try_from(self.scale()).expect("a Decimal128 scale of at most 38 fits i8")
    }
}

impl From<Decimal128Semantics> for FixedDecimalType {
    fn from(semantics: Decimal128Semantics) -> Self {
        Self::new(
            semantics.precision(),
            u8::try_from(semantics.scale())
                .expect("validated Decimal128 scale is non-negative"),
        )
        .expect("Decimal128 semantics are a fixed decimal type")
    }
}
