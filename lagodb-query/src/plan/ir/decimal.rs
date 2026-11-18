//! Fixed-point decimal execution types and the Arrow arithmetic rules over them.
//!
//! PostgreSQL NUMERIC arithmetic has an unbounded result type, while Arrow
//! decimals carry a static precision and scale. A [`FixedDecimalType`] is the
//! proven static execution type of one engine value: an Arrow decimal that
//! holds every value its operands can produce. Arithmetic, SUM and CASE derive
//! the narrowest such type; AVG takes the type DataFusion's native AVG returns
//! for its planned input ([`FixedDecimalType::average`]). Arrow derives `+`,
//! `-` and `*` result types with the Hive rules (`arrow-arith` `decimal_op`)
//! but only checks i128/i256 overflow, never the declared precision. Choosing
//! `Decimal256` whenever the proven precision exceeds 38 therefore keeps every
//! value inside its declared type, so no per-value precision check is needed.
//!
//! The result scale of each arithmetic rule equals the display scale
//! PostgreSQL derives from typmod-constrained columns and constants (`add_var`,
//! `mul_var`), so the fixed scale is also the PostgreSQL result scale. AVG is
//! the exception: its scale is DataFusion's, not PostgreSQL's division scale.

use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType};
use lagodb_core::datum::{NumericScaledDomain, numeric_typmod};
use lagodb_core::expr::ExprType;
use pgrx::pg_sys;

use super::Decimal128Semantics;

const _: () = assert!(NumericScaledDomain::MAX_PRECISION == DECIMAL256_MAX_PRECISION);
const _: () = assert!(
    FixedDecimalType::SUM_COUNT_DIGITS >= FixedDecimalType::NATIVE_SUM_DIGITS
);

/// The static Arrow decimal type of one NUMERIC engine value. The Arrow width
/// is a function of the precision: `Decimal128` up to 38 digits, otherwise
/// `Decimal256`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedDecimalType {
    domain: NumericScaledDomain,
}

/// PostgreSQL NUMERIC operators with a native fixed-point implementation.
/// Division is absent: Arrow truncates to `s1 + 4` fractional digits, while
/// PostgreSQL rounds to a value-dependent scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecimalOperator {
    Add,
    Subtract,
    Multiply,
}

impl DecimalOperator {
    /// The operator implemented by a built-in NUMERIC function.
    pub const fn for_function(function_oid: u32) -> Option<Self> {
        match function_oid {
            pg_sys::F_NUMERIC_ADD => Some(Self::Add),
            pg_sys::F_NUMERIC_SUB => Some(Self::Subtract),
            pg_sys::F_NUMERIC_MUL => Some(Self::Multiply),
            _ => None,
        }
    }

    pub(crate) const fn wire_id(self) -> i32 {
        match self {
            Self::Add => 1,
            Self::Subtract => 2,
            Self::Multiply => 3,
        }
    }

    pub(crate) const fn from_wire_id(id: i32) -> Option<Self> {
        match id {
            1 => Some(Self::Add),
            2 => Some(Self::Subtract),
            3 => Some(Self::Multiply),
            _ => None,
        }
    }

    pub const fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
        }
    }
}

impl FixedDecimalType {
    /// Digits a SUM can add to its input precision: a u64 row count is below
    /// 10^20.
    const SUM_COUNT_DIGITS: u8 = 20;
    /// Digits DataFusion's native SUM adds to its decimal input precision
    /// (`datafusion-functions-aggregate` `sum.rs`, Spark's rule).
    const NATIVE_SUM_DIGITS: u8 = 10;
    /// Digits DataFusion's native AVG adds to both the precision and the scale
    /// of its decimal input (`average.rs` `return_type`, Spark's rule).
    const NATIVE_AVERAGE_DIGITS: u8 = 4;
    /// The smallest Decimal128 input precision for which native AVG keeps its
    /// running sum in `Decimal256(76, s)`: `avg_sum_data_type` reserves 13
    /// digits above the input and leaves Decimal128 beyond 38.
    const NATIVE_AVERAGE_WIDE_SUM_PRECISION: u8 = 26;

    #[must_use]
    pub const fn new(precision: u8, scale: u8) -> Option<Self> {
        match NumericScaledDomain::new(precision, scale) {
            Some(domain) => Some(Self { domain }),
            None => None,
        }
    }

    /// The fixed type holding every value of a `numeric(p, s)` type
    /// ([`NumericScaledDomain::for_typmod`]). Execution types beyond
    /// Decimal128 travel in the same typmod form.
    #[must_use]
    pub fn for_type(value_type: ExprType) -> Option<Self> {
        if value_type.type_oid != pg_sys::NUMERICOID
            || value_type.collation != pg_sys::InvalidOid
        {
            return None;
        }
        NumericScaledDomain::for_typmod(value_type.typmod)
            .map(|domain| Self { domain })
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

    /// The fixed type of an Arrow decimal produced under this width rule.
    #[must_use]
    pub fn from_data_type(data_type: &DataType) -> Option<Self> {
        let (precision, scale) = match data_type {
            DataType::Decimal128(precision, scale)
            | DataType::Decimal256(precision, scale) => (*precision, *scale),
            _ => return None,
        };
        Self::new(precision, u8::try_from(scale).ok()?)
            .filter(|decimal| decimal.data_type() == *data_type)
    }

    #[inline]
    pub const fn precision(self) -> u8 {
        self.domain.precision()
    }

    #[inline]
    pub const fn scale(self) -> u8 {
        self.domain.scale()
    }

    #[inline]
    pub const fn domain(self) -> NumericScaledDomain {
        self.domain
    }

    #[inline]
    pub const fn is_decimal128(self) -> bool {
        self.precision() <= DECIMAL128_MAX_PRECISION
    }

    pub fn decimal128(self) -> Option<Decimal128Semantics> {
        Decimal128Semantics::new(self.precision(), self.arrow_scale())
    }

    pub fn data_type(self) -> DataType {
        self.data_type_in(self)
    }

    /// This value's precision and scale in the Arrow width of `result`, the
    /// physical operand type of an operation producing `result`.
    pub fn data_type_in(self, result: Self) -> DataType {
        if result.is_decimal128() {
            DataType::Decimal128(self.precision(), self.arrow_scale())
        } else {
            DataType::Decimal256(self.precision(), self.arrow_scale())
        }
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

    /// Arrow's result type for `self <operator> right`, or `None` beyond
    /// Decimal256.
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
        };
        Self::new(precision, scale)
    }

    /// DataFusion's common type for values of both types, used by CASE and
    /// COALESCE (`type_union_coercion`).
    #[must_use]
    pub fn common(self, other: Self) -> Option<Self> {
        let scale = self.scale().max(other.scale());
        Self::new(
            self.integer_digits().max(other.integer_digits()) + scale,
            scale,
        )
    }

    /// The type holding a SUM of at most `u64::MAX` values of this type.
    #[must_use]
    pub const fn sum(self) -> Option<Self> {
        Self::new(self.precision() + Self::SUM_COUNT_DIGITS, self.scale())
    }

    /// The physical operand type under which DataFusion's native SUM computes
    /// [`Self::sum`]: the precision raised so native SUM returns exactly the
    /// sum type, in the sum type's Arrow width so its wrapping accumulator
    /// holds every sum. A compiler-only type for the SUM call, never an
    /// execution type; the widening cast keeps every value.
    #[must_use]
    pub fn native_sum_input(self) -> Option<DataType> {
        let sum = self.sum()?;
        let input =
            Self::new(sum.precision() - Self::NATIVE_SUM_DIGITS, self.scale())?;
        Some(input.data_type_in(sum))
    }

    /// The type native AVG returns for [`Self::native_average_input`]:
    /// `(max(p, 26) + 4, s + 4)`, in the Arrow width of that precision.
    ///
    /// Only Decimal128 inputs are admitted. Native AVG sums them in
    /// `Decimal256(76, s)` with a wrapping add, and scales the sum up by
    /// `10^4` before a truncating division. Fewer than 2^64 values of at most
    /// 38 digits keep that sum below `10^58` and the scaled sum below
    /// `10^62`, so no step can wrap.
    #[must_use]
    pub fn average(self) -> Option<Self> {
        if !self.is_decimal128() {
            return None;
        }
        Self::new(
            self.precision()
                .max(Self::NATIVE_AVERAGE_WIDE_SUM_PRECISION)
                + Self::NATIVE_AVERAGE_DIGITS,
            self.scale() + Self::NATIVE_AVERAGE_DIGITS,
        )
    }

    /// The physical operand type for DataFusion's native AVG. The precision is
    /// raised to at least 26, a same-width metadata cast, so native AVG keeps
    /// a Decimal256 sum; the width is that of [`Self::average`], so an input
    /// above 34 digits becomes Decimal256 and native AVG returns exactly that
    /// type instead of capping it at 38 digits. A compiler-only type for the
    /// AVG call, never an execution type; the cast keeps every value.
    #[must_use]
    pub fn native_average_input(self) -> Option<DataType> {
        let average = self.average()?;
        let input = Self::new(
            self.precision()
                .max(Self::NATIVE_AVERAGE_WIDE_SUM_PRECISION),
            self.scale(),
        )?;
        Some(input.data_type_in(average))
    }

    const fn integer_digits(self) -> u8 {
        self.precision() - self.scale()
    }

    pub fn arrow_scale(self) -> i8 {
        i8::try_from(self.scale()).expect("a decimal scale of at most 76 fits i8")
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
