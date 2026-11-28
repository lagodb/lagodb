//! Native PostgreSQL-to-DataFusion scalar function identities.

use std::fmt;

use lagodb_core::expr::ExprType;
use pgrx::pg_sys;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarFunctionKind {
    Ascii,
    Repeat,
    StartsWith,
    Replace,
    CharacterLength,
    Substring,
    Reverse,
    Abs,
    Ceil,
    Floor,
    Greatest,
    Least,
    Coalesce,
    NullIf,
    Rtrim,
    /// PostgreSQL's built-in integer and floating-point `+`.
    Add,
    Subtract,
    Multiply,
    Divide,
    /// Integer `%`; PostgreSQL has no floating-point modulo operator.
    Modulo,
    /// PostgreSQL's `int2/int4/int8 -> float4/float8` and `float4 -> float8`
    /// cast functions, whether inserted implicitly for mixed integer and float
    /// operands or written explicitly. Some are rounding conversions
    /// (`int8 -> float8`), so this is not a widening.
    FloatCast,
    /// `extract(year from date)`. PostgreSQL returns NUMERIC; the engine
    /// computes the year as Int32 ([`Self::execution_result_type`]).
    ExtractYear,
    /// `text LIKE pattern` with a constant pattern PostgreSQL accepts.
    /// Arrow's LIKE kernel reads `%`, `_` and backslash escapes as PostgreSQL
    /// does under a deterministic collation.
    Like,
}

/// How EXPLAIN writes a call of a scalar function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionNotation {
    Call,
    Infix(&'static str),
    Cast,
}

impl ScalarFunctionKind {
    pub(crate) const fn wire_id(self) -> i32 {
        self as i32 + 1
    }

    pub(crate) const fn from_wire_id(id: i32) -> Option<Self> {
        Some(match id {
            1 => Self::Ascii,
            2 => Self::Repeat,
            3 => Self::StartsWith,
            4 => Self::Replace,
            5 => Self::CharacterLength,
            6 => Self::Substring,
            7 => Self::Reverse,
            8 => Self::Abs,
            9 => Self::Ceil,
            10 => Self::Floor,
            11 => Self::Greatest,
            12 => Self::Least,
            13 => Self::Coalesce,
            14 => Self::NullIf,
            15 => Self::Rtrim,
            16 => Self::Add,
            17 => Self::Subtract,
            18 => Self::Multiply,
            19 => Self::Divide,
            20 => Self::Modulo,
            21 => Self::FloatCast,
            22 => Self::ExtractYear,
            23 => Self::Like,
            _ => return None,
        })
    }

    pub fn supports_signature(
        self,
        arguments: &[ExprType],
        input_collation: pg_sys::Oid,
        result: ExprType,
    ) -> bool {
        let has_oids = |expected: &[pg_sys::Oid]| {
            arguments.len() == expected.len()
                && arguments
                    .iter()
                    .zip(expected)
                    .all(|(argument, expected)| argument.type_oid == *expected)
        };
        let scalar_result = |oid| {
            result.type_oid == oid
                && result.typmod == -1
                && result.collation == pg_sys::InvalidOid
        };
        let text_result = || {
            result.type_oid == pg_sys::TEXTOID
                && result.typmod == -1
                && result.collation == input_collation
        };
        match self {
            Self::Ascii => {
                has_oids(&[pg_sys::TEXTOID]) && scalar_result(pg_sys::INT4OID)
            }
            Self::Repeat => {
                has_oids(&[pg_sys::TEXTOID, pg_sys::INT4OID]) && text_result()
            }
            Self::StartsWith | Self::Like => {
                has_oids(&[pg_sys::TEXTOID, pg_sys::TEXTOID])
                    && scalar_result(pg_sys::BOOLOID)
                    && Self::is_deterministic_collation(input_collation)
            }
            Self::Replace => {
                has_oids(&[pg_sys::TEXTOID, pg_sys::TEXTOID, pg_sys::TEXTOID])
                    && text_result()
                    && Self::is_deterministic_collation(input_collation)
            }
            Self::CharacterLength => {
                has_oids(&[pg_sys::TEXTOID]) && scalar_result(pg_sys::INT4OID)
            }
            Self::Substring => {
                matches!(arguments.len(), 2 | 3)
                    && arguments[0].type_oid == pg_sys::TEXTOID
                    && arguments[1..]
                        .iter()
                        .all(|argument| argument.type_oid == pg_sys::INT4OID)
                    && text_result()
            }
            Self::Reverse => has_oids(&[pg_sys::TEXTOID]) && text_result(),
            // PostgreSQL's `text(bpchar)` cast strips trailing blanks from any
            // character(n) or unconstrained bpchar value.
            Self::Rtrim => has_oids(&[pg_sys::BPCHAROID]) && text_result(),
            Self::Abs => {
                arguments.len() == 1
                    && matches!(
                        arguments[0].type_oid,
                        pg_sys::INT2OID
                            | pg_sys::INT4OID
                            | pg_sys::INT8OID
                            | pg_sys::FLOAT4OID
                            | pg_sys::FLOAT8OID
                    )
                    && scalar_result(arguments[0].type_oid)
            }
            Self::Ceil | Self::Floor => {
                has_oids(&[pg_sys::FLOAT8OID]) && scalar_result(pg_sys::FLOAT8OID)
            }
            Self::Greatest | Self::Least => {
                !arguments.is_empty()
                    && arguments.iter().all(|argument| {
                        matches!(
                            argument.type_oid,
                            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                        ) && argument.type_oid == result.type_oid
                    })
                    && scalar_result(result.type_oid)
            }
            Self::Coalesce => {
                // Equal PG JSONB OIDs do not imply equal Arrow container
                // types. Keep branch combination in PG; direct nested column
                // output and null bitmap predicates remain independent.
                result.type_oid != pg_sys::JSONBOID
                    && !arguments.is_empty()
                    && arguments.iter().all(|argument| *argument == result)
                    && input_collation == result.collation
            }
            Self::NullIf => {
                arguments.len() == 2
                    && arguments[0] == result
                    && arguments[1] == result
                    && matches!(
                        result.type_oid,
                        pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                    )
                    && result.typmod == -1
                    && result.collation == pg_sys::InvalidOid
            }
            Self::Add | Self::Subtract | Self::Multiply | Self::Divide => {
                matches!(arguments, [left, right]
                    if Self::arithmetic_result(left.type_oid, right.type_oid)
                        .is_some_and(scalar_result))
            }
            Self::Modulo => {
                matches!(arguments, [left, right]
                    if matches!(
                        left.type_oid,
                        pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                    ) && right.type_oid == left.type_oid
                        && scalar_result(left.type_oid))
            }
            Self::FloatCast => {
                matches!(arguments, [value]
                    if matches!(
                        (value.type_oid, result.type_oid),
                        (
                            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID,
                            pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
                        ) | (pg_sys::FLOAT4OID, pg_sys::FLOAT8OID)
                    ) && scalar_result(result.type_oid))
            }
            Self::ExtractYear => {
                has_oids(&[pg_sys::DATEOID]) && scalar_result(pg_sys::NUMERICOID)
            }
        }
    }

    /// The engine type of a result PostgreSQL types differently; the output
    /// codec converts it to the PostgreSQL type. DataFusion's `date_part`
    /// returns the year as Int32, an exact image of PostgreSQL's integral
    /// NUMERIC result.
    pub const fn execution_result_type(self) -> Option<ExprType> {
        match self {
            Self::ExtractYear => Some(ExprType {
                type_oid: pg_sys::INT4OID,
                typmod: -1,
                collation: pg_sys::InvalidOid,
            }),
            _ => None,
        }
    }

    /// How EXPLAIN writes this function.
    pub const fn notation(self) -> ScalarFunctionNotation {
        match self {
            Self::Add => ScalarFunctionNotation::Infix("+"),
            Self::Subtract => ScalarFunctionNotation::Infix("-"),
            Self::Multiply => ScalarFunctionNotation::Infix("*"),
            Self::Divide => ScalarFunctionNotation::Infix("/"),
            Self::Modulo => ScalarFunctionNotation::Infix("%"),
            Self::FloatCast => ScalarFunctionNotation::Cast,
            Self::Like => ScalarFunctionNotation::Infix("~~"),
            _ => ScalarFunctionNotation::Call,
        }
    }

    /// The result type of PostgreSQL's built-in `+ - * /` over two operand
    /// types: integers compute in the wider integer, floats in the wider
    /// float. PostgreSQL has no mixed integer/float operator; it casts the
    /// integer operand to float first.
    const fn arithmetic_result(
        left: pg_sys::Oid,
        right: pg_sys::Oid,
    ) -> Option<pg_sys::Oid> {
        Some(match (left, right) {
            (pg_sys::INT2OID, pg_sys::INT2OID) => pg_sys::INT2OID,
            (
                pg_sys::INT2OID | pg_sys::INT4OID,
                pg_sys::INT2OID | pg_sys::INT4OID,
            ) => pg_sys::INT4OID,
            (
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID,
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID,
            ) => pg_sys::INT8OID,
            (pg_sys::FLOAT4OID, pg_sys::FLOAT4OID) => pg_sys::FLOAT4OID,
            (
                pg_sys::FLOAT4OID | pg_sys::FLOAT8OID,
                pg_sys::FLOAT4OID | pg_sys::FLOAT8OID,
            ) => pg_sys::FLOAT8OID,
            _ => return None,
        })
    }

    fn is_deterministic_collation(collation: pg_sys::Oid) -> bool {
        collation != pg_sys::InvalidOid
            && unsafe { pg_sys::get_collation_isdeterministic(collation) }
    }
}

impl fmt::Display for ScalarFunctionKind {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(match self {
            Self::Ascii => "ascii",
            Self::Repeat => "repeat",
            Self::StartsWith => "starts_with",
            Self::Replace => "replace",
            Self::CharacterLength => "character_length",
            Self::Substring => "substring",
            Self::Reverse => "reverse",
            Self::Abs => "abs",
            Self::Ceil => "ceil",
            Self::Floor => "floor",
            Self::Greatest => "greatest",
            Self::Least => "least",
            Self::Coalesce => "coalesce",
            Self::NullIf => "nullif",
            Self::Rtrim => "rtrim",
            Self::Add => "add",
            Self::Subtract => "subtract",
            Self::Multiply => "multiply",
            Self::Divide => "divide",
            Self::Modulo => "modulo",
            Self::FloatCast => "float_cast",
            Self::ExtractYear => "extract_year",
            Self::Like => "like",
        })
    }
}
