//! Built-in integer and floating-point arithmetic operators, the integer
//! `mod()` functions, and the casts PostgreSQL inserts between integer and
//! float operands (`pg_operator.dat`, `pg_proc.dat`, `pg_cast.dat`).
//! PostgreSQL has no mixed integer/float operator: it casts the integer
//! operand to float first.

use lagodb_query::plan::ScalarFunctionKind::{
    self, Add, Divide, FloatCast, Modulo, Multiply, Subtract,
};
use pgrx::pg_sys::{self, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID};

use super::{CollationPolicy, FunctionSignature};

impl FunctionSignature {
    pub(super) fn for_arithmetic(function: u32) -> Option<Self> {
        let (kind, argument_types, result_type): (
            ScalarFunctionKind,
            &'static [pg_sys::Oid],
            pg_sys::Oid,
        ) = match function {
            pg_sys::F_INT2PL => (Add, &[INT2OID, INT2OID], INT2OID),
            pg_sys::F_INT24PL => (Add, &[INT2OID, INT4OID], INT4OID),
            pg_sys::F_INT28PL => (Add, &[INT2OID, INT8OID], INT8OID),
            pg_sys::F_INT42PL => (Add, &[INT4OID, INT2OID], INT4OID),
            pg_sys::F_INT4PL => (Add, &[INT4OID, INT4OID], INT4OID),
            pg_sys::F_INT48PL => (Add, &[INT4OID, INT8OID], INT8OID),
            pg_sys::F_INT82PL => (Add, &[INT8OID, INT2OID], INT8OID),
            pg_sys::F_INT84PL => (Add, &[INT8OID, INT4OID], INT8OID),
            pg_sys::F_INT8PL => (Add, &[INT8OID, INT8OID], INT8OID),
            pg_sys::F_INT2MI => (Subtract, &[INT2OID, INT2OID], INT2OID),
            pg_sys::F_INT24MI => (Subtract, &[INT2OID, INT4OID], INT4OID),
            pg_sys::F_INT28MI => (Subtract, &[INT2OID, INT8OID], INT8OID),
            pg_sys::F_INT42MI => (Subtract, &[INT4OID, INT2OID], INT4OID),
            pg_sys::F_INT4MI => (Subtract, &[INT4OID, INT4OID], INT4OID),
            pg_sys::F_INT48MI => (Subtract, &[INT4OID, INT8OID], INT8OID),
            pg_sys::F_INT82MI => (Subtract, &[INT8OID, INT2OID], INT8OID),
            pg_sys::F_INT84MI => (Subtract, &[INT8OID, INT4OID], INT8OID),
            pg_sys::F_INT8MI => (Subtract, &[INT8OID, INT8OID], INT8OID),
            pg_sys::F_INT2MUL => (Multiply, &[INT2OID, INT2OID], INT2OID),
            pg_sys::F_INT24MUL => (Multiply, &[INT2OID, INT4OID], INT4OID),
            pg_sys::F_INT28MUL => (Multiply, &[INT2OID, INT8OID], INT8OID),
            pg_sys::F_INT42MUL => (Multiply, &[INT4OID, INT2OID], INT4OID),
            pg_sys::F_INT4MUL => (Multiply, &[INT4OID, INT4OID], INT4OID),
            pg_sys::F_INT48MUL => (Multiply, &[INT4OID, INT8OID], INT8OID),
            pg_sys::F_INT82MUL => (Multiply, &[INT8OID, INT2OID], INT8OID),
            pg_sys::F_INT84MUL => (Multiply, &[INT8OID, INT4OID], INT8OID),
            pg_sys::F_INT8MUL => (Multiply, &[INT8OID, INT8OID], INT8OID),
            pg_sys::F_INT2DIV => (Divide, &[INT2OID, INT2OID], INT2OID),
            pg_sys::F_INT24DIV => (Divide, &[INT2OID, INT4OID], INT4OID),
            pg_sys::F_INT28DIV => (Divide, &[INT2OID, INT8OID], INT8OID),
            pg_sys::F_INT42DIV => (Divide, &[INT4OID, INT2OID], INT4OID),
            pg_sys::F_INT4DIV => (Divide, &[INT4OID, INT4OID], INT4OID),
            pg_sys::F_INT48DIV => (Divide, &[INT4OID, INT8OID], INT8OID),
            pg_sys::F_INT82DIV => (Divide, &[INT8OID, INT2OID], INT8OID),
            pg_sys::F_INT84DIV => (Divide, &[INT8OID, INT4OID], INT8OID),
            pg_sys::F_INT8DIV => (Divide, &[INT8OID, INT8OID], INT8OID),
            pg_sys::F_INT2MOD => (Modulo, &[INT2OID, INT2OID], INT2OID),
            pg_sys::F_INT4MOD => (Modulo, &[INT4OID, INT4OID], INT4OID),
            pg_sys::F_INT8MOD => (Modulo, &[INT8OID, INT8OID], INT8OID),
            // `mod()` shares its `prosrc` with the `%` operator function.
            pg_sys::F_MOD_INT2_INT2 => (Modulo, &[INT2OID, INT2OID], INT2OID),
            pg_sys::F_MOD_INT4_INT4 => (Modulo, &[INT4OID, INT4OID], INT4OID),
            pg_sys::F_MOD_INT8_INT8 => (Modulo, &[INT8OID, INT8OID], INT8OID),
            pg_sys::F_FLOAT4PL => (Add, &[FLOAT4OID, FLOAT4OID], FLOAT4OID),
            pg_sys::F_FLOAT48PL => (Add, &[FLOAT4OID, FLOAT8OID], FLOAT8OID),
            pg_sys::F_FLOAT84PL => (Add, &[FLOAT8OID, FLOAT4OID], FLOAT8OID),
            pg_sys::F_FLOAT8PL => (Add, &[FLOAT8OID, FLOAT8OID], FLOAT8OID),
            pg_sys::F_FLOAT4MI => (Subtract, &[FLOAT4OID, FLOAT4OID], FLOAT4OID),
            pg_sys::F_FLOAT48MI => (Subtract, &[FLOAT4OID, FLOAT8OID], FLOAT8OID),
            pg_sys::F_FLOAT84MI => (Subtract, &[FLOAT8OID, FLOAT4OID], FLOAT8OID),
            pg_sys::F_FLOAT8MI => (Subtract, &[FLOAT8OID, FLOAT8OID], FLOAT8OID),
            pg_sys::F_FLOAT4MUL => (Multiply, &[FLOAT4OID, FLOAT4OID], FLOAT4OID),
            pg_sys::F_FLOAT48MUL => (Multiply, &[FLOAT4OID, FLOAT8OID], FLOAT8OID),
            pg_sys::F_FLOAT84MUL => (Multiply, &[FLOAT8OID, FLOAT4OID], FLOAT8OID),
            pg_sys::F_FLOAT8MUL => (Multiply, &[FLOAT8OID, FLOAT8OID], FLOAT8OID),
            pg_sys::F_FLOAT4DIV => (Divide, &[FLOAT4OID, FLOAT4OID], FLOAT4OID),
            pg_sys::F_FLOAT48DIV => (Divide, &[FLOAT4OID, FLOAT8OID], FLOAT8OID),
            pg_sys::F_FLOAT84DIV => (Divide, &[FLOAT8OID, FLOAT4OID], FLOAT8OID),
            pg_sys::F_FLOAT8DIV => (Divide, &[FLOAT8OID, FLOAT8OID], FLOAT8OID),
            pg_sys::F_FLOAT4_INT2 => (FloatCast, &[INT2OID], FLOAT4OID),
            pg_sys::F_FLOAT4_INT4 => (FloatCast, &[INT4OID], FLOAT4OID),
            pg_sys::F_FLOAT4_INT8 => (FloatCast, &[INT8OID], FLOAT4OID),
            pg_sys::F_FLOAT8_INT2 => (FloatCast, &[INT2OID], FLOAT8OID),
            pg_sys::F_FLOAT8_INT4 => (FloatCast, &[INT4OID], FLOAT8OID),
            pg_sys::F_FLOAT8_INT8 => (FloatCast, &[INT8OID], FLOAT8OID),
            pg_sys::F_FLOAT8_FLOAT4 => (FloatCast, &[FLOAT4OID], FLOAT8OID),
            _ => return None,
        };
        Some(Self {
            kind,
            argument_types,
            result_type,
            collation_policy: CollationPolicy::Ignored,
        })
    }
}
