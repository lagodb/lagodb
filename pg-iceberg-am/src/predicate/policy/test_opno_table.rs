//! Shared comparison-operator fixture data for host and backend tests.

use pgrx::pg_sys;

use super::ComparisonOpClass;
use super::pg_operator_oid as op;

// Per-type rows use `[Eq, NotEq, Lt, Le, Gt, Ge]` column order.
pub(crate) const INT2: [u32; 6] = [
    op::INT2_EQ,
    op::INT2_NE,
    op::INT2_LT,
    op::INT2_LE,
    op::INT2_GT,
    op::INT2_GE,
];
pub(crate) const INT4: [u32; 6] = [
    op::INT4_EQ,
    op::INT4_NE,
    op::INT4_LT,
    op::INT4_LE,
    op::INT4_GT,
    op::INT4_GE,
];
pub(crate) const INT8: [u32; 6] = [
    op::INT8_EQ,
    op::INT8_NE,
    op::INT8_LT,
    op::INT8_LE,
    op::INT8_GT,
    op::INT8_GE,
];
pub(crate) const DATE: [u32; 6] = [
    op::DATE_EQ,
    op::DATE_NE,
    op::DATE_LT,
    op::DATE_LE,
    op::DATE_GT,
    op::DATE_GE,
];
pub(crate) const TIMESTAMP: [u32; 6] = [
    op::TIMESTAMP_EQ,
    op::TIMESTAMP_NE,
    op::TIMESTAMP_LT,
    op::TIMESTAMP_LE,
    op::TIMESTAMP_GT,
    op::TIMESTAMP_GE,
];
pub(crate) const TIMESTAMPTZ: [u32; 6] = [
    op::TIMESTAMPTZ_EQ,
    op::TIMESTAMPTZ_NE,
    op::TIMESTAMPTZ_LT,
    op::TIMESTAMPTZ_LE,
    op::TIMESTAMPTZ_GT,
    op::TIMESTAMPTZ_GE,
];
pub(crate) const TEXT: [u32; 6] = [
    op::TEXT_EQ,
    op::TEXT_NE,
    op::TEXT_LT,
    op::TEXT_LE,
    op::TEXT_GT,
    op::TEXT_GE,
];

pub(crate) const CLASS_BY_COLUMN: [ComparisonOpClass; 6] = [
    ComparisonOpClass::Eq,
    ComparisonOpClass::NotEq,
    ComparisonOpClass::Lt,
    ComparisonOpClass::Le,
    ComparisonOpClass::Gt,
    ComparisonOpClass::Ge,
];

/// Built-in comparison rows mirrored from `pg_operator.dat`.
pub(crate) fn opno_table() -> [(pg_sys::Oid, [u32; 6]); 7] {
    [
        (pg_sys::INT2OID, INT2),
        (pg_sys::INT4OID, INT4),
        (pg_sys::INT8OID, INT8),
        (pg_sys::DATEOID, DATE),
        (pg_sys::TIMESTAMPOID, TIMESTAMP),
        (pg_sys::TIMESTAMPTZOID, TIMESTAMPTZ),
        (pg_sys::TEXTOID, TEXT),
    ]
}
