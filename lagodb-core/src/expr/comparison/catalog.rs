//! Declarative identities of the PostgreSQL built-in comparisons we recognize.

use pgrx::pg_sys;

use super::{PgComparisonKind, PgComparisonSignature};

// Names mirror PostgreSQL's pg_operator rows. pgrx exposes only a subset of
// operator OIDs from server headers, so the remaining stable catalog OIDs are
// named once here rather than repeated in provider policy code.
mod operator_oid {
    use pgrx::pg_sys;

    pub const BOOL_EQ: u32 = pg_sys::BooleanEqualOperator;
    pub const BOOL_NE: u32 = pg_sys::BooleanNotEqualOperator;

    pub const INT2_EQ: u32 = 94;
    pub const INT2_NE: u32 = 519;
    pub const INT2_LT: u32 = 95;
    pub const INT2_LE: u32 = 522;
    pub const INT2_GT: u32 = 520;
    pub const INT2_GE: u32 = 524;
    pub const INT4_EQ: u32 = pg_sys::Int4EqualOperator;
    pub const INT4_NE: u32 = 518;
    pub const INT4_LT: u32 = pg_sys::Int4LessOperator;
    pub const INT4_LE: u32 = 523;
    pub const INT4_GT: u32 = 521;
    pub const INT4_GE: u32 = 525;
    pub const INT8_EQ: u32 = 410;
    pub const INT8_NE: u32 = 411;
    pub const INT8_LT: u32 = pg_sys::Int8LessOperator;
    pub const INT8_LE: u32 = 414;
    pub const INT8_GT: u32 = 413;
    pub const INT8_GE: u32 = 415;

    pub const INT2_INT4_EQ: u32 = 532;
    pub const INT2_INT4_NE: u32 = 538;
    pub const INT2_INT4_LT: u32 = 534;
    pub const INT2_INT4_LE: u32 = 540;
    pub const INT2_INT4_GT: u32 = 536;
    pub const INT2_INT4_GE: u32 = 542;
    pub const INT4_INT2_EQ: u32 = 533;
    pub const INT4_INT2_NE: u32 = 539;
    pub const INT4_INT2_LT: u32 = 535;
    pub const INT4_INT2_LE: u32 = 541;
    pub const INT4_INT2_GT: u32 = 537;
    pub const INT4_INT2_GE: u32 = 543;
    pub const INT2_INT8_EQ: u32 = 1862;
    pub const INT2_INT8_NE: u32 = 1863;
    pub const INT2_INT8_LT: u32 = 1864;
    pub const INT2_INT8_LE: u32 = 1866;
    pub const INT2_INT8_GT: u32 = 1865;
    pub const INT2_INT8_GE: u32 = 1867;
    pub const INT8_INT2_EQ: u32 = 1868;
    pub const INT8_INT2_NE: u32 = 1869;
    pub const INT8_INT2_LT: u32 = 1870;
    pub const INT8_INT2_LE: u32 = 1872;
    pub const INT8_INT2_GT: u32 = 1871;
    pub const INT8_INT2_GE: u32 = 1873;
    pub const INT4_INT8_EQ: u32 = 15;
    pub const INT4_INT8_NE: u32 = 36;
    pub const INT4_INT8_LT: u32 = 37;
    pub const INT4_INT8_LE: u32 = 80;
    pub const INT4_INT8_GT: u32 = 76;
    pub const INT4_INT8_GE: u32 = 82;
    pub const INT8_INT4_EQ: u32 = 416;
    pub const INT8_INT4_NE: u32 = 417;
    pub const INT8_INT4_LT: u32 = 418;
    pub const INT8_INT4_LE: u32 = 420;
    pub const INT8_INT4_GT: u32 = 419;
    pub const INT8_INT4_GE: u32 = 430;

    pub const FLOAT4_EQ: u32 = 620;
    pub const FLOAT4_NE: u32 = 621;
    pub const FLOAT4_LT: u32 = 622;
    pub const FLOAT4_GT: u32 = 623;
    pub const FLOAT4_LE: u32 = 624;
    pub const FLOAT4_GE: u32 = 625;
    pub const FLOAT8_EQ: u32 = 670;
    pub const FLOAT8_NE: u32 = 671;
    pub const FLOAT8_LT: u32 = 672;
    pub const FLOAT8_LE: u32 = 673;
    pub const FLOAT8_GT: u32 = 674;
    pub const FLOAT8_GE: u32 = 675;
    pub const FLOAT4_FLOAT8_EQ: u32 = 1120;
    pub const FLOAT4_FLOAT8_NE: u32 = 1121;
    pub const FLOAT4_FLOAT8_LT: u32 = 1122;
    pub const FLOAT4_FLOAT8_GT: u32 = 1123;
    pub const FLOAT4_FLOAT8_LE: u32 = 1124;
    pub const FLOAT4_FLOAT8_GE: u32 = 1125;
    pub const FLOAT8_FLOAT4_EQ: u32 = 1130;
    pub const FLOAT8_FLOAT4_NE: u32 = 1131;
    pub const FLOAT8_FLOAT4_LT: u32 = 1132;
    pub const FLOAT8_FLOAT4_GT: u32 = 1133;
    pub const FLOAT8_FLOAT4_LE: u32 = 1134;
    pub const FLOAT8_FLOAT4_GE: u32 = 1135;

    pub const NUMERIC_EQ: u32 = 1752;
    pub const NUMERIC_NE: u32 = 1753;
    pub const NUMERIC_LT: u32 = 1754;
    pub const NUMERIC_LE: u32 = 1755;
    pub const NUMERIC_GT: u32 = 1756;
    pub const NUMERIC_GE: u32 = 1757;

    pub const DATE_EQ: u32 = 1093;
    pub const DATE_NE: u32 = 1094;
    pub const DATE_LT: u32 = 1095;
    pub const DATE_LE: u32 = 1096;
    pub const DATE_GT: u32 = 1097;
    pub const DATE_GE: u32 = 1098;
    pub const TIMESTAMP_EQ: u32 = 2060;
    pub const TIMESTAMP_NE: u32 = 2061;
    pub const TIMESTAMP_LT: u32 = 2062;
    pub const TIMESTAMP_LE: u32 = 2063;
    pub const TIMESTAMP_GT: u32 = 2064;
    pub const TIMESTAMP_GE: u32 = 2065;
    pub const TIMESTAMPTZ_EQ: u32 = 1320;
    pub const TIMESTAMPTZ_NE: u32 = 1321;
    pub const TIMESTAMPTZ_LT: u32 = 1322;
    pub const TIMESTAMPTZ_LE: u32 = 1323;
    pub const TIMESTAMPTZ_GT: u32 = 1324;
    pub const TIMESTAMPTZ_GE: u32 = 1325;

    pub const UUID_EQ: u32 = 2972;

    pub const NAME_EQ: u32 = 93;
    pub const BPCHAR_EQ: u32 = pg_sys::BpcharEqualOperator;
    pub const TEXT_EQ: u32 = pg_sys::TextEqualOperator;
    pub const TEXT_NE: u32 = 531;
    pub const TEXT_LT: u32 = pg_sys::TextLessOperator;
    pub const TEXT_LE: u32 = 665;
    pub const TEXT_GT: u32 = 666;
    pub const TEXT_GE: u32 = pg_sys::TextGreaterEqualOperator;
}

use operator_oid as op;

macro_rules! signatures {
    ($(
        ($left:expr, $right:expr) {
            $(($operator:expr, $function:expr, $kind:ident)),+ $(,)?
        }
    )+) => {
        pub(super) const SIGNATURES: &[PgComparisonSignature] = &[
            $(
                $(
                    PgComparisonSignature {
                        operator_oid: $operator,
                        left_type: $left,
                        right_type: $right,
                        function_oid: $function,
                        kind: PgComparisonKind::$kind,
                    },
                )+
            )+
        ];
    };
}

signatures! {
    (pg_sys::BOOLOID, pg_sys::BOOLOID) {
        (op::BOOL_EQ, pg_sys::F_BOOLEQ, Equal),
        (op::BOOL_NE, pg_sys::F_BOOLNE, NotEqual),
    }
    (pg_sys::INT2OID, pg_sys::INT2OID) {
        (op::INT2_EQ, pg_sys::F_INT2EQ, Equal),
        (op::INT2_NE, pg_sys::F_INT2NE, NotEqual),
        (op::INT2_LT, pg_sys::F_INT2LT, Less),
        (op::INT2_LE, pg_sys::F_INT2LE, LessEqual),
        (op::INT2_GT, pg_sys::F_INT2GT, Greater),
        (op::INT2_GE, pg_sys::F_INT2GE, GreaterEqual),
    }
    (pg_sys::INT4OID, pg_sys::INT4OID) {
        (op::INT4_EQ, pg_sys::F_INT4EQ, Equal),
        (op::INT4_NE, pg_sys::F_INT4NE, NotEqual),
        (op::INT4_LT, pg_sys::F_INT4LT, Less),
        (op::INT4_LE, pg_sys::F_INT4LE, LessEqual),
        (op::INT4_GT, pg_sys::F_INT4GT, Greater),
        (op::INT4_GE, pg_sys::F_INT4GE, GreaterEqual),
    }
    (pg_sys::INT8OID, pg_sys::INT8OID) {
        (op::INT8_EQ, pg_sys::F_INT8EQ, Equal),
        (op::INT8_NE, pg_sys::F_INT8NE, NotEqual),
        (op::INT8_LT, pg_sys::F_INT8LT, Less),
        (op::INT8_LE, pg_sys::F_INT8LE, LessEqual),
        (op::INT8_GT, pg_sys::F_INT8GT, Greater),
        (op::INT8_GE, pg_sys::F_INT8GE, GreaterEqual),
    }
    (pg_sys::INT2OID, pg_sys::INT4OID) {
        (op::INT2_INT4_EQ, pg_sys::F_INT24EQ, Equal),
        (op::INT2_INT4_NE, pg_sys::F_INT24NE, NotEqual),
        (op::INT2_INT4_LT, pg_sys::F_INT24LT, Less),
        (op::INT2_INT4_LE, pg_sys::F_INT24LE, LessEqual),
        (op::INT2_INT4_GT, pg_sys::F_INT24GT, Greater),
        (op::INT2_INT4_GE, pg_sys::F_INT24GE, GreaterEqual),
    }
    (pg_sys::INT4OID, pg_sys::INT2OID) {
        (op::INT4_INT2_EQ, pg_sys::F_INT42EQ, Equal),
        (op::INT4_INT2_NE, pg_sys::F_INT42NE, NotEqual),
        (op::INT4_INT2_LT, pg_sys::F_INT42LT, Less),
        (op::INT4_INT2_LE, pg_sys::F_INT42LE, LessEqual),
        (op::INT4_INT2_GT, pg_sys::F_INT42GT, Greater),
        (op::INT4_INT2_GE, pg_sys::F_INT42GE, GreaterEqual),
    }
    (pg_sys::INT2OID, pg_sys::INT8OID) {
        (op::INT2_INT8_EQ, pg_sys::F_INT28EQ, Equal),
        (op::INT2_INT8_NE, pg_sys::F_INT28NE, NotEqual),
        (op::INT2_INT8_LT, pg_sys::F_INT28LT, Less),
        (op::INT2_INT8_LE, pg_sys::F_INT28LE, LessEqual),
        (op::INT2_INT8_GT, pg_sys::F_INT28GT, Greater),
        (op::INT2_INT8_GE, pg_sys::F_INT28GE, GreaterEqual),
    }
    (pg_sys::INT8OID, pg_sys::INT2OID) {
        (op::INT8_INT2_EQ, pg_sys::F_INT82EQ, Equal),
        (op::INT8_INT2_NE, pg_sys::F_INT82NE, NotEqual),
        (op::INT8_INT2_LT, pg_sys::F_INT82LT, Less),
        (op::INT8_INT2_LE, pg_sys::F_INT82LE, LessEqual),
        (op::INT8_INT2_GT, pg_sys::F_INT82GT, Greater),
        (op::INT8_INT2_GE, pg_sys::F_INT82GE, GreaterEqual),
    }
    (pg_sys::INT4OID, pg_sys::INT8OID) {
        (op::INT4_INT8_EQ, pg_sys::F_INT48EQ, Equal),
        (op::INT4_INT8_NE, pg_sys::F_INT48NE, NotEqual),
        (op::INT4_INT8_LT, pg_sys::F_INT48LT, Less),
        (op::INT4_INT8_LE, pg_sys::F_INT48LE, LessEqual),
        (op::INT4_INT8_GT, pg_sys::F_INT48GT, Greater),
        (op::INT4_INT8_GE, pg_sys::F_INT48GE, GreaterEqual),
    }
    (pg_sys::INT8OID, pg_sys::INT4OID) {
        (op::INT8_INT4_EQ, pg_sys::F_INT84EQ, Equal),
        (op::INT8_INT4_NE, pg_sys::F_INT84NE, NotEqual),
        (op::INT8_INT4_LT, pg_sys::F_INT84LT, Less),
        (op::INT8_INT4_LE, pg_sys::F_INT84LE, LessEqual),
        (op::INT8_INT4_GT, pg_sys::F_INT84GT, Greater),
        (op::INT8_INT4_GE, pg_sys::F_INT84GE, GreaterEqual),
    }
    (pg_sys::FLOAT4OID, pg_sys::FLOAT4OID) {
        (op::FLOAT4_EQ, pg_sys::F_FLOAT4EQ, Equal),
        (op::FLOAT4_NE, pg_sys::F_FLOAT4NE, NotEqual),
        (op::FLOAT4_LT, pg_sys::F_FLOAT4LT, Less),
        (op::FLOAT4_LE, pg_sys::F_FLOAT4LE, LessEqual),
        (op::FLOAT4_GT, pg_sys::F_FLOAT4GT, Greater),
        (op::FLOAT4_GE, pg_sys::F_FLOAT4GE, GreaterEqual),
    }
    (pg_sys::FLOAT8OID, pg_sys::FLOAT8OID) {
        (op::FLOAT8_EQ, pg_sys::F_FLOAT8EQ, Equal),
        (op::FLOAT8_NE, pg_sys::F_FLOAT8NE, NotEqual),
        (op::FLOAT8_LT, pg_sys::F_FLOAT8LT, Less),
        (op::FLOAT8_LE, pg_sys::F_FLOAT8LE, LessEqual),
        (op::FLOAT8_GT, pg_sys::F_FLOAT8GT, Greater),
        (op::FLOAT8_GE, pg_sys::F_FLOAT8GE, GreaterEqual),
    }
    (pg_sys::FLOAT4OID, pg_sys::FLOAT8OID) {
        (op::FLOAT4_FLOAT8_EQ, pg_sys::F_FLOAT48EQ, Equal),
        (op::FLOAT4_FLOAT8_NE, pg_sys::F_FLOAT48NE, NotEqual),
        (op::FLOAT4_FLOAT8_LT, pg_sys::F_FLOAT48LT, Less),
        (op::FLOAT4_FLOAT8_LE, pg_sys::F_FLOAT48LE, LessEqual),
        (op::FLOAT4_FLOAT8_GT, pg_sys::F_FLOAT48GT, Greater),
        (op::FLOAT4_FLOAT8_GE, pg_sys::F_FLOAT48GE, GreaterEqual),
    }
    (pg_sys::FLOAT8OID, pg_sys::FLOAT4OID) {
        (op::FLOAT8_FLOAT4_EQ, pg_sys::F_FLOAT84EQ, Equal),
        (op::FLOAT8_FLOAT4_NE, pg_sys::F_FLOAT84NE, NotEqual),
        (op::FLOAT8_FLOAT4_LT, pg_sys::F_FLOAT84LT, Less),
        (op::FLOAT8_FLOAT4_LE, pg_sys::F_FLOAT84LE, LessEqual),
        (op::FLOAT8_FLOAT4_GT, pg_sys::F_FLOAT84GT, Greater),
        (op::FLOAT8_FLOAT4_GE, pg_sys::F_FLOAT84GE, GreaterEqual),
    }
    (pg_sys::NUMERICOID, pg_sys::NUMERICOID) {
        (op::NUMERIC_EQ, pg_sys::F_NUMERIC_EQ, Equal),
        (op::NUMERIC_NE, pg_sys::F_NUMERIC_NE, NotEqual),
        (op::NUMERIC_LT, pg_sys::F_NUMERIC_LT, Less),
        (op::NUMERIC_LE, pg_sys::F_NUMERIC_LE, LessEqual),
        (op::NUMERIC_GT, pg_sys::F_NUMERIC_GT, Greater),
        (op::NUMERIC_GE, pg_sys::F_NUMERIC_GE, GreaterEqual),
    }
    (pg_sys::DATEOID, pg_sys::DATEOID) {
        (op::DATE_EQ, pg_sys::F_DATE_EQ, Equal),
        (op::DATE_NE, pg_sys::F_DATE_NE, NotEqual),
        (op::DATE_LT, pg_sys::F_DATE_LT, Less),
        (op::DATE_LE, pg_sys::F_DATE_LE, LessEqual),
        (op::DATE_GT, pg_sys::F_DATE_GT, Greater),
        (op::DATE_GE, pg_sys::F_DATE_GE, GreaterEqual),
    }
    (pg_sys::TIMESTAMPOID, pg_sys::TIMESTAMPOID) {
        (op::TIMESTAMP_EQ, pg_sys::F_TIMESTAMP_EQ, Equal),
        (op::TIMESTAMP_NE, pg_sys::F_TIMESTAMP_NE, NotEqual),
        (op::TIMESTAMP_LT, pg_sys::F_TIMESTAMP_LT, Less),
        (op::TIMESTAMP_LE, pg_sys::F_TIMESTAMP_LE, LessEqual),
        (op::TIMESTAMP_GT, pg_sys::F_TIMESTAMP_GT, Greater),
        (op::TIMESTAMP_GE, pg_sys::F_TIMESTAMP_GE, GreaterEqual),
    }
    (pg_sys::TIMESTAMPTZOID, pg_sys::TIMESTAMPTZOID) {
        (op::TIMESTAMPTZ_EQ, pg_sys::F_TIMESTAMPTZ_EQ, Equal),
        (op::TIMESTAMPTZ_NE, pg_sys::F_TIMESTAMPTZ_NE, NotEqual),
        (op::TIMESTAMPTZ_LT, pg_sys::F_TIMESTAMPTZ_LT, Less),
        (op::TIMESTAMPTZ_LE, pg_sys::F_TIMESTAMPTZ_LE, LessEqual),
        (op::TIMESTAMPTZ_GT, pg_sys::F_TIMESTAMPTZ_GT, Greater),
        (op::TIMESTAMPTZ_GE, pg_sys::F_TIMESTAMPTZ_GE, GreaterEqual),
    }
    (pg_sys::UUIDOID, pg_sys::UUIDOID) {
        (op::UUID_EQ, pg_sys::F_UUID_EQ, Equal),
    }
    (pg_sys::NAMEOID, pg_sys::NAMEOID) {
        (op::NAME_EQ, pg_sys::F_NAMEEQ, Equal),
    }
    (pg_sys::BPCHAROID, pg_sys::BPCHAROID) {
        (op::BPCHAR_EQ, pg_sys::F_BPCHAREQ, Equal),
    }
    (pg_sys::TEXTOID, pg_sys::TEXTOID) {
        (op::TEXT_EQ, pg_sys::F_TEXTEQ, Equal),
        (op::TEXT_NE, pg_sys::F_TEXTNE, NotEqual),
        (op::TEXT_LT, pg_sys::F_TEXT_LT, Less),
        (op::TEXT_LE, pg_sys::F_TEXT_LE, LessEqual),
        (op::TEXT_GT, pg_sys::F_TEXT_GT, Greater),
        (op::TEXT_GE, pg_sys::F_TEXT_GE, GreaterEqual),
    }
}
