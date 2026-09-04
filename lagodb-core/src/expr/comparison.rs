//! PostgreSQL built-in comparison signatures shared by query and providers.

use pgrx::pg_sys;

use super::contract::PgComparisonOp;

/// Semantic class of a PostgreSQL comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgComparisonKind {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

/// Catalog identity of a built-in comparison operator.
///
/// This describes PostgreSQL facts only. A query engine or storage provider
/// must still apply its own type, collation, and exactness policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgComparisonSignature {
    operator_oid: u32,
    left_type: pg_sys::Oid,
    right_type: pg_sys::Oid,
    function_oid: u32,
    kind: PgComparisonKind,
}

impl PgComparisonSignature {
    pub fn for_operator(operator: pg_sys::Oid) -> Option<Self> {
        let operator = u32::from(operator);
        SIGNATURES
            .iter()
            .copied()
            .find(|signature| signature.operator_oid == operator)
    }

    pub fn for_types(
        left_type: pg_sys::Oid,
        right_type: pg_sys::Oid,
        kind: PgComparisonKind,
    ) -> Option<Self> {
        SIGNATURES.iter().copied().find(|signature| {
            signature.left_type == left_type
                && signature.right_type == right_type
                && signature.kind == kind
        })
    }

    #[inline]
    pub fn operator_oid(self) -> pg_sys::Oid {
        pg_sys::Oid::from(self.operator_oid)
    }

    #[inline]
    pub const fn left_type(self) -> pg_sys::Oid {
        self.left_type
    }

    #[inline]
    pub const fn right_type(self) -> pg_sys::Oid {
        self.right_type
    }

    #[inline]
    pub fn function_oid(self) -> pg_sys::Oid {
        pg_sys::Oid::from(self.function_oid)
    }

    #[inline]
    pub const fn kind(self) -> PgComparisonKind {
        self.kind
    }

    #[inline]
    pub fn matches(self, operator: PgComparisonOp) -> bool {
        operator.opno == self.operator_oid()
            && operator.opfuncid == self.function_oid()
            && operator.opresulttype == pg_sys::BOOLOID
    }
}

impl PgComparisonOp {
    #[inline]
    pub fn builtin_signature(self) -> Option<PgComparisonSignature> {
        PgComparisonSignature::for_operator(self.opno)
            .filter(|signature| signature.matches(self))
    }
}

macro_rules! signature {
    ($operator:expr, $left:expr, $right:expr, $function:expr, $kind:ident) => {
        PgComparisonSignature {
            operator_oid: $operator,
            left_type: $left,
            right_type: $right,
            function_oid: $function,
            kind: PgComparisonKind::$kind,
        }
    };
}

// Names mirror PostgreSQL's pg_operator rows. pgrx exposes only a subset of
// operator OIDs from server headers, so the remaining stable catalog OIDs are
// named once at this boundary rather than repeated in provider policy code.
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

    pub const TEXT_EQ: u32 = pg_sys::TextEqualOperator;
    pub const TEXT_NE: u32 = 531;
    pub const TEXT_LT: u32 = pg_sys::TextLessOperator;
    pub const TEXT_LE: u32 = 665;
    pub const TEXT_GT: u32 = 666;
    pub const TEXT_GE: u32 = pg_sys::TextGreaterEqualOperator;
}

use operator_oid as op;

const SIGNATURES: &[PgComparisonSignature] = &[
    signature!(
        op::BOOL_EQ,
        pg_sys::BOOLOID,
        pg_sys::BOOLOID,
        pg_sys::F_BOOLEQ,
        Equal
    ),
    signature!(
        op::BOOL_NE,
        pg_sys::BOOLOID,
        pg_sys::BOOLOID,
        pg_sys::F_BOOLNE,
        NotEqual
    ),
    signature!(
        op::INT2_EQ,
        pg_sys::INT2OID,
        pg_sys::INT2OID,
        pg_sys::F_INT2EQ,
        Equal
    ),
    signature!(
        op::INT2_NE,
        pg_sys::INT2OID,
        pg_sys::INT2OID,
        pg_sys::F_INT2NE,
        NotEqual
    ),
    signature!(
        op::INT2_LT,
        pg_sys::INT2OID,
        pg_sys::INT2OID,
        pg_sys::F_INT2LT,
        Less
    ),
    signature!(
        op::INT2_LE,
        pg_sys::INT2OID,
        pg_sys::INT2OID,
        pg_sys::F_INT2LE,
        LessEqual
    ),
    signature!(
        op::INT2_GT,
        pg_sys::INT2OID,
        pg_sys::INT2OID,
        pg_sys::F_INT2GT,
        Greater
    ),
    signature!(
        op::INT2_GE,
        pg_sys::INT2OID,
        pg_sys::INT2OID,
        pg_sys::F_INT2GE,
        GreaterEqual
    ),
    signature!(
        op::INT4_EQ,
        pg_sys::INT4OID,
        pg_sys::INT4OID,
        pg_sys::F_INT4EQ,
        Equal
    ),
    signature!(
        op::INT4_NE,
        pg_sys::INT4OID,
        pg_sys::INT4OID,
        pg_sys::F_INT4NE,
        NotEqual
    ),
    signature!(
        op::INT4_LT,
        pg_sys::INT4OID,
        pg_sys::INT4OID,
        pg_sys::F_INT4LT,
        Less
    ),
    signature!(
        op::INT4_LE,
        pg_sys::INT4OID,
        pg_sys::INT4OID,
        pg_sys::F_INT4LE,
        LessEqual
    ),
    signature!(
        op::INT4_GT,
        pg_sys::INT4OID,
        pg_sys::INT4OID,
        pg_sys::F_INT4GT,
        Greater
    ),
    signature!(
        op::INT4_GE,
        pg_sys::INT4OID,
        pg_sys::INT4OID,
        pg_sys::F_INT4GE,
        GreaterEqual
    ),
    signature!(
        op::INT8_EQ,
        pg_sys::INT8OID,
        pg_sys::INT8OID,
        pg_sys::F_INT8EQ,
        Equal
    ),
    signature!(
        op::INT8_NE,
        pg_sys::INT8OID,
        pg_sys::INT8OID,
        pg_sys::F_INT8NE,
        NotEqual
    ),
    signature!(
        op::INT8_LT,
        pg_sys::INT8OID,
        pg_sys::INT8OID,
        pg_sys::F_INT8LT,
        Less
    ),
    signature!(
        op::INT8_LE,
        pg_sys::INT8OID,
        pg_sys::INT8OID,
        pg_sys::F_INT8LE,
        LessEqual
    ),
    signature!(
        op::INT8_GT,
        pg_sys::INT8OID,
        pg_sys::INT8OID,
        pg_sys::F_INT8GT,
        Greater
    ),
    signature!(
        op::INT8_GE,
        pg_sys::INT8OID,
        pg_sys::INT8OID,
        pg_sys::F_INT8GE,
        GreaterEqual
    ),
    signature!(
        op::INT2_INT4_EQ,
        pg_sys::INT2OID,
        pg_sys::INT4OID,
        pg_sys::F_INT24EQ,
        Equal
    ),
    signature!(
        op::INT2_INT4_NE,
        pg_sys::INT2OID,
        pg_sys::INT4OID,
        pg_sys::F_INT24NE,
        NotEqual
    ),
    signature!(
        op::INT2_INT4_LT,
        pg_sys::INT2OID,
        pg_sys::INT4OID,
        pg_sys::F_INT24LT,
        Less
    ),
    signature!(
        op::INT2_INT4_LE,
        pg_sys::INT2OID,
        pg_sys::INT4OID,
        pg_sys::F_INT24LE,
        LessEqual
    ),
    signature!(
        op::INT2_INT4_GT,
        pg_sys::INT2OID,
        pg_sys::INT4OID,
        pg_sys::F_INT24GT,
        Greater
    ),
    signature!(
        op::INT2_INT4_GE,
        pg_sys::INT2OID,
        pg_sys::INT4OID,
        pg_sys::F_INT24GE,
        GreaterEqual
    ),
    signature!(
        op::INT4_INT2_EQ,
        pg_sys::INT4OID,
        pg_sys::INT2OID,
        pg_sys::F_INT42EQ,
        Equal
    ),
    signature!(
        op::INT4_INT2_NE,
        pg_sys::INT4OID,
        pg_sys::INT2OID,
        pg_sys::F_INT42NE,
        NotEqual
    ),
    signature!(
        op::INT4_INT2_LT,
        pg_sys::INT4OID,
        pg_sys::INT2OID,
        pg_sys::F_INT42LT,
        Less
    ),
    signature!(
        op::INT4_INT2_LE,
        pg_sys::INT4OID,
        pg_sys::INT2OID,
        pg_sys::F_INT42LE,
        LessEqual
    ),
    signature!(
        op::INT4_INT2_GT,
        pg_sys::INT4OID,
        pg_sys::INT2OID,
        pg_sys::F_INT42GT,
        Greater
    ),
    signature!(
        op::INT4_INT2_GE,
        pg_sys::INT4OID,
        pg_sys::INT2OID,
        pg_sys::F_INT42GE,
        GreaterEqual
    ),
    signature!(
        op::INT2_INT8_EQ,
        pg_sys::INT2OID,
        pg_sys::INT8OID,
        pg_sys::F_INT28EQ,
        Equal
    ),
    signature!(
        op::INT2_INT8_NE,
        pg_sys::INT2OID,
        pg_sys::INT8OID,
        pg_sys::F_INT28NE,
        NotEqual
    ),
    signature!(
        op::INT2_INT8_LT,
        pg_sys::INT2OID,
        pg_sys::INT8OID,
        pg_sys::F_INT28LT,
        Less
    ),
    signature!(
        op::INT2_INT8_LE,
        pg_sys::INT2OID,
        pg_sys::INT8OID,
        pg_sys::F_INT28LE,
        LessEqual
    ),
    signature!(
        op::INT2_INT8_GT,
        pg_sys::INT2OID,
        pg_sys::INT8OID,
        pg_sys::F_INT28GT,
        Greater
    ),
    signature!(
        op::INT2_INT8_GE,
        pg_sys::INT2OID,
        pg_sys::INT8OID,
        pg_sys::F_INT28GE,
        GreaterEqual
    ),
    signature!(
        op::INT8_INT2_EQ,
        pg_sys::INT8OID,
        pg_sys::INT2OID,
        pg_sys::F_INT82EQ,
        Equal
    ),
    signature!(
        op::INT8_INT2_NE,
        pg_sys::INT8OID,
        pg_sys::INT2OID,
        pg_sys::F_INT82NE,
        NotEqual
    ),
    signature!(
        op::INT8_INT2_LT,
        pg_sys::INT8OID,
        pg_sys::INT2OID,
        pg_sys::F_INT82LT,
        Less
    ),
    signature!(
        op::INT8_INT2_LE,
        pg_sys::INT8OID,
        pg_sys::INT2OID,
        pg_sys::F_INT82LE,
        LessEqual
    ),
    signature!(
        op::INT8_INT2_GT,
        pg_sys::INT8OID,
        pg_sys::INT2OID,
        pg_sys::F_INT82GT,
        Greater
    ),
    signature!(
        op::INT8_INT2_GE,
        pg_sys::INT8OID,
        pg_sys::INT2OID,
        pg_sys::F_INT82GE,
        GreaterEqual
    ),
    signature!(
        op::INT4_INT8_EQ,
        pg_sys::INT4OID,
        pg_sys::INT8OID,
        pg_sys::F_INT48EQ,
        Equal
    ),
    signature!(
        op::INT4_INT8_NE,
        pg_sys::INT4OID,
        pg_sys::INT8OID,
        pg_sys::F_INT48NE,
        NotEqual
    ),
    signature!(
        op::INT4_INT8_LT,
        pg_sys::INT4OID,
        pg_sys::INT8OID,
        pg_sys::F_INT48LT,
        Less
    ),
    signature!(
        op::INT4_INT8_LE,
        pg_sys::INT4OID,
        pg_sys::INT8OID,
        pg_sys::F_INT48LE,
        LessEqual
    ),
    signature!(
        op::INT4_INT8_GT,
        pg_sys::INT4OID,
        pg_sys::INT8OID,
        pg_sys::F_INT48GT,
        Greater
    ),
    signature!(
        op::INT4_INT8_GE,
        pg_sys::INT4OID,
        pg_sys::INT8OID,
        pg_sys::F_INT48GE,
        GreaterEqual
    ),
    signature!(
        op::INT8_INT4_EQ,
        pg_sys::INT8OID,
        pg_sys::INT4OID,
        pg_sys::F_INT84EQ,
        Equal
    ),
    signature!(
        op::INT8_INT4_NE,
        pg_sys::INT8OID,
        pg_sys::INT4OID,
        pg_sys::F_INT84NE,
        NotEqual
    ),
    signature!(
        op::INT8_INT4_LT,
        pg_sys::INT8OID,
        pg_sys::INT4OID,
        pg_sys::F_INT84LT,
        Less
    ),
    signature!(
        op::INT8_INT4_LE,
        pg_sys::INT8OID,
        pg_sys::INT4OID,
        pg_sys::F_INT84LE,
        LessEqual
    ),
    signature!(
        op::INT8_INT4_GT,
        pg_sys::INT8OID,
        pg_sys::INT4OID,
        pg_sys::F_INT84GT,
        Greater
    ),
    signature!(
        op::INT8_INT4_GE,
        pg_sys::INT8OID,
        pg_sys::INT4OID,
        pg_sys::F_INT84GE,
        GreaterEqual
    ),
    signature!(
        op::NUMERIC_EQ,
        pg_sys::NUMERICOID,
        pg_sys::NUMERICOID,
        pg_sys::F_NUMERIC_EQ,
        Equal
    ),
    signature!(
        op::NUMERIC_NE,
        pg_sys::NUMERICOID,
        pg_sys::NUMERICOID,
        pg_sys::F_NUMERIC_NE,
        NotEqual
    ),
    signature!(
        op::NUMERIC_LT,
        pg_sys::NUMERICOID,
        pg_sys::NUMERICOID,
        pg_sys::F_NUMERIC_LT,
        Less
    ),
    signature!(
        op::NUMERIC_LE,
        pg_sys::NUMERICOID,
        pg_sys::NUMERICOID,
        pg_sys::F_NUMERIC_LE,
        LessEqual
    ),
    signature!(
        op::NUMERIC_GT,
        pg_sys::NUMERICOID,
        pg_sys::NUMERICOID,
        pg_sys::F_NUMERIC_GT,
        Greater
    ),
    signature!(
        op::NUMERIC_GE,
        pg_sys::NUMERICOID,
        pg_sys::NUMERICOID,
        pg_sys::F_NUMERIC_GE,
        GreaterEqual
    ),
    signature!(
        op::DATE_EQ,
        pg_sys::DATEOID,
        pg_sys::DATEOID,
        pg_sys::F_DATE_EQ,
        Equal
    ),
    signature!(
        op::DATE_NE,
        pg_sys::DATEOID,
        pg_sys::DATEOID,
        pg_sys::F_DATE_NE,
        NotEqual
    ),
    signature!(
        op::DATE_LT,
        pg_sys::DATEOID,
        pg_sys::DATEOID,
        pg_sys::F_DATE_LT,
        Less
    ),
    signature!(
        op::DATE_LE,
        pg_sys::DATEOID,
        pg_sys::DATEOID,
        pg_sys::F_DATE_LE,
        LessEqual
    ),
    signature!(
        op::DATE_GT,
        pg_sys::DATEOID,
        pg_sys::DATEOID,
        pg_sys::F_DATE_GT,
        Greater
    ),
    signature!(
        op::DATE_GE,
        pg_sys::DATEOID,
        pg_sys::DATEOID,
        pg_sys::F_DATE_GE,
        GreaterEqual
    ),
    signature!(
        op::TIMESTAMP_EQ,
        pg_sys::TIMESTAMPOID,
        pg_sys::TIMESTAMPOID,
        pg_sys::F_TIMESTAMP_EQ,
        Equal
    ),
    signature!(
        op::TIMESTAMP_NE,
        pg_sys::TIMESTAMPOID,
        pg_sys::TIMESTAMPOID,
        pg_sys::F_TIMESTAMP_NE,
        NotEqual
    ),
    signature!(
        op::TIMESTAMP_LT,
        pg_sys::TIMESTAMPOID,
        pg_sys::TIMESTAMPOID,
        pg_sys::F_TIMESTAMP_LT,
        Less
    ),
    signature!(
        op::TIMESTAMP_LE,
        pg_sys::TIMESTAMPOID,
        pg_sys::TIMESTAMPOID,
        pg_sys::F_TIMESTAMP_LE,
        LessEqual
    ),
    signature!(
        op::TIMESTAMP_GT,
        pg_sys::TIMESTAMPOID,
        pg_sys::TIMESTAMPOID,
        pg_sys::F_TIMESTAMP_GT,
        Greater
    ),
    signature!(
        op::TIMESTAMP_GE,
        pg_sys::TIMESTAMPOID,
        pg_sys::TIMESTAMPOID,
        pg_sys::F_TIMESTAMP_GE,
        GreaterEqual
    ),
    signature!(
        op::TIMESTAMPTZ_EQ,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::F_TIMESTAMPTZ_EQ,
        Equal
    ),
    signature!(
        op::TIMESTAMPTZ_NE,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::F_TIMESTAMPTZ_NE,
        NotEqual
    ),
    signature!(
        op::TIMESTAMPTZ_LT,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::F_TIMESTAMPTZ_LT,
        Less
    ),
    signature!(
        op::TIMESTAMPTZ_LE,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::F_TIMESTAMPTZ_LE,
        LessEqual
    ),
    signature!(
        op::TIMESTAMPTZ_GT,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::F_TIMESTAMPTZ_GT,
        Greater
    ),
    signature!(
        op::TIMESTAMPTZ_GE,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::TIMESTAMPTZOID,
        pg_sys::F_TIMESTAMPTZ_GE,
        GreaterEqual
    ),
    signature!(
        op::TEXT_EQ,
        pg_sys::TEXTOID,
        pg_sys::TEXTOID,
        pg_sys::F_TEXTEQ,
        Equal
    ),
    signature!(
        op::TEXT_NE,
        pg_sys::TEXTOID,
        pg_sys::TEXTOID,
        pg_sys::F_TEXTNE,
        NotEqual
    ),
    signature!(
        op::TEXT_LT,
        pg_sys::TEXTOID,
        pg_sys::TEXTOID,
        pg_sys::F_TEXT_LT,
        Less
    ),
    signature!(
        op::TEXT_LE,
        pg_sys::TEXTOID,
        pg_sys::TEXTOID,
        pg_sys::F_TEXT_LE,
        LessEqual
    ),
    signature!(
        op::TEXT_GT,
        pg_sys::TEXTOID,
        pg_sys::TEXTOID,
        pg_sys::F_TEXT_GT,
        Greater
    ),
    signature!(
        op::TEXT_GE,
        pg_sys::TEXTOID,
        pg_sys::TEXTOID,
        pg_sys::F_TEXT_GE,
        GreaterEqual
    ),
];
