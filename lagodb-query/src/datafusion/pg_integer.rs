//! PostgreSQL integer types as executed by DataFusion.

use arrow_schema::DataType;
use lagodb_core::diag::PgReportError;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum PgIntegerKind {
    SmallInt,
    Integer,
    BigInt,
}

impl PgIntegerKind {
    pub(super) const fn from_oid(type_oid: pg_sys::Oid) -> Option<Self> {
        match type_oid {
            pg_sys::INT2OID => Some(Self::SmallInt),
            pg_sys::INT4OID => Some(Self::Integer),
            pg_sys::INT8OID => Some(Self::BigInt),
            _ => None,
        }
    }

    pub(super) const fn data_type(self) -> DataType {
        match self {
            // Iceberg has no distinct smallint physical type. LagoDB widens
            // PostgreSQL int2 columns to Arrow Int32 at the scan boundary.
            Self::SmallInt | Self::Integer => DataType::Int32,
            Self::BigInt => DataType::Int64,
        }
    }

    /// PostgreSQL's error for a result outside this type.
    #[cold]
    #[inline(never)]
    pub(super) fn out_of_range(self) -> PgReportError {
        PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            match self {
                Self::SmallInt => "smallint out of range",
                Self::Integer => "integer out of range",
                Self::BigInt => "bigint out of range",
            },
        )
    }

    pub(super) const fn codec_tag(self) -> u8 {
        match self {
            Self::SmallInt => 1,
            Self::Integer => 2,
            Self::BigInt => 3,
        }
    }

    pub(super) const fn from_codec_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::SmallInt),
            2 => Some(Self::Integer),
            3 => Some(Self::BigInt),
            _ => None,
        }
    }
}
