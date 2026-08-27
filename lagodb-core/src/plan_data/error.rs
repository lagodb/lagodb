//! Errors produced while encoding or decoding plan-owned PostgreSQL nodes.

use pgrx::pg_sys;

#[derive(Debug, thiserror::Error)]
pub enum PlanDataError {
    #[error("plan-data list is NULL")]
    NullList,
    #[error("plan-data list at cell {field} has a negative length: {length}")]
    NegativeListLength { field: usize, length: i32 },
    #[error("plan-data cell {field} is NULL")]
    NullCell { field: usize },
    #[error("plan-data read past end: position {position}, length {len}")]
    ReadPastEnd { position: usize, len: usize },
    #[error("plan-data has trailing cells: read {read}, length {len}")]
    UnexpectedTrailingCells { read: usize, len: usize },
    #[error("plan-data cell {field} has node tag {found:?}, expected {expected:?}")]
    WrongNodeTag {
        field: usize,
        expected: pg_sys::NodeTag,
        found: pg_sys::NodeTag,
    },
    #[error("plan-data string cell {field} has a NULL value")]
    NullString { field: usize },
    #[error("plan-data string cell {field} is not valid UTF-8")]
    InvalidUtf8 { field: usize },
    #[error("plan-data count {value} exceeds PostgreSQL Integer range")]
    CountTooLarge { value: usize },
    #[error("plan-data count at cell {field} is negative: {value}")]
    NegativeCount { field: usize, value: i32 },
    #[error("plan-data string at position {position} contains an interior NUL")]
    InteriorNul { position: usize },
    #[error("plan-data i64 cell {field} is malformed")]
    MalformedI64 { field: usize },
}
