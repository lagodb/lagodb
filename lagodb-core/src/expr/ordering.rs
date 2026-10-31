//! PostgreSQL ordering direction across major-version planner APIs.

use pgrx::pg_sys;

/// Direction represented by a PostgreSQL pathkey or ordering operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgOrderingDirection {
    Ascending,
    Descending,
}

impl PgOrderingDirection {
    /// Decode the direction carried by a live PostgreSQL planner pathkey.
    #[inline]
    pub fn from_path_key(pathkey: &pg_sys::PathKey) -> Option<Self> {
        #[cfg(any(feature = "pg16", feature = "pg17"))]
        let direction = pathkey.pk_strategy;
        #[cfg(feature = "pg18")]
        let direction = pathkey.pk_cmptype;

        Self::from_pg_direction(direction)
    }

    /// Resolve the direction of a recognized PostgreSQL built-in sort operator.
    pub fn from_builtin_sort_operator(operator: pg_sys::Oid) -> Option<Self> {
        if u32::from(operator) >= pg_sys::FirstNormalObjectId {
            return None;
        }

        let mut opfamily = pg_sys::InvalidOid;
        let mut input_type = pg_sys::InvalidOid;
        #[cfg(any(feature = "pg16", feature = "pg17"))]
        let mut direction = 0_i16;
        #[cfg(feature = "pg18")]
        let mut direction = pg_sys::CompareType::COMPARE_INVALID;

        let found = unsafe {
            pg_sys::get_ordering_op_properties(
                operator,
                &mut opfamily,
                &mut input_type,
                &mut direction,
            )
        };
        #[cfg(any(feature = "pg16", feature = "pg17"))]
        let direction = i32::from(direction);
        found.then(|| Self::from_pg_direction(direction)).flatten()
    }

    #[cfg(any(feature = "pg16", feature = "pg17"))]
    #[inline]
    fn from_pg_direction(direction: i32) -> Option<Self> {
        match u32::try_from(direction).ok()? {
            pg_sys::BTLessStrategyNumber => Some(Self::Ascending),
            pg_sys::BTGreaterStrategyNumber => Some(Self::Descending),
            _ => None,
        }
    }

    #[cfg(feature = "pg18")]
    #[inline]
    fn from_pg_direction(direction: pg_sys::CompareType::Type) -> Option<Self> {
        match direction {
            pg_sys::CompareType::COMPARE_LT => Some(Self::Ascending),
            pg_sys::CompareType::COMPARE_GT => Some(Self::Descending),
            _ => None,
        }
    }
}
