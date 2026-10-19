//! Constants shared by PostgreSQL and Unix temporal representations.

use pgrx::pg_sys::{POSTGRES_EPOCH_JDATE, UNIX_EPOCH_JDATE};

/// PostgreSQL epoch (2000-01-01) minus Unix epoch (1970-01-01) in days.
pub const PG_EPOCH_DAYS_DIFF: i32 = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i32;

/// PostgreSQL epoch (2000-01-01) minus Unix epoch (1970-01-01) in microseconds.
pub const PG_EPOCH_USECS_DIFF: i64 =
    (PG_EPOCH_DAYS_DIFF as i64) * pgrx::datum::USECS_PER_DAY;
