//! Execution shape shared by query planning and executor construction.

use std::num::NonZeroUsize;

/// Default row count for one query-engine batch.
pub const DEFAULT_MAXIMUM_BATCH_ROWS: i32 = 8_192;
/// Product safety limit for one query-engine batch.
pub const MAXIMUM_BATCH_ROWS_LIMIT: i32 = 128_000;

/// Stable engine settings that affect both estimated and actual execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionProfile {
    maximum_batch_rows: NonZeroUsize,
}

impl ExecutionProfile {
    /// Construct the batch-shape facts captured by one selected plan.
    ///
    /// # Errors
    ///
    /// Returns an error when the row limit is zero or exceeds the product
    /// safety limit.
    pub fn try_new(maximum_batch_rows: usize) -> Result<Self, ExecutionProfileError> {
        let maximum_batch_rows = NonZeroUsize::new(maximum_batch_rows).ok_or(
            ExecutionProfileError::InvalidMaximumBatchRows {
                value: maximum_batch_rows,
            },
        )?;
        let supported_limit = usize::try_from(MAXIMUM_BATCH_ROWS_LIMIT)
            .expect("positive i32 batch-row limit fits usize");
        if maximum_batch_rows.get() > supported_limit {
            return Err(ExecutionProfileError::InvalidMaximumBatchRows {
                value: maximum_batch_rows.get(),
            });
        }
        Ok(Self { maximum_batch_rows })
    }

    #[inline]
    pub const fn maximum_batch_rows(self) -> NonZeroUsize {
        self.maximum_batch_rows
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExecutionProfileError {
    #[error("query maximum batch rows {value} is outside the supported range")]
    InvalidMaximumBatchRows { value: usize },
}
