//! Validated costing facts for one provider-owned table scan.

/// Physical work estimates supplied by the provider that owns a table scan.
///
/// `rows_read` is the number of source rows examined after provider pruning;
/// it is deliberately not the scan node's output cardinality. `bytes_read` is
/// the physical table data expected to be read. It is
/// not the size of the Arrow batches emitted after projection and must not be
/// used as DataFusion `Statistics::total_byte_size`. `startup_cost` is the
/// provider-specific cost paid once before the first row can be produced.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScanCost {
    rows_read: f64,
    bytes_read: f64,
    startup_cost: f64,
}

impl ScanCost {
    pub fn try_new(
        rows_read: f64,
        bytes_read: f64,
        startup_cost: f64,
    ) -> Result<Self, ScanCostError> {
        if !rows_read.is_finite() || rows_read < 0.0 {
            return Err(ScanCostError::InvalidRowsRead { value: rows_read });
        }
        if !bytes_read.is_finite() || bytes_read < 0.0 {
            return Err(ScanCostError::InvalidBytesRead { value: bytes_read });
        }
        if !startup_cost.is_finite() || startup_cost < 0.0 {
            return Err(ScanCostError::InvalidStartupCost {
                value: startup_cost,
            });
        }
        Ok(Self {
            rows_read,
            bytes_read,
            startup_cost,
        })
    }

    #[inline]
    pub const fn rows_read(self) -> f64 {
        self.rows_read
    }

    #[inline]
    pub const fn bytes_read(self) -> f64 {
        self.bytes_read
    }

    #[inline]
    pub const fn startup_cost(self) -> f64 {
        self.startup_cost
    }
}

/// Invalid provider cost facts at a typed or serialized boundary.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ScanCostError {
    #[error("table scan rows_read is invalid: {value}")]
    InvalidRowsRead { value: f64 },
    #[error("table scan bytes_read is invalid: {value}")]
    InvalidBytesRead { value: f64 },
    #[error("table scan startup_cost is invalid: {value}")]
    InvalidStartupCost { value: f64 },
}
