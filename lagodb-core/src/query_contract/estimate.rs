//! Validated physical estimates for one provider-owned table scan.

/// Planner estimates supplied by the provider that owns a table scan.
///
/// `estimated_rows` is the number of visible rows emitted to the query engine.
/// `estimated_scan_bytes` is a provider estimate of physical table data. It is
/// not the size of the Arrow batches emitted after projection and must not be
/// used as DataFusion `Statistics::total_byte_size`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScanEstimate {
    estimated_rows: f64,
    estimated_scan_bytes: f64,
}

impl ScanEstimate {
    pub fn try_new(
        estimated_rows: f64,
        estimated_scan_bytes: f64,
    ) -> Result<Self, ScanEstimateError> {
        if !estimated_rows.is_finite() || estimated_rows < 0.0 {
            return Err(ScanEstimateError::InvalidRows {
                value: estimated_rows,
            });
        }
        if !estimated_scan_bytes.is_finite() || estimated_scan_bytes < 0.0 {
            return Err(ScanEstimateError::InvalidScanBytes {
                value: estimated_scan_bytes,
            });
        }
        Ok(Self {
            estimated_rows,
            estimated_scan_bytes,
        })
    }

    #[inline]
    pub const fn estimated_rows(self) -> f64 {
        self.estimated_rows
    }

    #[inline]
    pub const fn estimated_scan_bytes(self) -> f64 {
        self.estimated_scan_bytes
    }
}

/// Invalid provider scan statistics at a typed or serialized boundary.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ScanEstimateError {
    #[error("table scan estimated rows are invalid: {value}")]
    InvalidRows { value: f64 },
    #[error("table scan estimated bytes are invalid: {value}")]
    InvalidScanBytes { value: f64 },
}
