//! Validated physical estimates for one provider-owned source leaf.

/// Planner estimates supplied by the provider that owns a source leaf.
///
/// `estimated_rows` is the number of visible rows emitted to the query engine.
/// `estimated_scan_bytes` is a provider estimate of physical source data. It is
/// not the size of the Arrow batches emitted after projection and must not be
/// used as DataFusion `Statistics::total_byte_size`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceEstimate {
    estimated_rows: f64,
    estimated_scan_bytes: f64,
}

impl SourceEstimate {
    pub fn try_new(
        estimated_rows: f64,
        estimated_scan_bytes: f64,
    ) -> Result<Self, SourceEstimateError> {
        if !estimated_rows.is_finite() || estimated_rows < 0.0 {
            return Err(SourceEstimateError::InvalidRows {
                value: estimated_rows,
            });
        }
        if !estimated_scan_bytes.is_finite() || estimated_scan_bytes < 0.0 {
            return Err(SourceEstimateError::InvalidScanBytes {
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

/// Invalid provider source statistics at a typed or serialized boundary.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SourceEstimateError {
    #[error("query source estimated rows are invalid: {value}")]
    InvalidRows { value: f64 },
    #[error("query source estimated scan bytes are invalid: {value}")]
    InvalidScanBytes { value: f64 },
}
