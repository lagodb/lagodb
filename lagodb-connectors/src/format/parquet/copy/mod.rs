//! Direction-specific native Parquet adapters for the canonical COPY bridge.

mod copy_from;
mod copy_to;

pub(in crate::format) use copy_from::ParquetCopySource;
pub(in crate::format) use copy_to::ParquetCopyDestination;
