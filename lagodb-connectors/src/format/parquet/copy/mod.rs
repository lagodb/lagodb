//! Direction-specific native Parquet adapters for the canonical COPY bridge.

mod copy_from;
mod copy_to;

pub(super) use copy_from::ParquetCopySource;
pub(super) use copy_to::ParquetCopyDestination;
