//! Format-specific PostgreSQL FDW adapters over shared format components.
//!
//! Factory selection, PG contexts and execution state stay here. File codecs
//! and typed predicates remain in `format` for both FDW and COPY consumers.

mod avro;
mod csv;
mod delimited_options;
mod delimited_scan;
mod delimited_write;
mod filter;
mod json;
mod parquet;
mod scan;
mod selection;
mod text;
mod write;

pub(super) use filter::{
    FormatBoundFilter, FormatFilterPlan, FormatFilterPlanner, FormatPlannedFilter,
};
pub(super) use scan::{
    FormatAnalyzer, FormatFdwReader, FormatScanPlanner, FormatScanPrivate,
    FormatScanState,
};
pub(super) use selection::FdwFormat;
pub(super) use write::{FormatWriteState, FormatWriter};
