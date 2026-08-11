//! Public provider boundary for the generic CustomScan framework.
//!
//! Planning, execution, and provider contracts are separate modules. This
//! module remains the stable facade consumed by storage providers.

mod context;
mod contract;
mod execution;
pub mod methods;
mod planning;
mod private_data;
pub mod registry;

pub use crate::customscan::error::CustomScanError;
pub use crate::customscan::plan_data::tuple_layout::{
    NeededColumns, ScanTupleDescriptor, ScanTupleLayout, WHOLEROW_NAME,
};
pub use contract::LakebaseCustomScanProvider;
pub use execution::{
    BeginContext, CreateStateContext, EndContext, NextSlotContext, ReScanContext,
};
pub(crate) use methods::method_tables_for;
pub use planning::*;
pub use private_data::{
    CustomScanPrivate, NoPrivateData, PrivateDataReader, PrivateDataWriter,
};
pub(crate) use registry::ErasedFilterPlanner;
pub use registry::register_provider;
pub(crate) use registry::{ErasedProvider, find_matching_provider};
