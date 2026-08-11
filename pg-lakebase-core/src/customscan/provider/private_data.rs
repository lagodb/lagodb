//! Provider-owned CustomScan plan data built on the shared plan-data codec.

use crate::customscan::error::CustomScanError;
pub use crate::plan_data::{
    PlanDataReader as PrivateDataReader, PlanDataWriter as PrivateDataWriter,
};

/// Provider-extensible encode/decode for the opaque CustomScan payload.
///
/// Encoded values must use the writer API so PostgreSQL can safely copy the
/// resulting plan tree with `copyObject`.
pub trait CustomScanPrivate: Sized + 'static {
    fn encode(&self, writer: &mut PrivateDataWriter) -> Result<(), CustomScanError>;

    fn decode(reader: &mut PrivateDataReader<'_>) -> Result<Self, CustomScanError>;
}

/// Provider plan data for providers that need no private payload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoPrivateData;

impl CustomScanPrivate for NoPrivateData {
    fn encode(&self, _writer: &mut PrivateDataWriter) -> Result<(), CustomScanError> {
        Ok(())
    }

    fn decode(_reader: &mut PrivateDataReader<'_>) -> Result<Self, CustomScanError> {
        Ok(Self)
    }
}
