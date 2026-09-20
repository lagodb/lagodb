use serde_cbor::{from_slice, to_writer};

use super::WorkerTaskMetadata;
use crate::scan::ScanError;

/// Self-describing codec for Iceberg runtime metadata shared with workers.
///
/// CBOR is used because Iceberg's serde implementations require
/// `deserialize_any`, while its decimal, binary, fixed, and UUID literals
/// require byte strings to remain distinct from sequences.
pub(super) struct WorkerTaskMetadataCodec;

impl WorkerTaskMetadataCodec {
    pub(super) fn encode_into(
        output: &mut Vec<u8>,
        metadata: &WorkerTaskMetadata,
    ) -> Result<(), ScanError> {
        to_writer(output, metadata).map_err(|error| {
            ScanError::WorkerPayload(format!(
                "Iceberg task metadata encoding failed: {error}"
            ))
        })
    }

    pub(super) fn decode(input: &[u8]) -> Result<WorkerTaskMetadata, ScanError> {
        from_slice(input).map_err(|error| {
            ScanError::WorkerPayload(format!(
                "Iceberg task metadata decoding failed: {error}"
            ))
        })
    }
}
