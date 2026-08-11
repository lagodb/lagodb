//! Path-stage CustomScan envelope.

use pgrx::pg_sys;

use crate::customscan::ScanPurpose;
use crate::customscan::error::CustomScanError;
use crate::customscan::plan_data::EnvelopeError;
use crate::plan_data::{PlanDataReader, PlanDataWriter};

/// Decoded wrapper carried between path creation and `PlanCustomPath`.
pub(crate) struct EncodedPathPrivate {
    pub(crate) purpose: ScanPurpose,
    pub(crate) requires_wholerow: bool,
    pub(crate) provider_metadata: *mut pg_sys::List,
}

/// # Safety
///
/// `provider_metadata` must be NIL or a copyObject-safe planner-owned `T_List`.
pub(crate) unsafe fn encode_path_private(
    purpose: ScanPurpose,
    requires_wholerow: bool,
    provider_metadata: *mut pg_sys::List,
) -> Result<*mut pg_sys::List, CustomScanError> {
    PlanDataWriter::encode_list(|writer| {
        writer
            .append_i32(purpose.to_wire())
            .append_bool(requires_wholerow);
        unsafe { writer.append_list(provider_metadata) };
        Ok(())
    })
}

/// # Safety
///
/// `list` must be a live path-owned PostgreSQL `T_List`.
pub(crate) unsafe fn decode_path_private(
    list: *mut pg_sys::List,
) -> Result<EncodedPathPrivate, CustomScanError> {
    unsafe {
        PlanDataReader::decode_checked_list(list, 0, |reader| {
            let purpose_raw = reader.read_i32()?;
            let purpose = ScanPurpose::from_wire(purpose_raw)
                .ok_or(EnvelopeError::UnknownScanPurpose { value: purpose_raw })?;
            let requires_wholerow = reader.read_bool()?;
            let provider_metadata =
                reader.read_optional_list(pg_sys::NodeTag::T_List)?;
            Ok(EncodedPathPrivate {
                purpose,
                requires_wholerow,
                provider_metadata,
            })
        })
    }
}
