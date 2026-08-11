//! Framework-owned `CustomScan.custom_private` envelope.

use std::ffi::{CStr, CString};

use pgrx::pg_sys;

use crate::customscan::ScanPurpose;
use crate::customscan::error::CustomScanError;
use crate::customscan::plan_data::{EnvelopeError, tuple_layout::ScanTupleLayout};
use crate::plan_data::{PlanDataReader, PlanDataWriter};

const FIELD_TUPLE_LAYOUT: i32 = 6;

/// Decoded framework envelope. Provider planned predicates and binding
/// metadata remain raw until the typed CustomScan filter adapter decodes them.
pub struct EncodedPrivate {
    pub provider_id_or_name: CString,
    pub purpose: ScanPurpose,
    pub relation_oid: pg_sys::Oid,
    pub planned_filter_count: usize,
    pub binding_count: usize,
    pub provider_metadata_raw: *mut pg_sys::List,
    pub tuple_layout: ScanTupleLayout,
    pub planned_filters_raw: *mut pg_sys::List,
    pub binding_slots_raw: *mut pg_sys::List,
}

pub(crate) struct CustomPrivatePlan<'a> {
    pub provider_id_or_name: &'a CStr,
    pub purpose: ScanPurpose,
    pub relation_oid: pg_sys::Oid,
    pub planned_filter_count: usize,
    pub binding_count: usize,
    pub provider_metadata: *mut pg_sys::List,
    pub tuple_layout: &'a ScanTupleLayout,
    pub planned_filters: *mut pg_sys::List,
    pub binding_slots: *mut pg_sys::List,
}

impl CustomPrivatePlan<'_> {
    /// Encode the final, authoritative planned-filter envelope.
    ///
    /// # Safety
    ///
    /// Every list field must be NIL or a live copyObject-safe PostgreSQL list
    /// in the current planner memory context.
    pub(crate) unsafe fn encode(self) -> Result<*mut pg_sys::List, CustomScanError> {
        PlanDataWriter::encode_list(|writer| {
            writer
                .append_cstr(self.provider_id_or_name)
                .append_i32(self.purpose.to_wire())
                .append_oid(self.relation_oid)
                .append_count(self.planned_filter_count)
                .append_count(self.binding_count);
            unsafe {
                writer.append_list(self.provider_metadata);
                writer.append_list(self.tuple_layout.encode_wire());
                writer.append_list(self.planned_filters);
                writer.append_list(self.binding_slots);
            }
            Ok(())
        })
    }
}

/// Fail closed if encoded provider name does not match `expected`.
pub fn assert_provider_name_matches(
    name_in_payload: &CStr,
    expected: &CStr,
) -> Result<(), CustomScanError> {
    if name_in_payload != expected {
        return Err(CustomScanError::provider_name_mismatch(
            expected.to_owned(),
            name_in_payload.to_owned(),
        ));
    }
    Ok(())
}

/// Decode the final CustomScan envelope once at Begin/EXPLAIN.
///
/// # Safety
///
/// `list` must be a live plan-owned PostgreSQL `T_List`.
pub unsafe fn decode_private(
    list: *mut pg_sys::List,
) -> Result<EncodedPrivate, CustomScanError> {
    unsafe {
        PlanDataReader::decode_checked_list(list, 0, |reader| {
            let provider_id_or_name = reader.read_cstr()?.to_owned();
            let purpose_raw = reader.read_i32()?;
            let purpose = ScanPurpose::from_wire(purpose_raw)
                .ok_or(EnvelopeError::UnknownScanPurpose { value: purpose_raw })?;
            let relation_oid = reader.read_oid()?;
            let planned_filter_count = reader.read_count()?;
            let binding_count = reader.read_count()?;
            let provider_metadata_raw =
                reader.read_optional_list(pg_sys::NodeTag::T_List)?;
            let tuple_layout_raw =
                reader.read_optional_list(pg_sys::NodeTag::T_IntList)?;
            let tuple_layout =
                ScanTupleLayout::decode_wire(tuple_layout_raw, FIELD_TUPLE_LAYOUT)?;
            let planned_filters_raw =
                reader.read_optional_list(pg_sys::NodeTag::T_List)?;
            let binding_slots_raw =
                reader.read_optional_list(pg_sys::NodeTag::T_List)?;
            Ok(EncodedPrivate {
                provider_id_or_name,
                purpose,
                relation_oid,
                planned_filter_count,
                binding_count,
                provider_metadata_raw,
                tuple_layout,
                planned_filters_raw,
                binding_slots_raw,
            })
        })
    }
}
