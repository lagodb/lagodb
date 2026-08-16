//! Scan-specific private-data envelopes built on the neutral FDW codec.

use core::ffi::CStr;

use pgrx::pg_sys;

use super::super::row_identity::ForeignRowIdentityRequirement;
use super::context::{ForeignPlanPrivate, PathVariantKind};
use super::contract::FdwScan;
use super::error::ForeignScanError;
use super::projection::{ColumnRequirements, ScanProjection, SlotWritePlan};
use crate::fdw::{ForeignPrivateReader, ForeignPrivateWriter};

const PRIVATE_PATH: i32 = 1;
const PRIVATE_SCAN: i32 = 2;

const VARIANT_PLAIN: i32 = 0;
const VARIANT_PARAMETERIZED: i32 = 1;

pub(crate) struct DecodedScanPrivate<D> {
    pub(crate) private_data: D,
    pub(crate) relation_oid: pg_sys::Oid,
    pub(crate) projection: ScanProjection,
    pub(crate) write_plan: SlotWritePlan,
    pub(crate) row_identity: ForeignRowIdentityRequirement,
    pub(crate) requirements: ColumnRequirements,
    pub(crate) planned_filter_count: usize,
    pub(crate) binding_count: usize,
    pub(crate) planned_filters_raw: *mut pg_sys::List,
    pub(crate) binding_slots_raw: *mut pg_sys::List,
}

pub(crate) struct DecodedPathPrivate<D> {
    pub(crate) private_data: D,
    pub(crate) kind: PathVariantKind,
}

pub(crate) fn encode_path_private<P>(
    provider_name: &'static CStr,
    variant: PathVariantKind,
    private_data: &P::PrivateData,
) -> Result<*mut pg_sys::List, ForeignScanError>
where
    P: FdwScan,
{
    let payload =
        ForeignPrivateWriter::encode_list(|writer| private_data.encode(writer))?;

    ForeignPrivateWriter::encode_list(|envelope| {
        envelope
            .append_i32(PRIVATE_PATH)
            .append_i32(match variant {
                PathVariantKind::Plain => VARIANT_PLAIN,
                PathVariantKind::JoinParameterized => VARIANT_PARAMETERIZED,
            })
            .append_cstr(provider_name);
        unsafe { envelope.append_list(payload) };
        Ok(())
    })
}

pub(crate) fn encode_scan_private<P>(
    provider_name: &'static CStr,
    relation_oid: pg_sys::Oid,
    private_data: &P::PrivateData,
    projection: &ScanProjection,
    write_plan: &SlotWritePlan,
    row_identity: ForeignRowIdentityRequirement,
    requirements: &ColumnRequirements,
    planned_filter_count: usize,
    binding_count: usize,
    planned_filters: *mut pg_sys::List,
    binding_slots: *mut pg_sys::List,
    explain_filters: *mut pg_sys::List,
) -> Result<*mut pg_sys::List, ForeignScanError>
where
    P: FdwScan,
{
    let payload =
        ForeignPrivateWriter::encode_list(|writer| private_data.encode(writer))?;

    ForeignPrivateWriter::encode_list(|envelope| {
        envelope
            .append_i32(PRIVATE_SCAN)
            .append_cstr(provider_name)
            .append_oid(relation_oid)
            .append_i32(row_identity.wire_kind());
        unsafe { envelope.append_list(payload) };
        envelope
            .append_i32(projection.wire_kind())
            .append_nested(|writer| {
                for &attno in projection.attnos() {
                    writer.append_i32(attno as i32);
                }
            })
            .append_bool(write_plan.is_complete())
            .append_nested(|writer| {
                for &attno in write_plan.attributes() {
                    writer.append_i32(attno as i32);
                }
            })
            .append_bool(requirements.needs_all_columns())
            .append_nested(|writer| {
                for attno in requirements.user_columns() {
                    writer.append_i32(attno as i32);
                }
            })
            .append_count(planned_filter_count)
            .append_count(binding_count);
        unsafe {
            envelope.append_list(planned_filters);
            envelope.append_list(binding_slots);
            envelope.append_list(explain_filters);
        }
        Ok(())
    })
}

/// # Safety
///
/// `raw` must be the live `ForeignPath.fdw_private` list produced by this
/// framework for the current planner invocation.  Its PostgreSQL memory must
/// remain live while the returned provider data is decoded.
pub(crate) unsafe fn decode_path_private<P>(
    raw: *mut pg_sys::List,
) -> Result<DecodedPathPrivate<P::PrivateData>, ForeignScanError>
where
    P: FdwScan,
{
    unsafe {
        ForeignPrivateReader::decode_checked_list(raw, 0, |reader| {
            let kind = reader.read_i32()?;
            let variant = match reader.read_i32()? {
                VARIANT_PLAIN => PathVariantKind::Plain,
                VARIANT_PARAMETERIZED => PathVariantKind::JoinParameterized,
                _ => {
                    return Err(ForeignScanError::framework(
                        "FDW path private data has an unknown path variant",
                    ));
                }
            };
            verify_header::<P>(reader, kind, PRIVATE_PATH)?;
            let private_data =
                reader.read_nested(|payload| P::PrivateData::decode(payload))?;
            Ok(DecodedPathPrivate {
                private_data,
                kind: variant,
            })
        })
    }
}

/// # Safety
///
/// `raw` must be the live `ForeignScan.fdw_private` list produced by this
/// framework for the current executor invocation.  Its PostgreSQL memory
/// must remain live while the returned provider data is decoded.
pub(crate) unsafe fn decode_scan_private<P>(
    raw: *mut pg_sys::List,
) -> Result<DecodedScanPrivate<P::PrivateData>, ForeignScanError>
where
    P: FdwScan,
{
    unsafe { decode_scan_envelope::<P>(raw) }.map(|(decoded, _)| decoded)
}

/// Decode only the dedicated EXPLAIN section from a live scan envelope.
///
/// # Safety
///
/// `raw` must satisfy [`decode_scan_private`].
pub(crate) unsafe fn decode_scan_explain_private<P>(
    raw: *mut pg_sys::List,
) -> Result<*mut pg_sys::List, ForeignScanError>
where
    P: FdwScan,
{
    unsafe { decode_scan_envelope::<P>(raw) }.map(|(_, explain)| explain)
}

unsafe fn decode_scan_envelope<P>(
    raw: *mut pg_sys::List,
) -> Result<(DecodedScanPrivate<P::PrivateData>, *mut pg_sys::List), ForeignScanError>
where
    P: FdwScan,
{
    unsafe {
        ForeignPrivateReader::decode_checked_list(raw, 0, |reader| {
            let kind = reader.read_i32()?;
            verify_header::<P>(reader, kind, PRIVATE_SCAN)?;
            let relation_oid = reader.read_oid()?;
            let row_identity =
                ForeignRowIdentityRequirement::from_wire(reader.read_i32()?)?;
            let private_data =
                reader.read_nested(|payload| P::PrivateData::decode(payload))?;

            let projection_kind = reader.read_i32()?;
            let projection_attnos = reader.read_nested(read_attnos)?;
            let projection =
                ScanProjection::from_wire(projection_kind, projection_attnos)?;

            let write_plan_complete = reader.read_bool()?;
            let write_plan_attnos = reader.read_nested(read_attnos)?;
            let write_plan =
                SlotWritePlan::from_wire(write_plan_complete, write_plan_attnos)?;

            let all_columns = reader.read_bool()?;
            let user_attnos = reader.read_nested(read_attnos)?;
            let mut requirements = ColumnRequirements::default();
            if all_columns {
                requirements.require_all_columns();
            }
            for attno in user_attnos {
                requirements.require_column(attno)?;
            }

            let planned_filter_count = reader.read_count()?;
            let binding_count = reader.read_count()?;
            let planned_filters_raw =
                reader.read_optional_list(pg_sys::NodeTag::T_List)?;
            let binding_slots_raw =
                reader.read_optional_list(pg_sys::NodeTag::T_List)?;
            let explain_filters_raw =
                reader.read_optional_list(pg_sys::NodeTag::T_List)?;
            let decoded = DecodedScanPrivate {
                private_data,
                relation_oid,
                projection,
                write_plan,
                row_identity,
                requirements,
                planned_filter_count,
                binding_count,
                planned_filters_raw,
                binding_slots_raw,
            };
            Ok((decoded, explain_filters_raw))
        })
    }
}

/// `reader` must be positioned at the header fields written by the matching
/// framework envelope.
fn verify_header<P: FdwScan>(
    reader: &mut ForeignPrivateReader<'_>,
    kind: i32,
    expected_kind: i32,
) -> Result<(), ForeignScanError> {
    if kind != expected_kind {
        return Err(ForeignScanError::framework(
            "FDW private data has the wrong envelope kind",
        ));
    }
    let provider = reader.read_cstr()?;
    if provider.to_bytes() != P::NAME.to_bytes() {
        return Err(ForeignScanError::framework(
            "FDW private data belongs to a different provider",
        ));
    }
    Ok(())
}

fn read_attnos(
    reader: &mut ForeignPrivateReader<'_>,
) -> Result<Vec<pg_sys::AttrNumber>, ForeignScanError> {
    let mut attnos = Vec::with_capacity(reader.remaining());
    while reader.remaining() > 0 {
        let value = reader.read_i32()?;
        let attno = pg_sys::AttrNumber::try_from(value).map_err(|_| {
            ForeignScanError::framework(
                "FDW private data contains an invalid attribute number",
            )
        })?;
        if attno <= 0 {
            return Err(ForeignScanError::framework(
                "FDW private data contains a non-positive attribute number",
            ));
        }
        if attnos.contains(&attno) {
            return Err(ForeignScanError::framework(
                "FDW private data contains a duplicate attribute number",
            ));
        }
        attnos.push(attno);
    }
    Ok(attnos)
}
