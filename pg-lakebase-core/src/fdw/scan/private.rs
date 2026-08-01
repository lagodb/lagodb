//! Scan-specific private-data envelopes built on the neutral FDW codec.

use core::ffi::CStr;

use pgrx::pg_sys;

use crate::expr::contract::{ColumnRef, PushdownContract};

use super::super::codec::{ForeignPrivateReader, ForeignPrivateWriter};
use super::super::row_identity::ForeignRowIdentityRequirement;
use super::context::{ForeignPlanPrivate, PathVariantKind};
use super::contract::FdwScan;
use super::error::ForeignScanError;
use super::projection::{ColumnRequirements, ScanProjection, SlotWritePlan};

const PRIVATE_PATH: i32 = 1;
const PRIVATE_SCAN: i32 = 2;

const CONTRACT_EXACT: i32 = 0;
const CONTRACT_CONSERVATIVE: i32 = 1;

const VARIANT_PLAIN: i32 = 0;
const VARIANT_PARAMETERIZED: i32 = 1;

pub(crate) struct DecodedScanPrivate<D> {
    pub(crate) private_data: D,
    pub(crate) relation_oid: pg_sys::Oid,
    pub(crate) projection: ScanProjection,
    pub(crate) write_plan: SlotWritePlan,
    pub(crate) row_identity: ForeignRowIdentityRequirement,
    pub(crate) requirements: ColumnRequirements,
    pub(crate) contracts: Vec<PushdownContract>,
    pub(crate) column_refs: Vec<ColumnRef>,
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
    let mut payload = ForeignPrivateWriter::new();
    private_data.encode(&mut payload)?;
    let payload = payload.finish()?;

    let mut envelope = ForeignPrivateWriter::new();
    envelope
        .append_i32(PRIVATE_PATH)
        .append_i32(match variant {
            PathVariantKind::Plain => VARIANT_PLAIN,
            PathVariantKind::JoinParameterized => VARIANT_PARAMETERIZED,
        })
        .append_cstr(provider_name);
    unsafe { envelope.append_list(payload) };
    Ok(envelope.finish()?)
}

pub(crate) fn encode_scan_private<P>(
    provider_name: &'static CStr,
    relation_oid: pg_sys::Oid,
    private_data: &P::PrivateData,
    projection: &ScanProjection,
    write_plan: &SlotWritePlan,
    row_identity: ForeignRowIdentityRequirement,
    requirements: &ColumnRequirements,
    contracts: &[PushdownContract],
    column_refs: &[ColumnRef],
) -> Result<*mut pg_sys::List, ForeignScanError>
where
    P: FdwScan,
{
    let mut payload = ForeignPrivateWriter::new();
    private_data.encode(&mut payload)?;
    let payload = payload.finish()?;

    let mut envelope = ForeignPrivateWriter::new();
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
        .append_nested(|writer| {
            for contract in contracts {
                let value = match contract {
                    PushdownContract::ExactRowFilter => CONTRACT_EXACT,
                    PushdownContract::ConservativePruning => CONTRACT_CONSERVATIVE,
                };
                writer.append_i32(value);
            }
        })
        .append_nested(|writer| {
            for column_ref in column_refs {
                writer.append_nested(|entry| {
                    entry
                        .append_count(column_ref.expr_index)
                        .append_oid(column_ref.rel_oid)
                        .append_i32(column_ref.attno as i32)
                        .append_oid(column_ref.atttypid)
                        .append_oid(column_ref.attcollation)
                        .append_bool(column_ref.name.is_some());
                    if let Some(name) = &column_ref.name {
                        entry.append_str(name);
                    }
                });
            }
        });
    Ok(envelope.finish()?)
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
    let mut reader = unsafe { reader_from_raw(raw)? };
    let kind = reader.read_i32()?;
    let variant = reader.read_i32()?;
    let variant = match variant {
        VARIANT_PLAIN => PathVariantKind::Plain,
        VARIANT_PARAMETERIZED => PathVariantKind::JoinParameterized,
        _ => {
            return Err(ForeignScanError::framework(
                "FDW path private data has an unknown path variant",
            ));
        }
    };
    unsafe { verify_header::<P>(&mut reader, kind, PRIVATE_PATH)? };
    let mut payload = reader.read_nested()?;
    let private_data = unsafe { P::PrivateData::decode(&mut payload) }?;
    payload.finish()?;
    reader.finish()?;
    Ok(DecodedPathPrivate {
        private_data,
        kind: variant,
    })
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
    let mut reader = unsafe { reader_from_raw(raw)? };
    let kind = reader.read_i32()?;
    unsafe { verify_header::<P>(&mut reader, kind, PRIVATE_SCAN)? };
    let relation_oid = reader.read_oid()?;
    let row_identity = ForeignRowIdentityRequirement::from_wire(reader.read_i32()?)?;
    let mut payload = reader.read_nested()?;
    let private_data = unsafe { P::PrivateData::decode(&mut payload) }?;
    payload.finish()?;

    let projection_kind = reader.read_i32()?;
    let mut projection_reader = reader.read_nested()?;
    let projection_attnos = read_attnos(&mut projection_reader)?;
    projection_reader.finish()?;
    let projection = ScanProjection::from_wire(projection_kind, projection_attnos)?;

    let write_plan_complete = reader.read_bool()?;
    let mut write_plan_reader = reader.read_nested()?;
    let write_plan_attnos = read_attnos(&mut write_plan_reader)?;
    write_plan_reader.finish()?;
    let write_plan =
        SlotWritePlan::from_wire(write_plan_complete, write_plan_attnos)?;

    let all_columns = reader.read_bool()?;
    let mut requirements_reader = reader.read_nested()?;
    let user_attnos = read_attnos(&mut requirements_reader)?;
    requirements_reader.finish()?;
    let mut requirements = ColumnRequirements::default();
    if all_columns {
        requirements.require_all_columns();
    }
    for attno in user_attnos {
        requirements.require_column(attno)?;
    }

    let mut contracts_reader = reader.read_nested()?;
    let contracts = read_contracts(&mut contracts_reader)?;
    contracts_reader.finish()?;
    let mut column_refs_reader = reader.read_nested()?;
    let column_refs = read_column_refs(&mut column_refs_reader)?;
    column_refs_reader.finish()?;
    if column_refs.iter().any(|column_ref| {
        column_ref.expr_index >= contracts.len()
            || column_ref.rel_oid != relation_oid
            || column_ref.attno <= 0
    }) {
        return Err(ForeignScanError::framework(
            "FDW private data contains a column reference outside the pushed-expression list",
        ));
    }
    reader.finish()?;
    Ok(DecodedScanPrivate {
        private_data,
        relation_oid,
        projection,
        write_plan,
        row_identity,
        requirements,
        contracts,
        column_refs,
    })
}

/// # Safety
///
/// `reader` must refer to a live, validated framework private-data list.  Its
/// cursor must be positioned at the envelope fields in the order written by
/// the matching encoder.
unsafe fn verify_header<P: FdwScan>(
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

/// # Safety
///
/// `raw` must be a PostgreSQL-owned list pointer whose allocation remains live
/// for the returned reader lifetime.  The caller must use the reader only
/// during the planner or executor callback that owns that memory.
unsafe fn reader_from_raw<'a>(
    raw: *mut pg_sys::List,
) -> Result<ForeignPrivateReader<'a>, ForeignScanError> {
    Ok(unsafe { ForeignPrivateReader::checked_from_list(raw, 0)? })
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

fn read_contracts(
    reader: &mut ForeignPrivateReader<'_>,
) -> Result<Vec<PushdownContract>, ForeignScanError> {
    let mut contracts = Vec::with_capacity(reader.remaining());
    while reader.remaining() > 0 {
        contracts.push(match reader.read_i32()? {
            CONTRACT_EXACT => PushdownContract::ExactRowFilter,
            CONTRACT_CONSERVATIVE => PushdownContract::ConservativePruning,
            _ => {
                return Err(ForeignScanError::framework(
                    "FDW private data contains an unknown pushdown contract",
                ));
            }
        });
    }
    Ok(contracts)
}

fn read_column_refs(
    reader: &mut ForeignPrivateReader<'_>,
) -> Result<Vec<ColumnRef>, ForeignScanError> {
    let mut refs = Vec::with_capacity(reader.remaining());
    while reader.remaining() > 0 {
        let mut entry = reader.read_nested()?;
        let expr_index = entry.read_count()?;
        let rel_oid = entry.read_oid()?;
        let attno =
            pg_sys::AttrNumber::try_from(entry.read_i32()?).map_err(|_| {
                ForeignScanError::framework(
                    "FDW private data contains an invalid column-reference attribute",
                )
            })?;
        let atttypid = entry.read_oid()?;
        let attcollation = entry.read_oid()?;
        let has_name = entry.read_bool()?;
        let name = has_name.then(|| entry.read_str()).transpose()?;
        entry.finish()?;
        refs.push(ColumnRef {
            expr_index,
            rel_oid,
            attno,
            atttypid,
            attcollation,
            name,
        });
    }
    Ok(refs)
}
