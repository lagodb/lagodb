//! Modify-specific private-data envelope.

use core::ffi::CStr;

use pgrx::pg_sys;

use super::contract::{
    FdwModify, ForeignModifyOperation, ForeignModifyPrivate, ForeignReturnedIdentity,
};
use super::error::ForeignModifyError;
use crate::fdw::{ForeignPrivateReader, ForeignPrivateWriter};

const PRIVATE_MODIFY: i32 = 3;

pub(crate) struct DecodedModifyPrivate<D> {
    pub(crate) private_data: D,
    pub(crate) relation_oid: pg_sys::Oid,
    pub(crate) operation: ForeignModifyOperation,
    pub(crate) updated_columns: Box<[pg_sys::AttrNumber]>,
    pub(crate) returned_identity: ForeignReturnedIdentity,
    pub(crate) returned_item_pointer_required: bool,
}

pub(crate) fn encode_modify_private<P: FdwModify>(
    provider_name: &'static CStr,
    relation_oid: pg_sys::Oid,
    operation: ForeignModifyOperation,
    updated_columns: &[pg_sys::AttrNumber],
    returned_identity: ForeignReturnedIdentity,
    returned_item_pointer_required: bool,
    private_data: &P::ModifyPrivateData,
) -> Result<*mut pg_sys::List, ForeignModifyError> {
    let payload =
        ForeignPrivateWriter::encode_list(|writer| private_data.encode(writer))?;

    ForeignPrivateWriter::encode_list(|envelope| {
        envelope
            .append_i32(PRIVATE_MODIFY)
            .append_cstr(provider_name)
            .append_oid(relation_oid)
            .append_i32(operation.as_pg() as i32)
            .append_i32(returned_identity.wire_kind())
            .append_bool(returned_item_pointer_required)
            .append_nested(|writer| {
                for &attno in updated_columns {
                    writer.append_i32(attno as i32);
                }
            });
        unsafe { envelope.append_list(payload) };
        Ok(())
    })
}

/// # Safety
///
/// `raw` must be the live `PlanForeignModify` private list produced by this
/// framework. Its PostgreSQL memory must remain live during decoding.
pub(crate) unsafe fn decode_modify_private<P: FdwModify>(
    raw: *mut pg_sys::List,
) -> Result<DecodedModifyPrivate<P::ModifyPrivateData>, ForeignModifyError> {
    unsafe {
        ForeignPrivateReader::decode_checked_list(raw, 0, |reader| {
            let kind = reader.read_i32()?;
            let provider = reader.read_cstr()?;
            if kind != PRIVATE_MODIFY {
                return Err(ForeignModifyError::framework(
                    "FDW private data has the wrong modify envelope kind",
                ));
            }
            if provider.to_bytes() != P::NAME.to_bytes() {
                return Err(ForeignModifyError::framework(
                    "FDW modify private data belongs to a different provider",
                ));
            }
            let relation_oid = reader.read_oid()?;
            let operation =
                ForeignModifyOperation::from_pg(reader.read_i32()? as u32)?;
            let returned_identity =
                ForeignReturnedIdentity::from_wire(reader.read_i32()?)?;
            let returned_item_pointer_required = reader.read_bool()?;
            if returned_item_pointer_required
                && !returned_identity.supports_item_pointer()
            {
                return Err(ForeignModifyError::framework(
                    "FDW modify private data requires an undeclared returned identity",
                ));
            }
            let updated_columns = reader.read_nested(|updated_columns_reader| {
                let mut updated_columns =
                    Vec::with_capacity(updated_columns_reader.remaining());
                while updated_columns_reader.remaining() > 0 {
                    let attno = pg_sys::AttrNumber::try_from(
                        updated_columns_reader.read_i32()?,
                    )
                    .map_err(|_| {
                        ForeignModifyError::framework(
                            "FDW modify private data contains an invalid updated column",
                        )
                    })?;
                    if attno <= 0 || updated_columns.contains(&attno) {
                        return Err(ForeignModifyError::framework(
                            "FDW modify private data contains a non-positive or duplicate updated column",
                        ));
                    }
                    updated_columns.push(attno);
                }
                Ok(updated_columns)
            })?;
            if matches!(
                operation,
                ForeignModifyOperation::Insert | ForeignModifyOperation::Delete
            ) && !updated_columns.is_empty()
            {
                return Err(ForeignModifyError::framework(
                    "INSERT or DELETE modify private data contains updated columns",
                ));
            }
            let private_data = reader
                .read_nested(|payload| P::ModifyPrivateData::decode(payload))?;
            Ok(DecodedModifyPrivate {
                private_data,
                relation_oid,
                operation,
                updated_columns: updated_columns.into_boxed_slice(),
                returned_identity,
                returned_item_pointer_required,
            })
        })
    }
}
