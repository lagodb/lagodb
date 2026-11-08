//! Modify-specific private-data envelope.

use pgrx::pg_sys;

use super::contract::{
    FdwModify, ForeignModifyOperation, ForeignModifyPrivate, ForeignReturnedIdentity,
};
use super::error::ForeignModifyError;
use super::return_requirements::ForeignModifyReturnRequirements;
use crate::fdw::{
    ForeignPrivateReader, ForeignPrivateWriter, ForeignRowIdentityKind,
};

const PRIVATE_MODIFY: i32 = 3;

pub(crate) struct DecodedModifyPrivate<D> {
    pub(crate) private_data: D,
    pub(crate) relation_oid: pg_sys::Oid,
    pub(crate) operation: ForeignModifyOperation,
    pub(crate) updated_columns: Box<[pg_sys::AttrNumber]>,
    pub(crate) row_identities: Box<[ForeignRowIdentityKind]>,
    pub(crate) returned_identity: ForeignReturnedIdentity,
    pub(crate) returned_item_pointer_required: bool,
    pub(crate) return_requirements: ForeignModifyReturnRequirements,
}

pub(crate) struct ModifyPrivateEnvelope<'a, P: FdwModify> {
    pub(crate) relation_oid: pg_sys::Oid,
    pub(crate) operation: ForeignModifyOperation,
    pub(crate) updated_columns: &'a [pg_sys::AttrNumber],
    pub(crate) row_identities: &'a [ForeignRowIdentityKind],
    pub(crate) returned_identity: ForeignReturnedIdentity,
    pub(crate) returned_item_pointer_required: bool,
    pub(crate) return_requirements: &'a ForeignModifyReturnRequirements,
    pub(crate) private_data: &'a P::ModifyPrivateData,
}

impl<P: FdwModify> ModifyPrivateEnvelope<'_, P> {
    pub(crate) fn encode(self) -> Result<*mut pg_sys::List, ForeignModifyError> {
        let payload = ForeignPrivateWriter::encode_list(|writer| {
            self.private_data.encode(writer)
        })?;

        ForeignPrivateWriter::encode_list(|envelope| {
            envelope
                .append_i32(PRIVATE_MODIFY)
                .append_cstr(P::NAME)
                .append_oid(self.relation_oid)
                .append_i32(self.operation.as_pg() as i32)
                .append_i32(self.returned_identity.wire_kind())
                .append_bool(self.returned_item_pointer_required)
                .append_bool(self.return_requirements.all_columns())
                .append_nested(|writer| {
                    for &attno in self.updated_columns {
                        writer.append_i32(attno as i32);
                    }
                })
                .append_nested(|writer| {
                    for &attno in self.return_requirements.columns() {
                        writer.append_i32(attno as i32);
                    }
                })
                .append_nested(|writer| {
                    for &identity in self.row_identities {
                        writer.append_i32(identity.wire_attno() as i32);
                    }
                });
            unsafe { envelope.append_list(payload) };
            Ok(())
        })
    }
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
            let returning_all_columns = reader.read_bool()?;
            let updated_columns = reader.read_nested(|updated_columns_reader| -> Result<_, ForeignModifyError> {
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
                    updated_columns.push(attno);
                }
                Ok(updated_columns)
            })?;
            let returning_columns = reader.read_nested(|returning_columns_reader| -> Result<_, ForeignModifyError> {
                let mut returning_columns =
                    Vec::with_capacity(returning_columns_reader.remaining());
                while returning_columns_reader.remaining() > 0 {
                    let attno = pg_sys::AttrNumber::try_from(
                        returning_columns_reader.read_i32()?,
                    )
                    .map_err(|_| {
                        ForeignModifyError::framework(
                            "FDW modify private data contains an invalid RETURNING column",
                        )
                    })?;
                    returning_columns.push(attno);
                }
                Ok(returning_columns)
            })?;
            let row_identities = reader.read_nested(|identities_reader| -> Result<_, ForeignModifyError> {
                let mut identities = Vec::with_capacity(identities_reader.remaining());
                while identities_reader.remaining() > 0 {
                    let attno = pg_sys::AttrNumber::try_from(
                        identities_reader.read_i32()?,
                    )
                    .map_err(|_| {
                        ForeignModifyError::framework(
                            "FDW modify private data contains an invalid row identity",
                        )
                    })?;
                    let identity = ForeignRowIdentityKind::from_wire_attno(attno)?;
                    identities.push(identity);
                }
                Ok(identities)
            })?;
            let return_requirements = ForeignModifyReturnRequirements::from_parts(
                returning_columns.into_boxed_slice(),
                returning_all_columns,
            );
            let private_data = reader
                .read_nested(|payload| P::ModifyPrivateData::decode(payload))?;
            Ok(DecodedModifyPrivate {
                private_data,
                relation_oid,
                operation,
                updated_columns: updated_columns.into_boxed_slice(),
                row_identities: row_identities.into_boxed_slice(),
                returned_identity,
                returned_item_pointer_required,
                return_requirements,
            })
        })
    }
}
