//! Stable names for relation-attribute targets registered with PostgreSQL.

use core::ffi::CStr;
use std::ffi::CString;
use std::io::Write;
use std::str;

use pgrx::pg_sys;

use crate::handles::TupleDescHandle;

#[derive(Debug, Clone, Copy)]
pub(crate) enum RelationAttributeTargetNameError {
    InvalidName,
}

/// Codec for a relation-column target registered through
/// PostgreSQL's `add_row_identity_var` mechanism.
///
/// The attribute name is the logical partition-column identity. The physical
/// attno distinguishes Vars that PostgreSQL cannot merge under one
/// `RowIdentityVarInfo` name.
pub(crate) struct RelationAttributeTargetName {
    prefix: &'static str,
}

impl RelationAttributeTargetName {
    pub(crate) const fn new(prefix: &'static str) -> Self {
        Self { prefix }
    }

    pub(crate) fn encode(
        &self,
        attribute: &pg_sys::FormData_pg_attribute,
    ) -> CString {
        // SAFETY: PostgreSQL stores NameData as a NUL-terminated byte array.
        let attribute_name =
            unsafe { CStr::from_ptr(attribute.attname.data.as_ptr()) };
        let mut encoded = Vec::with_capacity(
            self.prefix.len() + 6 + attribute_name.to_bytes().len(),
        );
        encoded.extend_from_slice(self.prefix.as_bytes());
        write!(&mut encoded, "{}_", attribute.attnum)
            .expect("writing a relation attribute target name to Vec cannot fail");
        encoded.extend_from_slice(attribute_name.to_bytes());
        CString::new(encoded)
            .expect("a relation attribute target name cannot contain NUL")
    }

    pub(crate) fn attno(
        &self,
        name: &CStr,
    ) -> Result<Option<pg_sys::AttrNumber>, RelationAttributeTargetNameError> {
        let Some((attno, _)) = self.parts(name)? else {
            return Ok(None);
        };
        Ok(Some(attno))
    }

    /// Resolve this target only when both its physical attno and logical name
    /// belong to the current leaf. Targets for other leaves resolve to `None`.
    ///
    /// # Safety
    ///
    /// `relation` must be live with the descriptor PostgreSQL created for it.
    pub(crate) unsafe fn decode(
        &self,
        relation: pg_sys::Relation,
        name: &CStr,
    ) -> Result<Option<pg_sys::AttrNumber>, RelationAttributeTargetNameError> {
        let Some((attno, attribute_name)) = self.parts(name)? else {
            return Ok(None);
        };
        let relation_index = usize::try_from(attno as i32 - 1)
            .map_err(|_| RelationAttributeTargetNameError::InvalidName)?;
        // SAFETY: guaranteed by this method's contract.
        let tuple_desc = unsafe { TupleDescHandle::from_raw((*relation).rd_att) };
        let Some(attribute) = tuple_desc.attributes().get(relation_index) else {
            return Ok(None);
        };
        if attribute.attisdropped {
            return Ok(None);
        }
        // SAFETY: PostgreSQL stores NameData as a NUL-terminated byte array.
        let relation_name =
            unsafe { CStr::from_ptr(attribute.attname.data.as_ptr()) };
        Ok((relation_name.to_bytes() == attribute_name).then_some(attno))
    }

    fn parts<'a>(
        &self,
        name: &'a CStr,
    ) -> Result<
        Option<(pg_sys::AttrNumber, &'a [u8])>,
        RelationAttributeTargetNameError,
    > {
        let Some(encoded) = name.to_bytes().strip_prefix(self.prefix.as_bytes())
        else {
            return Ok(None);
        };
        let Some(separator) = encoded.iter().position(|byte| *byte == b'_') else {
            return Err(RelationAttributeTargetNameError::InvalidName);
        };
        let attribute_name = &encoded[separator + 1..];
        if attribute_name.is_empty() {
            return Err(RelationAttributeTargetNameError::InvalidName);
        }
        let attno = str::from_utf8(&encoded[..separator])
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .and_then(|value| pg_sys::AttrNumber::try_from(value).ok())
            .filter(|attno| *attno > 0)
            .ok_or(RelationAttributeTargetNameError::InvalidName)?;
        Ok(Some((attno, attribute_name)))
    }
}
