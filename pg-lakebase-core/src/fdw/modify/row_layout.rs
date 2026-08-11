//! Begin-time metadata for foreign modify rows.

use core::ptr;
use std::slice;

use pgrx::pg_sys;

use crate::tuple::{ColumnDatumCodec, ColumnDatumTarget};

use super::error::ForeignModifyError;

/// Relation-local metadata reused by every row of one modify operation.
///
/// The descriptor attributes are copied at Begin time.  PostgreSQL keeps the
/// result relation open for the operation, but owning the small target array
/// here makes the row callback independent of repeated descriptor traversal.
/// Codec binding remains lazy because only columns that providers semantically
/// rewrite need a `Cell` conversion path.
pub(crate) struct ModifyRowLayout {
    relation_oid: pg_sys::Oid,
    tuple_desc: pg_sys::TupleDesc,
    attributes: Box<[ModifyAttribute]>,
    codecs: Box<[Option<ColumnDatumCodec>]>,
    has_live_attributes: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct ModifyAttribute {
    pub(crate) type_oid: pg_sys::Oid,
    pub(crate) type_mod: i32,
    target: ColumnDatumTarget,
}

impl ModifyRowLayout {
    /// Build all stable relation metadata before provider execution begins.
    ///
    /// # Safety
    ///
    /// `relation` must be a live result relation with a non-NULL, valid tuple
    /// descriptor that remains stable for the returned executor state.
    pub(crate) unsafe fn from_relation(relation: pg_sys::Relation) -> Self {
        let tuple_desc = unsafe { (*relation).rd_att };
        let natts = unsafe { (*tuple_desc).natts as usize };
        let attrs =
            unsafe { slice::from_raw_parts((*tuple_desc).attrs.as_ptr(), natts) };
        let attributes = attrs
            .iter()
            .map(|attr| ModifyAttribute {
                type_oid: attr.atttypid,
                type_mod: attr.atttypmod,
                target: ColumnDatumTarget::from_oid(attr.atttypid),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            relation_oid: unsafe { (*relation).rd_id },
            tuple_desc,
            codecs: vec![None; natts].into_boxed_slice(),
            attributes,
            has_live_attributes: attrs.iter().any(|attr| !attr.attisdropped),
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            relation_oid: pg_sys::InvalidOid,
            tuple_desc: ptr::null_mut(),
            attributes: Vec::new().into_boxed_slice(),
            codecs: Vec::new().into_boxed_slice(),
            has_live_attributes: false,
        }
    }

    #[inline]
    pub(crate) fn relation_oid(&self) -> pg_sys::Oid {
        self.relation_oid
    }

    #[inline]
    pub(crate) const fn tuple_desc(&self) -> pg_sys::TupleDesc {
        self.tuple_desc
    }

    #[inline]
    pub(crate) fn natts(&self) -> usize {
        self.attributes.len()
    }

    #[inline]
    pub(crate) fn has_live_attributes(&self) -> bool {
        self.has_live_attributes
    }

    /// Return metadata for a provider column derived from this relation layout.
    ///
    /// # Safety
    ///
    /// `index` must identify a live, non-dropped attribute in this layout.
    #[inline]
    pub(crate) unsafe fn attribute_unchecked(&self, index: usize) -> ModifyAttribute {
        unsafe { *self.attributes.get_unchecked(index) }
    }

    /// Bind and cache only the semantic conversion codec for one column.
    ///
    /// # Safety
    ///
    /// `index` must identify a live, non-dropped attribute in this layout.
    pub(crate) unsafe fn codec_for_unchecked(
        &mut self,
        index: usize,
    ) -> Result<ColumnDatumCodec, ForeignModifyError> {
        if let Some(codec) = unsafe { *self.codecs.get_unchecked(index) } {
            return Ok(codec);
        }
        let attr = unsafe { self.attribute_unchecked(index) };
        let codec = ColumnDatumCodec::bind(attr.target)
            .map_err(ForeignModifyError::provider)?;
        unsafe { *self.codecs.get_unchecked_mut(index) = Some(codec) };
        Ok(codec)
    }
}
