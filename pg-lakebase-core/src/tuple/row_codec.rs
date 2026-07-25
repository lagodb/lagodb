//! Relation-bound semantic Datum/Cell conversion for the row world.
//!
//! A row conversion must validate the relation's semantic representation once
//! before it starts materializing values.  Keeping that binding state here
//! prevents individual datum conversions from re-checking server encoding and
//! prevents `Option<Cell>` from being used as an error channel.

use pgrx::pg_sys::{self, Datum};

use super::cell::Cell;
use super::datum::{ColumnDatumCodec, ColumnDatumTarget, DatumConversionError};

/// A relation-bound plan for semantic row conversion.
///
/// The plan owns only copied OIDs and can therefore outlive the PostgreSQL
/// relation descriptor that was used to create it.  It is validated once at
/// relation/slot binding: semantic text and JSON values require a UTF-8
/// server encoding, while physical byte-copy paths do not use this type.
#[derive(Debug, Clone)]
pub struct RowDatumCodec {
    targets: Box<[ColumnDatumCodec]>,
}

impl RowDatumCodec {
    /// Bind a relation's target columns and validate semantic encoding once.
    pub fn from_targets(
        targets: &[ColumnDatumTarget],
    ) -> Result<Self, DatumConversionError> {
        if targets
            .iter()
            .any(|target| target.requires_utf8_server_encoding())
        {
            ColumnDatumTarget::validate_utf8_server_encoding()?;
        }
        let targets = targets
            .iter()
            .copied()
            .map(ColumnDatumCodec::from_validated)
            .collect();
        Ok(Self { targets })
    }

    /// Bind the attributes of a live PostgreSQL relation.
    ///
    /// # Safety
    ///
    /// `relation` must be a non-null, live relation with a valid tuple
    /// descriptor. PostgreSQL must be running on the current backend thread.
    pub unsafe fn from_relation(
        relation: pg_sys::Relation,
    ) -> Result<Self, DatumConversionError> {
        let targets = unsafe { ColumnDatumTarget::from_relation(relation) }
            .expect("RowDatumCodec requires a valid relation descriptor");
        Self::from_targets(&targets)
    }

    /// Bind the descriptor carried by a live tuple slot.
    ///
    /// # Safety
    ///
    /// `slot` must be a non-null, initialized slot with a valid tuple
    /// descriptor and attribute array. PostgreSQL must be running on the
    /// current backend thread.
    pub unsafe fn from_slot(
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<Self, DatumConversionError> {
        let tuple_desc = unsafe { (*slot).tts_tupleDescriptor };
        let natts = unsafe { (*tuple_desc).natts as usize };
        let attrs = unsafe {
            core::slice::from_raw_parts((*tuple_desc).attrs.as_ptr(), natts)
        };
        let targets = attrs
            .iter()
            .map(|attr| ColumnDatumTarget::from_oid(attr.atttypid))
            .collect::<Vec<_>>();
        Self::from_targets(&targets)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.targets.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    #[inline]
    pub fn target_at(&self, index: usize) -> Option<ColumnDatumTarget> {
        self.targets
            .get(index)
            .copied()
            .map(ColumnDatumCodec::target)
    }

    #[inline]
    pub(crate) fn target_codec_at(&self, index: usize) -> Option<ColumnDatumCodec> {
        self.targets.get(index).copied()
    }

    /// Convert every datum in a bound slot into the destination cells.
    ///
    /// The three slices must describe the same relation-shaped row. The
    /// length check is performed once; the conversion loop then advances the
    /// bound target and datum slices together without a per-datum index lookup.
    ///
    /// # Safety
    ///
    /// Every non-NULL datum must be valid for the corresponding bound target,
    /// and PostgreSQL must be running on the current backend thread.
    pub(crate) unsafe fn datums_to_cells(
        &self,
        values: &[Datum],
        nulls: &[bool],
        cells: &mut [Option<Cell>],
    ) -> Result<(), DatumConversionError> {
        if self.targets.len() != values.len()
            || values.len() != nulls.len()
            || nulls.len() != cells.len()
        {
            return Err(DatumConversionError::IncompatibleType {
                target: pg_sys::InvalidOid,
            });
        }

        for (((target, cell), datum), is_null) in self
            .targets
            .iter()
            .zip(cells.iter_mut())
            .zip(values.iter().copied())
            .zip(nulls.iter().copied())
        {
            *cell = unsafe {
                Cell::from_polymorphic_datum_checked(datum, is_null, target.oid())
            }?;
        }
        Ok(())
    }

    /// Convert an exact-size row of cells directly into slot datum arrays.
    ///
    /// The target, value, and NULL arrays are zipped with the bound plan.
    ///
    /// # Safety
    ///
    /// PostgreSQL must be running on the current backend thread, the caller
    /// must have selected the destination memory context, and `cells`,
    /// `values`, and `nulls` must each contain exactly one item for every bound
    /// target.
    pub(crate) unsafe fn cells_to_datums<I>(
        &self,
        cells: I,
        values: &mut [Datum],
        nulls: &mut [bool],
    ) -> Result<(), DatumConversionError>
    where
        I: ExactSizeIterator<Item = Option<Cell>>,
    {
        for (((target, value), is_null), cell) in self
            .targets
            .iter()
            .zip(values.iter_mut())
            .zip(nulls.iter_mut())
            .zip(cells)
        {
            match cell {
                Some(cell) => {
                    *value = unsafe { target.cell_to_datum(cell) }?;
                    *is_null = false;
                }
                None => *is_null = true,
            }
        }
        Ok(())
    }

    /// Convert one slot datum into a semantic Cell through the bound plan.
    ///
    /// `None` is returned only for SQL NULL. Any non-NULL value that cannot be
    /// materialized is returned as a structured conversion error.
    ///
    /// # Safety
    ///
    /// `datum` must be valid for the target attribute at `index`, and the
    /// PostgreSQL backend thread must be active. The plan must have been bound
    /// to the same slot/relation layout.
    pub unsafe fn datum_to_cell(
        &self,
        index: usize,
        datum: Datum,
        is_null: bool,
    ) -> Result<Option<Cell>, DatumConversionError> {
        let target =
            self.target_at(index)
                .ok_or(DatumConversionError::IncompatibleType {
                    target: pg_sys::InvalidOid,
                })?;
        unsafe { Cell::from_polymorphic_datum_checked(datum, is_null, target.oid()) }
    }

    /// Convert one semantic Cell into the destination attribute's Datum.
    ///
    /// # Safety
    ///
    /// PostgreSQL must be running on the current backend thread and the caller
    /// must have selected a memory context that owns returned by-reference
    /// datums. The plan must be bound to the same destination layout.
    pub unsafe fn cell_to_datum(
        &self,
        index: usize,
        cell: Cell,
    ) -> Result<Datum, DatumConversionError> {
        let target = self.target_codec_at(index).ok_or(
            DatumConversionError::IncompatibleType {
                target: pg_sys::InvalidOid,
            },
        )?;
        unsafe { target.cell_to_datum(cell) }
    }
}
