//! `SlotColumns`: the cohesive substrate that owns the only `unsafe` writes to
//! a slot's `tts_values`/`tts_isnull`, shared by the row world and the column
//! world.

use std::marker::PhantomData;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::diag::PgReportError;

use super::row::Row;

/// Per-column slot writer.
///
/// `target_ctx` is taken explicitly so the memory-context discipline lives at
/// the call site (the TableAM seqscan passes its per-row `tmp_ctx`; the
/// CustomScan emit path passes the scan node's per-tuple context), never
/// scattered into an access method.
pub struct SlotColumns<'a> {
    /// Mutable views over the slot's `tts_values`/`tts_isnull` C arrays, built
    /// once in `new`. The base pointers are stable for the slot's lifetime, so
    /// caching the slices here keeps `set_datum` a plain bounds-checked index
    /// instead of rebuilding both views (`from_raw_parts_mut`) on every
    /// per-column write in the scan hot path.
    values: &'a mut [pg_sys::Datum],
    nulls: &'a mut [bool],
    slot: *mut pg_sys::TupleTableSlot,
    target_ctx: pg_sys::MemoryContext,
    _marker: PhantomData<&'a mut pg_sys::TupleTableSlot>,
}

impl<'a> SlotColumns<'a> {
    /// # Safety
    ///
    /// `slot` must be a valid, initialized slot with at least `natts`
    /// attributes; `target_ctx` must be the context the caller wants varlena
    /// datums palloc'd into.
    pub unsafe fn new(
        slot: *mut pg_sys::TupleTableSlot,
        target_ctx: pg_sys::MemoryContext,
        natts: usize,
    ) -> Self {
        // SAFETY: the caller guarantees `slot` is valid with at least `natts`
        // attributes, so `tts_values`/`tts_isnull` are each a live array of
        // `natts` elements. These arrays are stable for the slot's lifetime
        // (allocated at slot init, never reallocated by `ExecClearTuple` /
        // `ExecStoreVirtualTuple`), and `SlotColumns` is the sole writer of
        // them, so holding `&mut` views for `'a` does not alias any other live
        // reference. `fill_from_row` only reads the *other* slot field
        // `tts_tupleDescriptor` through `slot`, which does not overlap this
        // memory.
        unsafe {
            Self {
                values: std::slice::from_raw_parts_mut((*slot).tts_values, natts),
                nulls: std::slice::from_raw_parts_mut((*slot).tts_isnull, natts),
                slot,
                target_ctx,
                _marker: PhantomData,
            }
        }
    }

    pub fn natts(&self) -> usize {
        self.values.len()
    }

    /// Write column `index`; `None` denotes SQL NULL. The sole writer of
    /// `tts_values`/`tts_isnull`.
    ///
    /// Positions this method never touches are **not** guaranteed SQL NULL by
    /// the slot itself: `ExecClearTuple` does not reset `tts_isnull`, and the
    /// slot's `tts_values`/`tts_isnull` arrays are `palloc0`'d (so they start
    /// non-NULL; only `ExecStoreAllNullTuple` sets them all NULL). Callers must
    /// therefore write every column the consumer can read — the data path
    /// relies on "every readable column is mapped" (whole-row refs fall back to
    /// all columns), not on untouched positions reading back as NULL.
    ///
    /// `index` must be `< natts`. This is an always-on assertion rather than a
    /// `debug_assert!`: as a shared data-path substrate the bound is a domain
    /// invariant, and a release-mode out-of-range write would otherwise still
    /// trip an opaque slice-bounds panic — a named check fails with a meaningful
    /// message instead.
    pub fn set_datum(&mut self, index: usize, value: Option<pg_sys::Datum>) {
        assert!(index < self.values.len(), "slot column index out of range");
        match value {
            Some(datum) => {
                self.values[index] = datum;
                self.nulls[index] = false;
            }
            None => self.nulls[index] = true,
        }
    }

    /// Row-world bridge: materialize an owned [`Row`] into the slot. Cells are
    /// converted under `target_ctx` and written through `set_datum`; the caller
    /// (core scan shim) owns the single `ExecStoreVirtualTuple`.
    pub(crate) fn fill_from_row(
        &mut self,
        row: &mut Row,
    ) -> Result<(), PgReportError> {
        let natts = self.values.len();
        if row.len() < natts {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INVALID_COLUMN_REFERENCE,
                format!(
                    "row has {} columns but tuple slot expects {}",
                    row.len(),
                    natts
                ),
            ));
        }

        unsafe {
            let tup_desc = (*self.slot).tts_tupleDescriptor;
            let attrs = std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts);

            PgMemoryContexts::For(self.target_ctx).switch_to(|_| {
                for (i, attr) in attrs.iter().enumerate().take(natts) {
                    match row.take_cell(i) {
                        Some(cell) => {
                            let datum = cell
                                .into_datum_typed(attr.atttypid, attr.atttypmod)
                                .ok_or_else(|| {
                                    PgReportError::from_message(
                                        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
                                        format!(
                                            "failed to convert row column {} to datum",
                                            i + 1
                                        ),
                                    )
                                })?;
                            self.set_datum(i, Some(datum));
                        }
                        None => self.set_datum(i, None),
                    }
                }
                Ok(())
            })
        }
    }
}
