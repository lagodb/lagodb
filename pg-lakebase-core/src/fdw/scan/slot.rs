//! ForeignScan slot write planning and HeapTuple-slot output.

use core::ptr;
use core::slice;

use pgrx::pg_sys;

use super::super::row_identity::ForeignRowIdentityRequirement;
use super::error::ForeignScanError;
use super::projection::{ScanProjection, SlotWritePlan};
use crate::handles::ValidItemPointer;

/// Executor layout derived once from the plan's slot-write contract.
///
/// The layout keeps all descriptor inspection and relation-column mapping out
/// of the per-row callback. Providers receive only the columns that must be
/// written for every datum row.
#[derive(Debug)]
pub(crate) struct SlotWriteLayout {
    output_columns: Box<[ScanOutputColumn]>,
    null_indices: Box<[usize]>,
}

impl Default for SlotWriteLayout {
    fn default() -> Self {
        Self {
            output_columns: Vec::new().into_boxed_slice(),
            null_indices: Vec::new().into_boxed_slice(),
        }
    }
}

impl SlotWriteLayout {
    /// Compile the runtime mapping for the validated executor scan slot.
    ///
    /// # Safety
    ///
    /// `slot` must be the live `TTSOpsHeapTuple` scan slot for the current
    /// ForeignScan.  Its descriptor must remain valid while the returned
    /// layout is used. `projection` and `write_plan` must be the validated
    /// private-data values produced for this scan. For
    /// `ScanProjection::Projected`, the slot descriptor must be the one
    /// PostgreSQL creates from `fdw_scan_tlist` with `ExecTypeFromTL`; its
    /// target-list attributes are not dropped.
    pub(crate) unsafe fn from_slot(
        slot: *mut pg_sys::TupleTableSlot,
        projection: &ScanProjection,
        write_plan: &SlotWritePlan,
    ) -> Self {
        let tuple_desc = unsafe { (*slot).tts_tupleDescriptor };
        let natts = unsafe { (*tuple_desc).natts as usize };
        let attrs =
            unsafe { slice::from_raw_parts((*tuple_desc).attrs.as_ptr(), natts) };
        let mut output_destinations = vec![false; natts];
        let mut output_columns = Vec::new();
        match projection {
            ScanProjection::Relation => match write_plan {
                SlotWritePlan::Complete => {
                    for (destination, attr) in attrs.iter().enumerate() {
                        if attr.attisdropped {
                            continue;
                        }
                        let attno = (destination + 1) as pg_sys::AttrNumber;
                        output_destinations[destination] = true;
                        output_columns.push(ScanOutputColumn { attno, destination });
                    }
                }
                SlotWritePlan::RequiredAttributes(attnos) => {
                    for &attno in attnos {
                        let destination = (attno - 1) as usize;
                        output_destinations[destination] = true;
                        output_columns.push(ScanOutputColumn { attno, destination });
                    }
                }
            },
            ScanProjection::Projected { attnos } => {
                for (destination, &attno) in attnos.iter().enumerate() {
                    output_destinations[destination] = true;
                    output_columns.push(ScanOutputColumn { attno, destination });
                }
            }
            ScanProjection::SyntheticNull => {}
        }

        let null_indices = output_destinations
            .iter()
            .enumerate()
            .filter_map(|(index, output)| (!output).then_some(index))
            .collect::<Vec<_>>();

        Self {
            output_columns: output_columns.into_boxed_slice(),
            null_indices: null_indices.into_boxed_slice(),
        }
    }

    #[inline]
    fn has_output_columns(&self) -> bool {
        !self.output_columns.is_empty()
    }

    #[inline]
    fn output_columns(&self) -> &[ScanOutputColumn] {
        &self.output_columns
    }

    /// Initialize positions that are never provider output for this scan.
    unsafe fn initialize_nulls(&self, values: *mut pg_sys::Datum, nulls: *mut bool) {
        for &index in self.null_indices.iter() {
            unsafe {
                ptr::write(values.add(index), pg_sys::Datum::from(0));
                ptr::write(nulls.add(index), true);
            }
        }
    }
}

/// Begin-time view of relation attributes bound to executor destinations.
#[derive(Clone, Copy)]
pub struct ScanOutputLayout<'a> {
    layout: &'a SlotWriteLayout,
}

impl<'a> ScanOutputLayout<'a> {
    pub(crate) const fn new(layout: &'a SlotWriteLayout) -> Self {
        Self { layout }
    }

    /// Columns that every datum row must write.
    #[inline]
    pub fn columns(self) -> &'a [ScanOutputColumn] {
        self.layout.output_columns()
    }
}

/// Relation attribute bound to an executor destination at Begin.
///
/// A column belongs to the scan whose [`ScanOutputLayout`] created it.
#[derive(Clone, Copy, Debug)]
pub struct ScanOutputColumn {
    attno: pg_sys::AttrNumber,
    destination: usize,
}

impl ScanOutputColumn {
    /// Base-relation attribute number represented by this output column.
    #[inline]
    pub const fn attno(self) -> pg_sys::AttrNumber {
        self.attno
    }
}

/// Prepared virtual-Datum output for one ForeignScan row.
pub struct ScanDatumWriter<'writer, 'scan> {
    writer: &'writer mut ScanSlotWriter<'scan>,
}

impl ScanDatumWriter<'_, '_> {
    /// Write one column previously bound from this scan's output layout.
    ///
    /// # Safety
    ///
    /// `column` must be one of this scan's [`ScanOutputLayout::columns`] and may
    /// be written at most once for the current row. Unless `is_null` is true,
    /// `datum` must match the bound attribute's PostgreSQL type and typmod.
    /// Pass-by-reference storage must remain valid for the current executor
    /// tuple cycle.
    #[inline]
    pub unsafe fn write(
        &mut self,
        column: ScanOutputColumn,
        datum: pg_sys::Datum,
        is_null: bool,
    ) {
        unsafe {
            self.writer.write_destination_unchecked(
                column.destination,
                datum,
                is_null,
            );
        }
    }
}

/// Provider-facing output writer for one ForeignScan row.
///
/// Datum output uses PostgreSQL's own `TTSOpsHeapTuple` slot arrays.  Ordinary
/// output is marked valid with `ExecStoreVirtualTuple`; PostgreSQL's heap-slot
/// ops materialize a physical tuple later only when a consumer requests one.
/// An explicit ItemPointer identity uses `heap_form_tuple` during completion,
/// while providers that already own a physical tuple use `store_heap_tuple`.
pub struct ScanSlotWriter<'a> {
    slot: *mut pg_sys::TupleTableSlot,
    layout: &'a SlotWriteLayout,
    values: *mut pg_sys::Datum,
    nulls: *mut bool,
    datum_defaults_initialized: &'a mut bool,
    row_identity_requirement: ForeignRowIdentityRequirement,
    item_pointer: Option<pg_sys::ItemPointerData>,
    datum_output_started: bool,
    stored: bool,
}

impl<'a> ScanSlotWriter<'a> {
    /// Construct a writer for the already validated executor slot.
    ///
    /// # Safety
    ///
    /// `slot` must be a live `TTSOpsHeapTuple` slot. `layout` must have been
    /// built for this slot descriptor, and `datum_defaults_initialized` must
    /// belong exclusively to this scan.
    pub(crate) unsafe fn new(
        slot: *mut pg_sys::TupleTableSlot,
        layout: &'a SlotWriteLayout,
        datum_defaults_initialized: &'a mut bool,
        row_identity_requirement: ForeignRowIdentityRequirement,
    ) -> Self {
        let (values, nulls) = unsafe { ((*slot).tts_values, (*slot).tts_isnull) };

        Self {
            slot,
            layout,
            values,
            nulls,
            datum_defaults_initialized,
            row_identity_requirement,
            item_pointer: None,
            datum_output_started: false,
            stored: false,
        }
    }

    /// Prepare virtual-Datum output and return its per-datum writer.
    ///
    /// # Safety
    ///
    /// This method may be called at most once for a produced row and must not be
    /// mixed with [`Self::store_heap_tuple`] for that row. Before the provider
    /// returns that row, it must write every column from the scan's
    /// [`ScanOutputLayout::columns`] exactly once.
    #[inline]
    pub unsafe fn datum_writer(&mut self) -> ScanDatumWriter<'_, 'a> {
        unsafe { self.begin_datum_output() };
        ScanDatumWriter { writer: self }
    }

    /// Write a destination supplied by the Begin-time output layout.
    ///
    /// # Safety
    ///
    /// `index` must be one of the layout's output destinations. The slot arrays
    /// must be the callback-scoped objects validated by Begin.
    #[inline]
    unsafe fn write_destination_unchecked(
        &mut self,
        index: usize,
        datum: pg_sys::Datum,
        is_null: bool,
    ) {
        unsafe {
            ptr::write(self.values.add(index), datum);
            ptr::write(self.nulls.add(index), is_null);
        }
    }

    /// Prepare the virtual Datum arrays before the provider writes any value.
    ///
    /// # Safety
    ///
    /// The writer must own the validated slot and layout for the current
    /// callback, and this method must run at most once for the row.
    #[inline]
    unsafe fn begin_datum_output(&mut self) {
        unsafe { pg_sys::ExecClearTuple(self.slot) };
        if !*self.datum_defaults_initialized {
            unsafe {
                self.layout.initialize_nulls(self.values, self.nulls);
            }
            *self.datum_defaults_initialized = true;
        }
        self.datum_output_started = true;
    }

    /// Write the item-pointer identity carried by a modify-purpose scan row.
    /// The value is committed to the physical tuple when the row is completed,
    /// after PostgreSQL clears the slot for the new row.
    #[inline]
    pub fn write_item_pointer(&mut self, item_pointer: &ValidItemPointer) {
        self.item_pointer = Some(item_pointer.to_pg_sys());
    }

    /// Store a provider-owned tuple that matches the scan slot descriptor.
    ///
    /// The framework always passes `shouldFree=false` to PostgreSQL.  The slot
    /// borrows the tuple for the current executor row, while the provider keeps
    /// allocation ownership.  This is intentional because PostgreSQL resets
    /// `ecxt_per_tuple_memory` before requesting the next row, and the framework
    /// must not ask PostgreSQL to free a tuple that that reset may already have
    /// reclaimed.
    ///
    /// A tuple allocated in the per-tuple context is callback-scoped: the
    /// provider must not retain or free it after this row.  A provider that
    /// needs to retain a physical tuple across callbacks must allocate it in
    /// provider-owned storage that remains valid while PostgreSQL can inspect
    /// the slot.  The provider must not mutate or free the tuple while the
    /// current row is visible to PostgreSQL. When an ItemPointer identity is
    /// required, t_self is the identity field; t_ctid remains provider-owned
    /// update-chain metadata and is not compared with t_self.
    /// The provider must call this at most once for a produced row, without
    /// datum or item-pointer writes for that row.
    ///
    /// # Safety
    ///
    /// `tuple` and its `t_data` must be non-NULL and use the same tuple
    /// descriptor as this writer's scan slot. Its allocation must obey the
    /// lifetime contract above. When this scan requires item-pointer identity,
    /// `tuple.t_self` must contain a valid item pointer.
    pub unsafe fn store_heap_tuple(&mut self, tuple: pg_sys::HeapTuple) {
        unsafe {
            pg_sys::ExecStoreHeapTuple(tuple, self.slot, false);
        }
        *self.datum_defaults_initialized = false;
        self.stored = true;
    }

    pub(crate) fn complete(&mut self) -> Result<(), ForeignScanError> {
        if self.stored {
            return Ok(());
        }
        if self.row_identity_requirement.needs_item_pointer()
            && self.item_pointer.is_none()
        {
            return Err(ForeignScanError::framework(
                "ScanSlotWriter returned a row without writing its item-pointer identity",
            ));
        }

        if !self.datum_output_started {
            if self.layout.has_output_columns() {
                return Err(ForeignScanError::framework(
                    "ScanSlotWriter returned a row without datum output",
                ));
            }
            unsafe { self.begin_datum_output() };
        }

        unsafe {
            if let Some(item_pointer) = self.item_pointer {
                // PG17's ForeignScan slot uses TTSOpsHeapTuple.  Its system
                // attribute implementation reads ctid from the physical
                // HeapTuple, so tts_tid alone is not a valid identity output.
                // The slot owns this framework-created tuple.  Allocate it in
                // the slot context so the per-tuple reset cannot reclaim it
                // before TTSOpsHeapTuple clears the slot and frees it.
                let slot_context = (*self.slot).tts_mcxt;
                let prior_context = pg_sys::MemoryContextSwitchTo(slot_context);
                let tuple = pg_sys::heap_form_tuple(
                    (*self.slot).tts_tupleDescriptor,
                    self.values,
                    self.nulls,
                );
                pg_sys::MemoryContextSwitchTo(prior_context);
                (*tuple).t_self = item_pointer;
                (*(*tuple).t_data).t_ctid = item_pointer;
                // `heap_form_tuple` initializes a DatumTupleFields header.
                // ForeignScan consumers use the HeapTupleFields view when
                // accessing system attributes, so initialize the fields that
                // PostgreSQL's standard FDW tuple builders initialize too.
                (*(*tuple).t_data).t_choice.t_heap.t_xmin = 0.into();
                (*(*tuple).t_data).t_choice.t_heap.t_xmax = 0.into();
                (*(*tuple).t_data).t_choice.t_heap.t_field3.t_cid = 0;
                pg_sys::ExecStoreHeapTuple(tuple, self.slot, true);
                *self.datum_defaults_initialized = false;
            } else {
                pg_sys::ExecStoreVirtualTuple(self.slot);
            }
        }
        self.stored = true;
        Ok(())
    }
}
