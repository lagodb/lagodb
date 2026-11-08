//! Ownership shared by ordinary AM scans and ModifyTable-bound target scans.

use pgrx::pg_sys;

use crate::diag::PgReportError;
use crate::handles::{OwnedScanKeys, RelationHandle};
use crate::table_am::AmScanSession;
use crate::table_am::callbacks::common::FfiContainer;
use crate::tuple::{Row, RowDatumCodec, SlotDatumWriter};

pub(crate) struct TableScanState<T> {
    pub(super) am_instance: T,
    pub(super) scan_keys: OwnedScanKeys,
    pub(super) row: Row,
    pub(super) tmp_ctx: pg_sys::MemoryContext,
    /// Bound only if this scan uses one of the Row-returning callbacks. The
    /// normal `scan_getnextslot` path may be slot-first and provider-physical.
    row_codec: Option<RowDatumCodec>,
    /// Zero-based positions of the relation's dropped attributes.
    ///
    /// Slot-first providers write only live columns, but PostgreSQL reads every
    /// attribute when it forms a whole-row datum or copies a slot into a
    /// HeapTuple, and expects dropped attributes to be NULL. The caller
    /// supplies the slot to every fetch, so these NULLs are written per fetch
    /// rather than once per scan.
    dropped_attributes: Box<[usize]>,
}

pub(crate) type TableAmScanDesc<T> =
    FfiContainer<pg_sys::TableScanDescData, TableScanState<T>>;

impl<T> TableScanState<T> {
    fn new(
        am_instance: T,
        scan_keys: OwnedScanKeys,
        tmp_ctx: pg_sys::MemoryContext,
        relation: &RelationHandle<'_>,
    ) -> Self {
        let dropped_attributes = relation
            .tuple_desc_handle()
            .attributes()
            .iter()
            .enumerate()
            .filter_map(|(index, attribute)| attribute.attisdropped.then_some(index))
            .collect();
        Self {
            am_instance,
            scan_keys,
            row: Row::new(),
            tmp_ctx,
            row_codec: None,
            dropped_attributes,
        }
    }

    pub(super) unsafe fn reset_tmp_context(&mut self) {
        unsafe { pg_sys::MemoryContextReset(self.tmp_ctx) };
    }

    /// Open a slot-first output writer whose dropped attributes read as NULL.
    ///
    /// The returned writer does not borrow `self`, so the caller can drive the
    /// provider session while it is alive.
    ///
    /// # Safety
    /// `slot` is the slot PostgreSQL passed to the current fetch callback; its
    /// descriptor is the scanned relation's, as `table_slot_create` makes it.
    /// The safety contract of [`SlotDatumWriter::new`] applies for `'s`.
    pub(super) unsafe fn slot_writer<'s>(
        &self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> SlotDatumWriter<'s> {
        let mut columns = unsafe { SlotDatumWriter::new(slot, self.tmp_ctx) };
        for &index in &self.dropped_attributes {
            // SAFETY: the index comes from the relation descriptor this slot
            // was created with, so it is below the slot width.
            unsafe { columns.set_datum_unchecked(index, None) };
        }
        columns
    }

    pub(super) unsafe fn write_row_to_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<(), PgReportError> {
        // Do not bind this at scan_begin: the ordinary scan_getnextslot path
        // can fill the slot through a provider physical codec without ever
        // materializing a semantic Row.
        if self.row_codec.is_none() {
            self.row_codec = Some(
                // SAFETY: the executor passes an initialized slot whose tuple
                // descriptor remains valid for this fetch callback.
                unsafe { RowDatumCodec::from_slot(slot) }
                    .map_err(PgReportError::from_domain_error)?,
            );
        }
        let mut columns = unsafe { SlotDatumWriter::new(slot, self.tmp_ctx) };
        unsafe {
            columns.fill_from_row(
                &mut self.row,
                self.row_codec
                    .as_ref()
                    .expect("row codec initialized before row write"),
            )
        }
    }
}

impl<T: AmScanSession> TableAmScanDesc<T> {
    /// Install a complete session under the descriptor's PG owner.
    ///
    /// # Safety
    /// The relation, snapshot, parallel descriptor and parent context must remain
    /// live for the scan. `keys` owns the buffer published through `rs_key`.
    pub(crate) unsafe fn create(
        parent: pg_sys::MemoryContext,
        rel: pg_sys::Relation,
        snapshot: pg_sys::Snapshot,
        parallel: pg_sys::ParallelTableScanDesc,
        flags: u32,
        keys: OwnedScanKeys,
        instance: T,
    ) -> *mut Self {
        // SAFETY: the caller supplies the PG owner; the registered session
        // destructor also runs when ERROR skips table_endscan.
        unsafe {
            let desc = Self::alloc(parent, c"lagodb TableScan lifecycle");
            let tmp = (*desc).create_child_context(c"lagodb TableScan tmp");
            let relation = RelationHandle::from_raw(rel);
            (*desc).init_session(TableScanState::new(instance, keys, tmp, &relation));
            let keys = &mut (*desc).session_mut().scan_keys;
            let nkeys = keys.len() as i32;
            let key = keys.rs_key_ptr();
            let base = (*desc).base_mut();
            base.rs_rd = rel;
            base.rs_snapshot = snapshot;
            base.rs_parallel = parallel;
            base.rs_flags = flags;
            base.rs_nkeys = nkeys;
            base.rs_key = key;
            desc
        }
    }
}
