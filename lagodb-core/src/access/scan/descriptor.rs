//! Ownership shared by ordinary AM scans and ModifyTable-bound target scans.

use pgrx::pg_sys;

use crate::access::common::FfiContainer;
use crate::api::{AmResult, AmScanSession};
use crate::diag::PgReportError;
use crate::handles::OwnedScanKeys;
use crate::tuple::{Row, RowDatumCodec, SlotColumns};

pub(crate) struct TableScanState<T> {
    pub(super) am_instance: T,
    pub(super) scan_keys: OwnedScanKeys,
    pub(super) row: Row,
    pub(super) tmp_ctx: pg_sys::MemoryContext,
    /// Bound only if this scan uses one of the Row-returning callbacks. The
    /// normal `scan_getnextslot` path may be slot-first and provider-physical.
    row_codec: Option<RowDatumCodec>,
}

pub(crate) type TableAmScanDesc<T> =
    FfiContainer<pg_sys::TableScanDescData, TableScanState<T>>;

impl<T> TableScanState<T> {
    fn new(
        am_instance: T,
        scan_keys: OwnedScanKeys,
        tmp_ctx: pg_sys::MemoryContext,
    ) -> Self {
        Self {
            am_instance,
            scan_keys,
            row: Row::new(),
            tmp_ctx,
            row_codec: None,
        }
    }

    pub(super) unsafe fn reset_tmp_context(&mut self) {
        unsafe { pg_sys::MemoryContextReset(self.tmp_ctx) };
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
        let mut columns = unsafe { SlotColumns::new(slot, self.tmp_ctx) };
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
    /// # Safety
    /// The descriptor owns a live session and is exclusively borrowed.
    pub(crate) unsafe fn instance_mut(&mut self) -> &mut T {
        let state = unsafe { self.session_mut() };
        &mut state.am_instance
    }

    /// Install an already constructed session under the descriptor's PG owner.
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
            (*desc).init_session(TableScanState::new(instance, keys, tmp));
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

    /// # Safety
    /// The descriptor has been initialized by `create` and has not ended.
    pub(crate) unsafe fn begin(&mut self) -> AmResult<()> {
        let state = unsafe { self.session_mut() };
        state.am_instance.scan_begin(&state.scan_keys)
    }
}
