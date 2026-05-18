//! Type-erasure adapter around `AmDmlSession`.
//!
//! `ErasedModifySession` lets the session manager store one dyn object per
//! relation regardless of which concrete AM backs it.

use crate::api::AmDmlSession;
use crate::handles::{
    BulkInsertStateHandle, ItemPointer, SnapshotHandle, TM_FailureData,
};
use crate::tuple::Row;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;

pub(super) struct ErasedModifySessionAdapter<T> {
    inner: T,
}

impl<T> ErasedModifySessionAdapter<T> {
    pub(super) fn new(inner: T) -> Self {
        Self { inner }
    }
}

macro_rules! define_erased_modify_session {
    ($(
        fn $method:ident(&mut self $(, $arg:ident: $arg_ty:ty)*) -> $ret:ty;
    )*) => {
        pub(super) trait ErasedModifySession {
            $(
                fn $method(&mut self $(, $arg: $arg_ty)*) -> Result<$ret, ErrorReport>;
            )*

            fn abort_modify(&mut self);
        }

        impl<T> ErasedModifySession for ErasedModifySessionAdapter<T>
        where
            T: AmDmlSession + 'static,
        {
            $(
                fn $method(&mut self $(, $arg: $arg_ty)*) -> Result<$ret, ErrorReport> {
                    self.inner.$method($($arg),*)
                }
            )*

            fn abort_modify(&mut self) {
                self.inner.abort_modify();
            }
        }
    };
}

define_erased_modify_session! {
    fn end_modify(&mut self) -> ();
    fn tuple_insert(
        &mut self,
        row: &Row,
        cid: pg_sys::CommandId,
        options: ::core::ffi::c_int,
        bistate: Option<&BulkInsertStateHandle>
    ) -> ();
    fn tuple_insert_speculative(
        &mut self,
        row: &Row,
        cid: pg_sys::CommandId,
        options: ::core::ffi::c_int,
        bistate: Option<&BulkInsertStateHandle>,
        spec_token: u32
    ) -> ();
    fn multi_insert(
        &mut self,
        rows: &[Row],
        cid: pg_sys::CommandId,
        options: ::core::ffi::c_int,
        bistate: Option<&BulkInsertStateHandle>
    ) -> ();
    fn tuple_delete(
        &mut self,
        tid: &ItemPointer,
        cid: pg_sys::CommandId,
        snapshot: &SnapshotHandle,
        crosscheck: Option<&SnapshotHandle>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        changing_part: bool
    ) -> pg_sys::TM_Result::Type;
    fn tuple_update(
        &mut self,
        otid: &ItemPointer,
        row: &Row,
        cid: pg_sys::CommandId,
        snapshot: &SnapshotHandle,
        crosscheck: Option<&SnapshotHandle>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut pg_sys::LockTupleMode::Type,
        update_indexes: &mut pg_sys::TU_UpdateIndexes::Type
    ) -> pg_sys::TM_Result::Type;
    fn tuple_lock(
        &mut self,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
        cid: pg_sys::CommandId,
        mode: pg_sys::LockTupleMode::Type,
        wait_policy: pg_sys::LockWaitPolicy::Type,
        flags: u8,
        tmfd: &mut TM_FailureData
    ) -> pg_sys::TM_Result::Type;
    fn finish_bulk_insert(&mut self, options: ::core::ffi::c_int) -> ();
    fn tuple_complete_speculative(
        &mut self,
        row: &Row,
        spec_token: u32,
        succeeded: bool
    ) -> ();
}
