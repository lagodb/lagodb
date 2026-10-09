//! Runtime ABI adapter for Bound routes in the base-owned volume config.

use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{
    CallbackErrorReport, TablespaceBindingEntry, TablespaceBindingsOutput,
};
use pgrx::{PgMemoryContexts, pg_guard};

use crate::storage::volume_config::StorageVolumeControl;

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn load_tablespace_bindings(
    output: *mut TablespaceBindingsOutput,
) -> u32 {
    // SAFETY: the exact-build ABI requires a live call-scoped output pointer.
    let output = unsafe { &mut *output };
    *output = TablespaceBindingsOutput::default();
    let mut error = CallbackErrorReport::default();
    // SAFETY: capture and string copies use the caller's live memory context.
    let status = unsafe {
        error.capture(|| {
            let Some(snapshot) = StorageVolumeControl::current()
                .binding_snapshot()
                .map_err(PgReportError::from_domain_error)?
            else {
                return Ok(());
            };
            // Only the query projection filters lifecycle. The worker's source
            // still includes Retiring volumes throughout their grace period.
            let bound = snapshot.volumes.values().filter_map(|volume| {
                volume
                    .lifecycle
                    .bound_tablespace_oid()
                    .map(|oid| (oid, volume))
            });
            let count = bound.clone().count();
            if count == 0 {
                return Ok(());
            }
            let mut context = PgMemoryContexts::CurrentMemoryContext;
            // SAFETY: zero is valid for every scalar/pointer field. The caller
            // owns the context; each entry is filled before publishing output.
            let entries = context.palloc0_slice::<TablespaceBindingEntry>(count);
            for (entry, (oid, volume)) in entries.iter_mut().zip(bound) {
                let route =
                    volume.route().map_err(PgReportError::from_domain_error)?;
                *entry = TablespaceBindingEntry {
                    tablespace_oid: oid.into(),
                    volume_id: volume.id.get(),
                    object_namespace: context.pstrdup(route.object_namespace()),
                    effective_base_uri: context.pstrdup(route.effective_base_uri()),
                };
            }
            output.entries = entries.as_ptr();
            output.count = count;
            Ok(())
        })
    };
    output.error = error;
    status
}
