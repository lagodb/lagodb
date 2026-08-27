//! PostgreSQL `ImportForeignSchema` callback trampoline.

use core::ffi::c_void;
use core::ptr;

use pgrx::{pg_guard, pg_sys};

use super::contract::{FdwImportSchema, ForeignImportSchemaContext};
use super::error::ForeignImportError;

#[pg_guard]
/// # Safety
///
/// PostgreSQL must supply its live import statement and resolved server OID.
pub(crate) unsafe extern "C-unwind" fn import_foreign_schema<P: FdwImportSchema>(
    statement: *mut pg_sys::ImportForeignSchemaStmt,
    server_oid: pg_sys::Oid,
) -> *mut pg_sys::List {
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        let context =
            unsafe { ForeignImportSchemaContext::from_raw(statement, server_oid) };
        let commands = P::import_schema(&context)?;
        let mut list = ptr::null_mut();
        for command in commands {
            // SAFETY: pstrdup copies the NUL-terminated command into the
            // callback's current PostgreSQL memory context; lappend owns only
            // the list cell and PostgreSQL owns the copied string.
            let command = unsafe { pg_sys::pstrdup(command.as_ptr()) };
            list = unsafe { pg_sys::lappend(list, command.cast::<c_void>()) };
        }
        Ok::<*mut pg_sys::List, ForeignImportError>(list)
    })();

    match result {
        Ok(commands) => commands,
        Err(error) => error
            .with_callback::<P>()
            .report_after_switch(prior_context),
    }
}
