//! Memory-context backed ownership for access-method session state.
//!
//! PostgreSQL may unwind past the normal `scan_end` / `index_fetch_end`
//! callbacks after an ERROR.  Session state that lives on the Rust heap must
//! therefore be tied to a PostgreSQL memory context cleanup callback, not only
//! to the normal end callback.

use std::ffi::CStr;

use pgrx::{pg_guard, pg_sys};

pub(super) unsafe fn create_child_context(
    parent: pg_sys::MemoryContext,
    name: &'static CStr,
) -> pg_sys::MemoryContext {
    unsafe {
        pg_sys::AllocSetContextCreateExtended(
            parent,
            name.as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        )
    }
}

pub(super) unsafe fn palloc0_in_context<T>(context: pg_sys::MemoryContext) -> *mut T {
    unsafe {
        pg_sys::MemoryContextAllocZero(context, std::mem::size_of::<T>()) as *mut T
    }
}

pub(super) unsafe fn leak_state_in_context<T>(
    context: pg_sys::MemoryContext,
    state: T,
) -> *mut T {
    unsafe {
        let callback = pg_sys::MemoryContextAllocZero(
            context,
            std::mem::size_of::<pg_sys::MemoryContextCallback>(),
        ) as *mut pg_sys::MemoryContextCallback;

        let state_ptr = Box::into_raw(Box::new(state));
        (*callback).func = Some(drop_state::<T>);
        (*callback).arg = state_ptr.cast();

        pg_sys::MemoryContextRegisterResetCallback(context, callback);
        state_ptr
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn drop_state<T>(arg: *mut std::ffi::c_void) {
    if arg.is_null() {
        return;
    }

    unsafe {
        drop(Box::from_raw(arg.cast::<T>()));
    }
}
