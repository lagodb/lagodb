//! Suppression of explicit PostgreSQL cleanup during ResourceOwner teardown.
//!
//! A ResourceOwner callback runs after PostgreSQL has released the resources
//! the owner tracks, including temporary files. Rust values dropped from that
//! callback must leave those resources alone, because closing them again would
//! act on state PostgreSQL has already freed.

use std::cell::Cell;

thread_local! {
    static SUPPRESSED: Cell<bool> = const { Cell::new(false) };
}

/// Run `operation` with explicit PostgreSQL cleanup suppressed in `Drop`.
pub fn without_pg_cleanup<R>(operation: impl FnOnce() -> R) -> R {
    struct Restore(bool);

    impl Drop for Restore {
        fn drop(&mut self) {
            SUPPRESSED.set(self.0);
        }
    }

    SUPPRESSED.with(|suppressed| {
        let restore = Restore(suppressed.replace(true));
        let result = operation();
        drop(restore);
        result
    })
}

/// Whether the caller is inside [`without_pg_cleanup`].
#[inline]
pub fn pg_cleanup_suppressed() -> bool {
    SUPPRESSED.get()
}
