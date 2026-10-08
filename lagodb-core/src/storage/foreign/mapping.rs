//! Optional user-mapping lookup with PostgreSQL's per-user/PUBLIC precedence.

use std::ptr::null_mut;

use pgrx::pg_sys;

use super::catalog::ForeignOptionView;

/// PostgreSQL-owned credentials selected for one effective user and server.
pub struct ForeignUserMapping {
    oid: pg_sys::Oid,
    options: *mut pg_sys::List,
}

impl ForeignUserMapping {
    /// Looks up the effective user's mapping, then PUBLIC, without reporting
    /// an error when neither exists. Callers decide whether credentials are
    /// required by their authentication mode.
    pub fn lookup(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Option<Self> {
        let cache_id = pg_sys::SysCacheIdentifier::USERMAPPINGUSERSERVER as i32;
        for user_oid in [effective_user, pg_sys::InvalidOid] {
            // SAFETY: USERMAPPINGUSERSERVER is keyed by (user, server). A
            // successful lookup owns one pin released below.
            let tuple = unsafe {
                pg_sys::SearchSysCache2(
                    cache_id,
                    pg_sys::Datum::from(u32::from(user_oid) as usize),
                    pg_sys::Datum::from(u32::from(server_oid) as usize),
                )
            };
            if tuple.is_null() {
                continue;
            }
            // SAFETY: the tuple is a pinned pg_user_mapping row.
            let oid = unsafe {
                (*(pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_user_mapping)).oid
            };
            let mut is_null = false;
            // SAFETY: this nullable catalog attribute is read while pinned.
            let datum = unsafe {
                pg_sys::SysCacheGetAttr(
                    cache_id,
                    tuple,
                    pg_sys::Anum_pg_user_mapping_umoptions as _,
                    &mut is_null,
                )
            };
            let options = if is_null {
                null_mut()
            } else {
                // SAFETY: untransformRelOptions copies catalog options into
                // the current PG memory context, independent of the pin.
                unsafe { pg_sys::untransformRelOptions(datum) }
            };
            // SAFETY: the successful lookup owns this pin.
            unsafe { pg_sys::ReleaseSysCache(tuple) };
            return Some(Self { oid, options });
        }
        None
    }

    pub const fn oid(&self) -> pg_sys::Oid {
        self.oid
    }

    pub fn options(&self) -> ForeignOptionView<'_> {
        ForeignOptionView::new(self.options)
    }
}
