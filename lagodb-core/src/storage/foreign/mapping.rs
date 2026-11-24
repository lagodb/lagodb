//! Optional user-mapping lookup with PostgreSQL's per-user/PUBLIC precedence.

use std::ptr::null_mut;

use pgrx::pg_sys;

use crate::handles::search_syscache2;

use super::catalog::ForeignOptionView;

/// PostgreSQL-owned credentials selected for one effective user and server.
pub struct ForeignUserMapping {
    oid: pg_sys::Oid,
    user_oid: pg_sys::Oid,
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
            let Some(tuple) = search_syscache2(
                cache_id,
                pg_sys::Datum::from(u32::from(user_oid) as usize),
                pg_sys::Datum::from(u32::from(server_oid) as usize),
            ) else {
                continue;
            };
            // SAFETY: USERMAPPINGUSERSERVER pins a pg_user_mapping tuple until
            // `tuple` is dropped.
            let oid = unsafe { tuple.form::<pg_sys::FormData_pg_user_mapping>().oid };
            let options = tuple
                .get_attr(pg_sys::Anum_pg_user_mapping_umoptions as _)
                .map_or(null_mut(), |datum| {
                    // SAFETY: untransformRelOptions copies catalog options into
                    // the current PG memory context, independent of the pin.
                    unsafe { pg_sys::untransformRelOptions(datum) }
                });
            return Some(Self {
                oid,
                user_oid,
                options,
            });
        }
        None
    }

    pub const fn oid(&self) -> pg_sys::Oid {
        self.oid
    }

    pub const fn user_oid(&self) -> pg_sys::Oid {
        self.user_oid
    }

    pub fn options(&self) -> ForeignOptionView<'_> {
        ForeignOptionView::new(self.options)
    }
}
