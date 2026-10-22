//! PostgreSQL foreign-server and user-mapping invalidation.

use std::cell::{Cell, RefCell};
use std::os::raw::c_int;

use pgrx::pg_sys;

use crate::wrapper::CacheRegisterSyscacheCallback;

/// One foreign-server or user-mapping syscache invalidation.
#[derive(Clone, Copy, Debug)]
pub enum ForeignConfigInvalidation {
    Server { hashvalue: u32 },
    UserMapping { hashvalue: u32 },
}

/// Syscache keys that affect one server and effective-user configuration.
#[derive(Clone, Copy, Debug)]
pub struct ForeignConfigInvalidationKeys {
    server: u32,
    user_mapping: u32,
    public_mapping: Option<u32>,
}

impl ForeignConfigInvalidationKeys {
    /// Always captures the per-user lookup. PUBLIC also matters when resolution
    /// selected PUBLIC or found no mapping, because a new per-user mapping then
    /// changes PostgreSQL's selected configuration.
    pub fn new(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        selected_mapping_user: Option<pg_sys::Oid>,
    ) -> Self {
        Self {
            server: syscache_hash(
                pg_sys::SysCacheIdentifier::FOREIGNSERVEROID as i32,
                server_oid,
                pg_sys::InvalidOid,
            ),
            user_mapping: syscache_hash(
                pg_sys::SysCacheIdentifier::USERMAPPINGUSERSERVER as i32,
                effective_user,
                server_oid,
            ),
            public_mapping: (selected_mapping_user != Some(effective_user)).then(
                || {
                    syscache_hash(
                        pg_sys::SysCacheIdentifier::USERMAPPINGUSERSERVER as i32,
                        pg_sys::InvalidOid,
                        server_oid,
                    )
                },
            ),
        }
    }

    pub fn matches(self, invalidation: ForeignConfigInvalidation) -> bool {
        match invalidation {
            ForeignConfigInvalidation::Server { hashvalue } => {
                hashvalue == 0 || hashvalue == self.server
            }
            ForeignConfigInvalidation::UserMapping { hashvalue } => {
                hashvalue == 0
                    || hashvalue == self.user_mapping
                    || self.public_mapping == Some(hashvalue)
            }
        }
    }
}

thread_local! {
    static OBSERVERS: RefCell<Vec<fn(ForeignConfigInvalidation)>> =
        const { RefCell::new(Vec::new()) };
    static CALLBACKS_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

/// Registers one backend-local observer for foreign configuration changes.
pub fn register_foreign_config_invalidation_observer(
    observer: fn(ForeignConfigInvalidation),
) {
    initialize_callbacks();
    OBSERVERS.with_borrow_mut(|observers| observers.push(observer));
}

fn initialize_callbacks() {
    CALLBACKS_REGISTERED.with(|registered| {
        if registered.get() {
            return;
        }
        // SAFETY: the callback has PostgreSQL's syscache ABI and remains loaded
        // for the backend lifetime. It only dispatches backend-local state.
        unsafe {
            CacheRegisterSyscacheCallback(
                pg_sys::SysCacheIdentifier::FOREIGNSERVEROID as i32,
                Some(foreign_config_invalidation_callback),
                0.into(),
            );
            CacheRegisterSyscacheCallback(
                pg_sys::SysCacheIdentifier::USERMAPPINGUSERSERVER as i32,
                Some(foreign_config_invalidation_callback),
                0.into(),
            );
        }
        registered.set(true);
    });
}

unsafe extern "C" fn foreign_config_invalidation_callback(
    _arg: pg_sys::Datum,
    cache_id: c_int,
    hashvalue: u32,
) {
    let invalidation =
        if cache_id == pg_sys::SysCacheIdentifier::FOREIGNSERVEROID as i32 {
            ForeignConfigInvalidation::Server { hashvalue }
        } else {
            debug_assert_eq!(
                cache_id,
                pg_sys::SysCacheIdentifier::USERMAPPINGUSERSERVER as i32,
            );
            ForeignConfigInvalidation::UserMapping { hashvalue }
        };
    OBSERVERS.with_borrow(|observers| {
        for observer in observers {
            observer(invalidation);
        }
    });
}

fn syscache_hash(cache_id: i32, first: pg_sys::Oid, second: pg_sys::Oid) -> u32 {
    // SAFETY: the selected caches use either the first OID key alone or both
    // supplied OID keys; unused Datum arguments are ignored by PostgreSQL.
    unsafe {
        pg_sys::GetSysCacheHashValue(
            cache_id,
            pg_sys::Datum::from(u32::from(first) as usize),
            pg_sys::Datum::from(u32::from(second) as usize),
            pg_sys::Datum::from(0usize),
            pg_sys::Datum::from(0usize),
        )
    }
}
