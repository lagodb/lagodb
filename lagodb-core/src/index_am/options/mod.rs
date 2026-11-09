//! Index reloptions.

mod def;
mod registry;

use std::marker::PhantomData;
use std::ptr::NonNull;

use pgrx::{PgSqlErrorCode, pg_guard, pg_sys};

use super::{IndexAccessMethod, IndexAmResult};
use crate::diag::{PgReportError, ReportableError};
use crate::handles::RelationHandle;

pub use def::{
    IndexEnumMember, IndexOption, IndexOptionDef, IndexOptionLock, IndexOptionValue,
};
pub use registry::register_index_am;

use registry::{OptionsLayout, RegisteredOptions};

/// Options of one index of `A`.
///
/// Every read goes through the relation again, as `BTGetFillFactor` does: an
/// invalidation can free and re-parse `rd_options` in the middle of a
/// callback (`RelationReloadIndexInfo`). NULL `rd_options` (no `WITH`
/// clause) reads as the declared defaults.
pub struct IndexOptions<'a, A> {
    source: OptionsSource,
    _relation: PhantomData<&'a ()>,
    _am: PhantomData<fn() -> A>,
}

impl<A> Clone for IndexOptions<'_, A> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<A> Copy for IndexOptions<'_, A> {}

#[derive(Clone, Copy)]
enum OptionsSource {
    /// Not `&RelationData`: PostgreSQL rewrites the relcache entry in place.
    Relation(pg_sys::Relation),
    Parsed(Option<NonNull<u8>>),
}

impl OptionsSource {
    #[inline]
    fn current(self) -> Option<NonNull<u8>> {
        match self {
            // SAFETY: the relation stays open for the options' lifetime.
            Self::Relation(relation) => {
                NonNull::new(unsafe { (*relation).rd_options }.cast())
            }
            Self::Parsed(raw) => raw,
        }
    }
}

impl<'a, A: IndexAccessMethod> IndexOptions<'a, A> {
    /// Options of `index`, which must be an index of `A`: callers are
    /// callbacks instantiated for `A`, passing the index they received.
    pub(in crate::index_am) fn of_index(index: &RelationHandle<'a>) -> Self {
        Self::new(OptionsSource::Relation(index.as_raw()))
    }

    fn parsed(raw: *mut u8) -> Self {
        Self::new(OptionsSource::Parsed(NonNull::new(raw)))
    }

    fn new(source: OptionsSource) -> Self {
        Self {
            source,
            _relation: PhantomData,
            _am: PhantomData,
        }
    }

    pub fn get<V: IndexOptionValue>(&self, key: IndexOption<A, V>) -> V {
        let position = key.position();
        match self.source.current() {
            // SAFETY: the key was checked against `A::OPTIONS`, so the slot
            // belongs to an option of type V in the layout `build_reloptions`
            // filled for `A`.
            Some(raw) => unsafe {
                V::read(raw, OptionsLayout::value_offset(position))
            },
            None => V::default_of(&A::OPTIONS[position]),
        }
    }

    /// Whether `key` was given explicitly rather than defaulted.
    #[cfg(feature = "pg18")]
    pub fn is_set<V: IndexOptionValue>(&self, key: IndexOption<A, V>) -> bool {
        let position = key.position();
        self.source.current().is_some_and(|raw| {
            // SAFETY: PostgreSQL 18 writes this flag at `isset_offset` of
            // every option `A` declares, and the key names one of them.
            unsafe {
                raw.add(OptionsLayout::isset_offset(position))
                    .cast::<bool>()
                    .read()
            }
        })
    }
}

/// `amoptions`: parse through `A`'s registered relopt kind and parse table.
#[pg_guard]
pub(super) extern "C-unwind" fn amoptions<A: IndexAccessMethod>(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    let options = parse::<A>(reloptions, validate).report_unwrap();
    options.cast()
}

fn parse<A: IndexAccessMethod>(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> IndexAmResult<*mut u8> {
    let registered = RegisteredOptions::of::<A>().ok_or_else(|| {
        PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            "index access method options are not registered; call register_index_am from _PG_init",
        )
    })?;
    // SAFETY: the parse table and struct size describe the same layout, and
    // PostgreSQL allocates the result in the caller's memory context.
    let raw = unsafe {
        pg_sys::build_reloptions(
            reloptions,
            validate,
            registered.kind,
            OptionsLayout::struct_size(A::OPTIONS.len()),
            registered.parse_table.as_ptr(),
            registered.parse_table.len() as i32,
        )
    }
    .cast::<u8>();
    if validate {
        A::validate_options(&IndexOptions::parsed(raw))?;
    }
    Ok(raw)
}
