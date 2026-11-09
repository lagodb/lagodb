//! Global reloption registration and the parsed-options layout.

use std::any::TypeId;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ptr;

use pgrx::pg_sys;

use super::def::{IndexOptionDef, OptionKind};
use crate::index_am::IndexAccessMethod;

/// Byte layout of the struct `build_reloptions` fills: the varlena header,
/// then one 8-byte slot per option at its `OPTIONS` position (followed on
/// PostgreSQL 18 by its `isset_offset` flag), then the strings
/// `fillRelOptions` appends.
pub(super) struct OptionsLayout;

impl OptionsLayout {
    /// `MAXALIGN(sizeof(int32))` for the varlena length word.
    const HEADER: usize = 8;
    const VALUE: usize = 8;
    #[cfg(feature = "pg18")]
    const SLOT: usize = Self::VALUE + 8;
    #[cfg(not(feature = "pg18"))]
    const SLOT: usize = Self::VALUE;

    pub(super) const fn value_offset(position: usize) -> usize {
        Self::HEADER + position * Self::SLOT
    }

    #[cfg(feature = "pg18")]
    pub(super) const fn isset_offset(position: usize) -> usize {
        Self::value_offset(position) + Self::VALUE
    }

    pub(super) const fn struct_size(count: usize) -> usize {
        Self::HEADER + count * Self::SLOT
    }
}

#[derive(Clone, Copy)]
pub(super) struct RegisteredOptions {
    pub(super) kind: pg_sys::relopt_kind::Type,
    pub(super) parse_table: &'static [pg_sys::relopt_parse_elt],
}

thread_local! {
    static REGISTERED: RefCell<HashMap<TypeId, RegisteredOptions>> =
        RefCell::new(HashMap::new());
}

impl RegisteredOptions {
    pub(super) fn of<A: IndexAccessMethod>() -> Option<Self> {
        REGISTERED
            .with_borrow(|registered| registered.get(&TypeId::of::<A>()).copied())
    }
}

/// Register `A`'s reloptions with PostgreSQL. Call once from `_PG_init`.
///
/// Every access method registers, even without options: its `amoptions`
/// parses through its own relopt kind, which is how PostgreSQL rejects
/// unrecognized `WITH (...)` parameters.
pub fn register_index_am<A: IndexAccessMethod>() {
    // SAFETY: PostgreSQL allocates the kind bit and copies option names and
    // descriptions into TopMemoryContext.
    let kind = unsafe { pg_sys::add_reloption_kind() };
    let mut parse_table = Vec::with_capacity(A::OPTIONS.len());
    for (position, def) in A::OPTIONS.iter().enumerate() {
        def.add_reloption(kind);
        parse_table.push(def.parse_elt(position));
    }
    let registered = RegisteredOptions {
        kind,
        parse_table: parse_table.leak(),
    };
    let previous = REGISTERED
        .with_borrow_mut(|options| options.insert(TypeId::of::<A>(), registered));
    assert!(
        previous.is_none(),
        "register_index_am must be called once per access method"
    );
}

impl IndexOptionDef {
    fn add_reloption(&self, kind: pg_sys::relopt_kind::Type) {
        let kinds = kind as pg_sys::bits32;
        let name = self.name.as_ptr();
        let desc = self.desc.as_ptr();
        let lockmode = self.lock.lockmode();
        // SAFETY: names and descriptions are copied by PostgreSQL; the enum
        // member array and detail are referenced and therefore 'static.
        unsafe {
            match self.kind {
                OptionKind::Bool { default } => {
                    pg_sys::add_bool_reloption(kinds, name, desc, default, lockmode)
                }
                OptionKind::Int { default, min, max } => pg_sys::add_int_reloption(
                    kinds, name, desc, default, min, max, lockmode,
                ),
                OptionKind::Real { default, min, max } => pg_sys::add_real_reloption(
                    kinds, name, desc, default, min, max, lockmode,
                ),
                OptionKind::Enum {
                    members,
                    default,
                    detail,
                } => pg_sys::add_enum_reloption(
                    kinds,
                    name,
                    desc,
                    leak_enum_members(members),
                    default,
                    detail.as_ptr(),
                    lockmode,
                ),
                OptionKind::String { default } => pg_sys::add_string_reloption(
                    kinds,
                    name,
                    desc,
                    default.map_or(ptr::null(), |value| value.as_ptr()),
                    None,
                    lockmode,
                ),
            }
        }
    }

    fn parse_elt(&self, position: usize) -> pg_sys::relopt_parse_elt {
        pg_sys::relopt_parse_elt {
            optname: self.name.as_ptr(),
            opttype: self.kind.relopt_type(),
            offset: OptionsLayout::value_offset(position) as i32,
            #[cfg(feature = "pg18")]
            isset_offset: OptionsLayout::isset_offset(position) as i32,
        }
    }
}

/// `add_enum_reloption` keeps the NULL-terminated member array by
/// reference (`reloptions.c`), so it lives for the process.
fn leak_enum_members(
    members: &'static [super::IndexEnumMember],
) -> *mut pg_sys::relopt_enum_elt_def {
    let elements: Box<[pg_sys::relopt_enum_elt_def]> = members
        .iter()
        .map(|member| pg_sys::relopt_enum_elt_def {
            string_val: member.name.as_ptr(),
            symbol_val: member.value,
        })
        .chain([pg_sys::relopt_enum_elt_def {
            string_val: ptr::null(),
            symbol_val: 0,
        }])
        .collect();
    Box::leak(elements).as_mut_ptr()
}
