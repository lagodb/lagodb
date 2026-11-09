//! Reloption declarations and the typed keys derived from them.

use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::ptr::NonNull;

use pgrx::pg_sys;

use crate::index_am::IndexAccessMethod;

/// Lock level `ALTER INDEX ... SET/RESET` takes for an option. An option
/// that sizes shared parallel-scan state needs [`Self::AccessExclusive`].
///
/// PostgreSQL looks the level up by option name before validation
/// (`AlterTableGetRelOptionsLockLevel`), in the table filled by
/// [`super::register_index_am`], so it applies only once the provider library
/// is loaded in that backend; otherwise the command takes
/// `ShareUpdateExclusiveLock`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOptionLock {
    ShareUpdateExclusive,
    AccessExclusive,
}

impl IndexOptionLock {
    pub(super) const fn lockmode(self) -> pg_sys::LOCKMODE {
        match self {
            Self::ShareUpdateExclusive => {
                pg_sys::ShareUpdateExclusiveLock as pg_sys::LOCKMODE
            }
            Self::AccessExclusive => pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
        }
    }
}

/// One accepted spelling of an enum option and the value stored for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEnumMember {
    pub name: &'static CStr,
    pub value: i32,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum OptionKind {
    Bool {
        default: bool,
    },
    Int {
        default: i32,
        min: i32,
        max: i32,
    },
    Real {
        default: f64,
        min: f64,
        max: f64,
    },
    Enum {
        members: &'static [IndexEnumMember],
        default: i32,
        detail: &'static CStr,
    },
    String {
        default: Option<&'static CStr>,
    },
}

impl OptionKind {
    pub(super) const fn relopt_type(&self) -> pg_sys::relopt_type::Type {
        match self {
            Self::Bool { .. } => pg_sys::relopt_type::RELOPT_TYPE_BOOL,
            Self::Int { .. } => pg_sys::relopt_type::RELOPT_TYPE_INT,
            Self::Real { .. } => pg_sys::relopt_type::RELOPT_TYPE_REAL,
            Self::Enum { .. } => pg_sys::relopt_type::RELOPT_TYPE_ENUM,
            Self::String { .. } => pg_sys::relopt_type::RELOPT_TYPE_STRING,
        }
    }
}

/// Declaration of one option, listed in
/// [`IndexAccessMethod::OPTIONS`](crate::index_am::IndexAccessMethod::OPTIONS).
#[derive(Debug, Clone, Copy)]
pub struct IndexOptionDef {
    pub(super) name: &'static CStr,
    pub(super) desc: &'static CStr,
    pub(super) lock: IndexOptionLock,
    pub(super) kind: OptionKind,
}

impl IndexOptionDef {
    const fn new(
        name: &'static CStr,
        desc: &'static CStr,
        lock: IndexOptionLock,
        kind: OptionKind,
    ) -> Self {
        Self {
            name,
            desc,
            lock,
            kind,
        }
    }

    pub const fn boolean(
        name: &'static CStr,
        desc: &'static CStr,
        default: bool,
        lock: IndexOptionLock,
    ) -> Self {
        Self::new(name, desc, lock, OptionKind::Bool { default })
    }

    pub const fn int(
        name: &'static CStr,
        desc: &'static CStr,
        default: i32,
        min: i32,
        max: i32,
        lock: IndexOptionLock,
    ) -> Self {
        Self::new(name, desc, lock, OptionKind::Int { default, min, max })
    }

    pub const fn real(
        name: &'static CStr,
        desc: &'static CStr,
        default: f64,
        min: f64,
        max: f64,
        lock: IndexOptionLock,
    ) -> Self {
        Self::new(name, desc, lock, OptionKind::Real { default, min, max })
    }

    /// An enum option. `detail` is the DETAIL PostgreSQL reports for an
    /// unknown spelling, and `default` is a member's `value`.
    pub const fn enumerated(
        name: &'static CStr,
        desc: &'static CStr,
        members: &'static [IndexEnumMember],
        default: i32,
        detail: &'static CStr,
        lock: IndexOptionLock,
    ) -> Self {
        let kind = OptionKind::Enum {
            members,
            default,
            detail,
        };
        Self::new(name, desc, lock, kind)
    }

    pub const fn string(
        name: &'static CStr,
        desc: &'static CStr,
        default: Option<&'static CStr>,
        lock: IndexOptionLock,
    ) -> Self {
        Self::new(name, desc, lock, OptionKind::String { default })
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for bool {}
    impl Sealed for i32 {}
    impl Sealed for f64 {}
    impl Sealed for Option<std::ffi::CString> {}
}

/// Value types of [`IndexOption`] keys.
pub trait IndexOptionValue: Sized + sealed::Sealed + 'static {
    #[doc(hidden)]
    fn default_of(def: &IndexOptionDef) -> Self;

    /// # Safety
    ///
    /// `slot` must be the value slot of an option of this type, inside
    /// parsed options at `base`.
    #[doc(hidden)]
    unsafe fn read(base: NonNull<u8>, slot: usize) -> Self;
}

impl IndexOptionValue for bool {
    fn default_of(def: &IndexOptionDef) -> bool {
        match def.kind {
            OptionKind::Bool { default } => default,
            _ => {
                unreachable!("IndexOption keys are checked against their declaration")
            }
        }
    }

    unsafe fn read(base: NonNull<u8>, slot: usize) -> bool {
        unsafe { base.add(slot).cast::<bool>().read() }
    }
}

/// Integer and enum options: an enum reads as its member's `value`.
impl IndexOptionValue for i32 {
    fn default_of(def: &IndexOptionDef) -> i32 {
        match def.kind {
            OptionKind::Int { default, .. } | OptionKind::Enum { default, .. } => {
                default
            }
            _ => {
                unreachable!("IndexOption keys are checked against their declaration")
            }
        }
    }

    unsafe fn read(base: NonNull<u8>, slot: usize) -> i32 {
        unsafe { base.add(slot).cast::<i32>().read() }
    }
}

impl IndexOptionValue for f64 {
    fn default_of(def: &IndexOptionDef) -> f64 {
        match def.kind {
            OptionKind::Real { default, .. } => default,
            _ => {
                unreachable!("IndexOption keys are checked against their declaration")
            }
        }
    }

    unsafe fn read(base: NonNull<u8>, slot: usize) -> f64 {
        unsafe { base.add(slot).cast::<f64>().read() }
    }
}

/// String options, copied out: the relcache may free and re-parse
/// `rd_options` while the caller still holds the value. `fillRelOptions`
/// stores the string's offset, or 0 when it is NULL.
impl IndexOptionValue for Option<CString> {
    fn default_of(def: &IndexOptionDef) -> Option<CString> {
        match def.kind {
            OptionKind::String { default } => default.map(CStr::to_owned),
            _ => {
                unreachable!("IndexOption keys are checked against their declaration")
            }
        }
    }

    unsafe fn read(base: NonNull<u8>, slot: usize) -> Option<CString> {
        unsafe {
            let offset = base.add(slot).cast::<i32>().read();
            (offset != 0).then(|| {
                CStr::from_ptr(base.add(offset as usize).as_ptr().cast()).to_owned()
            })
        }
    }
}

/// Typed key for one option of `A`. The constructors check that
/// `A::OPTIONS[position]` has the key's kind (a compile error in a `const`
/// item), and `A` in the type keeps keys of other access methods out, so
/// reads need no runtime check.
#[derive(Debug)]
pub struct IndexOption<A, V> {
    position: usize,
    _key: PhantomData<fn() -> (A, V)>,
}

impl<A, V> Clone for IndexOption<A, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<A, V> Copy for IndexOption<A, V> {}

impl<A, V> IndexOption<A, V> {
    const fn checked(position: usize, declared_as_key: bool) -> Self {
        assert!(
            declared_as_key,
            "the index option key's kind differs from its OPTIONS declaration"
        );
        Self {
            position,
            _key: PhantomData,
        }
    }

    pub(super) const fn position(self) -> usize {
        self.position
    }
}

impl<A: IndexAccessMethod> IndexOption<A, bool> {
    pub const fn bool_at(position: usize) -> Self {
        Self::checked(
            position,
            matches!(A::OPTIONS[position].kind, OptionKind::Bool { .. }),
        )
    }
}

impl<A: IndexAccessMethod> IndexOption<A, i32> {
    pub const fn int_at(position: usize) -> Self {
        Self::checked(
            position,
            matches!(A::OPTIONS[position].kind, OptionKind::Int { .. }),
        )
    }

    pub const fn enum_at(position: usize) -> Self {
        Self::checked(
            position,
            matches!(A::OPTIONS[position].kind, OptionKind::Enum { .. }),
        )
    }
}

impl<A: IndexAccessMethod> IndexOption<A, f64> {
    pub const fn real_at(position: usize) -> Self {
        Self::checked(
            position,
            matches!(A::OPTIONS[position].kind, OptionKind::Real { .. }),
        )
    }
}

impl<A: IndexAccessMethod> IndexOption<A, Option<CString>> {
    pub const fn string_at(position: usize) -> Self {
        Self::checked(
            position,
            matches!(A::OPTIONS[position].kind, OptionKind::String { .. }),
        )
    }
}
