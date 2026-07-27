//! Borrowed values supplied to execution-stage predicate translators.

use core::marker::PhantomData;

use pgrx::pg_sys;

use crate::expr::pg::PgConst;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgColumnRef<'a> {
    pub rel_oid: pg_sys::Oid,
    pub attno: pg_sys::AttrNumber,
    pub atttypid: pg_sys::Oid,
    pub attcollation: pg_sys::Oid,
    pub name: Option<&'a str>,
}

/// Datum whose validity is bounded by its PostgreSQL owner.
#[derive(Clone, Copy, Debug)]
pub struct PgDatumRef<'a> {
    raw: pg_sys::Datum,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> PgDatumRef<'a> {
    /// Construct a borrowed Datum view from a caller-provided owner.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the raw Datum remains valid for `'a`.
    pub(crate) unsafe fn from_raw(raw: pg_sys::Datum) -> Self {
        Self {
            raw,
            _lifetime: PhantomData,
        }
    }

    /// # Safety
    ///
    /// A pass-by-reference Datum must not be retained beyond `'a`.
    #[inline]
    pub unsafe fn as_raw(self) -> pg_sys::Datum {
        self.raw
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgLiteral<'a> {
    type_oid: pg_sys::Oid,
    collid: pg_sys::Oid,
    datum: PgDatumRef<'a>,
    is_null: bool,
}

impl<'a> PgLiteral<'a> {
    /// Convert an already-validated PostgreSQL `Const` view into the provider value view.
    #[inline]
    pub fn from_const(value: PgConst<'a>) -> Self {
        let (type_oid, collid, datum, is_null) = value.parts();
        Self {
            type_oid,
            collid,
            // SAFETY: the Datum is borrowed from the live PG Const view.
            datum: unsafe { PgDatumRef::from_raw(datum) },
            is_null,
        }
    }

    #[inline]
    pub fn type_oid(self) -> pg_sys::Oid {
        self.type_oid
    }

    #[inline]
    pub fn collid(self) -> pg_sys::Oid {
        self.collid
    }

    #[inline]
    pub fn is_null(self) -> bool {
        self.is_null
    }

    #[inline]
    pub fn datum(self) -> PgDatumRef<'a> {
        self.datum
    }
}
