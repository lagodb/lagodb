//! PostgreSQL relation layout indexed by `attno - 1`.
//!
//! `RelationLayout` captures the UTF-8 column names, declared PostgreSQL types,
//! and nullability flags from an open relation's `TupleDesc` in a single pass,
//! without retaining any borrow of relcache memory or depending on Iceberg
//! metadata types.

use std::ffi::{CStr, c_char};
use std::slice;

use lagodb_core::datum::PgType;
use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};

/// One live (non-dropped) PostgreSQL relation column.
#[derive(Debug)]
pub(crate) struct LiveColumn {
    /// One-based PostgreSQL attribute number (`1..=natts`).
    attno: pg_sys::AttrNumber,
    /// UTF-8 validated PostgreSQL column name.
    name: Box<str>,
    /// Declared PostgreSQL OID and typmod.
    pg_type: PgType,
    /// PostgreSQL's declared `NOT NULL` contract.
    required: bool,
}

impl LiveColumn {
    const fn new(
        attno: pg_sys::AttrNumber,
        name: Box<str>,
        pg_type: PgType,
        required: bool,
    ) -> Self {
        Self {
            attno,
            name,
            pg_type,
            required,
        }
    }

    #[inline]
    pub(crate) const fn attno(&self) -> pg_sys::AttrNumber {
        self.attno
    }

    /// Zero-based position in a relation-shaped tuple slot (`attno - 1`).
    #[inline]
    pub(super) const fn slot_index(&self) -> usize {
        (self.attno - 1) as usize
    }

    #[inline]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub(crate) const fn pg_type(&self) -> PgType {
        self.pg_type
    }

    #[inline]
    pub(crate) const fn is_required(&self) -> bool {
        self.required
    }
}

/// Descriptor-derived relation layout shared by scans, predicates, and writes.
#[derive(Debug)]
pub(crate) struct RelationLayout {
    /// Mirrors `TupleDesc->attrs`: indexed by `attno - 1`, `None` when dropped.
    attributes: Box<[Option<LiveColumn>]>,
    live_count: usize,
}

impl RelationLayout {
    pub(crate) fn from_relation(rel: &RelationHandle) -> IcebergResult<Self> {
        let descriptor = rel.tuple_desc_handle();
        let raw_attrs = descriptor.attributes();
        let mut attributes = Vec::with_capacity(raw_attrs.len());
        let mut live_count = 0;
        for attribute in raw_attrs {
            if attribute.attisdropped {
                attributes.push(None);
                continue;
            }
            // SAFETY: PostgreSQL stores `attname` as a NUL-terminated `NameData`
            // within the open relation's `TupleDesc`, valid for `raw_attrs`.
            let raw_name = unsafe {
                CStr::from_ptr(attribute.attname.data.as_ptr().cast::<c_char>())
            };
            let name = raw_name.to_str().map_err(|_| {
                IcebergError::SchemaBuildError(
                    "PostgreSQL column names must be valid UTF-8 for Iceberg"
                        .to_owned(),
                )
            })?;
            debug_assert_eq!(
                (attribute.attnum - 1) as usize,
                attributes.len(),
                "PostgreSQL TupleDesc attribute position must match attnum - 1",
            );
            attributes.push(Some(LiveColumn::new(
                attribute.attnum,
                name.into(),
                PgType::new(attribute.atttypid, attribute.atttypmod),
                attribute.attnotnull,
            )));
            live_count += 1;
        }
        Ok(Self {
            attributes: attributes.into_boxed_slice(),
            live_count,
        })
    }

    #[inline]
    pub(crate) fn live_column(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> IcebergResult<&LiveColumn> {
        let offset = usize::try_from(attno)
            .ok()
            .and_then(|attno| attno.checked_sub(1))
            .filter(|&offset| offset < self.attributes.len())
            .ok_or(IcebergError::InvariantViolated(
                "RelationLayout: attribute number is outside the relation descriptor",
            ))?;
        self.attributes[offset]
            .as_ref()
            .ok_or_else(|| IcebergError::ColumnNotFound(format!("attno {attno}")))
    }

    #[inline]
    pub(crate) fn live_columns(&self) -> LiveColumns<'_> {
        LiveColumns {
            inner: self.attributes.iter(),
            remaining: self.live_count,
        }
    }

    #[inline]
    pub(super) const fn live_count(&self) -> usize {
        self.live_count
    }

    #[inline]
    pub(super) const fn slot_width(&self) -> usize {
        self.attributes.len()
    }
}

/// Exact-size iterator over the non-dropped columns of a [`RelationLayout`].
#[derive(Clone)]
pub(crate) struct LiveColumns<'a> {
    inner: slice::Iter<'a, Option<LiveColumn>>,
    remaining: usize,
}

impl<'a> Iterator for LiveColumns<'a> {
    type Item = &'a LiveColumn;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        for slot in self.inner.by_ref() {
            if let Some(column) = slot.as_ref() {
                self.remaining -= 1;
                return Some(column);
            }
        }
        unreachable!("live_count tracks non-dropped relation attributes")
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for LiveColumns<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.remaining
    }
}
