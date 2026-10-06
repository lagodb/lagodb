//! Provider-neutral typed COPY contracts.
//!
//! These APIs expose PostgreSQL Datum/null and slot views only. Arrow, file
//! formats, object URIs, and storage-provider routing remain outside core.

use core::marker::PhantomData;

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::tuple::TupleSlotRow;

use super::{CopyColumn, CopyColumnLayout, CopyError};

/// A format-specific row rejection eligible for PostgreSQL `ON_ERROR IGNORE`.
///
/// Allocation occurs only on the exceptional rejected-row path. Object I/O,
/// schema, and provider-state failures must use `Err(CopyError)` instead.
#[derive(Debug)]
pub struct CopyRowRejection {
    message: String,
    sql_error_code: PgSqlErrorCode,
    column_index: Option<usize>,
    location: Option<String>,
}

impl CopyRowRejection {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            sql_error_code: PgSqlErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION,
            column_index: None,
            location: None,
        }
    }

    pub fn with_sql_error_code(mut self, sql_error_code: PgSqlErrorCode) -> Self {
        self.sql_error_code = sql_error_code;
        self
    }

    pub fn with_column_index(mut self, column_index: usize) -> Self {
        self.column_index = Some(column_index);
        self
    }

    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn sql_error_code(&self) -> PgSqlErrorCode {
        self.sql_error_code
    }

    pub fn column_index(&self) -> Option<usize> {
        self.column_index
    }

    pub fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }
}

#[derive(Debug)]
pub enum CopyRowOutcome {
    Row,
    End,
    Rejected(CopyRowRejection),
}

/// One COPY input column backed directly by the executor's target arrays.
pub struct CopyInputColumn<'a> {
    column: &'a CopyColumn,
    value: *mut pg_sys::Datum,
    is_null: *mut bool,
    materialized_bytes: *mut usize,
    _borrow: PhantomData<&'a mut (pg_sys::Datum, bool)>,
}

impl<'a> CopyInputColumn<'a> {
    pub fn column(&self) -> &CopyColumn {
        self.column
    }

    /// Store one datum produced in PostgreSQL's active per-tuple memory
    /// context. A by-reference datum must not be retained by the source.
    pub fn set(self, value: Option<pg_sys::Datum>) {
        // SAFETY: the layout was bound once from this COPY state's relation
        // descriptor, and the iterator yields every unique COPY attno at most
        // once.
        unsafe {
            match value {
                Some(value) => {
                    *self.value = value;
                    *self.is_null = false;
                    *self.materialized_bytes = (*self.materialized_bytes)
                        .saturating_add(self.column.datum_size(value));
                }
                None => *self.is_null = true,
            }
        }
    }
}

pub struct CopyInputColumns<'a> {
    columns: core::slice::Iter<'a, CopyColumn>,
    values: *mut pg_sys::Datum,
    nulls: *mut bool,
    materialized_bytes: *mut usize,
    _borrow: PhantomData<&'a mut [pg_sys::Datum]>,
}

impl<'a> Iterator for CopyInputColumns<'a> {
    type Item = CopyInputColumn<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let column = self.columns.next()?;
        let index = column.relation_index();
        Some(CopyInputColumn {
            column,
            // SAFETY: the layout was bound from the descriptor that owns both
            // target arrays before the callback was installed.
            value: unsafe { self.values.add(index) },
            is_null: unsafe { self.nulls.add(index) },
            materialized_bytes: self.materialized_bytes,
            _borrow: PhantomData,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.columns.size_hint()
    }
}

impl ExactSizeIterator for CopyInputColumns<'_> {}

/// Callback-scoped writable view of COPY FROM's relation-shaped Datum arrays.
pub struct CopyInputRow<'a> {
    values: *mut pg_sys::Datum,
    nulls: *mut bool,
    layout: &'a CopyColumnLayout,
    materialized_bytes: *mut usize,
    _borrow: PhantomData<&'a mut [pg_sys::Datum]>,
}

impl<'a> CopyInputRow<'a> {
    /// # Safety
    ///
    /// `values` and `nulls` must be the writable arrays for the relation
    /// descriptor from which `layout` was built. They must remain live in the
    /// current COPY per-tuple context for `'a`. The callback is synchronous;
    /// neither the row nor any datum pointer may be retained.
    pub(crate) unsafe fn from_raw(
        values: *mut pg_sys::Datum,
        nulls: *mut bool,
        layout: &'a CopyColumnLayout,
        materialized_bytes: &'a mut usize,
    ) -> Self {
        Self {
            values,
            nulls,
            layout,
            materialized_bytes,
            _borrow: PhantomData,
        }
    }

    pub fn columns(self) -> CopyInputColumns<'a> {
        CopyInputColumns {
            columns: self.layout.columns().iter(),
            values: self.values,
            nulls: self.nulls,
            materialized_bytes: self.materialized_bytes,
            _borrow: PhantomData,
        }
    }
}

/// One materialized value selected by a typed COPY TO layout.
#[derive(Clone, Copy)]
pub struct CopyOutputDatum<'a> {
    column: &'a CopyColumn,
    datum: pg_sys::Datum,
    is_null: bool,
}

impl<'a> CopyOutputDatum<'a> {
    pub fn column(self) -> &'a CopyColumn {
        self.column
    }

    pub fn value(self) -> Option<pg_sys::Datum> {
        (!self.is_null).then_some(self.datum)
    }
}

pub struct CopyOutputDatums<'a> {
    values: &'a [pg_sys::Datum],
    nulls: &'a [bool],
    columns: core::slice::Iter<'a, CopyColumn>,
}

impl<'a> Iterator for CopyOutputDatums<'a> {
    type Item = CopyOutputDatum<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let column = self.columns.next()?;
        let index = column.relation_index();
        // SAFETY: the layout was bound to this slot descriptor before the
        // callback was installed; both arrays were deformed at row entry.
        let (datum, is_null) = unsafe {
            (
                *self.values.get_unchecked(index),
                *self.nulls.get_unchecked(index),
            )
        };
        Some(CopyOutputDatum {
            column,
            datum,
            is_null,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.columns.size_hint()
    }
}

impl ExactSizeIterator for CopyOutputDatums<'_> {}

/// Callback-scoped read-only view of one executor output slot.
pub struct CopyOutputRow<'a> {
    values: &'a [pg_sys::Datum],
    nulls: &'a [bool],
    layout: &'a CopyColumnLayout,
}

impl<'a> CopyOutputRow<'a> {
    /// # Safety
    ///
    /// `slot` must be non-null, initialized, and descriptor-compatible with
    /// `layout` for `'a`. The destination must consume or copy every datum
    /// before returning and must not retain pass-by-reference pointers.
    pub(crate) unsafe fn from_raw(
        slot: *mut pg_sys::TupleTableSlot,
        layout: &'a CopyColumnLayout,
    ) -> Self {
        // SAFETY: the callback supplies a live descriptor-compatible slot.
        let row = unsafe { TupleSlotRow::from_raw(slot) };
        let (values, nulls) = row.datums().raw_parts();
        Self {
            values,
            nulls,
            layout,
        }
    }

    pub fn datums(self) -> CopyOutputDatums<'a> {
        CopyOutputDatums {
            values: self.values,
            nulls: self.nulls,
            columns: self.layout.columns().iter(),
        }
    }
}

/// A source already bound to the column layout supplied by the typed driver.
/// The driver constructs it after PostgreSQL validates options and columns.
pub trait CopyDatumSource {
    fn next_row(
        &mut self,
        row: CopyInputRow<'_>,
    ) -> Result<CopyRowOutcome, CopyError>;

    fn bytes_consumed(&self) -> u64 {
        0
    }

    fn finish(&mut self) -> Result<(), CopyError> {
        Ok(())
    }

    fn abort(&mut self) {}
}

pub trait CopyTupleDestination {
    fn initialize(&mut self, layout: &CopyColumnLayout) -> Result<(), CopyError>;

    fn write_slot(&mut self, row: CopyOutputRow<'_>) -> Result<(), CopyError>;

    /// Physical encoded bytes accepted by the destination so far.
    fn bytes_produced(&self) -> u64;

    fn finish(&mut self) -> Result<(), CopyError>;

    fn abort(&mut self) {}
}
