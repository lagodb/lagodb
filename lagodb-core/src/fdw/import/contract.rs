//! Provider contract and callback-scoped import context.

use core::marker::PhantomData;
use std::ffi::{CStr, CString};

use pgrx::pg_sys;

use super::super::provider::ForeignDataWrapper;
use super::error::ForeignImportError;
use crate::storage::foreign::ForeignOptionView;

/// Borrowed view of one PostgreSQL `IMPORT FOREIGN SCHEMA` statement.
pub struct ForeignImportSchemaContext<'a> {
    statement: *mut pg_sys::ImportForeignSchemaStmt,
    server_oid: pg_sys::Oid,
    _statement: PhantomData<&'a pg_sys::ImportForeignSchemaStmt>,
}

impl<'a> ForeignImportSchemaContext<'a> {
    /// # Safety
    ///
    /// `statement` must be the live statement supplied by PostgreSQL to
    /// `ImportForeignSchema` and remain valid for `'a`.
    pub(crate) unsafe fn from_raw(
        statement: *mut pg_sys::ImportForeignSchemaStmt,
        server_oid: pg_sys::Oid,
    ) -> Self {
        Self {
            statement,
            server_oid,
            _statement: PhantomData,
        }
    }

    #[inline]
    pub const fn server_oid(&self) -> pg_sys::Oid {
        self.server_oid
    }

    #[inline]
    pub fn server_name(&self) -> &'a CStr {
        unsafe { CStr::from_ptr((*self.statement).server_name) }
    }

    #[inline]
    pub fn remote_schema(&self) -> &'a CStr {
        unsafe { CStr::from_ptr((*self.statement).remote_schema) }
    }

    #[inline]
    pub fn local_schema(&self) -> &'a CStr {
        unsafe { CStr::from_ptr((*self.statement).local_schema) }
    }

    #[inline]
    pub fn options(&self) -> ForeignOptionView<'a> {
        unsafe { ForeignOptionView::from_raw((*self.statement).options) }
    }

    /// Apply PostgreSQL's `LIMIT TO` or `EXCEPT` selection to a remote table.
    #[inline]
    pub fn includes_table(&self, table_name: &CStr) -> bool {
        unsafe {
            pg_sys::IsImportableForeignTable(table_name.as_ptr(), self.statement)
        }
    }
}

/// Optional `IMPORT FOREIGN SCHEMA` capability of an FDW provider.
pub trait FdwImportSchema: ForeignDataWrapper + 'static {
    /// Return complete `CREATE FOREIGN TABLE` statements. PostgreSQL parses and
    /// executes them after this callback returns.
    fn import_schema(
        context: &ForeignImportSchemaContext<'_>,
    ) -> Result<Vec<CString>, ForeignImportError>;
}
