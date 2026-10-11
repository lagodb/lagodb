//! One-time cold-path resolution of a PostgreSQL foreign relation.

use lagodb_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;

use crate::access::{ConnectorAccess, LocalFilePolicy};
use crate::error::ConnectorError;
use crate::format::FormatKind;
use crate::storage::{ObjectLocationKind, ResolvedStorageLocation};

use super::format_adapters::{
    FdwFormat, FormatAnalyzer, FormatFdwReader, FormatWriter,
};
use super::{ResolvedTableOptions, resolve_table_options};

type ResolvedFormatParts<T> = (T, ResolvedStorageLocation);

/// Catalog identity, object location, and configured format resolved together
/// for one planner or executor callback.
pub(crate) struct ResolvedForeignRelation {
    server_oid: pg_sys::Oid,
    options: ResolvedTableOptions,
}

impl ResolvedForeignRelation {
    pub(crate) fn resolve(relation_oid: pg_sys::Oid) -> Result<Self, ConnectorError> {
        // SAFETY: PostgreSQL supplies a live foreign-table OID to each FDW
        // planner and executor callback that reaches this resolver.
        let table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        // SAFETY: the catalog-owned option list remains live throughout this
        // cold-path resolution call.
        let option_view = unsafe { ForeignOptionView::from_raw(table.options) };
        Ok(Self {
            server_oid: table.serverid,
            options: resolve_table_options(option_view)?,
        })
    }

    #[inline]
    pub(crate) const fn kind(&self) -> FormatKind {
        self.options.format.kind()
    }

    pub(crate) fn into_fdw_reader(self) -> Box<dyn FormatFdwReader> {
        FdwFormat::reader(self.options.format)
    }

    pub(crate) fn output_kind(&self) -> Result<ObjectLocationKind, ConnectorError> {
        self.kind().location_kind(&self.options.object)
    }

    pub(crate) fn validate_relation_columns(
        &self,
        relation_oid: pg_sys::Oid,
        natts: usize,
    ) -> Result<(), ConnectorError> {
        for attno in 1..=natts {
            // PostgreSQL bounds TupleDesc::natts by AttrNumber, so this
            // relation-level cold-path conversion cannot truncate.
            // SAFETY: the FDW callback supplies a live foreign relation and
            // every attno is within its TupleDesc.
            let options = unsafe {
                pg_sys::GetForeignColumnOptions(
                    relation_oid,
                    attno as pg_sys::AttrNumber,
                )
            };
            // SAFETY: PostgreSQL owns this option list for the duration of the
            // catalog lookup and validation call.
            self.options.format.validate_column_view(unsafe {
                ForeignOptionView::from_raw(options)
            })?;
        }
        Ok(())
    }

    pub(crate) fn into_scan_parts(
        self,
        effective_user: pg_sys::Oid,
    ) -> Result<ResolvedFormatParts<Box<dyn FormatFdwReader>>, ConnectorError> {
        let location = ConnectorAccess::resolve_foreign_object(
            self.options.object,
            self.server_oid,
            effective_user,
        )?;
        Ok((FdwFormat::reader(self.options.format), location))
    }

    pub(crate) fn into_analyze_parts(
        self,
        effective_user: pg_sys::Oid,
    ) -> Result<Option<ResolvedFormatParts<Box<dyn FormatAnalyzer>>>, ConnectorError>
    {
        let ResolvedTableOptions { object, format } = self.options;
        let Some(analyzer) = FdwFormat::reader(format).analyzer() else {
            return Ok(None);
        };
        let location = ConnectorAccess::resolve_foreign_object(
            object,
            self.server_oid,
            effective_user,
        )?;
        Ok(Some((analyzer, location)))
    }

    pub(crate) fn into_write_parts(
        self,
        effective_user: pg_sys::Oid,
    ) -> Result<ResolvedFormatParts<Box<dyn FormatWriter>>, ConnectorError> {
        let location = ConnectorAccess::resolve_foreign_object(
            self.options.object,
            self.server_oid,
            effective_user,
        )?;
        if location.local_path().is_some() {
            LocalFilePolicy::require_write(effective_user)?;
        }
        Ok((FdwFormat::writer(self.options.format), location))
    }
}
