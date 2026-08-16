//! One-time cold-path resolution of a PostgreSQL foreign relation.

use pg_lakebase_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{FormatAnalyzer, FormatKind, FormatReader, FormatWriter};
use crate::storage::ObjectLocationKind;
use crate::storage::ResolvedStorageLocation;

use super::{ResolvedTableOptions, resolve_table_options};

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

    pub(crate) fn into_reader(self) -> Box<dyn FormatReader> {
        self.options.format.into_reader()
    }

    pub(crate) fn into_writer(self) -> Box<dyn FormatWriter> {
        self.options.format.into_writer()
    }

    pub(crate) fn output_kind(&self) -> Result<ObjectLocationKind, ConnectorError> {
        ObjectLocationKind::classify(self.options.object.key(), self.kind())
    }

    pub(crate) fn validate_relation_columns(
        &self,
        relation_oid: pg_sys::Oid,
        natts: usize,
    ) -> Result<(), ConnectorError> {
        self.options
            .format
            .validate_relation_columns(relation_oid, natts)
    }

    pub(crate) fn into_scan_parts(
        self,
        effective_user: pg_sys::Oid,
    ) -> Result<(Box<dyn FormatReader>, ResolvedStorageLocation), ConnectorError>
    {
        let location = ResolvedStorageLocation::resolve_foreign_object(
            self.options.object,
            self.server_oid,
            effective_user,
        )?;
        Ok((self.options.format.into_reader(), location))
    }

    pub(crate) fn into_analyze_parts(
        self,
        effective_user: pg_sys::Oid,
    ) -> Result<
        Option<(Box<dyn FormatAnalyzer>, ResolvedStorageLocation)>,
        ConnectorError,
    > {
        let ResolvedTableOptions { object, format } = self.options;
        let Some(analyzer) = format.into_reader().analyzer() else {
            return Ok(None);
        };
        let location = ResolvedStorageLocation::resolve_foreign_object(
            object,
            self.server_oid,
            effective_user,
        )?;
        Ok(Some((analyzer, location)))
    }

    pub(crate) fn into_write_parts(
        self,
        effective_user: pg_sys::Oid,
    ) -> Result<(Box<dyn FormatWriter>, ResolvedStorageLocation), ConnectorError>
    {
        let location = ResolvedStorageLocation::resolve_foreign_object(
            self.options.object,
            self.server_oid,
            effective_user,
        )?;
        Ok((self.options.format.into_writer(), location))
    }
}
