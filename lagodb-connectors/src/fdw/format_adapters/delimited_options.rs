//! Relation-dependent COPY options assembled once at FDW scan startup.

use lagodb_core::fdw::ColumnRequirements;
use lagodb_core::handles::RelationHandle;
use lagodb_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{CsvColumnOptions, CsvOptions, DelimitedOptions};

/// Adds FDW projection and catalog options to a format's COPY option list.
/// PostgreSQL owns the list in the current scan memory context.
pub(super) struct DelimitedScanOptions {
    options: *mut pg_sys::List,
}

impl DelimitedScanOptions {
    pub(super) fn new(options: *mut pg_sys::List) -> Self {
        Self { options }
    }

    pub(super) fn select_columns(
        &mut self,
        relation: &RelationHandle<'_>,
        requirements: &ColumnRequirements,
    ) -> Result<(), ConnectorError> {
        if !requirements.needs_all_columns() {
            let columns = relation.live_columns();
            let mut columns_by_attno = vec![None; relation.natts()];
            for column in columns.iter() {
                columns_by_attno[(column.attno() - 1) as usize] = Some(column);
            }
            let selected = requirements
                .user_columns()
                .filter_map(|attno| columns_by_attno[(attno - 1) as usize])
                .map(|column| column.name());
            // An empty selection must still emit the option: omitting it
            // would request conversion of all columns.
            self.options = DelimitedOptions::append_identifier_list_option(
                self.options,
                "convert_selectively",
                selected,
            )?;
        }
        Ok(())
    }

    pub(super) fn append_csv_options(
        &mut self,
        format: &CsvOptions,
        relation: &RelationHandle<'_>,
    ) -> Result<(), ConnectorError> {
        self.options = format.append_postgres_input_options(self.options)?;

        let mut force_null = Vec::new();
        let mut force_not_null = Vec::new();
        let columns = relation.live_columns();
        for column in columns.iter() {
            // SAFETY: PostgreSQL owns this foreign-column option list for the
            // live relation and attribute during this begin callback.
            let raw = unsafe {
                pg_sys::GetForeignColumnOptions(relation.oid(), column.attno())
            };
            // SAFETY: the catalog-owned list remains live during parsing.
            let column_options = unsafe { ForeignOptionView::from_raw(raw) };
            let (is_force_null, is_force_not_null) =
                CsvColumnOptions::parse_view(column_options)?.flags();
            if is_force_null {
                force_null.push(column.name());
            }
            if is_force_not_null {
                force_not_null.push(column.name());
            }
        }
        if !force_null.is_empty() {
            self.options = DelimitedOptions::append_identifier_list_option(
                self.options,
                "force_null",
                force_null,
            )?;
        }
        if !force_not_null.is_empty() {
            self.options = DelimitedOptions::append_identifier_list_option(
                self.options,
                "force_not_null",
                force_not_null,
            )?;
        }
        Ok(())
    }

    pub(super) fn into_raw(self) -> *mut pg_sys::List {
        self.options
    }
}
