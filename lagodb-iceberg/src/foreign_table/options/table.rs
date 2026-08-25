use pg_lakebase_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::schema::{
    CATALOG_NAME, CATALOG_NAMESPACE, CATALOG_TABLE_NAME, MODE, OptionLayer,
    ParsedOptions, READ_ONLY, READ_WRITE,
};

pub(crate) type MaterializedForeignOptions = Vec<(&'static str, String)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForeignTableMode {
    ReadOnly,
    ReadWrite,
}

impl ForeignTableMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => READ_ONLY,
            Self::ReadWrite => READ_WRITE,
        }
    }

    pub(crate) const fn is_writable(self) -> bool {
        matches!(self, Self::ReadWrite)
    }

    pub(crate) fn parse(value: &str) -> Result<Self, IcebergFdwError> {
        match value {
            READ_ONLY => Ok(Self::ReadOnly),
            READ_WRITE => Ok(Self::ReadWrite),
            _ => Err(IcebergFdwError::invalid_option(
                MODE,
                "must be read_only or read_write",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignTableIdentity {
    catalog_name: String,
    namespace: String,
    table_name: String,
    mode: ForeignTableMode,
}

impl ForeignTableIdentity {
    pub(crate) fn with_mode(
        catalog_name: String,
        namespace: String,
        table_name: String,
        mode: ForeignTableMode,
    ) -> Self {
        Self {
            catalog_name,
            namespace,
            table_name,
            mode,
        }
    }

    pub(crate) fn resolve(
        relation_oid: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        let table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        let options = unsafe { ForeignOptionView::from_raw(table.options) };
        Self::from_view(options)
    }

    pub(crate) fn from_view(
        options: ForeignOptionView<'_>,
    ) -> Result<Self, IcebergFdwError> {
        let mut parsed = ParsedOptions::from_view(OptionLayer::Table, options, true)?;
        let mode = ForeignTableMode::parse(&parsed.take_required(MODE)?)?;
        Ok(Self {
            catalog_name: parsed.take_required(CATALOG_NAME)?,
            namespace: parsed.take_required(CATALOG_NAMESPACE)?,
            table_name: parsed.take_required(CATALOG_TABLE_NAME)?,
            mode,
        })
    }

    pub(crate) fn complete(
        options: ForeignOptionView<'_>,
        catalog_name: String,
        namespace: String,
        table_name: String,
    ) -> Result<(Self, MaterializedForeignOptions), IcebergFdwError> {
        let mut parsed =
            ParsedOptions::from_view(OptionLayer::Table, options, false)?;
        let defaults = [
            (CATALOG_NAME, catalog_name),
            (CATALOG_NAMESPACE, namespace),
            (CATALOG_TABLE_NAME, table_name),
            (MODE, READ_ONLY.to_owned()),
        ];
        let mut materialized = Vec::with_capacity(defaults.len());
        for (name, value) in defaults {
            if !parsed.values.contains_key(name) {
                parsed.values.insert(name.to_owned(), value.clone());
                materialized.push((name, value));
            }
        }
        parsed.validate_required(OptionLayer::Table)?;
        let mode = ForeignTableMode::parse(&parsed.take_required(MODE)?)?;
        Ok((
            Self {
                catalog_name: parsed.take_required(CATALOG_NAME)?,
                namespace: parsed.take_required(CATALOG_NAMESPACE)?,
                table_name: parsed.take_required(CATALOG_TABLE_NAME)?,
                mode,
            },
            materialized,
        ))
    }

    pub(crate) fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }

    pub(crate) const fn mode(&self) -> ForeignTableMode {
        self.mode
    }
}
