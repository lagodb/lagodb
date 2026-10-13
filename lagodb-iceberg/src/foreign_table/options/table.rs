use iceberg_lite::catalog::{NamespaceIdent, TableIdent};
use iceberg_lite::spec::{SchemaRef, TableMetadata};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::identifier::ForeignIdentifier;
use super::schema::{OptionLayer, ParsedOptions, SCHEMA_ID, TABLE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignTableIdentity {
    table: TableIdent,
    schema_id: Option<i32>,
}

impl ForeignTableIdentity {
    pub(crate) fn new(table: TableIdent, schema_id: Option<i32>) -> Self {
        Self { table, schema_id }
    }

    pub(crate) fn encode(&self, writer: &mut PlanDataWriter) {
        let namespace = self.table.namespace().as_ref();
        writer.append_count(namespace.len());
        for component in namespace {
            writer.append_str(component);
        }
        writer
            .append_str(self.table.name())
            .append_bool(self.schema_id.is_some());
        if let Some(id) = self.schema_id {
            writer.append_i32(id);
        }
    }

    pub(crate) fn decode(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self, IcebergFdwError> {
        let count = reader.read_count()?;
        let mut namespace = Vec::with_capacity(count);
        for _ in 0..count {
            namespace.push(reader.read_str()?);
        }
        let table =
            TableIdent::new(NamespaceIdent::from_vec(namespace)?, reader.read_str()?);
        let schema_id = if reader.read_bool()? {
            Some(reader.read_i32()?)
        } else {
            None
        };
        Ok(Self::new(table, schema_id))
    }

    pub(crate) fn resolve(
        relation_oid: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        let table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        Self::from_foreign_table(table)
    }

    pub(crate) fn from_foreign_table(
        table: &pg_sys::ForeignTable,
    ) -> Result<Self, IcebergFdwError> {
        Self::from_view(unsafe { ForeignOptionView::from_raw(table.options) })
    }

    pub(crate) fn from_view(
        options: ForeignOptionView<'_>,
    ) -> Result<Self, IcebergFdwError> {
        Self::from_options(ParsedOptions::from_view(
            OptionLayer::Table,
            options,
            true,
        )?)
    }

    pub(super) fn from_options(
        mut parsed: ParsedOptions,
    ) -> Result<Self, IcebergFdwError> {
        let table = ForeignIdentifier::table(&parsed.take_required(TABLE)?)?;
        let schema_id = parsed
            .values
            .get(SCHEMA_ID)
            .map(|value| {
                value.parse::<i32>().map_err(|_| {
                    IcebergFdwError::invalid_option(
                        SCHEMA_ID,
                        "must be a non-negative 32-bit integer",
                    )
                })
            })
            .transpose()?;
        Ok(Self::new(table, schema_id))
    }

    pub(crate) fn table(&self) -> &TableIdent {
        &self.table
    }
    pub(crate) const fn schema_id(&self) -> Option<i32> {
        self.schema_id
    }

    /// Resolve the schema describing local columns, not the physical scan schema.
    pub(crate) fn definition_schema<'a>(
        &self,
        metadata: &'a TableMetadata,
    ) -> Result<&'a SchemaRef, IcebergFdwError> {
        match self.schema_id {
            None => Ok(metadata.current_schema()),
            Some(id) => metadata
                .schema_by_id(id)
                .ok_or(IcebergFdwError::UnknownSchema { schema_id: id }),
        }
    }
}

pub(crate) struct ForeignImportOptions {
    pub(crate) strict: bool,
}

impl ForeignImportOptions {
    pub(crate) fn from_view(
        options: ForeignOptionView<'_>,
    ) -> Result<Self, IcebergFdwError> {
        let parsed = ParsedOptions::from_view(OptionLayer::Import, options, true)?;
        let strict = parsed
            .values
            .get("strict")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        Ok(Self { strict })
    }
}
