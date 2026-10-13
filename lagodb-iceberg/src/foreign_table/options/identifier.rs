//! Table option paths and IMPORT namespaces at the SQL/catalog boundary.

use iceberg_lite::catalog::{NamespaceIdent, TableIdent};
use lagodb_core::diag::PgError;
use pgrx::{Array, IntoDatum, PgTryBuilder, direct_function_call, pg_sys};

use super::super::error::IcebergFdwError;
use crate::error::IcebergError;

pub(crate) struct ForeignIdentifier;

impl ForeignIdentifier {
    fn components(value: &str) -> Result<Vec<String>, IcebergFdwError> {
        PgTryBuilder::new(|| {
            // parse_ident returns non-null text[] with non-null components for
            // non-null input. Copy them before the PG call context is released.
            let parts = unsafe {
                direct_function_call::<Array<&str>>(
                    pg_sys::parse_ident,
                    &[value.into_datum(), true.into_datum()],
                )
            }
            .expect("parse_ident is strict and returns a non-null array");
            Ok(parts
                .iter()
                .map(|part| {
                    part.expect("parse_ident components are non-null")
                        .to_owned()
                })
                .collect())
        })
        .catch_others(|error| Err(IcebergError::from(PgError::from(error)).into()))
        .execute()
    }

    pub(crate) fn table(value: &str) -> Result<TableIdent, IcebergFdwError> {
        let mut parts = Self::components(value)?;
        if parts.len() < 2 {
            return Err(IcebergFdwError::invalid_option(
                "table",
                "must include a namespace and table name",
            ));
        }
        let name = parts
            .pop()
            .expect("qualified table path has a final component");
        Ok(TableIdent::new(NamespaceIdent::from_vec(parts)?, name))
    }

    /// PG has already parsed IMPORT's remote schema as a SQL identifier.
    /// Dots separate namespace levels; preserve each component verbatim.
    /// Component-internal dots and nested quoting are not supported by IMPORT.
    pub(crate) fn import_namespace(
        value: &str,
    ) -> Result<NamespaceIdent, IcebergFdwError> {
        Ok(NamespaceIdent::from_vec(
            value.split('.').map(str::to_owned).collect(),
        )?)
    }

    /// Always quote each component: this preserves case, dots and keywords
    /// without depending on TableIdent's display-only dot joining.
    pub(crate) fn table_path(identifier: &TableIdent) -> String {
        let mut path = String::new();
        for component in identifier
            .namespace()
            .as_ref()
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(identifier.name()))
        {
            if !path.is_empty() {
                path.push('.');
            }
            path.push('"');
            for character in component.chars() {
                if character == '"' {
                    path.push('"');
                }
                path.push(character);
            }
            path.push('"');
        }
        path
    }
}
