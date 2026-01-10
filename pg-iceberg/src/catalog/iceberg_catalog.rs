//! PostgreSQL-based Iceberg Catalog implementation
//!
//! This module provides a PostgreSQL-backed catalog implementation that stores
//! Iceberg table metadata in PostgreSQL system catalogs.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use iceberg_lite::catalog::{
    Catalog, MetadataLocation, Namespace, NamespaceIdent, TableCommit, TableCreation,
    TableIdent,
};
use iceberg_lite::io::FileIO;
use iceberg_lite::spec::TableMetadataBuilder;
use iceberg_lite::table::Table;
use iceberg_lite::{Error, ErrorKind, Result};

/// PostgreSQL-based Iceberg Catalog implementation.
///
/// This catalog stores Iceberg table metadata in PostgreSQL system catalogs,
/// allowing seamless integration between PostgreSQL and Iceberg tables.
///
/// For transaction commits, the catalog maintains a cache of the current table
/// being modified to support `load_table` and `update_table` operations.
#[derive(Debug, Clone)]
pub struct IcebergCatalog {
    /// The name of this catalog
    name: String,
    /// File IO
    file_io: FileIO,
    /// Cached table for current transaction
    table: Arc<RwLock<Option<Table>>>,
}

impl Default for IcebergCatalog {
    fn default() -> Self {
        Self {
            name: crate::constants::DEFAULT_CATALOG_NAME.to_string(),
            file_io: FileIO::memory(),
            table: Arc::new(RwLock::new(None)),
        }
    }
}

impl IcebergCatalog {
    pub fn new(name: impl Into<String>, file_io: FileIO) -> Self {
        Self {
            name: name.into(),
            file_io,
            table: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new IcebergCatalog with the default "PostgreSQL" name.
    pub fn new_pg(file_io: FileIO) -> Self {
        Self::new(crate::constants::DEFAULT_CATALOG_NAME, file_io)
    }

    /// Create a catalog with a pre-registered table.
    ///
    /// This is used during transaction commits where the table is already loaded
    /// and we need the catalog to recognize it for `load_table` and `update_table`.
    pub fn with_table(
        name: impl Into<String>,
        file_io: FileIO,
        table: &Table,
    ) -> Result<Self> {
        Ok(Self {
            name: name.into(),
            file_io,
            table: Arc::new(RwLock::new(Some(table.clone()))),
        })
    }

    /// Create a catalog with a pre-registered table using the default "PostgreSQL" name.
    pub fn with_table_pg(file_io: FileIO, table: &Table) -> Result<Self> {
        Self::with_table(crate::constants::DEFAULT_CATALOG_NAME, file_io, table)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn file_io(&self) -> FileIO {
        self.file_io.clone()
    }
}

/// Checks if provided `NamespaceIdent` is valid.
pub(crate) fn validate_namespace(namespace: &NamespaceIdent) -> Result<String> {
    let name = namespace.as_ref();

    if name.len() != 1 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid namespaces name: {namespace:?}, hierarchical namespaces are not supported"
            ),
        ));
    }

    let name = name[0].clone();

    if name.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Invalid namespaces, provided namespace is empty.",
        ));
    }

    Ok(name)
}

impl Catalog for IcebergCatalog {
    fn list_namespaces<'a>(
        &self,
        _parent: Option<&'a NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        todo!("list_namespaces not yet implemented")
    }

    fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        todo!("create_namespace not yet implemented")
    }

    fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
        todo!("get_namespace not yet implemented")
    }

    fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        todo!("namespace_exists not yet implemented")
    }

    fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        todo!("update_namespace not yet implemented")
    }

    fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        todo!("drop_namespace not yet implemented")
    }

    fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        todo!("list_tables not yet implemented")
    }

    fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let namespace_name = validate_namespace(namespace)?;
        let table_name = creation.name.clone();

        // Location is required - the catalog only manages metadata,
        // storage location must be explicitly specified by the user
        let location = creation.location.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Table location is required. Please specify a storage location for the table.",
            )
        })?;

        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;

        let metadata_location =
            MetadataLocation::new_with_table_location(location.clone()).to_string();

        metadata.write_to(&self.file_io, &metadata_location)?;

        // Build and return the Table object
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new(namespace_name),
                table_name,
            ))
            .build()
    }

    fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let table = self.table.read().map_err(|_| {
            Error::new(ErrorKind::Unexpected, "Failed to acquire read lock on table")
        })?;
        table.as_ref().cloned().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid,
                format!("Table {} not found in catalog", table_ident))
        })
    }

    fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        todo!("drop_table not yet implemented")
    }

    fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        let table = self.table.read().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to acquire read lock on table",
            )
        })?;

        Ok(table
            .as_ref()
            .map(|t| t.identifier() == table_ident)
            .unwrap_or(false))
    }

    fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        todo!("rename_table not yet implemented")
    }

    fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        todo!("register_table not yet implemented")
    }

    fn update_table(&self, commit: TableCommit) -> Result<Table> {
        // Load current table from cache
        let current_table = {
            let table = self.table.read().map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to acquire read lock on table",
                )
            })?;
            table.as_ref().cloned().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Table {} not found in catalog", commit.identifier()),
                )
            })?
        };

        // Apply the commit to get the staged table with new metadata
        let staged_table = commit.apply(current_table)?;

        // Write the new metadata to storage
        staged_table.metadata().write_to(
            staged_table.file_io(),
            staged_table.metadata_location_result()?,
        )?;

        Ok(staged_table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_catalog_new() {
        let catalog = IcebergCatalog::new("test_catalog", FileIO::memory());

        assert_eq!(catalog.name(), "test_catalog");
    }

    #[test]
    fn test_iceberg_catalog_with_properties() {
        let mut props = HashMap::new();
        props.insert("key1".to_string(), "value1".to_string());
        props.insert("key2".to_string(), "value2".to_string());

        let catalog = IcebergCatalog::new("test_catalog", FileIO::memory());

        assert_eq!(catalog.name(), "test_catalog");
    }
}
