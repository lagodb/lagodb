//! PostgreSQL-backed Iceberg Catalog adapter.
//!
//! Long term, this should become a complete catalog implementation whose
//! authoritative table and namespace state lives in PostgreSQL system catalogs.
//! Today it is intentionally narrower: `iceberg-lite` is derived from
//! iceberg-rust, whose transaction flow is built around the Iceberg `Catalog`
//! trait, so pg-iceberg-am needs an adapter that can participate in
//! `create_table`, `load_table`, and `update_table` while the PostgreSQL catalog
//! layer remains the source of truth. Methods outside that integration surface
//! return `FeatureUnsupported` instead of panicking.

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

/// PostgreSQL-backed Iceberg Catalog adapter.
///
/// This type is not yet the complete PostgreSQL catalog implementation. It is
/// the bridge required by iceberg-lite's `Catalog`-centric transaction API while
/// pg-iceberg-am keeps authoritative metadata in PostgreSQL system catalogs.
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

    fn unsupported_catalog_method<T>(method: &'static str) -> Result<T> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "PostgreSQL Iceberg catalog method `{method}` is not implemented yet"
            ),
        ))
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
    fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        Self::unsupported_catalog_method("list_namespaces")
    }

    fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        Self::unsupported_catalog_method("create_namespace")
    }

    fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
        Self::unsupported_catalog_method("get_namespace")
    }

    fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        Self::unsupported_catalog_method("namespace_exists")
    }

    fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Self::unsupported_catalog_method("update_namespace")
    }

    fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        Self::unsupported_catalog_method("drop_namespace")
    }

    fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Self::unsupported_catalog_method("list_tables")
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
            Error::new(
                ErrorKind::Unexpected,
                "Failed to acquire read lock on table",
            )
        })?;
        table.as_ref().cloned().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Table {} not found in catalog", table_ident),
            )
        })
    }

    fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        Self::unsupported_catalog_method("drop_table")
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
        Self::unsupported_catalog_method("rename_table")
    }

    fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        Self::unsupported_catalog_method("register_table")
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

    #[test]
    fn unsupported_catalog_methods_return_feature_unsupported() {
        let catalog = IcebergCatalog::default();
        let namespace = NamespaceIdent::new("public".to_string());

        let error = catalog.list_tables(&namespace).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
    }
}
