// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This module contains memory catalog implementation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use itertools::Itertools;

use super::namespace_state::NamespaceState;
use crate::encryption::kms::{KeyManagementClient, KmsClientFactory};
use crate::io::FileIO;
use crate::spec::{TableMetadata, TableMetadataBuilder};
use crate::table::Table;
use crate::{
    Catalog, CatalogBuilder, Error, ErrorKind, MetadataLocation, Namespace,
    NamespaceIdent, Result, TableCommit, TableCreation, TableIdent,
};

/// Memory catalog warehouse location
pub const MEMORY_CATALOG_WAREHOUSE: &str = "warehouse";

/// namespace `location` property
const LOCATION: &str = "location";

/// Builder for [`MemoryCatalog`].
#[derive(Debug)]
pub struct MemoryCatalogBuilder {
    config: MemoryCatalogConfig,
    kms_client_factory: Option<Arc<dyn KmsClientFactory>>,
}

impl Default for MemoryCatalogBuilder {
    fn default() -> Self {
        Self {
            config: MemoryCatalogConfig {
                name: None,
                warehouse: "".to_string(),
                props: HashMap::new(),
            },
            kms_client_factory: None,
        }
    }
}

impl CatalogBuilder for MemoryCatalogBuilder {
    type C = MemoryCatalog;

    fn with_kms_client_factory(
        mut self,
        kms_client_factory: Arc<dyn KmsClientFactory>,
    ) -> Self {
        self.kms_client_factory = Some(kms_client_factory);
        self
    }

    fn load(
        mut self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> Result<Self::C> {
        self.config.name = Some(name.into());

        if props.contains_key(MEMORY_CATALOG_WAREHOUSE) {
            self.config.warehouse = props
                .get(MEMORY_CATALOG_WAREHOUSE)
                .cloned()
                .unwrap_or_default()
        }

        // Collect other remaining properties
        self.config.props = props
            .into_iter()
            .filter(|(k, _)| k != MEMORY_CATALOG_WAREHOUSE)
            .collect();

        if self.config.name.is_none() {
            Err(Error::new(
                ErrorKind::DataInvalid,
                "Catalog name is required",
            ))
        } else if self.config.warehouse.is_empty() {
            Err(Error::new(
                ErrorKind::DataInvalid,
                "Catalog warehouse is required",
            ))
        } else {
            let kms_client = match self.kms_client_factory {
                Some(factory) => Some(factory.create_kms_client(&self.config.props)?),
                None => None,
            };
            MemoryCatalog::new(self.config, kms_client)
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryCatalogConfig {
    name: Option<String>,
    warehouse: String,
    props: HashMap<String, String>,
}

/// Memory catalog implementation.
#[derive(Debug)]
pub struct MemoryCatalog {
    root_namespace_state: Mutex<NamespaceState>,
    file_io: FileIO,
    warehouse_location: String,
    kms_client: Option<Arc<dyn KeyManagementClient>>,
}

impl MemoryCatalog {
    /// Creates a memory catalog.
    fn new(
        config: MemoryCatalogConfig,
        kms_client: Option<Arc<dyn KeyManagementClient>>,
    ) -> Result<Self> {
        Ok(Self {
            root_namespace_state: Mutex::new(NamespaceState::default()),
            file_io: FileIO::from_path_with_props(&config.warehouse, config.props)?,
            warehouse_location: config.warehouse,
            kms_client,
        })
    }

    /// Loads a table from the locked namespace state.
    fn load_table_from_locked_state(
        &self,
        table_ident: &TableIdent,
        root_namespace_state: &MutexGuard<'_, NamespaceState>,
    ) -> Result<Table> {
        let metadata_location =
            root_namespace_state.get_existing_table_location(table_ident)?;
        let metadata = TableMetadata::read_from(&self.file_io, metadata_location)?;

        let mut builder = Table::builder()
            .identifier(table_ident.clone())
            .metadata(metadata)
            .metadata_location(metadata_location.to_string())
            .file_io(self.file_io.clone());
        if let Some(kms_client) = self.kms_client.clone() {
            builder = builder.kms_client(kms_client);
        }
        builder.build()
    }
}

impl Catalog for MemoryCatalog {
    /// List namespaces inside the catalog.
    fn list_namespaces(
        &self,
        maybe_parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().unwrap();

        match maybe_parent {
            None => {
                let namespaces = root_namespace_state
                    .list_top_level_namespaces()
                    .into_iter()
                    .map(|str| NamespaceIdent::new(str.to_string()))
                    .collect_vec();

                Ok(namespaces)
            }
            Some(parent_namespace_ident) => {
                let namespaces = root_namespace_state
                    .list_namespaces_under(parent_namespace_ident)?
                    .into_iter()
                    .map(|name| {
                        let mut names = parent_namespace_ident
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>();
                        names.push(name.to_string());
                        NamespaceIdent::from_vec(names)
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(namespaces)
            }
        }
    }

    /// Create a new namespace inside the catalog.
    fn create_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let mut root_namespace_state = self.root_namespace_state.lock().unwrap();

        root_namespace_state
            .insert_new_namespace(namespace_ident, properties.clone())?;
        let namespace =
            Namespace::with_properties(namespace_ident.clone(), properties);

        Ok(namespace)
    }

    /// Get a namespace information from the catalog.
    fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<Namespace> {
        let root_namespace_state = self.root_namespace_state.lock().unwrap();

        let namespace = Namespace::with_properties(
            namespace_ident.clone(),
            root_namespace_state
                .get_properties(namespace_ident)?
                .clone(),
        );

        Ok(namespace)
    }

    /// Check if namespace exists in catalog.
    fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> Result<bool> {
        let guarded_namespaces = self.root_namespace_state.lock().unwrap();

        Ok(guarded_namespaces.namespace_exists(namespace_ident))
    }

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    fn update_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().unwrap();

        root_namespace_state.replace_properties(namespace_ident, properties)
    }

    /// Drop a namespace from the catalog.
    fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().unwrap();

        root_namespace_state.remove_existing_namespace(namespace_ident)
    }

    /// List tables from namespace.
    fn list_tables(
        &self,
        namespace_ident: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().unwrap();

        let table_names = root_namespace_state.list_tables(namespace_ident)?;
        let table_idents = table_names
            .into_iter()
            .map(|table_name| {
                TableIdent::new(namespace_ident.clone(), table_name.clone())
            })
            .collect_vec();

        Ok(table_idents)
    }

    /// Create a new table inside the namespace.
    fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        table_creation: TableCreation,
    ) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().unwrap();

        let table_name = table_creation.name.clone();
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name);

        let table_creation = match table_creation.location.clone() {
            Some(_) => table_creation,
            None => {
                let namespace_properties =
                    root_namespace_state.get_properties(namespace_ident)?;
                let location_prefix = match namespace_properties.get(LOCATION) {
                    Some(namespace_location) => namespace_location.clone(),
                    None => format!(
                        "{}/{}",
                        self.warehouse_location,
                        namespace_ident.join("/")
                    ),
                };

                let location = format!("{}/{}", location_prefix, table_ident.name());

                TableCreation {
                    location: Some(location),
                    ..table_creation
                }
            }
        };

        let metadata = TableMetadataBuilder::from_table_creation(table_creation)?
            .build()?
            .metadata;
        let metadata_location =
            MetadataLocation::try_new_with_metadata(&metadata)?.to_string();

        metadata.write_to(&self.file_io, &metadata_location)?;

        root_namespace_state
            .insert_new_table(&table_ident, metadata_location.clone())?;

        let mut builder = Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident);
        if let Some(kms_client) = self.kms_client.clone() {
            builder = builder.kms_client(kms_client);
        }
        builder.build()
    }

    /// Load table from the catalog.
    fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let root_namespace_state = self.root_namespace_state.lock().unwrap();

        self.load_table_from_locked_state(table_ident, &root_namespace_state)
    }

    /// Drop a table from the catalog.
    fn drop_table(&self, table_ident: &TableIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().unwrap();

        root_namespace_state.remove_existing_table(table_ident)?;
        Ok(())
    }

    fn purge_table(&self, table_ident: &TableIdent) -> Result<()> {
        let table = self.load_table(table_ident)?;
        self.drop_table(table_ident)?;
        crate::catalog::utils::drop_table_data(&table)
    }

    /// Check if a table exists in the catalog.
    fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        let root_namespace_state = self.root_namespace_state.lock().unwrap();

        root_namespace_state.table_exists(table_ident)
    }

    /// Rename a table in the catalog.
    fn rename_table(
        &self,
        src_table_ident: &TableIdent,
        dst_table_ident: &TableIdent,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().unwrap();

        let mut new_root_namespace_state = root_namespace_state.clone();
        let metadata_location = new_root_namespace_state
            .get_existing_table_location(src_table_ident)?
            .clone();
        new_root_namespace_state.remove_existing_table(src_table_ident)?;
        new_root_namespace_state
            .insert_new_table(dst_table_ident, metadata_location)?;
        *root_namespace_state = new_root_namespace_state;

        Ok(())
    }

    fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().unwrap();
        root_namespace_state
            .insert_new_table(&table_ident.clone(), metadata_location.clone())?;

        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location)?;

        let mut builder = Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident.clone());
        if let Some(kms_client) = self.kms_client.clone() {
            builder = builder.kms_client(kms_client);
        }
        builder.build()
    }

    /// Update a table in the catalog.
    fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().unwrap();

        let current_table = self.load_table_from_locked_state(
            commit.identifier(),
            &root_namespace_state,
        )?;

        // Apply TableCommit to get staged table
        let staged_table = commit.apply(current_table)?;

        // Write table metadata to the new location
        staged_table.metadata().write_to(
            staged_table.file_io(),
            staged_table.metadata_location_result()?,
        )?;

        // Flip the pointer to reference the new metadata file.
        let updated_table = root_namespace_state.commit_table_update(staged_table)?;

        Ok(updated_table)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashSet;
    use std::hash::Hash;
    use std::iter::FromIterator;
    use std::vec;

    use regex::Regex;
    use tempfile::TempDir;

    use super::*;
    use crate::encryption::kms::MemoryKmsClientFactory;
    use crate::io::FileIO;
    use crate::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableProperties, Type,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    pub(crate) fn new_memory_catalog() -> impl Catalog {
        let warehouse_location = temp_path();
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_location,
                )]),
            )
            .unwrap()
    }

    fn create_namespace<C: Catalog>(catalog: &C, namespace_ident: &NamespaceIdent) {
        let _ = catalog
            .create_namespace(namespace_ident, HashMap::new())
            .unwrap();
    }

    fn create_namespaces<C: Catalog>(
        catalog: &C,
        namespace_idents: &Vec<&NamespaceIdent>,
    ) {
        for namespace_ident in namespace_idents {
            create_namespace(catalog, namespace_ident);
        }
    }

    fn to_set<T: Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
        HashSet::from_iter(vec)
    }

    fn simple_table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()
            .unwrap()
    }

    fn create_table<C: Catalog>(catalog: &C, table_ident: &TableIdent) -> Table {
        catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(simple_table_schema())
                    .build(),
            )
            .unwrap()
    }

    fn create_tables<C: Catalog>(catalog: &C, table_idents: Vec<&TableIdent>) {
        for table_ident in table_idents {
            create_table(catalog, table_ident);
        }
    }

    fn create_table_with_namespace<C: Catalog>(catalog: &C) -> Table {
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(catalog, &namespace_ident);
        let table_ident = TableIdent::new(namespace_ident, "test".to_string());
        create_table(catalog, &table_ident)
    }

    fn assert_table_eq(
        table: &Table,
        expected_table_ident: &TableIdent,
        expected_schema: &Schema,
    ) {
        assert_eq!(table.identifier(), expected_table_ident);

        let metadata = table.metadata();

        assert_eq!(metadata.current_schema().as_ref(), expected_schema);

        let expected_partition_spec =
            PartitionSpec::builder((*expected_schema).clone())
                .with_spec_id(0)
                .build()
                .unwrap();

        assert_eq!(
            metadata
                .partition_specs_iter()
                .map(|p| p.as_ref())
                .collect_vec(),
            vec![&expected_partition_spec]
        );

        let expected_sorted_order = SortOrder::builder()
            .with_order_id(0)
            .with_fields(vec![])
            .build(expected_schema)
            .unwrap();

        assert_eq!(
            metadata
                .sort_orders_iter()
                .map(|s| s.as_ref())
                .collect_vec(),
            vec![&expected_sorted_order]
        );

        assert_eq!(metadata.properties(), &HashMap::new());

        assert!(!table.readonly());
    }

    const UUID_REGEX_STR: &str =
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    fn assert_table_metadata_location_matches(table: &Table, regex_str: &str) {
        let actual = table.metadata_location().unwrap().to_string();
        let regex = Regex::new(regex_str).unwrap();
        assert!(
            regex.is_match(&actual),
            "Expected metadata location to match regex, but got location: {actual} and regex: {regex}"
        )
    }

    #[test]
    fn test_list_namespaces_returns_empty_vector() {
        let catalog = new_memory_catalog();

        assert_eq!(catalog.list_namespaces(None).unwrap(), vec![]);
    }

    #[test]
    fn test_list_namespaces_returns_single_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident);

        assert_eq!(
            catalog.list_namespaces(None).unwrap(),
            vec![namespace_ident]
        );
    }

    #[test]
    fn test_list_namespaces_returns_multiple_namespaces() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]);

        assert_eq!(
            to_set(catalog.list_namespaces(None).unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_2])
        );
    }

    #[test]
    fn test_list_namespaces_returns_only_top_level_namespaces() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("b".into());
        create_namespaces(
            &catalog,
            &vec![&namespace_ident_1, &namespace_ident_2, &namespace_ident_3],
        );

        assert_eq!(
            to_set(catalog.list_namespaces(None).unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_3])
        );
    }

    #[test]
    fn test_list_namespaces_returns_no_namespaces_under_parent() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]);

        assert_eq!(
            catalog.list_namespaces(Some(&namespace_ident_1)).unwrap(),
            vec![]
        );
    }

    #[test]
    fn test_list_namespaces_returns_namespace_under_parent() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("c".into());
        create_namespaces(
            &catalog,
            &vec![&namespace_ident_1, &namespace_ident_2, &namespace_ident_3],
        );

        assert_eq!(
            to_set(catalog.list_namespaces(None).unwrap()),
            to_set(vec![namespace_ident_1.clone(), namespace_ident_3])
        );

        assert_eq!(
            catalog.list_namespaces(Some(&namespace_ident_1)).unwrap(),
            vec![namespace_ident_2]
        );
    }

    #[test]
    fn test_list_namespaces_returns_multiple_namespaces_under_parent() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("a".to_string());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "a"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_4 = NamespaceIdent::from_strs(vec!["a", "c"]).unwrap();
        let namespace_ident_5 = NamespaceIdent::new("b".into());
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_1,
                &namespace_ident_2,
                &namespace_ident_3,
                &namespace_ident_4,
                &namespace_ident_5,
            ],
        );

        assert_eq!(
            to_set(catalog.list_namespaces(Some(&namespace_ident_1)).unwrap()),
            to_set(vec![
                namespace_ident_2,
                namespace_ident_3,
                namespace_ident_4
            ])
        );
    }

    #[test]
    fn test_namespace_exists_returns_false() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident);

        assert!(
            !catalog
                .namespace_exists(&NamespaceIdent::new("b".into()))
                .unwrap()
        );
    }

    #[test]
    fn test_namespace_exists_returns_true() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident);

        assert!(catalog.namespace_exists(&namespace_ident).unwrap());
    }

    #[test]
    fn test_create_namespace_with_empty_properties() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .unwrap(),
            Namespace::new(namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[test]
    fn test_create_namespace_with_properties() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, properties.clone())
                .unwrap(),
            Namespace::with_properties(namespace_ident.clone(), properties.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        );
    }

    #[test]
    fn test_create_namespace_throws_error_if_namespace_already_exists() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident);

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceAlreadyExists => Cannot create namespace {:?}. Namespace already exists.",
                namespace_ident
            )
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[test]
    fn test_create_nested_namespace() {
        let catalog = new_memory_catalog();
        let parent_namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &parent_namespace_ident);

        let child_namespace_ident =
            NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&child_namespace_ident, HashMap::new())
                .unwrap(),
            Namespace::new(child_namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&child_namespace_ident).unwrap(),
            Namespace::with_properties(child_namespace_ident, HashMap::new())
        );
    }

    #[test]
    fn test_create_deeply_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]);

        let namespace_ident_a_b_c =
            NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .unwrap(),
            Namespace::new(namespace_ident_a_b_c.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, HashMap::new())
        );
    }

    #[test]
    fn test_create_nested_namespace_throws_error_if_top_level_namespace_doesnt_exist()
    {
        let catalog = new_memory_catalog();

        let nested_namespace_ident =
            NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&nested_namespace_ident, HashMap::new())
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {:?}",
                NamespaceIdent::new("a".into())
            )
        );

        assert_eq!(catalog.list_namespaces(None).unwrap(), vec![]);
    }

    #[test]
    fn test_create_deeply_nested_namespace_throws_error_if_intermediate_namespace_doesnt_exist()
     {
        let catalog = new_memory_catalog();

        let namespace_ident_a = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident_a);

        let namespace_ident_a_b_c =
            NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {:?}",
                NamespaceIdent::from_strs(vec!["a", "b"]).unwrap()
            )
        );

        assert_eq!(
            catalog.list_namespaces(None).unwrap(),
            vec![namespace_ident_a.clone()]
        );

        assert_eq!(
            catalog.list_namespaces(Some(&namespace_ident_a)).unwrap(),
            vec![]
        );
    }

    #[test]
    fn test_get_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());
        let _ = catalog
            .create_namespace(&namespace_ident, properties.clone())
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident).unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        )
    }

    #[test]
    fn test_get_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]);

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b).unwrap(),
            Namespace::with_properties(namespace_ident_a_b, HashMap::new())
        );
    }

    #[test]
    fn test_get_deeply_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c =
            NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_a,
                &namespace_ident_a_b,
                &namespace_ident_a_b_c,
            ],
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, HashMap::new())
        );
    }

    #[test]
    fn test_get_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();
        create_namespace(&catalog, &NamespaceIdent::new("a".into()));

        let non_existent_namespace_ident = NamespaceIdent::new("b".into());
        assert_eq!(
            catalog
                .get_namespace(&non_existent_namespace_ident)
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"
            )
        )
    }

    #[test]
    fn test_update_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident);

        let mut new_properties: HashMap<String, String> = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident, new_properties.clone())
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident).unwrap(),
            Namespace::with_properties(namespace_ident, new_properties)
        )
    }

    #[test]
    fn test_update_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]);

        let mut new_properties = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident_a_b, new_properties.clone())
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b).unwrap(),
            Namespace::with_properties(namespace_ident_a_b, new_properties)
        );
    }

    #[test]
    fn test_update_deeply_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c =
            NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_a,
                &namespace_ident_a_b,
                &namespace_ident_a_b_c,
            ],
        );

        let mut new_properties = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident_a_b_c, new_properties.clone())
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, new_properties)
        );
    }

    #[test]
    fn test_update_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();
        create_namespace(&catalog, &NamespaceIdent::new("abc".into()));

        let non_existent_namespace_ident = NamespaceIdent::new("def".into());
        assert_eq!(
            catalog
                .update_namespace(&non_existent_namespace_ident, HashMap::new())
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"
            )
        )
    }

    #[test]
    fn test_drop_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident);

        catalog.drop_namespace(&namespace_ident).unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident).unwrap())
    }

    #[test]
    fn test_drop_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]);

        catalog.drop_namespace(&namespace_ident_a_b).unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident_a_b).unwrap());

        assert!(catalog.namespace_exists(&namespace_ident_a).unwrap());
    }

    #[test]
    fn test_drop_deeply_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c =
            NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_a,
                &namespace_ident_a_b,
                &namespace_ident_a_b_c,
            ],
        );

        catalog.drop_namespace(&namespace_ident_a_b_c).unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident_a_b_c).unwrap());

        assert!(catalog.namespace_exists(&namespace_ident_a_b).unwrap());

        assert!(catalog.namespace_exists(&namespace_ident_a).unwrap());
    }

    #[test]
    fn test_drop_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();

        let non_existent_namespace_ident = NamespaceIdent::new("abc".into());
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"
            )
        )
    }

    #[test]
    fn test_drop_namespace_throws_error_if_nested_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();
        create_namespace(&catalog, &NamespaceIdent::new("a".into()));

        let non_existent_namespace_ident =
            NamespaceIdent::from_vec(vec!["a".into(), "b".into()]).unwrap();
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"
            )
        )
    }

    #[test]
    fn test_dropping_a_namespace_also_drops_namespaces_nested_under_that_one() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]);

        catalog.drop_namespace(&namespace_ident_a).unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident_a).unwrap());

        assert!(!catalog.namespace_exists(&namespace_ident_a_b).unwrap());
    }

    #[test]
    fn test_create_table_with_location() {
        let tmp_dir = TempDir::new().unwrap();
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident);

        let table_name = "abc";
        let location = tmp_dir.path().to_str().unwrap().to_string();
        let table_creation = TableCreation::builder()
            .name(table_name.into())
            .location(location.clone())
            .schema(simple_table_schema())
            .build();

        let expected_table_ident =
            TableIdent::new(namespace_ident.clone(), table_name.into());

        assert_table_eq(
            &catalog
                .create_table(&namespace_ident, table_creation)
                .unwrap(),
            &expected_table_ident,
            &simple_table_schema(),
        );

        let table = catalog.load_table(&expected_table_ident).unwrap();

        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());

        assert!(
            table
                .metadata_location()
                .unwrap()
                .to_string()
                .starts_with(&location)
        )
    }

    #[test]
    fn test_create_table_falls_back_to_namespace_location_if_table_location_is_missing()
     {
        let warehouse_location = temp_path();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_location.clone(),
                )]),
            )
            .unwrap();

        let namespace_ident = NamespaceIdent::new("a".into());
        let mut namespace_properties = HashMap::new();
        let namespace_location = temp_path();
        namespace_properties
            .insert(LOCATION.to_string(), namespace_location.to_string());
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{namespace_location}/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$",
        );

        let table = catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(
            &table,
            &expected_table_metadata_location_regex,
        );

        let table = catalog.load_table(&expected_table_ident).unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(
            &table,
            &expected_table_metadata_location_regex,
        );
    }

    #[test]
    fn test_create_table_in_nested_namespace_falls_back_to_nested_namespace_location_if_table_location_is_missing()
     {
        let warehouse_location = temp_path();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_location.clone(),
                )]),
            )
            .unwrap();

        let namespace_ident = NamespaceIdent::new("a".into());
        let mut namespace_properties = HashMap::new();
        let namespace_location = temp_path();
        namespace_properties
            .insert(LOCATION.to_string(), namespace_location.to_string());
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .unwrap();

        let nested_namespace_ident =
            NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let mut nested_namespace_properties = HashMap::new();
        let nested_namespace_location = temp_path();
        nested_namespace_properties
            .insert(LOCATION.to_string(), nested_namespace_location.to_string());
        catalog
            .create_namespace(&nested_namespace_ident, nested_namespace_properties)
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(nested_namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{nested_namespace_location}/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$",
        );

        let table = catalog
            .create_table(
                &nested_namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(
            &table,
            &expected_table_metadata_location_regex,
        );

        let table = catalog.load_table(&expected_table_ident).unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(
            &table,
            &expected_table_metadata_location_regex,
        );
    }

    #[test]
    fn test_create_table_falls_back_to_warehouse_location_if_both_table_location_and_namespace_location_are_missing()
     {
        let warehouse_location = temp_path();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_location.clone(),
                )]),
            )
            .unwrap();

        let namespace_ident = NamespaceIdent::new("a".into());
        // note: no location specified in namespace_properties
        let namespace_properties = HashMap::new();
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{warehouse_location}/a/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$"
        );

        let table = catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(
            &table,
            &expected_table_metadata_location_regex,
        );

        let table = catalog.load_table(&expected_table_ident).unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(
            &table,
            &expected_table_metadata_location_regex,
        );
    }

    #[test]
    fn test_create_table_in_nested_namespace_falls_back_to_warehouse_location_if_both_table_location_and_namespace_location_are_missing()
     {
        let warehouse_location = temp_path();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_location.clone(),
                )]),
            )
            .unwrap();

        let namespace_ident = NamespaceIdent::new("a".into());
        catalog
            // note: no location specified in namespace_properties
            .create_namespace(&namespace_ident, HashMap::new())
            .unwrap();

        let nested_namespace_ident =
            NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        catalog
            // note: no location specified in namespace_properties
            .create_namespace(&nested_namespace_ident, HashMap::new())
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(nested_namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{warehouse_location}/a/b/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$"
        );

        let table = catalog
            .create_table(
                &nested_namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(
            &table,
            &expected_table_metadata_location_regex,
        );

        let table = catalog.load_table(&expected_table_ident).unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(
            &table,
            &expected_table_metadata_location_regex,
        );
    }

    #[test]
    fn test_create_table_throws_error_if_table_location_and_namespace_location_and_warehouse_location_are_missing()
     {
        let catalog =
            MemoryCatalogBuilder::default().load("memory", HashMap::from([]));

        assert!(catalog.is_err());
        assert_eq!(
            catalog.unwrap_err().to_string(),
            "DataInvalid => Catalog warehouse is required"
        );
    }

    #[test]
    fn test_create_table_throws_error_if_table_with_same_name_already_exists() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident);
        let table_name = "tbl1";
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        create_table(&catalog, &table_ident);

        let tmp_dir = TempDir::new().unwrap();
        let location = tmp_dir.path().to_str().unwrap().to_string();

        assert_eq!(
            catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(table_name.into())
                        .schema(simple_table_schema())
                        .location(location)
                        .build()
                )
                .unwrap_err()
                .to_string(),
            format!(
                "TableAlreadyExists => Cannot create table {:?}. Table already exists.",
                table_ident
            )
        );
    }

    #[test]
    fn test_list_tables_returns_empty_vector() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident);

        assert_eq!(catalog.list_tables(&namespace_ident).unwrap(), vec![]);
    }

    #[test]
    fn test_list_tables_returns_a_single_table() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);

        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident);

        assert_eq!(
            catalog.list_tables(&namespace_ident).unwrap(),
            vec![table_ident]
        );
    }

    #[test]
    fn test_list_tables_returns_multiple_tables() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);

        let table_ident_1 = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let table_ident_2 = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_tables(&catalog, vec![&table_ident_1, &table_ident_2]);

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident).unwrap()),
            to_set(vec![table_ident_1, table_ident_2])
        );
    }

    #[test]
    fn test_list_tables_returns_tables_from_correct_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_1 = NamespaceIdent::new("n1".into());
        let namespace_ident_2 = NamespaceIdent::new("n2".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]);

        let table_ident_1 = TableIdent::new(namespace_ident_1.clone(), "tbl1".into());
        let table_ident_2 = TableIdent::new(namespace_ident_1.clone(), "tbl2".into());
        let table_ident_3 = TableIdent::new(namespace_ident_2.clone(), "tbl1".into());
        create_tables(
            &catalog,
            vec![&table_ident_1, &table_ident_2, &table_ident_3],
        );

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident_1).unwrap()),
            to_set(vec![table_ident_1, table_ident_2])
        );

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident_2).unwrap()),
            to_set(vec![table_ident_3])
        );
    }

    #[test]
    fn test_list_tables_returns_table_under_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]);

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident);

        assert_eq!(
            catalog.list_tables(&namespace_ident_a_b).unwrap(),
            vec![table_ident]
        );
    }

    #[test]
    fn test_list_tables_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());

        assert_eq!(
            catalog
                .list_tables(&non_existent_namespace_ident)
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"
            ),
        );
    }

    #[test]
    fn test_drop_table() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let table = create_table(&catalog, &table_ident);
        let metadata_location = table.metadata_location().unwrap().to_string();

        catalog.drop_table(&table_ident).unwrap();

        assert!(!catalog.table_exists(&table_ident).unwrap());
        assert!(table.file_io().exists(&metadata_location).unwrap());
    }

    #[test]
    fn test_purge_table_drops_table_and_deletes_metadata() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);
        let table_ident = TableIdent::new(namespace_ident, "tbl1".into());
        let table = create_table(&catalog, &table_ident);
        let metadata_location = table.metadata_location().unwrap().to_string();

        assert!(table.file_io().exists(&metadata_location).unwrap());

        catalog.purge_table(&table_ident).unwrap();

        assert!(!catalog.table_exists(&table_ident).unwrap());
        assert!(!table.file_io().exists(&metadata_location).unwrap());
    }

    #[test]
    fn test_drop_table_drops_table_under_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]);

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident);

        catalog.drop_table(&table_ident).unwrap();

        assert_eq!(catalog.list_tables(&namespace_ident_a_b).unwrap(), vec![]);
    }

    #[test]
    fn test_drop_table_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());
        let non_existent_table_ident =
            TableIdent::new(non_existent_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .drop_table(&non_existent_table_ident)
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"
            ),
        );
    }

    #[test]
    fn test_drop_table_throws_error_if_table_doesnt_exist() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);

        let non_existent_table_ident =
            TableIdent::new(namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .drop_table(&non_existent_table_ident)
                .unwrap_err()
                .to_string(),
            format!("TableNotFound => No such table: {non_existent_table_ident:?}"),
        );
    }

    #[test]
    fn test_table_exists_returns_true() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident);

        assert!(catalog.table_exists(&table_ident).unwrap());
    }

    #[test]
    fn test_table_exists_returns_false() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);
        let non_existent_table_ident =
            TableIdent::new(namespace_ident.clone(), "tbl1".into());

        assert!(!catalog.table_exists(&non_existent_table_ident).unwrap());
    }

    #[test]
    fn test_table_exists_under_nested_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]);

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident);

        assert!(catalog.table_exists(&table_ident).unwrap());

        let non_existent_table_ident =
            TableIdent::new(namespace_ident_a_b.clone(), "tbl2".into());
        assert!(!catalog.table_exists(&non_existent_table_ident).unwrap());
    }

    #[test]
    fn test_table_exists_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());
        let non_existent_table_ident =
            TableIdent::new(non_existent_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .table_exists(&non_existent_table_ident)
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"
            ),
        );
    }

    #[test]
    fn test_rename_table_in_same_namespace() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident);

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .unwrap();

        assert_eq!(
            catalog.list_tables(&namespace_ident).unwrap(),
            vec![dst_table_ident],
        );
    }

    #[test]
    fn test_rename_table_across_namespaces() {
        let catalog = new_memory_catalog();
        let src_namespace_ident = NamespaceIdent::new("a".into());
        let dst_namespace_ident = NamespaceIdent::new("b".into());
        create_namespaces(
            &catalog,
            &vec![&src_namespace_ident, &dst_namespace_ident],
        );
        let src_table_ident =
            TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        let dst_table_ident =
            TableIdent::new(dst_namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident);

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .unwrap();

        assert_eq!(catalog.list_tables(&src_namespace_ident).unwrap(), vec![],);

        assert_eq!(
            catalog.list_tables(&dst_namespace_ident).unwrap(),
            vec![dst_table_ident],
        );
    }

    #[test]
    fn test_rename_table_src_table_is_same_as_dst_table() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl".into());
        create_table(&catalog, &table_ident);

        catalog.rename_table(&table_ident, &table_ident).unwrap();

        assert_eq!(
            catalog.list_tables(&namespace_ident).unwrap(),
            vec![table_ident],
        );
    }

    #[test]
    fn test_rename_table_across_nested_namespaces() {
        let catalog = new_memory_catalog();
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c =
            NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(
            &catalog,
            &vec![
                &namespace_ident_a,
                &namespace_ident_a_b,
                &namespace_ident_a_b_c,
            ],
        );
        let src_table_ident =
            TableIdent::new(namespace_ident_a_b_c.clone(), "tbl1".into());
        create_tables(&catalog, vec![&src_table_ident]);

        let dst_table_ident =
            TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .unwrap();

        assert!(!catalog.table_exists(&src_table_ident).unwrap());

        assert!(catalog.table_exists(&dst_table_ident).unwrap());
    }

    #[test]
    fn test_rename_table_throws_error_if_src_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();
        let non_existent_src_namespace_ident = NamespaceIdent::new("n1".into());
        let src_table_ident =
            TableIdent::new(non_existent_src_namespace_ident.clone(), "tbl1".into());
        let dst_namespace_ident = NamespaceIdent::new("n2".into());
        create_namespace(&catalog, &dst_namespace_ident);
        let dst_table_ident =
            TableIdent::new(dst_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {non_existent_src_namespace_ident:?}"
            ),
        );
    }

    #[test]
    fn test_rename_table_throws_error_if_dst_namespace_doesnt_exist() {
        let catalog = new_memory_catalog();
        let src_namespace_ident = NamespaceIdent::new("n1".into());
        let src_table_ident =
            TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        create_namespace(&catalog, &src_namespace_ident);
        create_table(&catalog, &src_table_ident);

        let non_existent_dst_namespace_ident = NamespaceIdent::new("n2".into());
        let dst_table_ident =
            TableIdent::new(non_existent_dst_namespace_ident.clone(), "tbl1".into());
        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {non_existent_dst_namespace_ident:?}"
            ),
        );
    }

    #[test]
    fn test_rename_table_throws_error_if_src_table_doesnt_exist() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .unwrap_err()
                .to_string(),
            format!("TableNotFound => No such table: {src_table_ident:?}"),
        );
    }

    #[test]
    fn test_rename_table_throws_error_if_dst_table_already_exists() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident);
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_tables(&catalog, vec![&src_table_ident, &dst_table_ident]);

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .unwrap_err()
                .to_string(),
            format!(
                "TableAlreadyExists => Cannot create table {:?}. Table already exists.",
                dst_table_ident
            ),
        );
    }

    #[test]
    fn test_register_table() {
        // Create a catalog and namespace
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(&catalog, &namespace_ident);
        // Create a table to get a valid metadata file
        let source_table_ident =
            TableIdent::new(namespace_ident.clone(), "source_table".into());
        create_table(&catalog, &source_table_ident);

        // Get the metadata location from the source table
        let source_table = catalog.load_table(&source_table_ident).unwrap();
        let metadata_location = source_table.metadata_location().unwrap().to_string();

        // Register a new table using the same metadata location
        let register_table_ident =
            TableIdent::new(namespace_ident.clone(), "register_table".into());
        let registered_table = catalog
            .register_table(&register_table_ident, metadata_location.clone())
            .unwrap();

        // Verify the registered table has the correct identifier
        assert_eq!(registered_table.identifier(), &register_table_ident);

        // Verify the registered table has the correct metadata location
        assert_eq!(
            registered_table.metadata_location().unwrap().to_string(),
            metadata_location
        );

        // Verify the table exists in the catalog
        assert!(catalog.table_exists(&register_table_ident).unwrap());

        // Verify we can load the registered table
        let loaded_table = catalog.load_table(&register_table_ident).unwrap();
        assert_eq!(loaded_table.identifier(), &register_table_ident);
        assert_eq!(
            loaded_table.metadata_location().unwrap().to_string(),
            metadata_location
        );
    }

    #[test]
    fn test_update_table() {
        let catalog = new_memory_catalog();
        let table = create_table_with_namespace(&catalog);

        // Assert the table doesn't contain the update yet
        assert!(!table.metadata().properties().contains_key("key"));

        // Update table metadata
        let tx = Transaction::new(&table);
        let updated_table = tx
            .update_table_properties()
            .set("key".to_string(), "value".to_string())
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .unwrap();

        assert_eq!(
            updated_table.metadata().properties().get("key").unwrap(),
            "value"
        );

        assert_eq!(table.identifier(), updated_table.identifier());
        assert_eq!(table.metadata().uuid(), updated_table.metadata().uuid());
        assert!(
            table.metadata().last_updated_ms()
                < updated_table.metadata().last_updated_ms()
        );
        assert_ne!(table.metadata_location(), updated_table.metadata_location());

        assert!(
            table.metadata().metadata_log().len()
                < updated_table.metadata().metadata_log().len()
        );
    }

    #[test]
    fn test_update_table_fails_if_table_doesnt_exist() {
        let catalog = new_memory_catalog();
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident);

        // This table is not known to the catalog.
        let table_ident = TableIdent::new(namespace_ident, "test".to_string());
        let table = build_table(table_ident);

        let tx = Transaction::new(&table);
        let err = tx
            .update_table_properties()
            .set("key".to_string(), "value".to_string())
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::TableNotFound);
    }

    #[test]
    fn catalog_kms_factory_client_reaches_table_encryption_manager() {
        let factory = MemoryKmsClientFactory::new();
        factory.add_master_key("master-1").unwrap();

        let catalog = MemoryCatalogBuilder::default()
            .with_kms_client_factory(Arc::new(factory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), temp_path())]),
            )
            .unwrap();

        let namespace_ident = NamespaceIdent::new("enc_ns".into());
        create_namespace(&catalog, &namespace_ident);

        let table_ident = TableIdent::new(namespace_ident.clone(), "enc".into());
        let table = catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_ident.name().to_string())
                    .schema(simple_table_schema())
                    .format_version(FormatVersion::V3)
                    .properties(HashMap::from([(
                        TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
                        "master-1".to_string(),
                    )]))
                    .build(),
            )
            .unwrap();
        assert!(table.encryption_manager().is_some());

        let registered_ident =
            TableIdent::new(namespace_ident, "registered_enc".into());
        let registered_table = catalog
            .register_table(
                &registered_ident,
                table.metadata_location().unwrap().to_string(),
            )
            .unwrap();
        assert!(registered_table.encryption_manager().is_some());

        let loaded_table = catalog.load_table(&registered_ident).unwrap();
        assert!(loaded_table.encryption_manager().is_some());
    }

    fn build_table(ident: TableIdent) -> Table {
        let file_io = FileIO::local();

        let temp_dir = TempDir::new().unwrap();
        let location = temp_dir.path().to_str().unwrap().to_string();

        let table_creation = TableCreation::builder()
            .name(ident.name().to_string())
            .schema(simple_table_schema())
            .location(location)
            .build();
        let metadata = TableMetadataBuilder::from_table_creation(table_creation)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        Table::builder()
            .identifier(ident)
            .metadata(metadata)
            .file_io(file_io)
            .build()
            .unwrap()
    }
}
