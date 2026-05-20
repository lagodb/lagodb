use super::schema_mapping::tuple_desc_to_schema;
use crate::error::{IcebergError, IcebergResult};
use crate::options::IcebergTableOptionCache;
use crate::storage::create_storage_context;
use crate::storage::transactional_artifacts::register_table_dir_created;
use iceberg_lite::catalog::{Catalog, NamespaceIdent, TableCreation};
use iceberg_lite::spec::{SortOrder, UnboundPartitionSpec};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::options::AmCache;
use pgrx::pg_sys;

use super::iceberg_catalog::IcebergCatalog;

fn get_tablespace_version_directory() -> String {
    let major_version = pg_sys::PG_MAJORVERSION.to_string_lossy();
    format!("PG_{}_{}", major_version, pg_sys::CATALOG_VERSION_NO)
}

/// Generate the table location path based on relation's file locator.
///
/// This implementation strictly follows PostgreSQL's `GetRelationPath` logic:
/// - pg_default (DEFAULTTABLESPACE_OID): base/{dbOid}/{relNumber}
/// - pg_global (GLOBALTABLESPACE_OID): global/{relNumber}
/// - others: pg_tblspc/{spcOid}/{VERSION_DIR}/{dbOid}/{relNumber}
///
/// The resulting path is used as the base directory for the Iceberg table.
pub fn generate_table_location(
    rel: &RelationHandle,
    base_path: &str,
    is_distributed: bool,
) -> String {
    let rel_ptr = rel.as_raw();
    unsafe {
        let locator = &(*rel_ptr).rd_locator;
        let spc_oid = u32::from(locator.spcOid);
        let db_oid = u32::from(locator.dbOid);
        let rel_num = locator.relNumber;

        if is_distributed {
            let base = base_path.trim_end_matches('/');
            // For distributed storage, we keep a cleaner hierarchy but still
            // use the OID structure to avoid collisions.
            format!("{}/{}/{}/{}_iceberg", base, spc_oid, db_oid, rel_num)
        } else {
            // Local storage: Follow PostgreSQL's internal directory structure exactly.
            // Use pg_sys constants instead of hardcoded OID values.
            let default_tblspc = u32::from(pg_sys::DEFAULTTABLESPACE_OID);
            let global_tblspc = u32::from(pg_sys::GLOBALTABLESPACE_OID);

            // Use relative paths from DataDir for local storage
            if spc_oid == default_tblspc {
                format!("base/{}/{}_iceberg", db_oid, rel_num)
            } else if spc_oid == global_tblspc {
                format!("global/{}_iceberg", rel_num)
            } else {
                let version_dir = get_tablespace_version_directory();
                format!(
                    "pg_tblspc/{}/{}/{}/{}_iceberg",
                    spc_oid, version_dir, db_oid, rel_num
                )
            }
        }
    }
}

/// Initialize Iceberg table storage metadata.
///
/// Creates the Iceberg metadata files on storage and returns the metadata location.
///
/// # Returns
/// The metadata file location (e.g., "s3://bucket/path/metadata/v1.metadata.json")
pub fn init_table_storage_metadata(rel: &RelationHandle) -> IcebergResult<String> {
    let spc_oid = rel.tablespace_oid();
    let rel_namespace = rel.namespace_oid();

    // Create storage context based on tablespace type
    let ctx = create_storage_context(spc_oid)?;

    let location = generate_table_location(rel, &ctx.base_path, ctx.is_distributed);

    let nsp_name = rel_namespace.to_string();

    let schema = unsafe {
        let tup_desc = (*rel.as_raw()).rd_att;
        tuple_desc_to_schema(tup_desc)?
    };

    // Get table options from rd_amcache (loads from catalog if not cached)
    let table_option = AmCache::get::<IcebergTableOptionCache>(rel)?;
    let properties = table_option.to_properties();
    let format_version = table_option.iceberg_format_version()?;

    let rel_num = unsafe { (*rel.as_raw()).rd_locator.relNumber };

    // Build TableCreation with all required fields
    let creation = TableCreation::builder()
        .name(rel_num.to_string())
        .location(location.clone())
        .schema(schema)
        .properties(properties)
        .partition_spec(UnboundPartitionSpec::default()) // TODO: parse partition spec
        .sort_order(SortOrder::unsorted_order()) // TODO: parse sort order
        .format_version(format_version)
        .build();

    // Create table in catalog and get the metadata location
    let catalog = IcebergCatalog::new_pg(ctx.file_io.clone());
    let namespace = NamespaceIdent::new(nsp_name);

    // Register cleanup BEFORE creating table metadata so that if
    // create_table fails mid-way (after creating the directory but
    // before finishing), the directory is still cleaned up on abort.
    // Deleting a non-existent directory is treated as OK by the cleanup handler.
    register_table_dir_created(location, ctx.file_io.clone());

    let table = catalog.create_table(&namespace, creation)?;

    let metadata_location = table
        .metadata_location()
        .ok_or(IcebergError::MetadataLocationNull)?;

    Ok(metadata_location.to_string())
}
