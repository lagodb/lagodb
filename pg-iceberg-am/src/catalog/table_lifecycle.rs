use super::bridge::{BootstrapWriter, IcebergTableId};
use super::schema_builder::tuple_desc_to_schema;
use crate::error::{IcebergError, IcebergResult};
use crate::options::IcebergTableOptionCache;
use crate::storage::StorageContext;
use crate::storage::transactional_artifacts::{
    register_table_dir_created, register_table_dir_dropped,
};
use iceberg_lite::catalog::TableCreation;
use iceberg_lite::spec::{SortOrder, UnboundPartitionSpec};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::options::AmCache;
use pgrx::pg_sys;
use std::sync::OnceLock;

/// Owns the lifecycle-time storage policy for a single Iceberg table relation:
/// path layout, `FileIO` resolution, and metadata bootstrap / teardown.
///
/// Every CREATE / DROP / future RELOCATE / TRUNCATE / REWRITE flow goes
/// through one instance, so the location string and the underlying
/// `FileIO` are computed in exactly one place. Concretely:
///
/// * `init()` writes the initial metadata file and registers abort cleanup
///   (CREATE TABLE).
/// * `register_drop_cleanup()` registers post-commit directory removal
///   (DROP TABLE).
///
/// Callers in the hook layer never see `FileIO` or the underlying
/// transactional artifact registry: this type owns those bindings so
/// `(location, FileIO)` is not a public hand-off shape.
///
/// The instance carries a freshly-resolved [`StorageContext`] alongside the
/// pre-computed location string, so callers do not pay for recomputation
/// when they need both pieces.
pub(crate) struct IcebergTableLifecycle<'a> {
    rel: &'a RelationHandle<'a>,
    ctx: StorageContext,
    location: String,
}

impl<'a> IcebergTableLifecycle<'a> {
    /// Resolve storage context and compute the table location for `rel`.
    ///
    /// The storage context honors `RelationNeedsWAL`, so this constructor
    /// is safe to use from both write paths (CREATE TABLE) and lifecycle
    /// paths (DROP TABLE).
    pub(crate) fn new(rel: &'a RelationHandle<'a>) -> IcebergResult<Self> {
        let ctx = StorageContext::for_tablespace_with_wal(
            rel.tablespace_oid(),
            rel.needs_wal(),
        )?;
        let location = compute_table_location(rel, &ctx.base_path, ctx.is_distributed);
        Ok(Self { rel, ctx, location })
    }

    /// Register the table directory for post-commit removal as part of
    /// DROP TABLE. Mirrors the abort-cleanup registration that [`Self::init`]
    /// performs internally for CREATE TABLE, so the hook layer never has to
    /// touch the storage artifact registry directly.
    pub(crate) fn register_drop_cleanup(self) {
        register_table_dir_dropped(self.location, self.ctx.file_io);
    }

    /// Bootstrap Iceberg metadata for this relation.
    ///
    /// Writes the initial Iceberg metadata file via [`BootstrapWriter`] and
    /// returns the metadata file location (e.g.
    /// `s3://bucket/path/metadata/v1.metadata.json`). The owning table
    /// directory is registered for abort cleanup *before* metadata creation,
    /// so a mid-write failure is still recoverable.
    pub(crate) fn init(self) -> IcebergResult<String> {
        let Self { rel, ctx, location } = self;

        let schema = tuple_desc_to_schema(rel)?;
        let table_option = AmCache::get::<IcebergTableOptionCache>(rel)?;
        let properties = table_option.to_properties();
        let format_version = table_option.iceberg_format_version()?;

        // The PostgreSQL relation OID is the authoritative identity; the
        // Iceberg `TableIdent` is synthesized from it via `IcebergTableId`.
        // The `name` field on `TableCreation` is required by upstream's
        // typed builder but is discarded by `from_table_creation`, so we
        // pass an explicit placeholder; `BootstrapWriter` overwrites it
        // from `id` before use. This keeps identity in exactly one place.
        let id = IcebergTableId::for_relation(rel.oid());
        let creation = TableCreation::builder()
            .name(String::new())
            .location(location.clone())
            .schema(schema)
            .properties(properties)
            .partition_spec(UnboundPartitionSpec::default()) // TODO: parse partition spec
            .sort_order(SortOrder::unsorted_order()) // TODO: parse sort order
            .format_version(format_version)
            .build();

        let writer = BootstrapWriter::new(ctx.file_io.clone());

        // Register cleanup BEFORE creating table metadata so that if the
        // write fails mid-way (after creating the directory but before
        // finishing), the directory is still cleaned up on abort. Deleting
        // a non-existent directory is treated as OK by the cleanup handler.
        register_table_dir_created(location, ctx.file_io);

        let table = writer.write_initial_metadata(id, creation)?;
        let metadata_location = table
            .metadata_location()
            .ok_or(IcebergError::MetadataLocationNull)?;

        Ok(metadata_location.to_string())
    }
}

/// Compute the on-disk / object-store path for a relation's table directory.
///
/// Strictly mirrors PostgreSQL's `GetRelationPath` for local storage:
/// - pg_default (`DEFAULTTABLESPACE_OID`): `base/{dbOid}/{relNumber}_iceberg`
/// - pg_global (`GLOBALTABLESPACE_OID`): `global/{relNumber}_iceberg`
/// - other tablespaces: `pg_tblspc/{spcOid}/{VERSION_DIR}/{dbOid}/{relNumber}_iceberg`
///
/// For distributed storage we keep a flatter `{base}/{spcOid}/{dbOid}/{rel}_iceberg`
/// hierarchy that is collision-free across databases and tablespaces.
fn compute_table_location(
    rel: &RelationHandle<'_>,
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
            return format!("{}/{}/{}/{}_iceberg", base, spc_oid, db_oid, rel_num);
        }

        // Local storage: relative paths from DataDir, mirroring PG layout.
        //
        // TODO(storage-layout): include a table UUID/storage id in this
        // local directory name. This path is derived from relfilenumber:
        // PostgreSQL's native relation files are protected against
        // relfilenumber reuse by smgr/md unlink-after-checkpoint rules, but
        // this `_iceberg` directory is extension-owned and is not covered by
        // those rules. Changing it requires a coordinated catalog/storage
        // layout design, so keep the debt visible here instead of hiding it
        // in cleanup code.
        let default_tblspc = u32::from(pg_sys::DEFAULTTABLESPACE_OID);
        let global_tblspc = u32::from(pg_sys::GLOBALTABLESPACE_OID);

        if spc_oid == default_tblspc {
            format!("base/{}/{}_iceberg", db_oid, rel_num)
        } else if spc_oid == global_tblspc {
            format!("global/{}_iceberg", rel_num)
        } else {
            format!(
                "pg_tblspc/{}/{}/{}/{}_iceberg",
                spc_oid,
                tablespace_version_directory(),
                db_oid,
                rel_num
            )
        }
    }
}

/// `PG_<major>_<catalog_version>` directory name used inside `pg_tblspc/`.
///
/// Both inputs are effectively constants for a running server, so the result
/// is computed once and cached. This is what PostgreSQL itself does in
/// `GetTablespaceVersionDirectory` and avoids one allocation per CREATE TABLE.
fn tablespace_version_directory() -> &'static str {
    static CACHED: OnceLock<String> = OnceLock::new();
    CACHED.get_or_init(|| {
        let major = pg_sys::PG_MAJORVERSION.to_string_lossy();
        format!("PG_{}_{}", major, pg_sys::CATALOG_VERSION_NO)
    })
}
