//! PostgreSQL ↔ Iceberg identity bridge and the two narrow adapters that
//! span the two boundaries our integration actually crosses.
//!
//! # Identity: PostgreSQL OID vs Iceberg `TableIdent`
//!
//! PostgreSQL's authoritative identity for a table is `pg_sys::Oid`. Iceberg
//! requires a `TableIdent` (`NamespaceIdent` + table name). These are two
//! disjoint naming systems and must not be conflated:
//!
//! * Stuffing PG namespace OIDs into `NamespaceIdent::new(...)` and PG
//!   relfilenumbers into the table name forges Iceberg identity out of
//!   unrelated PG fields. It also spreads the conversion across every call
//!   site, which silently encodes the assumption that "ident shape" is part
//!   of the public contract.
//! * The right shape is a single converter, [`IcebergTableId`], that owns the
//!   one-way `Oid -> TableIdent` mapping used purely as an opaque, stable
//!   handle inside iceberg-lite. The Iceberg side never observes PG OIDs in
//!   the identifier surface; it sees a synthetic `pg.<oid>` ident and treats
//!   it as opaque, exactly as it would a real catalog identifier.
//!
//! When a real Iceberg catalog backend lands, [`IcebergTableId`] is the
//! single point that needs to change.
//!
//! # Why two adapters and why only one of them implements `Catalog`
//!
//! `iceberg-lite::transaction::Transaction::commit` accepts `&dyn Catalog`,
//! so any code that drives a `Transaction` through commit must hand it a
//! type that implements `Catalog`. Inside this crate that path is exactly
//! one: mutation rebases pending appends and commits via [`StagedCatalog`].
//!
//! The other path — `CREATE TABLE` writing the very first metadata file —
//! does **not** go through `Transaction::commit`. It just needs to stamp out
//! a metadata file from a `TableCreation` and return a `Table`. Earlier this
//! was done through a `BootstrapCatalog: Catalog` adapter that left 13 of 14
//! trait methods as `FeatureUnsupported` stubs. That is implement-and-throw:
//! the type advertised capabilities it could not deliver and the type system
//! could not prevent a wrong-role caller from invoking the unsupported
//! methods. So bootstrap is now a plain object, [`BootstrapWriter`], with a
//! signature shaped for what it actually does.
//!
//! [`StagedCatalog`] still implements `Catalog` because `Transaction::commit`
//! requires it. It services exactly the two methods commit reaches for —
//! `load_table` and `update_table` — and stubs the rest via
//! [`unsupported_catalog_method!`]. Those stubs are unavoidable as long as
//! we consume the upstream `Catalog` trait surface; touching that surface
//! would create sustained merge tax on every rebase against `iceberg-rust`,
//! so we keep the stubs local and visible.

use std::collections::HashMap;
use std::fmt::Debug;

use iceberg_lite::catalog::{
    Catalog, MetadataLocation, Namespace, NamespaceIdent, TableCommit, TableCreation,
    TableIdent,
};
use iceberg_lite::io::FileIO;
use iceberg_lite::spec::TableMetadataBuilder;
use iceberg_lite::table::Table;
use iceberg_lite::{Error, ErrorKind, Result};
use pgrx::pg_sys;

// ---------------------------------------------------------------------------
// IcebergTableId — the one-way `Oid -> TableIdent` converter.
// ---------------------------------------------------------------------------

/// Single namespace marker used for every PG-derived Iceberg `TableIdent`.
///
/// The marker is intentionally not a real PG schema name. `IcebergTableId`
/// is an opaque handle inside iceberg-lite, not a catalog mapping; using a
/// fixed marker prevents call sites from accidentally treating the namespace
/// segment as semantic data (e.g. a PG schema name or schema OID).
const PG_NAMESPACE_MARKER: &str = "pg";

/// Stable, opaque Iceberg `TableIdent` derived from a PostgreSQL relation
/// OID.
///
/// All construction of Iceberg identifiers from PostgreSQL state goes
/// through this type. Call sites pass a `pg_sys::Oid` and receive a handle
/// that knows how to project itself into iceberg-lite's APIs; they never
/// hand-build `NamespaceIdent` / `TableIdent` from unrelated PG fields such
/// as `namespace_oid` or `relfilenumber`.
#[derive(Debug, Clone)]
pub(crate) struct IcebergTableId(TableIdent);

impl IcebergTableId {
    /// Build the canonical Iceberg ident for a PostgreSQL relation OID.
    pub(crate) fn for_relation(relid: pg_sys::Oid) -> Self {
        let ident = TableIdent::new(
            NamespaceIdent::new(PG_NAMESPACE_MARKER.to_string()),
            u32::from(relid).to_string(),
        );
        Self(ident)
    }

    /// Borrow the underlying `TableIdent` for use with iceberg-lite APIs
    /// that take a reference (e.g. `Catalog::load_table`).
    pub(crate) fn as_table_ident(&self) -> &TableIdent {
        &self.0
    }

    /// Consume the wrapper and yield the owned `TableIdent` for APIs that
    /// take ownership (e.g. `Table::builder().identifier(...)`).
    pub(crate) fn into_table_ident(self) -> TableIdent {
        self.0
    }
}

// ---------------------------------------------------------------------------
// BootstrapWriter — `CREATE TABLE` writes the very first metadata file.
// ---------------------------------------------------------------------------

/// Writes the initial Iceberg metadata file for a brand-new table.
///
/// This is not a `Catalog`: bootstrap never goes through
/// `Transaction::commit`, so there is no need to satisfy the upstream
/// `Catalog` trait surface here. The writer takes only the inputs the
/// bootstrap step actually consumes — a `FileIO`, an `IcebergTableId`, and a
/// `TableCreation` — and returns a loaded `Table`.
#[derive(Debug, Clone)]
pub(crate) struct BootstrapWriter {
    file_io: FileIO,
}

impl BootstrapWriter {
    pub(crate) fn new(file_io: FileIO) -> Self {
        Self { file_io }
    }

    /// Write the initial metadata file for `creation` under `id` and return
    /// the loaded `Table`.
    ///
    /// `creation.location` is required; this writer does not make up storage
    /// locations. `creation.name` is overwritten with the canonical name
    /// from `id` so call sites do not have to supply (or invent) a separate
    /// table name — `IcebergTableId` is the single source of identity.
    pub(crate) fn write_initial_metadata(
        &self,
        id: IcebergTableId,
        mut creation: TableCreation,
    ) -> Result<Table> {
        if creation.location.is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Table location is required: BootstrapWriter only manages \
                 metadata, callers must provide an explicit storage location",
            ));
        }

        // Force the canonical name. `TableCreation::builder().name(...)` is
        // required by the upstream typed builder, but `from_table_creation`
        // discards it (the ident is set when the `Table` is constructed
        // below). Overwriting here keeps the bookkeeping consistent and
        // makes "no two `Table`s ever disagree on identity for the same OID"
        // a local invariant.
        creation.name = id.as_table_ident().name().to_string();

        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;

        let metadata_location =
            MetadataLocation::try_new_with_metadata(&metadata)?.to_string();

        metadata.write_to(&self.file_io, &metadata_location)?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(id.into_table_ident())
            .build()
    }
}

// ---------------------------------------------------------------------------
// StagedCatalog — mutation rebase + commit against an already-loaded base table.
// ---------------------------------------------------------------------------

/// Build a `FeatureUnsupported` error for a method this adapter does not
/// service. Centralized here so the message stays uniform across stubs.
#[inline]
fn unsupported<T>(method: &'static str) -> Result<T> {
    Err(Error::new(
        ErrorKind::FeatureUnsupported,
        format!(
            "PostgreSQL Iceberg StagedCatalog does not implement `{method}`; \
             only methods reached by Transaction::commit are serviced"
        ),
    ))
}

/// Generates trait-method stubs for every `Catalog` method that
/// [`StagedCatalog`] does not service. See the module-level doc for why
/// these stubs are kept local instead of being pushed into iceberg-lite.
macro_rules! unsupported_catalog_method {
    ($method:ident, fn($($arg:ident: $argty:ty),* $(,)?) -> $ret:ty) => {
        fn $method(&self, $($arg: $argty),*) -> Result<$ret> {
            let _ = ($($arg,)*);
            unsupported(stringify!($method))
        }
    };
}

/// Adapter used while committing a transaction against a base table that has
/// already been loaded from PostgreSQL. The base table is captured at
/// construction time; this is a single-shot adapter and must not be reused
/// after `update_table` returns.
///
/// The adapter intentionally does **not** carry its own `FileIO`. A `Table`
/// already binds its `FileIO` at construction time, and `iceberg-lite`'s
/// commit machinery threads that same `FileIO` through manifest reads and
/// writes. Storing a second copy here would let callers construct a catalog
/// whose `FileIO` disagrees with `base.file_io()`, which would silently split
/// reads and writes across different storage backends.
#[derive(Debug, Clone)]
pub(crate) struct StagedCatalog {
    /// The base table this commit rebases onto. `iceberg-lite`'s
    /// `Transaction::do_commit` calls `load_table` once at the start of each
    /// retry to refresh the base; we just hand back the snapshot we loaded
    /// from PostgreSQL above this layer.
    base: Table,
}

impl StagedCatalog {
    pub(crate) fn new(base: &Table) -> Self {
        Self { base: base.clone() }
    }
}

impl Catalog for StagedCatalog {
    fn load_table(&self, ident: &TableIdent) -> Result<Table> {
        if self.base.identifier() != ident {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "StagedCatalog only services its base table {} but was \
                     asked for {}",
                    self.base.identifier(),
                    ident
                ),
            ));
        }
        Ok(self.base.clone())
    }

    fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        Ok(self.base.identifier() == ident)
    }

    fn update_table(&self, commit: TableCommit) -> Result<Table> {
        // Note: this adapter is single-shot. After `update_table` returns,
        // `self.base` no longer reflects the latest metadata. Reusing this
        // value for a second commit would silently rebase against stale
        // state. Callers (currently `metadata_tracker::commit_all`) honor
        // this by constructing a fresh `StagedCatalog` per commit attempt.
        let staged_table = commit.apply(self.base.clone())?;

        // Write through the staged table's own FileIO so reads (manifests
        // pulled in by `commit.apply`) and writes (the new metadata file)
        // share a single IO context.
        staged_table.metadata().write_to(
            staged_table.file_io(),
            staged_table.metadata_location_result()?,
        )?;

        Ok(staged_table)
    }

    // All other trait methods are off the staged-commit path.
    unsupported_catalog_method!(
        list_namespaces,
        fn(parent: Option<&NamespaceIdent>) -> Vec<NamespaceIdent>
    );
    unsupported_catalog_method!(
        create_namespace,
        fn(
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> Namespace
    );
    unsupported_catalog_method!(
        get_namespace,
        fn(namespace: &NamespaceIdent) -> Namespace
    );
    unsupported_catalog_method!(
        namespace_exists,
        fn(namespace: &NamespaceIdent) -> bool
    );
    unsupported_catalog_method!(
        update_namespace,
        fn(namespace: &NamespaceIdent, properties: HashMap<String, String>) -> ()
    );
    unsupported_catalog_method!(drop_namespace, fn(namespace: &NamespaceIdent) -> ());
    unsupported_catalog_method!(
        list_tables,
        fn(namespace: &NamespaceIdent) -> Vec<TableIdent>
    );
    unsupported_catalog_method!(
        create_table,
        fn(namespace: &NamespaceIdent, creation: TableCreation) -> Table
    );
    unsupported_catalog_method!(drop_table, fn(table: &TableIdent) -> ());
    unsupported_catalog_method!(purge_table, fn(table: &TableIdent) -> ());
    unsupported_catalog_method!(
        rename_table,
        fn(src: &TableIdent, dest: &TableIdent) -> ()
    );
    unsupported_catalog_method!(
        register_table,
        fn(table: &TableIdent, metadata_location: String) -> Table
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iceberg_table_id_is_stable_for_same_oid() {
        let oid = pg_sys::Oid::from(42_u32);
        let a = IcebergTableId::for_relation(oid);
        let b = IcebergTableId::for_relation(oid);
        assert_eq!(a.as_table_ident(), b.as_table_ident());
    }

    #[test]
    fn iceberg_table_id_distinguishes_oids() {
        let a = IcebergTableId::for_relation(pg_sys::Oid::from(1_u32));
        let b = IcebergTableId::for_relation(pg_sys::Oid::from(2_u32));
        assert_ne!(a.as_table_ident(), b.as_table_ident());
    }

    #[test]
    fn unsupported_helper_returns_feature_unsupported() {
        let err = unsupported::<()>("create_table").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }
}
