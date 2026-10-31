//! Iceberg DDL and object-access hooks.
//!
//! Files are organized by the DDL surface they cover, not by which option
//! parser they happen to call:
//!
//! - `table_ddl` — `CREATE TABLE` lifecycle plus guards against DDL forms we
//!   do not support yet (`CREATE TABLE AS`, `ALTER TABLE SET TABLESPACE`,
//!   `ALTER TABLE ALL IN TABLESPACE`).
//! - `enum_ddl` — prevents label renames while managed columns depend on the
//!   enum's text representation, including through native containers.
//! - `database_storage_policy` — rejects database-template cloning and
//!   database-default tablespace moves that would duplicate or strand managed
//!   Iceberg storage.
//!
//! Storage-volume tablespace binding is runtime-owned because it is a
//! cluster-level facility and must remain available independently of this AM.
//! - `object_access` — the authoritative placement check for relations in
//!   volume-backed tablespaces, plus relation teardown and the column-drop
//!   authorization boundary.
//!
//! Reloption schemas and `rd_amcache` layout live in `crate::managed_table::options`; this
//! module only routes PostgreSQL hook events into those parsers and into the
//! Iceberg catalog.

mod column_drop_guard;
mod database_storage_inventory;
mod database_storage_policy;
mod enum_ddl;
pub mod object_access;
pub mod table_ddl;

pub fn init_hooks() {
    database_storage_policy::init_hook();
    table_ddl::init_hook();
    enum_ddl::init_hook();
    object_access::init_hook();
}
