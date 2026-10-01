//! Iceberg DDL and object-access hooks.
//!
//! Files are organized by the DDL surface they cover, not by which option
//! parser they happen to call:
//!
//! - `table_ddl` — `CREATE TABLE` lifecycle plus guards against DDL forms we
//!   do not support yet (`CREATE TABLE AS`,
//!   `ALTER TABLE SET ACCESS METHOD/TABLESPACE`,
//!   `ALTER TABLE ALL IN TABLESPACE`).
//!   Its topology guard uses pg_inherits object-access events under PG's locks
//!   to keep managed partitioned tables out of PostgreSQL partition hierarchies.
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

use lagodb_core::access::mutation;

mod column_drop_guard;
mod copy;
mod database_storage_inventory;
mod database_storage_policy;
pub mod object_access;
pub mod table_ddl;

pub fn init_hooks() {
    mutation::init_lifecycle_hooks();
    copy::init();
    database_storage_policy::init_hook();
    table_ddl::init_hook();
    object_access::init_hook();
}
