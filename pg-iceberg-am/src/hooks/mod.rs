//! Iceberg DDL and object-access hooks.
//!
//! Files are organized by the DDL surface they cover, not by which option
//! parser they happen to call:
//!
//! - `table_ddl` — `CREATE TABLE` lifecycle plus guards against DDL forms we
//!   do not support yet (`CREATE TABLE AS`,
//!   `ALTER TABLE SET ACCESS METHOD/TABLESPACE`,
//!   `ALTER TABLE ALL IN TABLESPACE`).
//! - `tablespace_ddl` — `CREATE TABLESPACE` option persistence plus guards
//!   that protect the distributed-tablespace storage identity (RENAME /
//!   ALTER ... SET / RESET).
//! - `object_access` — `OAT_DROP` callbacks for relation teardown and the
//!   column-drop authorization boundary.
//!
//! Reloption schemas and `rd_amcache` layout live in `crate::options`; this
//! module only routes PostgreSQL hook events into those parsers and into the
//! Iceberg catalog.

use pg_lakebase_core::access::mutation;

mod column_drop_guard;
pub mod object_access;
pub mod table_ddl;
pub mod tablespace_ddl;

pub fn init_hooks() {
    mutation::init_lifecycle_hooks();
    tablespace_ddl::init_hook();
    table_ddl::init_hook();
    object_access::init_hook();
}
