use pg_lakebase_core::access::dml;

pub mod object_access;
pub mod table_option_cache;
pub mod table_options;
pub mod tablespace_options;

pub fn init_hooks() {
    dml::init_lifecycle_hooks();
    tablespace_options::init_hook();
    table_options::init_hook();
    object_access::init_hook();
}
