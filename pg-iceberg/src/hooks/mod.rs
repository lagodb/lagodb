pub mod object_access;
pub mod table_option_cache;
pub mod table_options;
pub mod tablespace_options;

pub fn init_hooks() {
    // Initialize transaction callback for pending delete cleanup
    pg_tam::access::xact::init_xact_callback();
    tablespace_options::init_hook();
    table_options::init_hook();
    object_access::init_hook();
}
