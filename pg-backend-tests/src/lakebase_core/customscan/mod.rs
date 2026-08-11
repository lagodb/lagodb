mod exec;
mod hook_integration;
mod method_tables;
mod support;
mod tuple_layout;

pub(crate) fn init_pg_test_extension() {
    hook_integration::install_hook_integration_provider();
}
