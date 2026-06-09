mod codec;
mod custom_private;
mod exec;
mod explain_output;
mod hook;
mod hook_integration;
mod method_tables;
mod provider;
mod referenced_attnos;

pub(crate) fn init_pg_test_extension() {
    hook_integration::install_hook_integration_provider();
}
