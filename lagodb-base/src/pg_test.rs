//! pgrx test-runner configuration.

#[cfg(test)]
pub fn setup(_options: Vec<&str>) {}

#[cfg(test)]
pub fn postgresql_conf_options() -> Vec<&'static str> {
    vec![
        "shared_preload_libraries = 'lagodb_base'",
        "max_worker_processes = 32",
    ]
}
