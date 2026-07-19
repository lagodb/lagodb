//! Reads the runtime-owned `pg_lakebase.customscan_mode` through the stable ABI.

#[derive(Clone, Copy, Eq, PartialEq)]
enum CustomScanMode {
    Off,
    Auto,
    Force,
}

fn mode() -> CustomScanMode {
    let runtime = crate::runtime_api::RuntimeClient::connect()
        .unwrap_or_else(|error| panic!("cannot read CustomScan settings: {error}"));
    match runtime.customscan_mode() {
        0 => CustomScanMode::Off,
        1 => CustomScanMode::Auto,
        2 => CustomScanMode::Force,
        value => panic!("runtime returned unknown customscan mode {value}"),
    }
}

#[inline]
pub(crate) fn enabled() -> bool {
    mode() != CustomScanMode::Off
}

#[inline]
pub(crate) fn force_mode() -> bool {
    mode() == CustomScanMode::Force
}
