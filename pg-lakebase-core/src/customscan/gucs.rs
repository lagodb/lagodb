//! Reads the runtime-owned `pg_lakebase.customscan_mode` through the stable ABI.

#[derive(Clone, Copy, Eq, PartialEq)]
enum CustomScanMode {
    Off,
    Auto,
    Force,
}

fn mode() -> CustomScanMode {
    let api = crate::table_maintenance::abi::runtime_api().unwrap_or_else(|| {
        panic!("pg_lakebase runtime API is unavailable while planning CustomScan")
    });
    match unsafe { (api.customscan_mode)() } {
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
