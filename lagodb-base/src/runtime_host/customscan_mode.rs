//! Runtime-owned `lagodb.customscan_mode`, published to providers through the
//! runtime ABI.

use pgrx::{
    GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum, pg_guard,
};

static CUSTOMSCAN_MODE: GucSetting<CustomScanMode> =
    GucSetting::<CustomScanMode>::new(CustomScanMode::Auto);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PostgresGucEnum)]
enum CustomScanMode {
    Off,
    Auto,
    Force,
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn customscan_mode() -> u32 {
    match CUSTOMSCAN_MODE.get() {
        CustomScanMode::Off => 0,
        CustomScanMode::Auto => 1,
        CustomScanMode::Force => 2,
    }
}

pub(super) fn define() {
    GucRegistry::define_enum_guc(
        c"lagodb.customscan_mode",
        c"Path-emission mode for the LagoDB CustomScan framework",
        c"off disables paths; auto ranks them by cost, except that a provider may \
          replace the table AM's SeqScan of a read scan, leaving cost to choose \
          among its own paths; force biases legal paths for tests.",
        &CUSTOMSCAN_MODE,
        GucContext::Userset,
        GucFlags::default(),
    );
}
