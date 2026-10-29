//! Release identity compared once when a provider connects to the runtime.

use std::fmt;

const SDK_VERSION_CAPACITY: usize = 32;

/// NUL-padded `lagodb-core` package version, stored inline so the runtime
/// table stays a plain `Sync` static and the comparison never dereferences a
/// pointer published by a possibly different build.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SdkVersion([u8; SDK_VERSION_CAPACITY]);

impl SdkVersion {
    /// Version of the `lagodb-core` this DSO was compiled against.
    pub const CURRENT: Self = Self::from_version(env!("CARGO_PKG_VERSION"));

    const fn from_version(version: &str) -> Self {
        let source = version.as_bytes();
        assert!(
            source.len() < SDK_VERSION_CAPACITY,
            "lagodb-core version exceeds SdkVersion capacity"
        );
        let mut bytes = [0; SDK_VERSION_CAPACITY];
        let mut index = 0;
        while index < source.len() {
            bytes[index] = source[index];
            index += 1;
        }
        Self(bytes)
    }
}

impl fmt::Display for SdkVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let length = self
            .0
            .iter()
            .position(|&byte| byte == 0)
            .unwrap_or(SDK_VERSION_CAPACITY);
        formatter.write_str(&String::from_utf8_lossy(&self.0[..length]))
    }
}
