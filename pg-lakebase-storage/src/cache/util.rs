//! Small cache-local helpers that do not warrant their own module.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::StorageResult;

pub(crate) fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default()
}

pub(crate) fn should_touch(
    last_access_ns: u64,
    now_ns: u64,
    touch_granularity_ns: u64,
) -> bool {
    last_access_ns == 0
        || touch_granularity_ns == 0
        || now_ns.saturating_sub(last_access_ns) >= touch_granularity_ns
}

/// Ensures parent segments exist before creating/truncating a cache file under `path`.
pub(crate) async fn create_parent_dir(path: &Path) -> StorageResult<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(())
}
