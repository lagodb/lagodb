//! Storage-service injection points.

use crate::injection_point::InjectionPoint;

/// Storage-client injection points at coarse foreground lifecycle boundaries.
pub(super) struct StorageServiceInjectionPoints;

impl StorageServiceInjectionPoints {
    /// A backend client has been acquired, but its foreground OPEN request has
    /// not yet been written to the storage socket.
    pub(super) const FOREGROUND_BEFORE_OPEN: InjectionPoint =
        InjectionPoint::new(c"lakebase-storage-foreground-before-open");
}
