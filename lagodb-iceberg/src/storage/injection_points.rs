use lagodb_core::injection_point::InjectionPoint;

/// Object-storage injection points at coarse client lifecycle boundaries.
pub(super) struct StorageInjectionPoints;

impl StorageInjectionPoints {
    /// An object reader is about to drop its open storage-service handle.
    pub(super) const OBJECT_READER_BEFORE_DROP: InjectionPoint =
        InjectionPoint::new(c"lagodb-iceberg-object-reader-before-drop");
}
