use lagodb_core::backend::InjectionPoint;

/// Object-storage injection points at coarse client lifecycle boundaries.
pub(super) struct ObjectInjectionPoints;

impl ObjectInjectionPoints {
    /// An object reader is about to drop its open storage-service handle.
    pub(super) const OBJECT_READER_BEFORE_DROP: InjectionPoint =
        InjectionPoint::new(c"lagodb-iceberg-object-reader-before-drop");
}
