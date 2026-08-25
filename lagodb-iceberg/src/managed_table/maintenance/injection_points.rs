use pg_lakebase_core::injection_point::InjectionPoint;

/// Iceberg maintenance injection points at transactionally meaningful edges.
pub(super) struct IcebergInjectionPoints;

impl IcebergInjectionPoints {
    /// Rewrite output files exist, but no replacement snapshot has been staged.
    pub(super) const VACUUM_AFTER_REWRITE: InjectionPoint =
        InjectionPoint::new(c"lakebase-iceberg-vacuum-after-rewrite");
}
