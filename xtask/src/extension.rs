/// pgrx extension crates in this workspace.
#[derive(Clone, Copy)]
pub(crate) enum Extension {
    BackendTests,
    Runtime,
    Iceberg,
    DeltaAm,
    Connectors,
}

impl Extension {
    pub(crate) const ALL: [Self; 5] = [
        Self::BackendTests,
        Self::Runtime,
        Self::Iceberg,
        Self::DeltaAm,
        Self::Connectors,
    ];

    pub(crate) const fn package(self) -> &'static str {
        match self {
            Self::BackendTests => "pg-backend-tests",
            Self::Runtime => "lagodb-base",
            Self::Iceberg => "lagodb-iceberg",
            Self::DeltaAm => "pg-delta-am",
            Self::Connectors => "lagodb-connectors",
        }
    }

    pub(crate) const fn library(self) -> &'static str {
        match self {
            Self::BackendTests => "pg_backend_tests",
            Self::Runtime => "lagodb_base",
            Self::Iceberg => "lagodb_iceberg",
            Self::DeltaAm => "pg_delta_am",
            Self::Connectors => "lagodb_connectors",
        }
    }
}
