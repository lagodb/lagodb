//! Provider contract for the generic Custom ModifyTable framework.

use core::ffi::CStr;

use crate::api::{AmModifyState, TableAccessMethod};
use crate::customscan::error::CustomScanError;
use crate::customscan::provider::{LagodbCustomScanProvider, RelationContext};

use super::binding::ModifyBindContext;

/// PostgreSQL executor features accepted by one Custom ModifyTable provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModifyCapabilities {
    postgres_indexes: bool,
    speculative_insert: bool,
}

impl ModifyCapabilities {
    pub const NONE: Self = Self::new(false, false);
    /// Core maintains PostgreSQL index entries after provider INSERT/UPDATE.
    pub const POSTGRES_INDEXES: Self = Self::new(true, false);
    /// PostgreSQL indexes and speculative insertion are supported.
    pub const POSTGRES_INDEXES_AND_SPECULATIVE_INSERT: Self = Self::new(true, true);

    const fn new(postgres_indexes: bool, speculative_insert: bool) -> Self {
        Self {
            postgres_indexes,
            speculative_insert,
        }
    }

    pub const fn postgres_indexes(self) -> bool {
        self.postgres_indexes
    }

    pub const fn speculative_insert(self) -> bool {
        self.speculative_insert
    }
}

/// Provider contract for the generic Custom ModifyTable framework.
pub trait LagodbCustomModifyProvider: LagodbCustomScanProvider {
    type AccessMethod: TableAccessMethod;

    /// Unique PostgreSQL CustomScan method name for the outer ModifyTable
    /// wrapper.
    const MODIFY_NAME: &'static CStr;

    const MODIFY_CAPABILITIES: ModifyCapabilities;

    /// Attach this provider's Modify-purpose scan to the stable relation state
    /// owned by the outer ModifyTable execution.
    fn bind_modify(ctx: ModifyBindContext<'_, Self>) -> Result<(), CustomScanError>
    where
        Self: Sized;

    /// Whether the relation can be the nominal target of the outer
    /// ModifyTable.
    fn supports_modify_target(context: &RelationContext<'_>) -> bool {
        Self::supports_relation(context)
    }

    /// Return the immutable storage context captured when a Modify-purpose
    /// scan opened.
    fn modify_scan_context(
        state: &Self::State,
    ) -> Option<
        <<Self::AccessMethod as TableAccessMethod>::ModifyState as AmModifyState>::ScanContext,
    >;
}
