//! Provider contract for the generic Custom ModifyTable framework.

use core::ffi::CStr;

use crate::access::mutation::ModifyScanBinding;
use crate::api::{AmModifyState, TableAccessMethod};
use crate::customscan::error::CustomScanError;
use crate::customscan::provider::{
    LagodbCustomScanProvider, NeededColumns, RelationContext,
};
use crate::expr::pushdown::BoundFilterSet;
use crate::handles::{RelationHandle, SnapshotHandle};

use super::binding::ModifyBindContext;

/// Prepared AM session and the immutable storage context required by its writer.
pub struct PreparedModifyScan<A: TableAccessMethod> {
    /// Session whose row cursor is opened only after ModifyTable binds it.
    pub session: A::ScanSession,
    /// Snapshot and task ownership passed to the corresponding writer.
    pub context: <A::ModifyState as AmModifyState>::ScanContext,
}

/// PostgreSQL executor features accepted by one Custom ModifyTable provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModifyCapabilities {
    postgres_indexes: bool,
}

impl ModifyCapabilities {
    pub const NONE: Self = Self::new(false);
    /// Core maintains PostgreSQL index entries after provider INSERT/UPDATE.
    pub const POSTGRES_INDEXES: Self = Self::new(true);

    const fn new(postgres_indexes: bool) -> Self {
        Self { postgres_indexes }
    }

    pub const fn postgres_indexes(self) -> bool {
        self.postgres_indexes
    }
}

/// Provider contract for the generic Custom ModifyTable framework.
pub trait LagodbCustomModifyProvider: LagodbCustomScanProvider {
    type AccessMethod: TableAccessMethod;

    /// Unique PostgreSQL CustomScan method name for the outer ModifyTable
    /// wrapper.
    const MODIFY_NAME: &'static CStr;

    const MODIFY_CAPABILITIES: ModifyCapabilities;

    /// Prepare a native SeqScan target without opening its row cursor. Filters
    /// are negotiated rescan-stable predicates (constants and external params),
    /// bound for this execution. Providers select scan and conflict predicates
    /// separately from their source metadata. PostgreSQL retains every qual,
    /// including residuals for conservative predicates, for final qualification.
    ///
    /// Keep even exact pushed predicates in the native SeqScan's qual. In PG17,
    /// `ExecScanFetch` can return an EPQ replacement tuple without calling
    /// `SeqNext` or the TableAM reader, and `SeqRecheck` simply returns true;
    /// `ExecScan` must still evaluate the qual on that tuple. Unlike our
    /// CustomScan, native SeqScan has no provider-owned exact-filter recheck
    /// callback. Reader filtering does not transfer qualification ownership.
    ///
    /// `needed_columns` covers the final SeqScan targetlist and every qual.
    /// Providers must preserve the relation-shaped slot: subset attributes use
    /// their original positions, and whole-row references require all columns.
    fn prepare_modify_seq_scan(
        relation: &RelationHandle<'_>,
        snapshot: &SnapshotHandle<'_>,
        filters: BoundFilterSet<'_, Self::BoundPredicate>,
        needed_columns: NeededColumns<'_>,
    ) -> Result<PreparedModifyScan<Self::AccessMethod>, CustomScanError>;

    /// Complete the prepared AM session's one-time binding before its first row.
    fn bind_modify_seq_scan(
        session: &mut <Self::AccessMethod as TableAccessMethod>::ScanSession,
        binding: ModifyScanBinding<
            <Self::AccessMethod as TableAccessMethod>::ModifyQueryState,
        >,
    ) -> Result<(), CustomScanError>;

    /// Attach this provider's ModifyTarget-purpose scan to the stable relation state
    /// owned by the outer ModifyTable execution.
    fn bind_modify(ctx: ModifyBindContext<'_, Self>) -> Result<(), CustomScanError>
    where
        Self: Sized;

    /// Whether the relation can be the nominal target of the outer
    /// ModifyTable.
    fn supports_modify_target(context: &RelationContext<'_>) -> bool {
        Self::supports_relation(context)
    }

    /// Return the immutable storage context captured when a ModifyTarget-purpose
    /// scan opened.
    fn modify_scan_context(
        state: &Self::State,
    ) -> Option<
        <<Self::AccessMethod as TableAccessMethod>::ModifyState as AmModifyState>::ScanContext,
    >;
}
