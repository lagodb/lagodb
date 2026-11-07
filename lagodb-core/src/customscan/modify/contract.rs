//! Provider contract for the generic Custom ModifyTable framework.

use core::ffi::CStr;

use crate::customscan::error::CustomScanError;
use crate::customscan::provider::{
    LagodbCustomScanProvider, NeededColumns, RelationContext,
};
use crate::expr::pushdown::BoundFilterSet;
use crate::handles::{RelationHandle, SnapshotHandle};
use crate::table_am::ModifyScanBinding;
use crate::table_am::{AmModifyState, TableAccessMethod};

use super::binding::ModifyBindContext;

/// Prepared native SeqScan target and the immutable storage context required
/// by its writer.
pub struct PreparedModifyScan<P: LagodbCustomModifyProvider> {
    /// Provider preparation that becomes a scan session only once bound.
    pub scan: P::PreparedSeqScan,
    /// Snapshot and task ownership passed to the corresponding writer.
    pub context:
        <<P::AccessMethod as TableAccessMethod>::ModifyState as AmModifyState>::ScanContext,
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

    /// Native SeqScan target prepared before the outer ModifyTable binds it.
    /// It is not a scan session; [`Self::open_modify_seq_scan`] produces one.
    type PreparedSeqScan;

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
    /// Keep even exact pushed predicates in the native SeqScan's qual.
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
    ) -> Result<PreparedModifyScan<Self>, CustomScanError>
    where
        Self: Sized;

    /// Consume the prepared target with its one-time ModifyTable binding and
    /// return the complete, row-ready AM scan session.
    fn open_modify_seq_scan(
        prepared: Self::PreparedSeqScan,
        binding: ModifyScanBinding<
            <Self::AccessMethod as TableAccessMethod>::ModifyQueryState,
        >,
    ) -> Result<<Self::AccessMethod as TableAccessMethod>::ScanSession, CustomScanError>;

    /// Consume this provider's begun ModifyTarget-purpose scan state and
    /// return it attached to the stable relation state owned by the outer
    /// ModifyTable execution. The outer Begin binds each such scan once,
    /// before its Start. On failure the consumed state is dropped before the
    /// framework reports the error.
    fn bind_modify(
        state: Self::State,
        ctx: ModifyBindContext<'_, Self>,
    ) -> Result<Self::State, CustomScanError>
    where
        Self: Sized;

    /// Whether the relation can be the nominal target of the outer
    /// ModifyTable.
    ///
    /// The target's access method must belong to a registered table provider,
    /// whose hierarchy guard makes every result relation this provider's.
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
