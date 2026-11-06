//! ModifyTable contracts, query-level state, and core-owned trigger rows.
//!
//! [`ModifyQueryState`] is the provider-facing handle to state shared by all
//! ModifyTable nodes of one executor query. Its core-owned half, the trigger-row
//! store in `trigger_rows`, stays crate-private.

mod binding;
mod query_state;
pub(crate) mod trigger_rows;

use pgrx::pg_sys;

pub use binding::ModifyScanBinding;
pub(crate) use query_state::ModifyQueryShared;
pub use query_state::ModifyQueryState;

use super::{AmResult, unsupported_callback};
use crate::handles::{ItemPointer, RelationHandle, SnapshotHandle};
use crate::tuple::TupleSlotRow;

/// First `ItemPointer` block number reserved for core-owned temporary rows.
/// Provider physical row-identity codecs must remain below this boundary.
pub const TRIGGER_ROW_BLOCK_BASE: u32 = 0xC000_0000;

/// Per-row INSERT context passed by the forked ModifyTable executor.
#[derive(Debug, Clone, Copy)]
pub struct MutationWriteContext {
    pub cid: pg_sys::CommandId,
    pub options: i32,
}

/// Per-row UPDATE context passed by the forked ModifyTable executor.
pub struct MutationUpdateContext<'a> {
    pub cid: pg_sys::CommandId,
    pub snapshot: &'a SnapshotHandle<'a>,
    pub crosscheck: Option<&'a SnapshotHandle<'a>>,
    pub wait: bool,
}

/// Per-row DELETE context passed by the forked ModifyTable executor.
pub struct MutationDeleteContext<'a> {
    pub cid: pg_sys::CommandId,
    pub snapshot: &'a SnapshotHandle<'a>,
    pub crosscheck: Option<&'a SnapshotHandle<'a>>,
    pub wait: bool,
}

/// Storage-format-neutral result of a row mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOutcome {
    Applied,
    /// A ModifyState in the current PostgreSQL transaction already processed
    /// this physical row.
    ///
    /// Providers report the physical-row fact and the modifying command's ID.
    /// The PostgreSQL adapter owns the conversion to `TM_SelfModified` and its
    /// `TM_FailureData`.
    AlreadyModifiedInCurrentTransaction {
        modifying_command_id: pg_sys::CommandId,
    },
    Deleted,
}

/// Row-level operations that one PostgreSQL ModifyTable node may execute.
///
/// A plain INSERT/UPDATE/DELETE contains exactly one operation. MERGE carries
/// the union of its concrete actions so providers can allocate only the sinks
/// and validation state that the planned statement can actually use.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModifyActions(u8);

impl ModifyActions {
    pub const NONE: Self = Self(0);
    pub const INSERT: Self = Self(1 << 0);
    pub const UPDATE: Self = Self(1 << 1);
    pub const DELETE: Self = Self(1 << 2);

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn may_insert(self) -> bool {
        self.0 & Self::INSERT.0 != 0
    }

    pub const fn may_update(self) -> bool {
        self.0 & Self::UPDATE.0 != 0
    }

    pub const fn may_delete(self) -> bool {
        self.0 & Self::DELETE.0 != 0
    }

    pub const fn writes_rows(self) -> bool {
        self.may_insert() || self.may_update()
    }

    pub const fn writes_position_deletes(self) -> bool {
        self.may_update() || self.may_delete()
    }
}

/// Storage-specific state shared by all ModifyTable nodes belonging to one
/// PostgreSQL executor query.
pub trait AmModifyQueryState: 'static {
    /// Storage source shared by a group of physical row identities, such as an
    /// Iceberg data-file path. The source is registered once per physical scan
    /// run, never once per logical row.
    type ScanIdentitySource<'a>;

    /// Compact source token returned to the scan after query-level registration.
    type RegisteredScanIdentity: Copy + 'static;

    /// Storage-specific per-row identity within a registered source.
    type ScanIdentity<'a>;

    fn new() -> AmResult<Self>
    where
        Self: Sized;

    /// Register a storage identity source in the query-level namespace for one
    /// result relation.
    fn register_scan_identity_source(
        &mut self,
        relation_oid: pg_sys::Oid,
        source: &Self::ScanIdentitySource<'_>,
    ) -> AmResult<Self::RegisteredScanIdentity>;

    /// Encode one row within a registered source into PostgreSQL's row-id
    /// carrier. This is deliberately stateless and remains on the per-row scan
    /// hot path. Core reserves `ItemPointer::block_number >= 0xC0000000` for
    /// query-local AFTER-trigger row identities; provider physical identities
    /// must stay below that range.
    fn encode_row_identity(
        source: Self::RegisteredScanIdentity,
        identity: &Self::ScanIdentity<'_>,
    ) -> AmResult<ItemPointer>;
}

/// Immutable context supplied when a relation-local modify state is created.
#[derive(Clone)]
pub struct ModifyStateContext<Q: AmModifyQueryState, C> {
    query_state: ModifyQueryState<Q>,
    cmd_type: pg_sys::CmdType::Type,
    command_id: pg_sys::CommandId,
    actions: ModifyActions,
    scan_context: Option<C>,
}

impl<Q: AmModifyQueryState, C> ModifyStateContext<Q, C> {
    /// Construct a session whose writes do not depend on a target scan.
    pub fn independent(
        query_state: ModifyQueryState<Q>,
        cmd_type: pg_sys::CmdType::Type,
        actions: ModifyActions,
        command_id: pg_sys::CommandId,
    ) -> Self {
        Self {
            query_state,
            cmd_type,
            command_id,
            actions,
            scan_context: None,
        }
    }

    /// Construct a session tied to the supplied Modify-scan context.
    pub fn target_read(
        query_state: ModifyQueryState<Q>,
        cmd_type: pg_sys::CmdType::Type,
        actions: ModifyActions,
        command_id: pg_sys::CommandId,
        scan_context: C,
    ) -> Self {
        Self {
            query_state,
            cmd_type,
            command_id,
            actions,
            scan_context: Some(scan_context),
        }
    }

    /// PostgreSQL command type for the owning ModifyTable execution.
    pub fn cmd_type(&self) -> pg_sys::CmdType::Type {
        self.cmd_type
    }

    /// Output CID fixed by the owning executor or COPY command.
    pub fn command_id(&self) -> pg_sys::CommandId {
        self.command_id
    }

    /// Concrete row operations present in the owning plan.
    pub fn actions(&self) -> ModifyActions {
        self.actions
    }

    /// The target-scan context, absent for independent INSERT/COPY.
    pub fn scan_context(&self) -> Option<&C> {
        self.scan_context.as_ref()
    }

    /// Consume this context into its independently owned parts.
    pub fn into_parts(
        self,
    ) -> (
        ModifyQueryState<Q>,
        pg_sys::CmdType::Type,
        ModifyActions,
        Option<C>,
    ) {
        (
            self.query_state,
            self.cmd_type,
            self.actions,
            self.scan_context,
        )
    }
}

/// Relation-local state owned directly by one Custom ModifyTable execution.
///
/// Abort paths are separate. If PostgreSQL raises ERROR, aborts the transaction,
/// or rolls back to a savepoint, the ResourceOwner callback and owning
/// MemoryContext are order-independent cleanup anchors for the same execution
/// state. Whichever consumes it first ensures that unfinalized sessions receive
/// [`abort_modify`](Self::abort_modify) instead of `end_modify`; the other path
/// must remain an idempotent no-op.
pub trait AmModifyState {
    type QueryState: AmModifyQueryState;

    /// Storage-specific metadata captured by the ModifyTarget-purpose target scan.
    type ScanContext: Clone + PartialEq + 'static;

    /// Begin and construct the complete write session for one result relation.
    ///
    /// `rel` is borrowed only for the duration of construction (symmetric with
    /// [`AmScanSession::scan_begin`](super::AmScanSession::scan_begin)). This is the
    /// relation-local execution boundary: implementations may load metadata,
    /// create writers, and allocate buffers here. A successful return must be a fully usable session; a failure
    /// returns before any provider state is published to the executor.
    ///
    /// The AM derives everything it needs from the handle (relation OID, file
    /// locator, WAL requirement, and any column layout) and must capture it into
    /// owned fields rather than retaining the handle. `context` carries the
    /// PostgreSQL command and, for target-reading mutation, the provider context
    /// captured by the Modify scan.
    fn begin_modify(
        rel: &RelationHandle,
        context: ModifyStateContext<Self::QueryState, Self::ScanContext>,
    ) -> AmResult<Self>
    where
        Self: Sized;

    /// Finishes the relation-local write session for a successful execution.
    ///
    /// A single PostgreSQL ModifyTable execution can host multiple AM sessions,
    /// for example an UPDATE or DELETE across an inheritance tree. Implementations
    /// should not make per-session writes externally visible here unless that
    /// publication can remain correct when a later session in the same frame
    /// fails. Prefer staging publication and deferring the externally visible
    /// commit to a transaction-scoped resource, with matching cleanup such as
    /// pending-delete registration, so all sessions in the frame become visible
    /// atomically.
    fn end_modify(&mut self) -> AmResult<()>;

    /// Cleans up relation-local resources after ERROR/abort/rollback.
    ///
    /// This may run during ResourceOwner cleanup after pgrx has completed the
    /// guarded Rust unwind and returned control to PostgreSQL. Keep it
    /// best-effort and idempotent; it must not assume `end_modify()` ran.
    fn abort_modify(&mut self) {}

    /// Insert the final relation-shaped tuple produced by PostgreSQL.
    fn insert_slot(
        &mut self,
        new: TupleSlotRow<'_>,
        context: MutationWriteContext,
    ) -> AmResult<()> {
        let _ = (new, context);
        unsupported_callback("insert_slot")
    }

    /// Update a row using explicit physical identity plus OLD and final NEW
    /// relation-shaped slots.
    fn update_slot(
        &mut self,
        row_id: ItemPointer,
        old: TupleSlotRow<'_>,
        new: TupleSlotRow<'_>,
        context: MutationUpdateContext<'_>,
    ) -> AmResult<MutationOutcome> {
        let _ = (row_id, old, new, context);
        unsupported_callback("update_slot")
    }

    /// Delete a row using explicit physical identity.
    ///
    /// PostgreSQL executor consumers such as row triggers, transition tables,
    /// and `RETURNING` obtain OLD from the planner-visible `wholerow`; storage
    /// deletion does not receive an unconditional copy of the business row.
    fn delete_slot(
        &mut self,
        row_id: ItemPointer,
        context: MutationDeleteContext<'_>,
    ) -> AmResult<MutationOutcome> {
        let _ = (row_id, context);
        unsupported_callback("delete_slot")
    }
}
