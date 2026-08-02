//! Provider traits and stable metadata for foreign modify callbacks.

use core::ffi::c_int;

use pgrx::pg_sys;

use super::super::codec::{ForeignPrivateReader, ForeignPrivateWriter};
use super::super::provider::ForeignDataWrapper;
use super::super::row_identity::{ForeignRowIdentityError, ModifyPlanSlot};
use super::error::ForeignModifyError;
use super::execution_context::{
    ForeignInsertBeginContext, ForeignModifyBeginContext,
};
use super::planning_context::{
    ForeignModifyPlanContext, ForeignModifyRelationContext,
    ForeignUpdateTargetContext,
};
use super::slot::ModifySlot;

/// PostgreSQL modify operations exposed to providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignModifyOperation {
    Insert,
    Update,
    Delete,
}

impl ForeignModifyOperation {
    pub(crate) fn from_pg(
        value: pg_sys::CmdType::Type,
    ) -> Result<Self, ForeignModifyError> {
        match value {
            pg_sys::CmdType::CMD_INSERT => Ok(Self::Insert),
            pg_sys::CmdType::CMD_UPDATE => Ok(Self::Update),
            pg_sys::CmdType::CMD_DELETE => Ok(Self::Delete),
            _ => Err(ForeignModifyError::unsupported(
                "FDW framework supports INSERT, UPDATE, and DELETE only",
            )),
        }
    }

    pub(crate) const fn as_pg(self) -> pg_sys::CmdType::Type {
        match self {
            Self::Insert => pg_sys::CmdType::CMD_INSERT,
            Self::Update => pg_sys::CmdType::CMD_UPDATE,
            Self::Delete => pg_sys::CmdType::CMD_DELETE,
        }
    }
}

/// Capabilities returned by a provider for one foreign relation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForeignModifyCapabilities {
    insert: bool,
    update: bool,
    delete: bool,
}

impl ForeignModifyCapabilities {
    pub const fn new(insert: bool, update: bool, delete: bool) -> Self {
        Self {
            insert,
            update,
            delete,
        }
    }

    pub const fn insert_update() -> Self {
        Self::new(true, true, false)
    }

    pub const fn insert_update_delete() -> Self {
        Self::new(true, true, true)
    }

    #[inline]
    pub const fn supports_insert(self) -> bool {
        self.insert
    }

    #[inline]
    pub const fn supports_update(self) -> bool {
        self.update
    }

    #[inline]
    pub const fn supports_delete(self) -> bool {
        self.delete
    }

    pub(crate) const fn flags(self) -> c_int {
        let mut flags = 0;
        if self.insert {
            flags |= 1_i32 << pg_sys::CmdType::CMD_INSERT;
        }
        if self.update {
            flags |= 1_i32 << pg_sys::CmdType::CMD_UPDATE;
        }
        if self.delete {
            flags |= 1_i32 << pg_sys::CmdType::CMD_DELETE;
        }
        flags
    }

    pub(crate) const fn supports(self, operation: ForeignModifyOperation) -> bool {
        match operation {
            ForeignModifyOperation::Insert => self.insert,
            ForeignModifyOperation::Update => self.update,
            ForeignModifyOperation::Delete => self.delete,
        }
    }
}

/// System identity a provider can place on a returned modify row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ForeignReturnedIdentity {
    /// The provider returns relation user columns only.
    #[default]
    None,
    /// The provider can provide a valid external identity through ctid.
    ItemPointer,
}

impl ForeignReturnedIdentity {
    #[inline]
    pub(crate) const fn wire_kind(self) -> i32 {
        match self {
            Self::None => 0,
            Self::ItemPointer => 1,
        }
    }

    pub(crate) fn from_wire(value: i32) -> Result<Self, ForeignModifyError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::ItemPointer),
            _ => Err(ForeignModifyError::framework(
                "FDW modify private data has an invalid returned identity",
            )),
        }
    }

    #[inline]
    pub(crate) const fn supports_item_pointer(self) -> bool {
        matches!(self, Self::ItemPointer)
    }
}

/// Framework-owned plan metadata returned together with provider modify data.
pub struct ForeignModifyPlanSpec<D> {
    pub(crate) private_data: D,
    pub(crate) returned_identity: ForeignReturnedIdentity,
}

impl<D> ForeignModifyPlanSpec<D> {
    /// Create modify plan metadata with no returned system identity.
    #[must_use]
    pub fn new(private_data: D) -> Self {
        Self {
            private_data,
            returned_identity: ForeignReturnedIdentity::None,
        }
    }

    /// Declare that every returned row can carry a valid ItemPointer identity
    /// when the modify plan requires target-table ctid.
    #[must_use]
    pub fn with_returned_item_pointer(mut self) -> Self {
        self.returned_identity = ForeignReturnedIdentity::ItemPointer;
        self
    }
}

/// Result of one provider row operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignModifyOutcome {
    Applied,
    Skipped,
}

/// Copy-object-safe provider modify private data.
pub trait ForeignModifyPrivate: Sized + 'static {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignModifyError>;

    /// # Safety
    ///
    /// The reader must refer to a live, validated modify private-data payload.
    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignModifyError>;
}

/// Relation-local provider state. PostgreSQL serializes calls on one state.
pub trait ForeignModifyState: 'static {
    fn prepare_insert(
        &mut self,
        _slot: &mut ModifySlot<'_>,
    ) -> Result<(), ForeignModifyError> {
        Ok(())
    }

    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError>;

    fn prepare_update(
        &mut self,
        _slot: &mut ModifySlot<'_>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<(), ForeignModifyError> {
        Ok(())
    }

    fn update(
        &mut self,
        slot: &mut ModifySlot<'_>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError>;

    fn prepare_delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<(), ForeignModifyError> {
        Ok(())
    }

    fn delete(
        &mut self,
        returned_slot: Option<&mut ModifySlot<'_>>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError>;

    fn finish(&mut self) -> Result<(), ForeignModifyError>;
}

/// Combined statically dispatched scan and modify provider.
pub trait FdwModify: ForeignDataWrapper + 'static {
    /// Modify private data is independent from scan planner private data.
    type ModifyPrivateData: ForeignModifyPrivate;
    /// Modify state is independent from scan executor state.
    type ModifyState: ForeignModifyState;

    fn capabilities(
        ctx: &ForeignModifyRelationContext<'_>,
    ) -> Result<ForeignModifyCapabilities, ForeignModifyError>;

    fn add_update_targets(
        ctx: &mut ForeignUpdateTargetContext<'_>,
    ) -> Result<(), ForeignModifyError>;

    fn plan_modify(
        ctx: &ForeignModifyPlanContext<'_>,
    ) -> Result<ForeignModifyPlanSpec<Self::ModifyPrivateData>, ForeignModifyError>;

    fn begin_modify(
        ctx: ForeignModifyBeginContext<'_, Self::ModifyPrivateData>,
    ) -> Result<Self::ModifyState, ForeignModifyError>;

    fn begin_insert(
        _ctx: &mut ForeignInsertBeginContext<'_>,
    ) -> Result<Self::ModifyState, ForeignModifyError> {
        Err(ForeignModifyError::unsupported(
            "foreign provider does not support routed or COPY INSERT",
        ))
    }
}

impl From<ForeignRowIdentityError> for ForeignModifyError {
    fn from(error: ForeignRowIdentityError) -> Self {
        Self::framework(error)
    }
}
