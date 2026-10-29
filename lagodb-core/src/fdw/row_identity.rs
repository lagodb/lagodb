//! Row identity contracts shared by FDW scan and modify execution.

use core::ffi::{CStr, c_int};
use core::marker::PhantomData;
use std::ffi::CString;

use pgrx::pg_sys;

use crate::handles::{TupleDescHandle, ValidItemPointer};
use crate::tuple::PgDatumRef;

use super::attribute_target_name::RelationAttributeTargetName;

const ATTRIBUTE_IDENTITY_PREFIX: &str = "__lagodb_fdw_identity_attr_";
const ITEM_POINTER_IDENTITY_NAME: &CStr = c"__lagodb_fdw_identity_ctid";
const ATTRIBUTE_IDENTITY_NAME: RelationAttributeTargetName =
    RelationAttributeTargetName::new(ATTRIBUTE_IDENTITY_PREFIX);

/// Scan-side request for the special system identity representation produced
/// by a modify-purpose scan.
///
/// Positive relation attributes are ordinary scan columns and therefore do
/// not need a separate scan writer mode. Only an ItemPointer identity needs a
/// physical tuple representation in PostgreSQL's `TTSOpsHeapTuple` slot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ForeignRowIdentityRequirement {
    #[default]
    None,
    ItemPointer,
}

impl ForeignRowIdentityRequirement {
    pub(crate) const fn wire_kind(self) -> i32 {
        match self {
            Self::None => 0,
            Self::ItemPointer => 1,
        }
    }

    pub(crate) fn from_wire(value: i32) -> Result<Self, ForeignRowIdentityError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::ItemPointer),
            _ => Err(ForeignRowIdentityError::InvalidRequirement),
        }
    }

    pub(crate) const fn needs_item_pointer(self) -> bool {
        matches!(self, Self::ItemPointer)
    }
}

/// Identity categories that can occur in a foreign modify plan slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignRowIdentityKind {
    ItemPointer,
    Attribute { attno: pg_sys::AttrNumber },
}

impl ForeignRowIdentityKind {
    pub(crate) const fn wire_attno(self) -> pg_sys::AttrNumber {
        match self {
            Self::ItemPointer => {
                pg_sys::SelfItemPointerAttributeNumber as pg_sys::AttrNumber
            }
            Self::Attribute { attno } => attno,
        }
    }

    pub(crate) fn from_wire_attno(
        attno: pg_sys::AttrNumber,
    ) -> Result<Self, ForeignRowIdentityError> {
        if attno == pg_sys::SelfItemPointerAttributeNumber as pg_sys::AttrNumber {
            Ok(Self::ItemPointer)
        } else if attno > 0 {
            Ok(Self::Attribute { attno })
        } else {
            Err(ForeignRowIdentityError::InvalidRelationAttribute)
        }
    }

    /// Build the framework-owned planner target name for a validated positive
    /// relation attribute.
    pub(crate) fn attribute_target_name(
        attribute: &pg_sys::FormData_pg_attribute,
    ) -> CString {
        ATTRIBUTE_IDENTITY_NAME.encode(attribute)
    }

    pub(crate) const fn item_pointer_target_name() -> &'static CStr {
        ITEM_POINTER_IDENTITY_NAME
    }

    pub(crate) fn from_target_name(
        name: &CStr,
    ) -> Result<Option<Self>, ForeignRowIdentityError> {
        if name == ITEM_POINTER_IDENTITY_NAME {
            return Ok(Some(Self::ItemPointer));
        }
        ATTRIBUTE_IDENTITY_NAME
            .attno(name)
            .map(|attno| attno.map(|attno| Self::Attribute { attno }))
            .map_err(|_| ForeignRowIdentityError::InvalidIdentityName)
    }
}

/// One cached identity target entry in the ModifyTable subplan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowIdentityEntry {
    plan_index: usize,
    kind: ForeignRowIdentityKind,
    type_oid: pg_sys::Oid,
    type_mod: i32,
}

impl RowIdentityEntry {
    #[inline]
    pub(crate) const fn kind(self) -> ForeignRowIdentityKind {
        self.kind
    }

    #[inline]
    pub(crate) const fn plan_index(self) -> usize {
        self.plan_index
    }

    #[inline]
    pub(crate) const fn type_oid(self) -> pg_sys::Oid {
        self.type_oid
    }

    #[inline]
    pub(crate) const fn type_mod(self) -> i32 {
        self.type_mod
    }
}

/// Executor-side identity layout, built once from the final subplan tlist.
#[derive(Debug, Clone)]
pub(crate) struct RowIdentityLayout {
    entries: Box<[RowIdentityEntry]>,
}

impl RowIdentityLayout {
    pub(crate) fn empty() -> Self {
        Self {
            entries: Vec::new().into_boxed_slice(),
        }
    }

    /// # Safety
    ///
    /// `targetlist` must be a live ModifyTable subplan targetlist whose nodes
    /// remain valid for the resulting executor state.
    pub(crate) unsafe fn from_targetlist(
        targetlist: *mut pg_sys::List,
        relation: pg_sys::Relation,
        expected: &[ForeignRowIdentityKind],
    ) -> Result<Self, ForeignRowIdentityError> {
        if targetlist.is_null()
            || relation.is_null()
            || unsafe { (*relation).rd_att.is_null() }
        {
            return Err(ForeignRowIdentityError::MissingTargetList);
        }

        let mut entries = Vec::new();
        let length = unsafe { pg_sys::list_length(targetlist) };
        if length < 0 {
            return Err(ForeignRowIdentityError::NegativeTargetListLength);
        }
        for index in 0..length {
            let target_entry = unsafe { pg_sys::list_nth(targetlist, index) }
                as *mut pg_sys::TargetEntry;
            if target_entry.is_null()
                || unsafe { (*target_entry).xpr.type_ }
                    != pg_sys::NodeTag::T_TargetEntry
            {
                return Err(ForeignRowIdentityError::MalformedTargetEntry);
            }
            if !unsafe { (*target_entry).resjunk }
                || unsafe { (*target_entry).resname.is_null() }
            {
                continue;
            }

            let name = unsafe { CStr::from_ptr((*target_entry).resname) };
            let Some(kind) = (unsafe { Self::kind_for_target_name(relation, name) })?
            else {
                continue;
            };
            if !expected.contains(&kind) {
                continue;
            }
            let expr = unsafe { (*target_entry).expr };
            if expr.is_null() || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var {
                return Err(ForeignRowIdentityError::MalformedIdentityExpression);
            }
            let var = expr.cast::<pg_sys::Var>();
            if unsafe { (*var).varlevelsup } != 0 {
                return Err(ForeignRowIdentityError::MalformedIdentityExpression);
            }
            let plan_attno = unsafe { (*target_entry).resno };
            if plan_attno <= 0 {
                return Err(ForeignRowIdentityError::InvalidPlanAttribute);
            }
            let plan_index = usize::try_from(plan_attno as i32 - 1)
                .map_err(|_| ForeignRowIdentityError::InvalidPlanAttribute)?;
            if entries
                .iter()
                .any(|entry: &RowIdentityEntry| entry.plan_index == plan_index)
            {
                return Err(ForeignRowIdentityError::DuplicatePlanAttribute);
            }

            if matches!(kind, ForeignRowIdentityKind::ItemPointer)
                && (unsafe { (*var).vartype } != pg_sys::TIDOID
                    || unsafe { (*var).vartypmod } != -1
                    || unsafe { (*var).varcollid } != pg_sys::InvalidOid)
            {
                return Err(ForeignRowIdentityError::MalformedIdentityExpression);
            }
            let (type_oid, type_mod) = match kind {
                ForeignRowIdentityKind::ItemPointer => (pg_sys::TIDOID, -1),
                ForeignRowIdentityKind::Attribute { attno } => {
                    let attribute_index =
                        usize::try_from(attno as i32 - 1).map_err(|_| {
                            ForeignRowIdentityError::InvalidRelationAttribute
                        })?;
                    let tuple_desc = unsafe { (*relation).rd_att };
                    // SAFETY: the live relation owns this descriptor.
                    let tuple_desc = unsafe { TupleDescHandle::from_raw(tuple_desc) };
                    let attrs = tuple_desc.attributes();
                    if attribute_index >= attrs.len() {
                        return Err(
                            ForeignRowIdentityError::InvalidRelationAttribute,
                        );
                    }
                    let attribute = &attrs[attribute_index];
                    if attribute.attisdropped
                        || unsafe { (*var).vartype } != attribute.atttypid
                        || unsafe { (*var).vartypmod } != attribute.atttypmod
                        || unsafe { (*var).varcollid } != attribute.attcollation
                    {
                        return Err(
                            ForeignRowIdentityError::RelationAttributeMismatch,
                        );
                    }
                    (attribute.atttypid, attribute.atttypmod)
                }
            };

            entries.push(RowIdentityEntry {
                plan_index,
                kind,
                type_oid,
                type_mod,
            });
        }

        Ok(Self {
            entries: entries.into_boxed_slice(),
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn entries(&self) -> &[RowIdentityEntry] {
        &self.entries
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Resolve an identity target only when its physical attno and logical
    /// attribute name both belong to the current result relation. Other leaf
    /// variants appear as NULL targets in an inherited ModifyTable tlist.
    ///
    /// # Safety
    ///
    /// `relation` must be live with the descriptor PostgreSQL created for it.
    unsafe fn kind_for_target_name(
        relation: pg_sys::Relation,
        name: &CStr,
    ) -> Result<Option<ForeignRowIdentityKind>, ForeignRowIdentityError> {
        if name == ITEM_POINTER_IDENTITY_NAME {
            return Ok(Some(ForeignRowIdentityKind::ItemPointer));
        }
        // SAFETY: guaranteed by this method's contract.
        unsafe { ATTRIBUTE_IDENTITY_NAME.decode(relation, name) }
            .map(|attno| {
                attno.map(|attno| ForeignRowIdentityKind::Attribute { attno })
            })
            .map_err(|_| ForeignRowIdentityError::InvalidIdentityName)
    }
}

/// A validated identity value read from one row's plan slot.
#[derive(Clone, Copy)]
pub enum ForeignRowIdentity<'slot> {
    ItemPointer(ValidItemPointer),
    Attribute {
        attno: pg_sys::AttrNumber,
        value: PgDatumRef<'slot>,
    },
}

/// Borrowed view of the ModifyTable plan slot and its cached identity layout.
///
/// The framework also uses this view to read validated plan values for DELETE
/// return projections; row identity is the provider-facing part of the view,
/// not its complete representation.
pub struct ModifyPlanSlot<'slot> {
    slot: *mut pg_sys::TupleTableSlot,
    layout: &'slot RowIdentityLayout,
    _marker: PhantomData<&'slot pg_sys::TupleTableSlot>,
}

impl<'slot> ModifyPlanSlot<'slot> {
    /// Construct a plan-slot view from PostgreSQL's current modify row.
    ///
    /// # Safety
    ///
    /// `slot` must be live for `'slot` and contain the targetlist represented
    /// by `layout`. Its Datum arrays must remain valid for the returned view.
    pub(crate) unsafe fn from_raw_unchecked(
        slot: *mut pg_sys::TupleTableSlot,
        layout: &'slot RowIdentityLayout,
    ) -> Self {
        debug_assert!(!slot.is_null());
        debug_assert!(!unsafe { (*slot).tts_values }.is_null());
        debug_assert!(!unsafe { (*slot).tts_isnull }.is_null());
        debug_assert_eq!(
            unsafe { (*slot).tts_flags as u32 } & pg_sys::TTS_FLAG_EMPTY,
            0
        );
        Self {
            slot,
            layout,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn identity_count(&self) -> usize {
        self.layout.len()
    }

    /// Read a plan-slot Datum whose index was resolved from the final target
    /// layout during Begin.
    ///
    /// # Safety
    ///
    /// `plan_index` must name an attribute in the slot's plan targetlist.
    pub(crate) unsafe fn datum_at(&self, plan_index: usize) -> (pg_sys::Datum, bool) {
        let mut is_null = false;
        let datum = unsafe {
            pg_sys::slot_getattr(self.slot, (plan_index + 1) as c_int, &mut is_null)
        };
        (datum, is_null)
    }

    /// Read one identity by its stable targetlist order. The plan index was
    /// resolved during Begin; PostgreSQL deforms only as far as this junk
    /// attribute when the slot has not already done so.
    pub fn identity(
        &self,
        index: usize,
    ) -> Result<ForeignRowIdentity<'slot>, ForeignRowIdentityError> {
        let entry = self
            .layout
            .entries()
            .get(index)
            .copied()
            .ok_or(ForeignRowIdentityError::IdentityIndexOutOfRange)?;
        let (datum, is_null) = unsafe { self.datum_at(entry.plan_index()) };
        let value = PgDatumRef::from_parts(
            datum,
            is_null,
            entry.type_oid(),
            entry.type_mod(),
            entry.plan_index(),
        );
        match entry.kind() {
            ForeignRowIdentityKind::ItemPointer => {
                if value.is_null() {
                    return Err(ForeignRowIdentityError::NullIdentity);
                }
                let raw = unsafe { pg_sys::DatumGetItemPointer(value.datum()) };
                if raw.is_null() || !unsafe { pg_sys::ItemPointerIsValid(raw) } {
                    return Err(ForeignRowIdentityError::InvalidItemPointer);
                }
                Ok(ForeignRowIdentity::ItemPointer(unsafe {
                    ValidItemPointer::from_raw(raw)
                }))
            }
            ForeignRowIdentityKind::Attribute { attno } => {
                Ok(ForeignRowIdentity::Attribute { attno, value })
            }
        }
    }
}

/// Error raised when a row identity plan or value violates the framework
/// contract.
#[derive(Debug, thiserror::Error)]
pub enum ForeignRowIdentityError {
    #[error("FDW row identity requirement has an unknown wire value")]
    InvalidRequirement,
    #[error("FDW modify subplan has no targetlist")]
    MissingTargetList,
    #[error("FDW modify subplan has a malformed TargetEntry")]
    MalformedTargetEntry,
    #[error("FDW modify subplan targetlist has a negative length")]
    NegativeTargetListLength,
    #[error("FDW row identity target has an invalid plan attribute number")]
    InvalidPlanAttribute,
    #[error("FDW row identity target has a duplicate plan attribute number")]
    DuplicatePlanAttribute,
    #[error("FDW row identity target has a malformed expression")]
    MalformedIdentityExpression,
    #[error("FDW row identity relation attribute is not positive")]
    InvalidRelationAttribute,
    #[error("FDW row identity target does not match its relation attribute")]
    RelationAttributeMismatch,
    #[error("FDW row identity name is invalid")]
    InvalidIdentityName,
    #[error("FDW modify callback received no plan slot")]
    MissingPlanSlot,
    #[error("FDW modify plan slot has an invalid tuple layout")]
    MalformedPlanSlot,
    #[error("FDW row identity index is outside the cached layout")]
    IdentityIndexOutOfRange,
    #[error("FDW row identity is NULL")]
    NullIdentity,
    #[error("FDW row identity contains an invalid item pointer")]
    InvalidItemPointer,
}
