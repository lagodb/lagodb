//! Row identity contracts shared by FDW scan and modify execution.

use core::ffi::{CStr, c_int};
use core::marker::PhantomData;
use std::ffi::CString;
use std::str;

use pgrx::pg_sys::{self, INDEX_VAR, INNER_VAR, OUTER_VAR};

use crate::handles::ValidItemPointer;
use crate::tuple::PgDatumRef;

const ATTRIBUTE_IDENTITY_PREFIX: &str = "__lagodb_fdw_identity_attr_";
const ITEM_POINTER_IDENTITY_NAME: &CStr = c"__lagodb_fdw_identity_ctid";

/// Scan-side request for the special system identity representation produced
/// by a modify-purpose scan.
///
/// Positive relation attributes are ordinary scan columns and therefore do
/// not need a separate scan writer mode. Only an ItemPointer identity needs a
/// physical tuple representation in PG17's `TTSOpsHeapTuple` slot.
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
    max_plan_index: Option<usize>,
}

impl RowIdentityLayout {
    pub(crate) fn empty() -> Self {
        Self {
            entries: Vec::new().into_boxed_slice(),
            max_plan_index: None,
        }
    }

    /// # Safety
    ///
    /// `targetlist` must be a live ModifyTable subplan targetlist whose nodes
    /// remain valid for the resulting executor state.
    pub(crate) unsafe fn from_targetlist(
        targetlist: *mut pg_sys::List,
        relation: pg_sys::Relation,
        rtindex: pg_sys::Index,
    ) -> Result<Self, ForeignRowIdentityError> {
        if targetlist.is_null()
            || relation.is_null()
            || rtindex == 0
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
            let Some(kind) = Self::kind_for_name(name)? else {
                continue;
            };
            let expr = unsafe { (*target_entry).expr };
            if expr.is_null() || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var {
                return Err(ForeignRowIdentityError::MalformedIdentityExpression);
            }
            let var = expr.cast::<pg_sys::Var>();
            if unsafe { (*var).varlevelsup } != 0 {
                return Err(ForeignRowIdentityError::MalformedIdentityExpression);
            }
            let source_rtindex = unsafe {
                if (*var).varno == INNER_VAR
                    || (*var).varno == OUTER_VAR
                    || (*var).varno == INDEX_VAR
                {
                    Some((*var).varnosyn)
                } else {
                    pg_sys::Index::try_from((*var).varno)
                        .ok()
                        .filter(|source| *source != 0)
                }
            }
            .ok_or(ForeignRowIdentityError::MalformedIdentityExpression)?;
            if source_rtindex != rtindex {
                continue;
            }
            let plan_attno = unsafe { (*target_entry).resno };
            if plan_attno <= 0 {
                return Err(ForeignRowIdentityError::InvalidPlanAttribute);
            }
            let plan_index = usize::try_from(plan_attno as i32 - 1)
                .map_err(|_| ForeignRowIdentityError::InvalidPlanAttribute)?;
            if plan_index >= length as usize {
                return Err(ForeignRowIdentityError::PlanAttributeOutOfRange);
            }
            if entries
                .iter()
                .any(|entry: &RowIdentityEntry| entry.plan_index == plan_index)
            {
                return Err(ForeignRowIdentityError::DuplicatePlanAttribute);
            }

            let source_attno = match kind {
                ForeignRowIdentityKind::ItemPointer => {
                    pg_sys::SelfItemPointerAttributeNumber as pg_sys::AttrNumber
                }
                ForeignRowIdentityKind::Attribute { attno } => attno,
            };
            let is_direct_relation_var = unsafe {
                (*var).varno == rtindex as c_int && (*var).varattno == source_attno
            };
            let is_join_output_var = unsafe {
                ((*var).varno == INNER_VAR || (*var).varno == OUTER_VAR)
                    && (*var).varnosyn == rtindex
                    && (*var).varattnosyn == source_attno
            };
            // A projected ForeignScan exposes its output tlist through
            // INDEX_VAR.  In that shape the original relation Var metadata is
            // consumed by setrefs, so the internal identity name plus the
            // type checks below are the remaining stable contract.
            let is_projected_scan_var = unsafe {
                (*var).varno == INDEX_VAR
                    && (*var).varattno > 0
                    && (*var).varnosyn == rtindex
                    && (*var).varattnosyn == source_attno
            };
            if !(is_direct_relation_var
                || is_join_output_var
                || is_projected_scan_var)
            {
                return Err(ForeignRowIdentityError::MalformedIdentityExpression);
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
                    let natts = unsafe { (*tuple_desc).natts };
                    if natts < 0 || unsafe { (*tuple_desc).attrs.as_ptr().is_null() }
                    {
                        return Err(
                            ForeignRowIdentityError::InvalidRelationAttribute,
                        );
                    }
                    let natts = natts as usize;
                    if attribute_index >= natts {
                        return Err(
                            ForeignRowIdentityError::InvalidRelationAttribute,
                        );
                    }
                    let attribute = unsafe {
                        &*(*tuple_desc).attrs.as_ptr().add(attribute_index)
                    };
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

        let max_plan_index = entries.iter().map(|entry| entry.plan_index).max();
        Ok(Self {
            entries: entries.into_boxed_slice(),
            max_plan_index,
        })
    }

    /// Validate the executor descriptor once, after the subplan has been
    /// initialized. The descriptor is stable for all rows of this modify
    /// operation, so row callbacks do not repeat identity type checks.
    ///
    /// # Safety
    ///
    /// `tuple_desc` must be the initialized result descriptor of the modify
    /// subplan represented by this layout.
    pub(crate) unsafe fn validate_tuple_desc(
        &self,
        tuple_desc: pg_sys::TupleDesc,
    ) -> Result<(), ForeignRowIdentityError> {
        if tuple_desc.is_null() {
            return Err(ForeignRowIdentityError::MalformedPlanSlot);
        }
        let natts = unsafe { (*tuple_desc).natts };
        if natts < 0
            || (natts > 0 && unsafe { (*tuple_desc).attrs.as_ptr().is_null() })
        {
            return Err(ForeignRowIdentityError::MalformedPlanSlot);
        }
        let natts = natts as usize;
        if let Some(max_plan_index) = self.max_plan_index
            && max_plan_index >= natts
        {
            return Err(ForeignRowIdentityError::PlanAttributeOutOfRange);
        }
        for entry in &self.entries {
            let attribute =
                unsafe { &*(*tuple_desc).attrs.as_ptr().add(entry.plan_index) };
            if attribute.atttypid != entry.type_oid
                || attribute.atttypmod != entry.type_mod
            {
                return Err(ForeignRowIdentityError::IdentityTypeMismatch);
            }
        }
        Ok(())
    }

    /// Check the planner targetlist for the framework-owned ItemPointer row
    /// identity. This runs after `AddForeignUpdateTargets` and before the
    /// foreign scan plan is encoded, so the scan contract follows the
    /// provider's explicit identity registration instead of the command type.
    ///
    /// # Safety
    ///
    /// `targetlist` must be a live planner targetlist owned by `root`'s
    /// planner memory context. Its nodes must remain valid for this call.
    pub(crate) unsafe fn has_item_pointer_identity_in_targetlist(
        targetlist: *mut pg_sys::List,
        rtindex: pg_sys::Index,
    ) -> Result<bool, ForeignRowIdentityError> {
        if rtindex == 0 {
            return Err(ForeignRowIdentityError::MissingTargetList);
        }
        // A DELETE has no user target entries. Its processed targetlist stays
        // NIL when the FDW does not register a row identity, which is a valid
        // planner state (and is later rejected by modify capability checks).
        if targetlist.is_null() {
            return Ok(false);
        }
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
            if name != ITEM_POINTER_IDENTITY_NAME {
                continue;
            }
            let expr = unsafe { (*target_entry).expr };
            if expr.is_null() || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var {
                return Err(ForeignRowIdentityError::MalformedIdentityExpression);
            }
            let var = expr.cast::<pg_sys::Var>();
            if unsafe {
                (*var).varlevelsup != 0
                    || (*var).varno != rtindex as c_int
                    || (*var).varattno
                        != pg_sys::SelfItemPointerAttributeNumber
                            as pg_sys::AttrNumber
                    || (*var).vartype != pg_sys::TIDOID
                    || (*var).vartypmod != -1
                    || (*var).varcollid != pg_sys::InvalidOid
            } {
                return Err(ForeignRowIdentityError::MalformedIdentityExpression);
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Build the framework-owned row identity name for a positive relation
    /// attribute.  This runs in the planner, where a small CString allocation
    /// is part of PostgreSQL's Node-list construction rather than row work.
    pub(crate) fn attribute_identity_name(
        attno: pg_sys::AttrNumber,
    ) -> Result<CString, ForeignRowIdentityError> {
        if attno <= 0 {
            return Err(ForeignRowIdentityError::InvalidRelationAttribute);
        }
        CString::new(format!("{}{}", ATTRIBUTE_IDENTITY_PREFIX, attno))
            .map_err(|_| ForeignRowIdentityError::InvalidIdentityName)
    }

    pub(crate) const fn item_pointer_identity_name() -> &'static CStr {
        ITEM_POINTER_IDENTITY_NAME
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

    fn kind_for_name(
        name: &CStr,
    ) -> Result<Option<ForeignRowIdentityKind>, ForeignRowIdentityError> {
        if name == ITEM_POINTER_IDENTITY_NAME {
            return Ok(Some(ForeignRowIdentityKind::ItemPointer));
        }
        let bytes = name.to_bytes();
        if !bytes.starts_with(ATTRIBUTE_IDENTITY_PREFIX.as_bytes()) {
            return Ok(None);
        }
        let digits = &bytes[ATTRIBUTE_IDENTITY_PREFIX.len()..];
        if digits.is_empty() {
            return Err(ForeignRowIdentityError::InvalidIdentityName);
        }
        let attno = str::from_utf8(digits)
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .and_then(|value| pg_sys::AttrNumber::try_from(value).ok())
            .filter(|attno| *attno > 0)
            .ok_or(ForeignRowIdentityError::InvalidRelationAttribute)?;
        Ok(Some(ForeignRowIdentityKind::Attribute { attno }))
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
    tuple_desc: pg_sys::TupleDesc,
    layout: &'slot RowIdentityLayout,
    _marker: PhantomData<&'slot pg_sys::TupleTableSlot>,
}

impl<'slot> ModifyPlanSlot<'slot> {
    /// Construct a plan-slot view after Begin-time validation.
    ///
    /// # Safety
    ///
    /// `slot` and `tuple_desc` must be live for `'slot`. Begin must have
    /// validated `tuple_desc` against `layout`, and PostgreSQL must be invoking
    /// this callback with that descriptor's initialized, non-empty plan slot.
    /// The slot's Datum arrays must remain valid for the returned view.
    pub(crate) unsafe fn from_raw_unchecked(
        slot: *mut pg_sys::TupleTableSlot,
        layout: &'slot RowIdentityLayout,
        tuple_desc: pg_sys::TupleDesc,
    ) -> Self {
        debug_assert!(!slot.is_null());
        debug_assert!(!tuple_desc.is_null());
        debug_assert_eq!(unsafe { (*slot).tts_tupleDescriptor }, tuple_desc);
        debug_assert!(!unsafe { (*slot).tts_values }.is_null());
        debug_assert!(!unsafe { (*slot).tts_isnull }.is_null());
        debug_assert_eq!(
            unsafe { (*slot).tts_flags as u32 } & pg_sys::TTS_FLAG_EMPTY,
            0
        );
        Self {
            slot,
            tuple_desc,
            layout,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn identity_count(&self) -> usize {
        self.layout.len()
    }

    /// Read a plan-slot Datum whose index was validated while building the
    /// final target layout and during Begin.
    ///
    /// # Safety
    ///
    /// `plan_index` must be less than the slot descriptor's attribute count.
    pub(crate) unsafe fn datum_at(&self, plan_index: usize) -> (pg_sys::Datum, bool) {
        debug_assert!(plan_index < unsafe { (*self.tuple_desc).natts as usize });
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
    #[error("FDW row identity plan attribute is outside the plan slot")]
    PlanAttributeOutOfRange,
    #[error("FDW row identity is NULL")]
    NullIdentity,
    #[error("FDW row identity has an unexpected PostgreSQL type")]
    IdentityTypeMismatch,
    #[error("FDW row identity contains an invalid item pointer")]
    InvalidItemPointer,
}
