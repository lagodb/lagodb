//! Non-FFI scan executor validation and layout construction.

use core::slice;

use pgrx::pg_sys;

use crate::expr::contract::ColumnRef;

use super::super::row_identity::ForeignRowIdentityRequirement;
use super::error::ForeignScanError;
use super::projection::{ColumnRequirements, ScanProjection, SlotWritePlan};
use super::slot::SlotWriteLayout;

/// Validate executor-owned scan objects once and compile the row-write layout.
///
/// # Safety
///
/// `plan`, `relation`, and `slot` must be PostgreSQL executor pointers from the
/// same live ForeignScan callback. The function validates their layout before
/// any borrowed descriptor data escapes this call.
pub(crate) unsafe fn validate_executor_layout(
    plan: *mut pg_sys::ForeignScan,
    relation: pg_sys::Relation,
    projection: &ScanProjection,
    write_plan: &SlotWritePlan,
    row_identity: ForeignRowIdentityRequirement,
    requirements: &ColumnRequirements,
    column_refs: &[ColumnRef],
    slot: *mut pg_sys::TupleTableSlot,
) -> Result<SlotWriteLayout, ForeignScanError> {
    if plan.is_null() {
        return Err(ForeignScanError::framework(
            "ForeignScan executor has no ForeignScan plan",
        ));
    }
    if slot.is_null() {
        return Err(ForeignScanError::framework(
            "ForeignScan executor did not initialize a scan slot",
        ));
    }
    // SAFETY: slot is non-null and remains executor-owned for this validation.
    if unsafe { (*slot).tts_ops } != unsafe { &pg_sys::TTSOpsHeapTuple } {
        return Err(ForeignScanError::framework(
            "ForeignScan executor scan slot is not TTSOpsHeapTuple",
        ));
    }
    let desc = unsafe { (*slot).tts_tupleDescriptor };
    if desc.is_null() {
        return Err(ForeignScanError::framework(
            "ForeignScan executor scan slot has no TupleDesc",
        ));
    }
    if unsafe { (*slot).tts_mcxt }.is_null() {
        return Err(ForeignScanError::framework(
            "ForeignScan executor scan slot has no memory context",
        ));
    }
    if relation.is_null() || unsafe { (*relation).rd_att }.is_null() {
        return Err(ForeignScanError::framework(
            "ForeignScan executor relation has no TupleDesc",
        ));
    }
    let natts = unsafe { (*desc).natts };
    if natts < 0 {
        return Err(ForeignScanError::framework(
            "FDW executor scan slot has a TupleDesc with a negative width",
        ));
    }
    let natts = natts as usize;
    let relation_desc = unsafe { (*relation).rd_att };
    let relation_natts = unsafe { (*relation_desc).natts };
    if relation_natts < 0 {
        return Err(ForeignScanError::framework(
            "FDW executor relation has a TupleDesc with a negative width",
        ));
    }
    let relation_natts = relation_natts as usize;
    if row_identity.needs_item_pointer() && !projection.is_relation() {
        return Err(ForeignScanError::framework(
            "FDW item-pointer identity requires a relation-shaped scan slot",
        ));
    }
    // SAFETY: relation_desc is live and stores relation_natts contiguous attrs.
    let relation_attrs = unsafe {
        slice::from_raw_parts((*relation_desc).attrs.as_ptr(), relation_natts)
    };
    let valid_user_attno = |attno: pg_sys::AttrNumber| {
        attno > 0
            && (attno as usize) <= relation_attrs.len()
            && !relation_attrs[attno as usize - 1].attisdropped
    };

    for attno in requirements.user_columns() {
        if !valid_user_attno(attno) {
            return Err(ForeignScanError::framework(
                "FDW private data requires an invalid or dropped relation attribute",
            ));
        }
    }
    for column_ref in column_refs {
        if !valid_user_attno(column_ref.attno)
            || !requirements.contains_user_column(column_ref.attno)
            || column_ref.atttypid
                != relation_attrs[column_ref.attno as usize - 1].atttypid
            || column_ref.attcollation
                != relation_attrs[column_ref.attno as usize - 1].attcollation
        {
            return Err(ForeignScanError::framework(
                "FDW private column-reference metadata does not match the relation descriptor",
            ));
        }
    }

    // SAFETY: plan is non-null from the function entry check and remains live.
    let tlist = unsafe { (*plan).fdw_scan_tlist };
    match projection {
        ScanProjection::Relation => {
            if !tlist.is_null() || natts != relation_natts {
                return Err(ForeignScanError::framework(
                    "FDW relation-shaped projection does not match the opened relation descriptor",
                ));
            }
        }
        ScanProjection::Projected { attnos, .. } => {
            if tlist.is_null()
                || unsafe { pg_sys::list_length(tlist) as usize } != natts
            {
                return Err(ForeignScanError::framework(
                    "FDW projected scan tuple and executor descriptor have different widths",
                ));
            }
            if attnos.len() != natts {
                return Err(ForeignScanError::framework(
                    "FDW private projected attribute count differs from the executor descriptor",
                ));
            }
            for (index, &attno) in attnos.iter().enumerate() {
                if !valid_user_attno(attno)
                    || !requirements.contains_user_column(attno)
                {
                    return Err(ForeignScanError::framework(
                        "FDW projected scan tuple contains an invalid or dropped attribute",
                    ));
                }
                // SAFETY: tlist is live and index is bounded by its length.
                let entry = unsafe { pg_sys::list_nth(tlist, index as i32) }
                    as *mut pg_sys::TargetEntry;
                if entry.is_null()
                    || unsafe { (*entry).xpr.type_ } != pg_sys::NodeTag::T_TargetEntry
                    || unsafe { (*entry).resno as usize } != index + 1
                {
                    return Err(ForeignScanError::framework(
                        "FDW fdw_scan_tlist is not a contiguous TargetEntry list",
                    ));
                }
                // SAFETY: the preceding checks establish a live TargetEntry.
                let expr = unsafe { (*entry).expr };
                if expr.is_null()
                    || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var
                    || unsafe { (*expr.cast::<pg_sys::Var>()).varattno } != attno
                    || unsafe { (*expr.cast::<pg_sys::Var>()).varlevelsup } != 0
                    || unsafe { (*expr.cast::<pg_sys::Var>()).varno as pg_sys::Index }
                        != unsafe { (*plan).scan.scanrelid }
                    || unsafe { (*expr.cast::<pg_sys::Var>()).vartype }
                        != relation_attrs[attno as usize - 1].atttypid
                    || unsafe { (*expr.cast::<pg_sys::Var>()).vartypmod }
                        != relation_attrs[attno as usize - 1].atttypmod
                    || unsafe { (*expr.cast::<pg_sys::Var>()).varcollid }
                        != relation_attrs[attno as usize - 1].attcollation
                {
                    return Err(ForeignScanError::framework(
                        "FDW fdw_scan_tlist Var does not match private projection metadata",
                    ));
                }
            }
        }
        ScanProjection::SyntheticNull => {
            if natts != 1
                || tlist.is_null()
                || unsafe { pg_sys::list_length(tlist) as usize } != 1
            {
                return Err(ForeignScanError::framework(
                    "FDW synthetic-null projection must have one scan-tlist entry",
                ));
            }
            let entry =
                unsafe { pg_sys::list_nth(tlist, 0) } as *mut pg_sys::TargetEntry;
            if entry.is_null()
                || unsafe { (*entry).xpr.type_ } != pg_sys::NodeTag::T_TargetEntry
                || unsafe { (*entry).resno } != 1
                || !unsafe { (*entry).resjunk }
            {
                return Err(ForeignScanError::framework(
                    "FDW synthetic-null scan tlist has an invalid TargetEntry",
                ));
            }
            let expr = unsafe { (*entry).expr };
            if expr.is_null() || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Const
            {
                return Err(ForeignScanError::framework(
                    "FDW synthetic-null scan tlist is not a Const",
                ));
            }
            let constant = expr.cast::<pg_sys::Const>();
            if unsafe { (*constant).consttype } != pg_sys::INT4OID
                || unsafe { (*constant).consttypmod } != -1
                || unsafe { (*constant).constcollid } != pg_sys::InvalidOid
                || !unsafe { (*constant).constisnull }
            {
                return Err(ForeignScanError::framework(
                    "FDW synthetic-null scan tlist Const does not match its contract",
                ));
            }
        }
    }
    Ok(unsafe { SlotWriteLayout::from_slot(slot, projection, write_plan) })
}

/// # Safety
///
/// If non-null, `list` must point to a live PostgreSQL List owned by the current
/// plan or executor callback.
pub(crate) unsafe fn list_len(list: *mut pg_sys::List) -> usize {
    if list.is_null() {
        0
    } else {
        unsafe { pg_sys::list_length(list) as usize }
    }
}

/// # Safety
///
/// `slot` must point to a live executor TupleTableSlot owned by the current
/// ForeignScan callback.
pub(crate) unsafe fn slot_is_empty(slot: *mut pg_sys::TupleTableSlot) -> bool {
    unsafe { ((*slot).tts_flags as u32 & pg_sys::TTS_FLAG_EMPTY) != 0 }
}
