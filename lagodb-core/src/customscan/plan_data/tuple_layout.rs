//! Base-scan tuple plan-data layout and its executor-side contract.

use core::ffi::c_int;
use core::ptr;
use std::collections::HashSet;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::customscan::plan_data::EnvelopeError;

const LAYOUT_RELATION: i32 = 0;
const LAYOUT_PROJECTED_BASE: i32 = 1;
const LAYOUT_RELATION_PRUNED: i32 = 2;
const LAYOUT_ROW_ONLY: i32 = 3;

pub const WHOLEROW_NAME: &std::ffi::CStr = c"wholerow";

/// Borrowed storage-column requirement exposed to scan providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeededColumns<'a> {
    /// The provider must materialize every live user column.
    All,
    /// Base-relation attribute numbers in raw scan-tuple order.
    Subset(&'a [pg_sys::AttrNumber]),
}

/// Opaque raw scan-tuple contract.
///
/// The representation stays private so future metadata/computed/join layouts
/// can be added without spreading enum matches across builders, planners,
/// and providers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanTupleLayout {
    kind: ScanTupleLayoutKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ScanTupleLayoutKind {
    Relation {
        /// When `Some`, only these base attnos are referenced by expressions;
        /// the provider may read just this subset even though the slot is
        /// full-width. When `None`, the provider must read all columns
        /// (whole-row Var or analysis could not determine the set).
        storage_attnos: Option<Box<[pg_sys::AttrNumber]>>,
    },
    ProjectedBase {
        attnos_by_resno: Box<[pg_sys::AttrNumber]>,
    },
    RowOnly,
}

impl ScanTupleLayout {
    pub(crate) fn relation() -> Self {
        Self {
            kind: ScanTupleLayoutKind::Relation {
                storage_attnos: None,
            },
        }
    }

    pub(crate) fn relation_with_storage_hint(
        attnos: Option<Vec<pg_sys::AttrNumber>>,
    ) -> Self {
        Self {
            kind: ScanTupleLayoutKind::Relation {
                storage_attnos: attnos.map(Vec::into_boxed_slice),
            },
        }
    }

    pub(crate) fn projected_base(attnos_by_resno: Vec<pg_sys::AttrNumber>) -> Self {
        Self {
            kind: ScanTupleLayoutKind::ProjectedBase {
                attnos_by_resno: attnos_by_resno.into_boxed_slice(),
            },
        }
    }

    pub(crate) fn row_only() -> Self {
        Self {
            kind: ScanTupleLayoutKind::RowOnly,
        }
    }

    pub(crate) fn is_row_only(&self) -> bool {
        matches!(&self.kind, ScanTupleLayoutKind::RowOnly)
    }

    /// Columns the provider must read, without rebuilding expression usage at
    /// executor start.
    pub fn required_columns(&self) -> NeededColumns<'_> {
        match &self.kind {
            ScanTupleLayoutKind::Relation {
                storage_attnos: Some(attnos),
            } => NeededColumns::Subset(attnos),
            ScanTupleLayoutKind::Relation {
                storage_attnos: None,
            } => NeededColumns::All,
            ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                NeededColumns::Subset(attnos_by_resno)
            }
            ScanTupleLayoutKind::RowOnly => NeededColumns::Subset(&[]),
        }
    }

    /// Resolve a base attribute to its zero-based destination in the raw scan
    /// slot. Bounds against the actual descriptor are applied by
    /// [`ScanTupleDescriptor::destination_for_attno`].
    fn destination_for_attno(&self, attno: pg_sys::AttrNumber) -> Option<usize> {
        if attno <= 0 {
            return None;
        }
        match &self.kind {
            ScanTupleLayoutKind::Relation { .. } => Some(attno as usize - 1),
            ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                attnos_by_resno.iter().position(|&source| source == attno)
            }
            ScanTupleLayoutKind::RowOnly => None,
        }
    }

    pub(crate) unsafe fn encode_wire(&self) -> *mut pg_sys::List {
        unsafe {
            let mut wire = ptr::null_mut();
            match &self.kind {
                ScanTupleLayoutKind::Relation {
                    storage_attnos: None,
                } => {
                    wire = pg_sys::lappend_int(wire, LAYOUT_RELATION);
                }
                ScanTupleLayoutKind::Relation {
                    storage_attnos: Some(attnos),
                } => {
                    wire = pg_sys::lappend_int(wire, LAYOUT_RELATION_PRUNED);
                    for &attno in attnos.iter() {
                        wire = pg_sys::lappend_int(wire, attno as i32);
                    }
                }
                ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                    wire = pg_sys::lappend_int(wire, LAYOUT_PROJECTED_BASE);
                    for &attno in attnos_by_resno.iter() {
                        wire = pg_sys::lappend_int(wire, attno as i32);
                    }
                }
                ScanTupleLayoutKind::RowOnly => {
                    wire = pg_sys::lappend_int(wire, LAYOUT_ROW_ONLY);
                }
            }
            wire
        }
    }

    pub(crate) unsafe fn decode_wire(
        wire: *mut pg_sys::List,
        field: i32,
    ) -> Result<Self, EnvelopeError> {
        if wire.is_null() {
            return Err(EnvelopeError::MalformedTupleLayout {
                reason: "layout list is NULL",
            });
        }
        if unsafe { (*wire).type_ } != pg_sys::NodeTag::T_IntList {
            return Err(EnvelopeError::WrongNodeTag {
                field,
                expected: pg_sys::NodeTag::T_IntList,
                found: unsafe { (*wire).type_ },
            });
        }
        let len = unsafe { pg_sys::list_length(wire) } as usize;
        if len == 0 {
            return Err(EnvelopeError::MalformedTupleLayout {
                reason: "layout list has no kind tag",
            });
        }
        let kind = unsafe { pg_sys::list_nth_int(wire, 0) };
        match kind {
            LAYOUT_RELATION if len == 1 => Ok(Self::relation()),
            LAYOUT_RELATION => Err(EnvelopeError::MalformedTupleLayout {
                reason: "relation layout contains trailing data",
            }),
            LAYOUT_RELATION_PRUNED => {
                let attnos = unsafe { decode_attno_tail(wire, len)? };
                Ok(Self::relation_with_storage_hint(Some(attnos)))
            }
            LAYOUT_PROJECTED_BASE if len == 1 => {
                Err(EnvelopeError::MalformedTupleLayout {
                    reason: "projected base layout is empty",
                })
            }
            LAYOUT_PROJECTED_BASE => {
                let attnos = unsafe { decode_attno_tail(wire, len)? };
                Ok(Self::projected_base(attnos))
            }
            LAYOUT_ROW_ONLY if len == 1 => Ok(Self::row_only()),
            LAYOUT_ROW_ONLY => Err(EnvelopeError::MalformedTupleLayout {
                reason: "row-only layout contains trailing data",
            }),
            value => Err(EnvelopeError::UnknownTupleLayoutKind { value }),
        }
    }

    pub(crate) unsafe fn validate_executor(
        &self,
        cscan: *mut pg_sys::CustomScan,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<(), CustomScanError> {
        // ExecInitCustomScan supplies live `cscan`/`slot` pointers and sets
        // `slot->tts_tupleDescriptor` before the provider begin callback.
        let tlist = unsafe { (*cscan).custom_scan_tlist };
        let tuple_desc = unsafe { (*slot).tts_tupleDescriptor };

        match &self.kind {
            ScanTupleLayoutKind::Relation { .. } => {
                if !tlist.is_null() {
                    return Err(CustomScanError::framework(
                        "customscan tuple-layout invariant violated: relation layout has a non-NIL custom_scan_tlist",
                    ));
                }
            }
            ScanTupleLayoutKind::ProjectedBase { attnos_by_resno } => {
                if tlist.is_null() {
                    return Err(CustomScanError::framework(
                        "customscan tuple-layout invariant violated: projected base layout has a NIL custom_scan_tlist",
                    ));
                }
                let tlist_len = unsafe { pg_sys::list_length(tlist) } as usize;
                let slot_width = unsafe { (*tuple_desc).natts } as usize;
                if tlist_len != attnos_by_resno.len()
                    || slot_width != attnos_by_resno.len()
                {
                    return Err(CustomScanError::framework(
                        "customscan tuple-layout invariant violated: layout, custom_scan_tlist, and scan slot widths differ",
                    ));
                }
                let scan_relid = unsafe { (*cscan).scan.scanrelid } as c_int;
                for (index, &attno) in attnos_by_resno.iter().enumerate() {
                    let tle = unsafe { pg_sys::list_nth(tlist, index as i32) }
                        as *mut pg_sys::TargetEntry;
                    if tle.is_null()
                        || unsafe { (*tle).xpr.type_ }
                            != pg_sys::NodeTag::T_TargetEntry
                        || unsafe { (*tle).resno as usize } != index + 1
                    {
                        return Err(CustomScanError::framework(
                            "customscan tuple-layout invariant violated: custom_scan_tlist entries are not contiguous TargetEntry nodes",
                        ));
                    }
                    let expr = unsafe { (*tle).expr };
                    if expr.is_null()
                        || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var
                    {
                        return Err(CustomScanError::framework(
                            "customscan tuple-layout invariant violated: custom_scan_tlist contains a non-Var expression",
                        ));
                    }
                    let var = expr.cast::<pg_sys::Var>();
                    if unsafe { (*var).varno } != scan_relid
                        || unsafe { (*var).varattno } != attno
                        || unsafe { (*var).varlevelsup } != 0
                    {
                        return Err(CustomScanError::framework(
                            "customscan tuple-layout invariant violated: custom_scan_tlist Var does not match the encoded base attribute",
                        ));
                    }
                }
            }
            ScanTupleLayoutKind::RowOnly => {
                if tlist.is_null()
                    || unsafe { pg_sys::list_length(tlist) } != 1
                    || unsafe { (*tuple_desc).natts } != 1
                {
                    return Err(CustomScanError::framework(
                        "customscan tuple-layout invariant violated: row-only layout must have one synthetic scan column",
                    ));
                }
                let tle =
                    unsafe { pg_sys::list_nth(tlist, 0) } as *mut pg_sys::TargetEntry;
                if tle.is_null()
                    || unsafe { (*tle).xpr.type_ } != pg_sys::NodeTag::T_TargetEntry
                    || unsafe { (*tle).resno } != 1
                    || !unsafe { (*tle).resjunk }
                {
                    return Err(CustomScanError::framework(
                        "customscan tuple-layout invariant violated: row-only scan tlist has an invalid TargetEntry",
                    ));
                }
                let expression = unsafe { (*tle).expr };
                if expression.is_null()
                    || unsafe { (*expression).type_ } != pg_sys::NodeTag::T_Const
                {
                    return Err(CustomScanError::framework(
                        "customscan tuple-layout invariant violated: row-only scan tlist is not a Const",
                    ));
                }
                let constant = expression.cast::<pg_sys::Const>();
                if unsafe { (*constant).consttype } != pg_sys::INT4OID
                    || unsafe { (*constant).consttypmod } != -1
                    || unsafe { (*constant).constcollid } != pg_sys::InvalidOid
                    || !unsafe { (*constant).constisnull }
                {
                    return Err(CustomScanError::framework(
                        "customscan tuple-layout invariant violated: row-only Const does not match its contract",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Initialize the synthetic row-only cell once. `ExecClearTuple` preserves
    /// the slot arrays, so providers can publish each row without a per-row
    /// branch or write while still exposing a valid SQL NULL cell.
    pub(crate) unsafe fn initialize_executor_slot(
        &self,
        slot: *mut pg_sys::TupleTableSlot,
    ) {
        if self.is_row_only() {
            unsafe {
                *(*slot).tts_values = pg_sys::Datum::from(0);
                *(*slot).tts_isnull = true;
            }
        }
    }
}

/// Decode the `[1..len]` tail of a wire IntList as deduplicated positive AttrNumbers.
unsafe fn decode_attno_tail(
    wire: *mut pg_sys::List,
    len: usize,
) -> Result<Vec<pg_sys::AttrNumber>, EnvelopeError> {
    let mut seen = HashSet::with_capacity(len - 1);
    let mut attnos = Vec::with_capacity(len - 1);
    for index in 1..len {
        let raw = unsafe { pg_sys::list_nth_int(wire, index as i32) };
        let attno = pg_sys::AttrNumber::try_from(raw).map_err(|_| {
            EnvelopeError::InvalidTupleLayoutAttno {
                index: index - 1,
                value: raw,
            }
        })?;
        if attno <= 0 {
            return Err(EnvelopeError::InvalidTupleLayoutAttno {
                index: index - 1,
                value: raw,
            });
        }
        if !seen.insert(attno) {
            return Err(EnvelopeError::DuplicateTupleLayoutAttno { attno });
        }
        attnos.push(attno);
    }
    Ok(attnos)
}

impl Default for ScanTupleLayout {
    fn default() -> Self {
        Self::relation()
    }
}

/// Read-only view of the actual executor scan slot descriptor paired with the
/// decoded plan-time layout contract.
#[derive(Clone, Copy)]
pub struct ScanTupleDescriptor<'a> {
    tuple_desc: pg_sys::TupleDesc,
    layout: &'a ScanTupleLayout,
}

impl<'a> ScanTupleDescriptor<'a> {
    ///
    /// # Safety
    ///
    /// `tuple_desc` must be the live descriptor installed by
    /// `ExecInitCustomScan` for the corresponding scan slot.
    pub(crate) unsafe fn new(
        tuple_desc: pg_sys::TupleDesc,
        layout: &'a ScanTupleLayout,
    ) -> Self {
        Self { tuple_desc, layout }
    }

    /// Number of physical cells in the raw scan slot.
    pub fn len(&self) -> usize {
        unsafe { (*self.tuple_desc).natts as usize }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Base attribute to zero-based raw scan-slot destination.
    pub fn destination_for_attno(&self, attno: pg_sys::AttrNumber) -> Option<usize> {
        self.layout
            .destination_for_attno(attno)
            .filter(|&destination| destination < self.len())
    }

    /// Actual scan-slot target types, indexed by raw scan position.
    pub fn attr_types(&self) -> Vec<(pg_sys::Oid, i32)> {
        let len = self.len();
        let attrs = unsafe {
            std::slice::from_raw_parts((*self.tuple_desc).attrs.as_ptr(), len)
        };
        attrs
            .iter()
            .map(|attr| (attr.atttypid, attr.atttypmod))
            .collect()
    }
}
