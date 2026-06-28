//! Framework `CustomScan.custom_private` wire layout (`encode_split` /
//! `decode_private`). Positional `copyObject`-safe `List*`; provider payload
//! in cell 6. `pre_setrefs_scan_rti` is debug-only (stale after `rtoffset`).

use std::ffi::{CStr, CString};
use std::ptr;

use pgrx::pg_sys;

use crate::customscan::codec::{PrivateDataReader, PrivateDataWriter};
use crate::customscan::error::CustomScanError;
use crate::customscan::tuple_layout::ScanTupleLayout;
use crate::expr::split::{ColumnRef, PushdownContract};

// Positional indices into the top-level `T_List` payload. Both encode and
// decode reference these so the layout is described in exactly one place.
const FIELD_PROVIDER_NAME: i32 = 0;
const FIELD_RELATION_OID: i32 = 1;
const FIELD_PUSHED_COUNT: i32 = 2;
const FIELD_RECHECK_COUNT: i32 = 3;
const FIELD_PUSHED_CONTRACTS: i32 = 4;
const FIELD_COLUMN_REFS: i32 = 5;
const FIELD_PROVIDER_METADATA: i32 = 6;
const FIELD_PRE_SETREFS_SCAN_RTI: i32 = 7;
const FIELD_TUPLE_LAYOUT: i32 = 8;
const TOP_LEVEL_LEN: usize = 9;

// Wire encoding for `PushdownContract` inside the `T_IntList` at
// `FIELD_PUSHED_CONTRACTS`. Decode is exhaustive over these values.
const CONTRACT_EXACT_ROW_FILTER: i32 = 0;
const CONTRACT_CONSERVATIVE_PRUNING: i32 = 1;

// Each column_refs entry: [T_IntList(5 ints), T_String | NIL].
const COLUMN_REF_ENTRY_LEN: usize = 2;
const COLUMN_REF_SUBCELL_INTS: i32 = 0;
const COLUMN_REF_SUBCELL_NAME: i32 = 1;

// The `[0]` sub-cell of each `column_refs` entry is a `T_IntList` holding
// exactly these five fields, in this order.
const COLUMN_REF_LEN: usize = 5;
const COLUMN_REF_EXPR_INDEX: i32 = 0;
const COLUMN_REF_REL_OID: i32 = 1;
const COLUMN_REF_ATTNO: i32 = 2;
const COLUMN_REF_ATTTYPID: i32 = 3;
const COLUMN_REF_ATTCOLLATION: i32 = 4;

/// Internal codec/wire-format errors for `CustomScan.custom_private`.
///
/// These stay inside Core as typed sources. Public customscan APIs surface them
/// through [`CustomScanError`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DecodeError {
    /// Top-level `List*` was NULL.
    #[error("custom_private payload is NULL")]
    NullPayload,
    /// Top-level list length differed from [`TOP_LEVEL_LEN`].
    #[error(
        "custom_private top-level list has wrong length: found {found}, expected {expected}"
    )]
    WrongTopLevelLength { found: usize, expected: usize },
    /// A list cell that should have held a pointer-typed Node held NULL.
    #[error("custom_private cell {field} is NULL but a Node* was expected")]
    NullCell { field: i32 },
    /// A cell's `NodeTag` did not match the expected tag.
    #[error(
        "custom_private cell {field} has wrong NodeTag: found {found:?}, expected {expected:?}"
    )]
    WrongNodeTag {
        field: i32,
        expected: pg_sys::NodeTag,
        found: pg_sys::NodeTag,
    },
    /// `provider_id_or_name` (`T_String`) had a NULL `sval` pointer.
    #[error("custom_private provider_id_or_name has NULL sval")]
    NullProviderName,
    /// A `column_refs` entry (or its inner 5-int sub-cell) did not have the
    /// expected number of fields. The outer entry is a 2-cell `T_List`
    /// ([`COLUMN_REF_ENTRY_LEN`]); its `[0]` sub-cell is a 5-int `T_IntList`
    /// ([`COLUMN_REF_LEN`]).
    #[error(
        "custom_private column_refs[{entry}] has wrong length: found {found}, expected {expected}"
    )]
    MalformedColumnRef {
        entry: usize,
        found: usize,
        expected: usize,
    },
    /// Encoded `pushed_contracts` value was outside the known set.
    #[error(
        "custom_private pushed_contracts[{entry}] holds unknown encoding {value}"
    )]
    UnknownContract { entry: usize, value: i32 },
    /// A negative count was encoded for a length field.
    #[error("custom_private cell {field} encodes negative count {value}")]
    NegativeCount { field: i32, value: i32 },
    /// `pushed_contracts.len()` must equal `pushed_count`.
    #[error(
        "custom_private cross-field invariant violated: \
         pushed_contracts.len() = {pushed_contracts_len}, \
         expected to equal pushed_count = {pushed_count}"
    )]
    PushedContractsLengthMismatch {
        pushed_count: usize,
        pushed_contracts_len: usize,
    },
    /// `column_refs[i].expr_index` must be `< pushed_count`.
    #[error(
        "custom_private column_refs[{entry}].expr_index = {expr_index} \
         is out of range for pushed_count = {pushed_count}"
    )]
    ColumnRefExprIndexOutOfRange {
        entry: usize,
        expr_index: usize,
        pushed_count: usize,
    },
    /// Count exceeds `i32::MAX` (PG `Integer` is signed 32-bit).
    #[error("custom_private cannot encode count {value}: exceeds i32::MAX")]
    CountTooLargeToEncode { value: usize },
    /// A codec `read_*` was called with the cursor at or past the end of the
    /// payload. Covers the Iceberg fail-closed posture for a NULL/empty
    /// `provider_metadata` payload, where any read fails rather than
    /// substituting a default.
    #[error(
        "custom_private read past end of payload: position {position}, len {len}"
    )]
    ReadPastEnd { position: usize, len: usize },
    /// `PrivateDataReader::finish` found cells the provider never read — the
    /// payload is longer than the shape the provider decoded. Lets Core reject
    /// a payload with extra trailing cells.
    #[error(
        "custom_private payload has unexpected trailing cells: read {read}, len {len}"
    )]
    UnexpectedTrailingCells { read: usize, len: usize },
    /// `append_str` was given a string containing an interior NUL byte, which
    /// cannot be encoded as a C string. Surfaced as an error rather than
    /// silently truncating at the NUL.
    #[error(
        "custom_private cannot encode string at position {position}: contains interior NUL byte"
    )]
    StringContainsInteriorNul { position: usize },
    /// Defensive: malformed i64 in a float cell (corrupt plan tree).
    #[error("custom_private cell at position {position} holds a malformed i64 value")]
    MalformedI64Cell { position: usize },
    /// Defensive: a `T_String` cell held an `sval` that does not contain valid
    /// UTF-8. This cannot occur for a payload produced by [`PrivateDataWriter`];
    /// it fails closed against a corrupt plan tree.
    #[error(
        "custom_private cell at position {position} holds a malformed string value"
    )]
    MalformedStringCell { position: usize },
    /// Tuple-layout wire list is structurally invalid.
    #[error("custom_private tuple layout is malformed: {reason}")]
    MalformedTupleLayout { reason: &'static str },
    /// Tuple-layout kind tag is not recognized.
    #[error("custom_private tuple layout has unknown kind tag {value}")]
    UnknownTupleLayoutKind { value: i32 },
    /// Tuple-layout column lists only contain positive `AttrNumber` values.
    #[error("custom_private tuple layout attnos[{index}] has invalid value {value}")]
    InvalidTupleLayoutAttno { index: usize, value: i32 },
    /// A base attribute may appear only once in a tuple-layout column list.
    #[error("custom_private tuple layout contains duplicate base attno {attno}")]
    DuplicateTupleLayoutAttno { attno: pg_sys::AttrNumber },
}

/// Provider-extensible encode/decode for the opaque tail of `custom_private`.
///
/// Encoded lists MUST be `copyObject`-safe (PG `Node*` values only).
pub trait CustomScanPrivate: Sized {
    /// Encode provider fields via `writer.append_*`.
    fn encode(&self, writer: &mut PrivateDataWriter) -> Result<(), CustomScanError>;

    /// Decode fields previously written by `encode`.
    fn decode(reader: &mut PrivateDataReader<'_>) -> Result<Self, CustomScanError>;
}

/// Decoded framework envelope from `CustomScan.custom_private`.
#[derive(Debug)]
pub struct EncodedPrivate {
    /// Matches the registered `LakebaseCustomScanProvider::NAME`.
    pub provider_id_or_name: CString,

    /// `pg_class` OID of the scan relation.
    pub relation_oid: pg_sys::Oid,

    /// Length of the pushed section in `custom_exprs`. EXPLAIN and the
    /// executor MUST respect this boundary.
    pub pushed_count: usize,

    /// Length of the recheck section in `custom_exprs`.
    pub recheck_count: usize,

    /// Per-pushed-expression pushdown contract.
    /// Aligned by index with the pushed section of `custom_exprs`.
    pub pushed_contracts: Vec<PushdownContract>,

    /// Pre-resolved column metadata produced by relation-aware expression
    /// inspection over the pushed expressions.
    pub column_refs: Vec<ColumnRef>,

    /// Raw provider-private payload — handed to the provider's
    /// [`CustomScanPrivate::decode`] separately. NULL is permitted: a
    /// provider with no private state can ignore it.
    pub provider_metadata_raw: *mut pg_sys::List,

    /// Debug-only RTI; invalid after `rtoffset` — do not use for correctness.
    pub pre_setrefs_scan_rti: i32,

    /// Raw scan-tuple shape and base-attribute mapping.
    pub tuple_layout: ScanTupleLayout,
}

/// Encode plan-stage pushdown split into `CustomScan.custom_private`.
///
/// `provider_metadata` MUST be `copyObject`-safe (debug-asserted).
///
/// # Safety
///
/// Returns a list in the current memory context; caller hands ownership to PG.
pub unsafe fn encode_split(
    provider_id_or_name: &CStr,
    relation_oid: pg_sys::Oid,
    pushed_count: usize,
    recheck_count: usize,
    pushed_contracts: &[PushdownContract],
    column_refs: &[ColumnRef],
    provider_metadata: *mut pg_sys::List,
    pre_setrefs_scan_rti: i32,
) -> Result<*mut pg_sys::List, CustomScanError> {
    let tuple_layout = ScanTupleLayout::relation();
    unsafe {
        encode_split_with_layout(
            provider_id_or_name,
            relation_oid,
            pushed_count,
            recheck_count,
            pushed_contracts,
            column_refs,
            provider_metadata,
            pre_setrefs_scan_rti,
            &tuple_layout,
        )
    }
}

pub(crate) unsafe fn encode_split_with_layout(
    provider_id_or_name: &CStr,
    relation_oid: pg_sys::Oid,
    pushed_count: usize,
    recheck_count: usize,
    pushed_contracts: &[PushdownContract],
    column_refs: &[ColumnRef],
    provider_metadata: *mut pg_sys::List,
    pre_setrefs_scan_rti: i32,
    tuple_layout: &ScanTupleLayout,
) -> Result<*mut pg_sys::List, CustomScanError> {
    unsafe {
        encode_split_impl(
            provider_id_or_name,
            relation_oid,
            pushed_count,
            recheck_count,
            pushed_contracts,
            column_refs,
            provider_metadata,
            pre_setrefs_scan_rti,
            tuple_layout,
        )
    }
    .map_err(CustomScanError::private_codec)
}

unsafe fn encode_split_impl(
    provider_id_or_name: &CStr,
    relation_oid: pg_sys::Oid,
    pushed_count: usize,
    recheck_count: usize,
    pushed_contracts: &[PushdownContract],
    column_refs: &[ColumnRef],
    provider_metadata: *mut pg_sys::List,
    pre_setrefs_scan_rti: i32,
    tuple_layout: &ScanTupleLayout,
) -> Result<*mut pg_sys::List, DecodeError> {
    debug_assert_eq!(
        pushed_contracts.len(),
        pushed_count,
        "encode_split: pushed_contracts.len() must equal pushed_count",
    );
    // SAFETY: forwarded from this function's `unsafe` contract — the caller
    // already promised `provider_metadata` is either NULL or a valid
    // `*mut pg_sys::List`.
    unsafe {
        debug_assert_provider_metadata_safe(provider_metadata);
    }

    unsafe {
        let mut top: *mut pg_sys::List = ptr::null_mut();

        // FIELD_PROVIDER_NAME: T_String
        let name_cstring = CString::new(provider_id_or_name.to_bytes())
            .expect("provider name is already a CStr; conversion cannot fail");
        let name_pg = pg_sys::pstrdup(name_cstring.as_ptr());
        let provider_name_node = pg_sys::makeString(name_pg);
        top = pg_sys::lappend(top, provider_name_node.cast());

        // FIELD_RELATION_OID: T_Integer (Oid round-tripped via i32 bitcast)
        let relation_oid_node = pg_sys::makeInteger(relation_oid.to_u32() as i32);
        top = pg_sys::lappend(top, relation_oid_node.cast());

        // FIELD_PUSHED_COUNT: T_Integer
        let pushed_count_node = pg_sys::makeInteger(usize_to_int(pushed_count)?);
        top = pg_sys::lappend(top, pushed_count_node.cast());

        // FIELD_RECHECK_COUNT: T_Integer
        let recheck_count_node = pg_sys::makeInteger(usize_to_int(recheck_count)?);
        top = pg_sys::lappend(top, recheck_count_node.cast());

        // FIELD_PUSHED_CONTRACTS: T_IntList
        let mut contracts_list: *mut pg_sys::List = ptr::null_mut();
        for g in pushed_contracts {
            let encoded = match g {
                PushdownContract::ExactRowFilter => CONTRACT_EXACT_ROW_FILTER,
                PushdownContract::ConservativePruning => {
                    CONTRACT_CONSERVATIVE_PRUNING
                }
            };
            contracts_list = pg_sys::lappend_int(contracts_list, encoded);
        }
        top = pg_sys::lappend(top, contracts_list.cast());

        // FIELD_COLUMN_REFS: T_List of 2-cell T_List entries. Each entry is
        // `[T_IntList(5 ints), T_String | NIL]`.
        let mut column_refs_list: *mut pg_sys::List = ptr::null_mut();
        for c in column_refs {
            // Sub-cell [0]: 5-int block.
            let mut ints: *mut pg_sys::List = ptr::null_mut();
            ints = pg_sys::lappend_int(ints, usize_to_int(c.expr_index)?);
            ints = pg_sys::lappend_int(ints, c.rel_oid.to_u32() as i32);
            ints = pg_sys::lappend_int(ints, c.attno as i32);
            ints = pg_sys::lappend_int(ints, c.atttypid.to_u32() as i32);
            ints = pg_sys::lappend_int(ints, c.attcollation.to_u32() as i32);

            // Sub-cell [1]: column name or NIL.
            let name_node: *mut pg_sys::Node = match &c.name {
                Some(name) => {
                    let name_cstring = CString::new(name.as_bytes()).expect(
                        "ColumnRef::name came from a NUL-free Rust String; \
                         conversion cannot fail",
                    );
                    let name_pg = pg_sys::pstrdup(name_cstring.as_ptr());
                    pg_sys::makeString(name_pg).cast()
                }
                None => ptr::null_mut(),
            };

            // Outer 2-cell entry: [ints, name | NIL].
            let mut entry: *mut pg_sys::List = ptr::null_mut();
            entry = pg_sys::lappend(entry, ints.cast());
            entry = pg_sys::lappend(entry, name_node.cast());

            column_refs_list = pg_sys::lappend(column_refs_list, entry.cast());
        }
        top = pg_sys::lappend(top, column_refs_list.cast());

        // FIELD_PROVIDER_METADATA: T_List or NIL
        top = pg_sys::lappend(top, provider_metadata.cast());

        // FIELD_PRE_SETREFS_SCAN_RTI: T_Integer (debug-only)
        let rti_node = pg_sys::makeInteger(pre_setrefs_scan_rti);
        top = pg_sys::lappend(top, rti_node.cast());

        // FIELD_TUPLE_LAYOUT: non-empty T_IntList owned by the layout domain.
        let tuple_layout_wire = tuple_layout.encode_wire();
        top = pg_sys::lappend(top, tuple_layout_wire.cast());

        Ok(top)
    }
}

/// Fail closed if encoded provider name does not match `expected`.
pub fn assert_provider_name_matches(
    name_in_payload: &CStr,
    expected: &CStr,
) -> Result<(), CustomScanError> {
    if name_in_payload != expected {
        return Err(CustomScanError::provider_name_mismatch(
            expected.to_owned(),
            name_in_payload.to_owned(),
        ));
    }
    Ok(())
}

/// Decode top-level `custom_private` from [`encode_split`].
///
/// # Safety
///
/// `list` must be a valid plan-tree `List*` (not owned by caller).
pub unsafe fn decode_private(
    list: *mut pg_sys::List,
) -> Result<EncodedPrivate, CustomScanError> {
    unsafe { decode_private_impl(list) }.map_err(CustomScanError::private_codec)
}

unsafe fn decode_private_impl(
    list: *mut pg_sys::List,
) -> Result<EncodedPrivate, DecodeError> {
    if list.is_null() {
        return Err(DecodeError::NullPayload);
    }

    let len = unsafe { (*list).length } as usize;
    if len != TOP_LEVEL_LEN {
        return Err(DecodeError::WrongTopLevelLength {
            found: len,
            expected: TOP_LEVEL_LEN,
        });
    }

    // Helper: read cell `i` as `*mut pg_sys::Node`, NULL-checked.
    let cell_node = |idx: i32| -> Result<*mut pg_sys::Node, DecodeError> {
        let ptr = unsafe { pg_sys::list_nth(list, idx) } as *mut pg_sys::Node;
        if ptr.is_null() {
            return Err(DecodeError::NullCell { field: idx });
        }
        Ok(ptr)
    };

    // FIELD_PROVIDER_NAME: T_String
    let provider_name_node = cell_node(FIELD_PROVIDER_NAME)?;
    let provider_id_or_name = unsafe {
        expect_node_tag(
            provider_name_node,
            pg_sys::NodeTag::T_String,
            FIELD_PROVIDER_NAME,
        )?;
        let s = provider_name_node.cast::<pg_sys::String>();
        let sval = (*s).sval;
        if sval.is_null() {
            return Err(DecodeError::NullProviderName);
        }
        CStr::from_ptr(sval).to_owned()
    };

    // FIELD_RELATION_OID: T_Integer (Oid round-tripped via i32 bitcast)
    let relation_oid = unsafe {
        let raw = read_integer_cell(list, FIELD_RELATION_OID)?;
        pg_sys::Oid::from(raw as u32)
    };

    // FIELD_PUSHED_COUNT: T_Integer
    let pushed_count_raw = unsafe { read_integer_cell(list, FIELD_PUSHED_COUNT)? };
    if pushed_count_raw < 0 {
        return Err(DecodeError::NegativeCount {
            field: FIELD_PUSHED_COUNT,
            value: pushed_count_raw,
        });
    }
    let pushed_count = pushed_count_raw as usize;

    // FIELD_RECHECK_COUNT: T_Integer
    let recheck_count_raw = unsafe { read_integer_cell(list, FIELD_RECHECK_COUNT)? };
    if recheck_count_raw < 0 {
        return Err(DecodeError::NegativeCount {
            field: FIELD_RECHECK_COUNT,
            value: recheck_count_raw,
        });
    }
    let recheck_count = recheck_count_raw as usize;

    // FIELD_PUSHED_CONTRACTS: T_IntList (NIL == empty list, PG convention)
    let contracts_raw_ptr = unsafe { pg_sys::list_nth(list, FIELD_PUSHED_CONTRACTS) };
    let mut pushed_contracts: Vec<PushdownContract> = Vec::new();
    if !contracts_raw_ptr.is_null() {
        let contracts_list = contracts_raw_ptr.cast::<pg_sys::List>();
        unsafe {
            expect_list_tag(
                contracts_list,
                pg_sys::NodeTag::T_IntList,
                FIELD_PUSHED_CONTRACTS,
            )?;
        }
        let contracts_len = unsafe { (*contracts_list).length } as usize;
        pushed_contracts.reserve(contracts_len);
        for i in 0..contracts_len {
            let v = unsafe { pg_sys::list_nth_int(contracts_list, i as i32) };
            let g = match v {
                CONTRACT_EXACT_ROW_FILTER => PushdownContract::ExactRowFilter,
                CONTRACT_CONSERVATIVE_PRUNING => {
                    PushdownContract::ConservativePruning
                }
                other => {
                    return Err(DecodeError::UnknownContract {
                        entry: i,
                        value: other,
                    });
                }
            };
            pushed_contracts.push(g);
        }
    }

    // FIELD_COLUMN_REFS: T_List of 2-cell T_List entries (NIL == empty list).
    // Each entry is `[T_IntList(5 ints), T_String | NIL]`.
    let column_refs_raw_ptr = unsafe { pg_sys::list_nth(list, FIELD_COLUMN_REFS) };
    let mut column_refs: Vec<ColumnRef> = Vec::new();
    if !column_refs_raw_ptr.is_null() {
        let column_refs_list = column_refs_raw_ptr.cast::<pg_sys::List>();
        unsafe {
            expect_list_tag(
                column_refs_list,
                pg_sys::NodeTag::T_List,
                FIELD_COLUMN_REFS,
            )?;
        }
        let column_refs_len = unsafe { (*column_refs_list).length } as usize;
        column_refs.reserve(column_refs_len);
        for i in 0..column_refs_len {
            // Outer entry: a 2-cell `T_List`.
            let entry_ptr = unsafe { pg_sys::list_nth(column_refs_list, i as i32) }
                as *mut pg_sys::List;
            if entry_ptr.is_null() {
                return Err(DecodeError::NullCell {
                    field: FIELD_COLUMN_REFS,
                });
            }
            unsafe {
                expect_list_tag(
                    entry_ptr,
                    pg_sys::NodeTag::T_List,
                    FIELD_COLUMN_REFS,
                )?;
            }
            let entry_len = unsafe { (*entry_ptr).length } as usize;
            if entry_len != COLUMN_REF_ENTRY_LEN {
                return Err(DecodeError::MalformedColumnRef {
                    entry: i,
                    found: entry_len,
                    expected: COLUMN_REF_ENTRY_LEN,
                });
            }

            // Sub-cell [0]: the 5-int `T_IntList` block.
            let ints_ptr =
                unsafe { pg_sys::list_nth(entry_ptr, COLUMN_REF_SUBCELL_INTS) }
                    as *mut pg_sys::List;
            if ints_ptr.is_null() {
                return Err(DecodeError::NullCell {
                    field: FIELD_COLUMN_REFS,
                });
            }
            unsafe {
                expect_list_tag(
                    ints_ptr,
                    pg_sys::NodeTag::T_IntList,
                    FIELD_COLUMN_REFS,
                )?;
            }
            let ints_len = unsafe { (*ints_ptr).length } as usize;
            if ints_len != COLUMN_REF_LEN {
                return Err(DecodeError::MalformedColumnRef {
                    entry: i,
                    found: ints_len,
                    expected: COLUMN_REF_LEN,
                });
            }
            let read = |slot: i32| -> i32 {
                unsafe { pg_sys::list_nth_int(ints_ptr, slot) }
            };

            // Sub-cell [1]: the column name as `T_String`, or NIL → `None`.
            let name_node =
                unsafe { pg_sys::list_nth(entry_ptr, COLUMN_REF_SUBCELL_NAME) }
                    as *mut pg_sys::Node;
            let name: Option<String> = if name_node.is_null() {
                None
            } else {
                unsafe {
                    expect_node_tag(
                        name_node,
                        pg_sys::NodeTag::T_String,
                        FIELD_COLUMN_REFS,
                    )?;
                }
                let s = name_node.cast::<pg_sys::String>();
                let sval = unsafe { (*s).sval };
                if sval.is_null() {
                    None
                } else {
                    // `get_attname`-derived names are always valid UTF-8 in
                    // practice; defensively map a non-UTF-8 name to `None`
                    // (the provider falls back to a fresh lookup).
                    unsafe { CStr::from_ptr(sval) }
                        .to_str()
                        .ok()
                        .map(|s| s.to_string())
                }
            };

            column_refs.push(ColumnRef {
                expr_index: read(COLUMN_REF_EXPR_INDEX) as usize,
                rel_oid: pg_sys::Oid::from(read(COLUMN_REF_REL_OID) as u32),
                attno: read(COLUMN_REF_ATTNO) as pg_sys::AttrNumber,
                atttypid: pg_sys::Oid::from(read(COLUMN_REF_ATTTYPID) as u32),
                attcollation: pg_sys::Oid::from(read(COLUMN_REF_ATTCOLLATION) as u32),
                name,
            });
        }
    }

    // FIELD_PROVIDER_METADATA: T_List or NIL — passed through verbatim.
    let provider_metadata_raw = unsafe {
        pg_sys::list_nth(list, FIELD_PROVIDER_METADATA) as *mut pg_sys::List
    };

    // FIELD_PRE_SETREFS_SCAN_RTI: T_Integer (debug-only)
    let pre_setrefs_scan_rti =
        unsafe { read_integer_cell(list, FIELD_PRE_SETREFS_SCAN_RTI)? };

    // FIELD_TUPLE_LAYOUT: T_IntList with kind tag and optional base attnos.
    let tuple_layout_wire =
        unsafe { pg_sys::list_nth(list, FIELD_TUPLE_LAYOUT) as *mut pg_sys::List };
    let tuple_layout = unsafe {
        ScanTupleLayout::decode_wire(tuple_layout_wire, FIELD_TUPLE_LAYOUT)?
    };

    // Cross-field invariants: contracts align with pushed_count; expr_index in range.
    if pushed_contracts.len() != pushed_count {
        return Err(DecodeError::PushedContractsLengthMismatch {
            pushed_count,
            pushed_contracts_len: pushed_contracts.len(),
        });
    }
    for (entry_idx, cr) in column_refs.iter().enumerate() {
        if cr.expr_index >= pushed_count {
            return Err(DecodeError::ColumnRefExprIndexOutOfRange {
                entry: entry_idx,
                expr_index: cr.expr_index,
                pushed_count,
            });
        }
    }

    Ok(EncodedPrivate {
        provider_id_or_name,
        relation_oid,
        pushed_count,
        recheck_count,
        pushed_contracts,
        column_refs,
        provider_metadata_raw,
        pre_setrefs_scan_rti,
        tuple_layout,
    })
}

/// Read a positional cell as a `T_Integer` and return its `ival`.
///
/// # Safety
///
/// `list` must be a valid `List*` whose cell at `field` (if present) holds a
/// `Node*` pointer. The function NUL-checks the pointer and the tag.
unsafe fn read_integer_cell(
    list: *mut pg_sys::List,
    field: i32,
) -> Result<i32, DecodeError> {
    let node_ptr = unsafe { pg_sys::list_nth(list, field) } as *mut pg_sys::Node;
    if node_ptr.is_null() {
        return Err(DecodeError::NullCell { field });
    }
    unsafe { expect_node_tag(node_ptr, pg_sys::NodeTag::T_Integer, field)? };
    let int_ptr = node_ptr.cast::<pg_sys::Integer>();
    Ok(unsafe { (*int_ptr).ival })
}

/// Verify the `NodeTag` of a non-null `Node*` matches `expected`.
///
/// # Safety
///
/// `node` must be a non-null pointer to a PG `Node` (i.e. a struct whose
/// first field is a `NodeTag`).
unsafe fn expect_node_tag(
    node: *mut pg_sys::Node,
    expected: pg_sys::NodeTag,
    field: i32,
) -> Result<(), DecodeError> {
    let found = unsafe { (*node).type_ };
    if found != expected {
        return Err(DecodeError::WrongNodeTag {
            field,
            expected,
            found,
        });
    }
    Ok(())
}

/// Verify the `NodeTag` of a non-null `List*` matches `expected`. PG lists
/// are themselves `Node*`s — the `List`'s `type_` field is one of `T_List`,
/// `T_IntList`, or `T_OidList`.
///
/// # Safety
///
/// `list` must be a non-null pointer to a `pg_sys::List`.
unsafe fn expect_list_tag(
    list: *mut pg_sys::List,
    expected: pg_sys::NodeTag,
    field: i32,
) -> Result<(), DecodeError> {
    let found = unsafe { (*list).type_ };
    if found != expected {
        return Err(DecodeError::WrongNodeTag {
            field,
            expected,
            found,
        });
    }
    Ok(())
}

/// Convert `usize` count to PG `Integer` `c_int`; errors if `> i32::MAX`.
#[inline]
pub(crate) fn usize_to_int(value: usize) -> Result<i32, DecodeError> {
    if value > i32::MAX as usize {
        return Err(DecodeError::CountTooLargeToEncode { value });
    }
    Ok(value as i32)
}

/// Debug-only: assert provider metadata cells are copyObject-safe nodes.
///
/// # Safety
///
/// `metadata` may be NULL; otherwise must point to a valid `List`.
unsafe fn debug_assert_provider_metadata_safe(metadata: *mut pg_sys::List) {
    if !cfg!(debug_assertions) {
        return;
    }
    if metadata.is_null() {
        return;
    }

    unsafe {
        let tag = (*metadata).type_;
        debug_assert!(
            matches!(
                tag,
                pg_sys::NodeTag::T_List
                    | pg_sys::NodeTag::T_IntList
                    | pg_sys::NodeTag::T_OidList,
            ),
            "provider_metadata has unexpected NodeTag {tag:?}; \
             custom_private must contain only copyObject-safe Node*",
        );

        // Only walk cells of T_List (pointer cells); T_IntList and
        // T_OidList hold scalar values that are by definition safe.
        if tag != pg_sys::NodeTag::T_List {
            return;
        }

        let len = (*metadata).length as usize;
        for i in 0..len {
            let raw = pg_sys::list_nth(metadata, i as i32);
            if raw.is_null() {
                continue;
            }
            let node_tag = (*(raw as *mut pg_sys::Node)).type_;
            debug_assert!(
                node_tag_is_copyobject_safe(node_tag),
                "provider_metadata cell {i} has NodeTag {node_tag:?} which is \
                 not copyObject-safe. Use Integer/String/Boolean/Float/List/ \
                 IntList/OidList/Bitmapset only.",
            );
        }
    }
}

/// Allow-list of `NodeTag`s safe for `copyObject` in `custom_private`.
fn node_tag_is_copyobject_safe(tag: pg_sys::NodeTag) -> bool {
    matches!(
        tag,
        pg_sys::NodeTag::T_Integer
            | pg_sys::NodeTag::T_Float
            | pg_sys::NodeTag::T_Boolean
            | pg_sys::NodeTag::T_String
            | pg_sys::NodeTag::T_BitString
            | pg_sys::NodeTag::T_List
            | pg_sys::NodeTag::T_IntList
            | pg_sys::NodeTag::T_OidList
            | pg_sys::NodeTag::T_Bitmapset,
    )
}

#[cfg(test)]
mod tests {
    use super::{DecodeError, assert_provider_name_matches, usize_to_int};
    use proptest::prelude::*;

    use std::ffi::CString;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        #[test]
        fn usize_to_int_never_truncates(v in any::<usize>()) {
            match usize_to_int(v) {
                Ok(i) => {
                    prop_assert!(v <= i32::MAX as usize);
                    prop_assert_eq!(i as usize, v);
                    prop_assert!(i >= 0);
                }
                Err(DecodeError::CountTooLargeToEncode { value }) => {
                    prop_assert!(v > i32::MAX as usize);
                    prop_assert_eq!(value, v);
                }
                Err(other) => {
                    prop_assert!(false, "unexpected error variant: {:?}", other)
                }
            }
        }
    }

    #[test]
    fn usize_to_int_boundary_values() {
        assert_eq!(usize_to_int(0), Ok(0));
        assert_eq!(usize_to_int(i32::MAX as usize), Ok(i32::MAX));

        let just_over = i32::MAX as usize + 1;
        assert_eq!(
            usize_to_int(just_over),
            Err(DecodeError::CountTooLargeToEncode { value: just_over })
        );
        assert_eq!(
            usize_to_int(usize::MAX),
            Err(DecodeError::CountTooLargeToEncode { value: usize::MAX })
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        #[test]
        fn assert_provider_name_matches_returns_err_on_mismatch(
            expected in "[^\u{0}]{0,32}",
            found in "[^\u{0}]{0,32}",
        ) {
            let expected_c = CString::new(expected.as_bytes())
                .expect("generator excludes NUL");
            let found_c = CString::new(found.as_bytes())
                .expect("generator excludes NUL");

            let result = assert_provider_name_matches(
                found_c.as_c_str(),
                expected_c.as_c_str(),
            );

            if expected == found {
                prop_assert!(result.is_ok(), "expected Ok(()) on match, got {:?}", result);
            } else {
                let err = result.unwrap_err();
                let rendered = format!("{err}");
                let reference = format!(
                    "customscan: provider name mismatch in custom_private \
                     (expected {:?}, found {:?}); this indicates a corrupt \
                     plan tree or a stale cached plan referencing a renamed \
                     provider",
                    expected_c.as_c_str(),
                    found_c.as_c_str(),
                );
                prop_assert_eq!(rendered, reference);
            }
        }
    }

    #[test]
    fn assert_provider_name_matches_ok_on_match() {
        let name = c"my_provider";
        assert!(assert_provider_name_matches(name, name).is_ok());
    }

    #[test]
    fn assert_provider_name_matches_err_and_display_on_mismatch() {
        let found = c"foo";
        let expected = c"bar";
        let result = assert_provider_name_matches(found, expected);

        let err = result.unwrap_err();
        assert_eq!(
            format!("{err}"),
            "customscan: provider name mismatch in custom_private \
             (expected \"bar\", found \"foo\"); this indicates a corrupt \
             plan tree or a stale cached plan referencing a renamed provider"
        );
    }
}
