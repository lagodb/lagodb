//! Framework plan-data layout for `CustomScan.custom_private` (`encode_split` /
//! `decode_private`). Positional `copyObject`-safe `List*`; provider payload
//! in cell 7.

use std::ffi::{CStr, CString};
use std::ptr;

use pgrx::pg_sys;

use crate::customscan::ScanPurpose;
use crate::customscan::error::CustomScanError;
use crate::customscan::plan_data::{EnvelopeError, tuple_layout::ScanTupleLayout};
use crate::expr::contract::{ColumnRef, PushdownContract};

fn usize_to_int(value: usize) -> Result<i32, EnvelopeError> {
    i32::try_from(value).map_err(|_| EnvelopeError::CountTooLargeToEncode { value })
}

// Positional indices into the top-level `T_List` payload. Both encode and
// decode reference these so the layout is described in exactly one place.
const FIELD_PROVIDER_NAME: i32 = 0;
const FIELD_SCAN_PURPOSE: i32 = 1;
const FIELD_RELATION_OID: i32 = 2;
const FIELD_PUSHED_COUNT: i32 = 3;
const FIELD_RECHECK_COUNT: i32 = 4;
const FIELD_PUSHED_CONTRACTS: i32 = 5;
const FIELD_COLUMN_REFS: i32 = 6;
const FIELD_PROVIDER_METADATA: i32 = 7;
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

/// Decoded path-stage wrapper. Unlike final `custom_private`, this exists only
/// between `create_path` and `PlanCustomPath`.
pub(crate) struct EncodedPathPrivate {
    pub(crate) purpose: ScanPurpose,
    pub(crate) requires_wholerow: bool,
    pub(crate) provider_metadata: *mut pg_sys::List,
}

/// Wrap provider path metadata with the semantic scan purpose.
///
/// # Safety
///
/// `provider_metadata` must be NULL or a copyObject-safe PostgreSQL list.
pub(crate) unsafe fn encode_path_private(
    purpose: ScanPurpose,
    requires_wholerow: bool,
    provider_metadata: *mut pg_sys::List,
) -> *mut pg_sys::List {
    unsafe {
        let mut list = ptr::null_mut();
        list = pg_sys::lappend(list, pg_sys::makeInteger(purpose.to_wire()).cast());
        list = pg_sys::lappend(
            list,
            pg_sys::makeInteger(i32::from(requires_wholerow)).cast(),
        );
        pg_sys::lappend(list, provider_metadata.cast())
    }
}

/// Decode the path-stage purpose wrapper.
///
/// # Safety
///
/// `list` must be a live path-owned PostgreSQL list.
pub(crate) unsafe fn decode_path_private(
    list: *mut pg_sys::List,
) -> Result<EncodedPathPrivate, CustomScanError> {
    let decoded = (|| {
        if list.is_null() || unsafe { pg_sys::list_length(list) } != 3 {
            return Err(EnvelopeError::MalformedPathPrivate {
                reason: "expected a three-cell list",
            });
        }
        let purpose_node =
            unsafe { pg_sys::list_nth(list, 0) }.cast::<pg_sys::Node>();
        if purpose_node.is_null()
            || unsafe { (*purpose_node).type_ } != pg_sys::NodeTag::T_Integer
        {
            return Err(EnvelopeError::MalformedPathPrivate {
                reason: "purpose is not an Integer node",
            });
        }
        let purpose_raw = unsafe { (*purpose_node.cast::<pg_sys::Integer>()).ival };
        let purpose = ScanPurpose::from_wire(purpose_raw)
            .ok_or(EnvelopeError::UnknownScanPurpose { value: purpose_raw })?;
        let wholerow_node =
            unsafe { pg_sys::list_nth(list, 1) }.cast::<pg_sys::Node>();
        if wholerow_node.is_null()
            || unsafe { (*wholerow_node).type_ } != pg_sys::NodeTag::T_Integer
        {
            return Err(EnvelopeError::MalformedPathPrivate {
                reason: "requires_wholerow is not an Integer node",
            });
        }
        let requires_wholerow =
            unsafe { (*wholerow_node.cast::<pg_sys::Integer>()).ival };
        if !matches!(requires_wholerow, 0 | 1) {
            return Err(EnvelopeError::MalformedPathPrivate {
                reason: "requires_wholerow is not boolean",
            });
        }
        let provider_metadata =
            unsafe { pg_sys::list_nth(list, 2) }.cast::<pg_sys::List>();
        Ok(EncodedPathPrivate {
            purpose,
            requires_wholerow: requires_wholerow == 1,
            provider_metadata,
        })
    })();
    decoded.map_err(CustomScanError::private_codec)
}

/// Decoded framework envelope from `CustomScan.custom_private`.
#[derive(Debug)]
pub struct EncodedPrivate {
    /// Matches the registered `LakebaseCustomScanProvider::NAME`.
    pub provider_id_or_name: CString,

    /// Query scan or modification-target scan using the same provider.
    pub purpose: ScanPurpose,

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
    /// The provider decodes this cell separately. NULL is permitted: a
    /// provider with no private state can ignore it.
    pub provider_metadata_raw: *mut pg_sys::List,

    /// Raw scan-tuple shape and base-attribute mapping.
    pub tuple_layout: ScanTupleLayout,
}

/// Encode a relation-shaped query envelope for backend tests.
///
/// # Safety
///
/// `provider_metadata` must be NULL or a live copyObject-safe PostgreSQL list
/// in the current memory context. The expression and metadata slices must
/// remain valid for the duration of this call.
pub unsafe fn encode_split(
    provider_id_or_name: &CStr,
    relation_oid: pg_sys::Oid,
    pushed_count: usize,
    recheck_count: usize,
    pushed_contracts: &[PushdownContract],
    column_refs: &[ColumnRef],
    provider_metadata: *mut pg_sys::List,
) -> Result<*mut pg_sys::List, CustomScanError> {
    let tuple_layout = ScanTupleLayout::relation();
    unsafe {
        encode_split_with_layout(
            provider_id_or_name,
            ScanPurpose::Query,
            relation_oid,
            pushed_count,
            recheck_count,
            pushed_contracts,
            column_refs,
            provider_metadata,
            &tuple_layout,
        )
    }
}

pub(crate) unsafe fn encode_split_with_layout(
    provider_id_or_name: &CStr,
    purpose: ScanPurpose,
    relation_oid: pg_sys::Oid,
    pushed_count: usize,
    recheck_count: usize,
    pushed_contracts: &[PushdownContract],
    column_refs: &[ColumnRef],
    provider_metadata: *mut pg_sys::List,
    tuple_layout: &ScanTupleLayout,
) -> Result<*mut pg_sys::List, CustomScanError> {
    unsafe {
        encode_split_impl(
            provider_id_or_name,
            purpose,
            relation_oid,
            pushed_count,
            recheck_count,
            pushed_contracts,
            column_refs,
            provider_metadata,
            tuple_layout,
        )
    }
    .map_err(CustomScanError::private_codec)
}

unsafe fn encode_split_impl(
    provider_id_or_name: &CStr,
    purpose: ScanPurpose,
    relation_oid: pg_sys::Oid,
    pushed_count: usize,
    recheck_count: usize,
    pushed_contracts: &[PushdownContract],
    column_refs: &[ColumnRef],
    provider_metadata: *mut pg_sys::List,
    tuple_layout: &ScanTupleLayout,
) -> Result<*mut pg_sys::List, EnvelopeError> {
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

        // FIELD_SCAN_PURPOSE: T_Integer
        let purpose_node = pg_sys::makeInteger(purpose.to_wire());
        top = pg_sys::lappend(top, purpose_node.cast());

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
) -> Result<EncodedPrivate, EnvelopeError> {
    if list.is_null() {
        return Err(EnvelopeError::NullPayload);
    }

    let len = unsafe { (*list).length } as usize;
    if len != TOP_LEVEL_LEN {
        return Err(EnvelopeError::WrongTopLevelLength {
            found: len,
            expected: TOP_LEVEL_LEN,
        });
    }

    // Helper: read cell `i` as `*mut pg_sys::Node`, NULL-checked.
    let cell_node = |idx: i32| -> Result<*mut pg_sys::Node, EnvelopeError> {
        let ptr = unsafe { pg_sys::list_nth(list, idx) } as *mut pg_sys::Node;
        if ptr.is_null() {
            return Err(EnvelopeError::NullCell { field: idx });
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
            return Err(EnvelopeError::NullProviderName);
        }
        CStr::from_ptr(sval).to_owned()
    };

    // FIELD_SCAN_PURPOSE: T_Integer
    let purpose_raw = unsafe { read_integer_cell(list, FIELD_SCAN_PURPOSE)? };
    let purpose = ScanPurpose::from_wire(purpose_raw)
        .ok_or(EnvelopeError::UnknownScanPurpose { value: purpose_raw })?;

    // FIELD_RELATION_OID: T_Integer (Oid round-tripped via i32 bitcast)
    let relation_oid = unsafe {
        let raw = read_integer_cell(list, FIELD_RELATION_OID)?;
        pg_sys::Oid::from(raw as u32)
    };

    // FIELD_PUSHED_COUNT: T_Integer
    let pushed_count_raw = unsafe { read_integer_cell(list, FIELD_PUSHED_COUNT)? };
    if pushed_count_raw < 0 {
        return Err(EnvelopeError::NegativeCount {
            field: FIELD_PUSHED_COUNT,
            value: pushed_count_raw,
        });
    }
    let pushed_count = pushed_count_raw as usize;

    // FIELD_RECHECK_COUNT: T_Integer
    let recheck_count_raw = unsafe { read_integer_cell(list, FIELD_RECHECK_COUNT)? };
    if recheck_count_raw < 0 {
        return Err(EnvelopeError::NegativeCount {
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
                    return Err(EnvelopeError::UnknownContract {
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
                return Err(EnvelopeError::NullCell {
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
                return Err(EnvelopeError::MalformedColumnRef {
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
                return Err(EnvelopeError::NullCell {
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
                return Err(EnvelopeError::MalformedColumnRef {
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

    // FIELD_TUPLE_LAYOUT: T_IntList with kind tag and optional base attnos.
    let tuple_layout_wire =
        unsafe { pg_sys::list_nth(list, FIELD_TUPLE_LAYOUT) as *mut pg_sys::List };
    let tuple_layout = unsafe {
        ScanTupleLayout::decode_wire(tuple_layout_wire, FIELD_TUPLE_LAYOUT)?
    };

    // Cross-field invariants: contracts align with pushed_count; expr_index in range.
    if pushed_contracts.len() != pushed_count {
        return Err(EnvelopeError::PushedContractsLengthMismatch {
            pushed_count,
            pushed_contracts_len: pushed_contracts.len(),
        });
    }
    for (entry_idx, cr) in column_refs.iter().enumerate() {
        if cr.expr_index >= pushed_count {
            return Err(EnvelopeError::ColumnRefExprIndexOutOfRange {
                entry: entry_idx,
                expr_index: cr.expr_index,
                pushed_count,
            });
        }
    }

    Ok(EncodedPrivate {
        provider_id_or_name,
        purpose,
        relation_oid,
        pushed_count,
        recheck_count,
        pushed_contracts,
        column_refs,
        provider_metadata_raw,
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
) -> Result<i32, EnvelopeError> {
    let node_ptr = unsafe { pg_sys::list_nth(list, field) } as *mut pg_sys::Node;
    if node_ptr.is_null() {
        return Err(EnvelopeError::NullCell { field });
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
) -> Result<(), EnvelopeError> {
    let found = unsafe { (*node).type_ };
    if found != expected {
        return Err(EnvelopeError::WrongNodeTag {
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
) -> Result<(), EnvelopeError> {
    let found = unsafe { (*list).type_ };
    if found != expected {
        return Err(EnvelopeError::WrongNodeTag {
            field,
            expected,
            found,
        });
    }
    Ok(())
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
    use super::{EnvelopeError, assert_provider_name_matches, usize_to_int};
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
                Err(EnvelopeError::CountTooLargeToEncode { value }) => {
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
            Err(EnvelopeError::CountTooLargeToEncode { value: just_over })
        );
        assert_eq!(
            usize_to_int(usize::MAX),
            Err(EnvelopeError::CountTooLargeToEncode { value: usize::MAX })
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
