//! Plan-time analysis of target-table values needed by DELETE output.

use core::ffi::c_int;
#[cfg(feature = "pg18")]
use core::ffi::c_void;
use core::ptr;
use std::ffi::{CStr, CString};

#[cfg(feature = "pg18")]
use pgrx::pg_guard;
use pgrx::pg_sys;

use super::contract::ForeignModifyOperation;
use super::error::ForeignModifyError;
use crate::fdw::attribute_target_name::RelationAttributeTargetName;
use crate::handles::TupleDescHandle;

const RETURN_COLUMN_PREFIX: &str = "__lagodb_fdw_return_attr_";
const RETURN_COLUMN_NAME: RelationAttributeTargetName =
    RelationAttributeTargetName::new(RETURN_COLUMN_PREFIX);

/// Target-table user columns referenced by PostgreSQL's later DELETE output.
#[derive(Debug, Clone, Default)]
pub(crate) struct ForeignModifyReturnRequirements {
    columns: Box<[pg_sys::AttrNumber]>,
    all_columns: bool,
}

impl ForeignModifyReturnRequirements {
    pub(super) fn from_parts(
        columns: Box<[pg_sys::AttrNumber]>,
        all_columns: bool,
    ) -> Self {
        Self {
            columns,
            all_columns,
        }
    }

    /// # Safety
    ///
    /// `returning` must be a live planner targetlist for `rtindex`, and
    /// `relation` must remain open for the duration of this call.
    pub(crate) unsafe fn from_returning_list(
        returning: *mut pg_sys::List,
        relation: pg_sys::Relation,
        rtindex: pg_sys::Index,
    ) -> Result<Self, ForeignModifyError> {
        if returning.is_null() {
            return Ok(Self::default());
        }

        let referenced_attributes = unsafe {
            Self::referenced_attributes(
                returning,
                rtindex,
                ForeignModifyOperation::Delete,
            )
        }?;

        let mut all_columns = false;
        let mut columns = Vec::new();
        for attno in referenced_attributes {
            if attno == 0 {
                all_columns = true;
            } else if attno > 0 {
                columns.push(attno);
            }
        }

        if all_columns {
            columns = unsafe { Self::all_user_columns(relation) }?;
        }
        Ok(Self {
            columns: columns.into_boxed_slice(),
            all_columns,
        })
    }

    /// Collect target-relation attributes whose values exist for this command.
    ///
    /// PG18 represents explicit `OLD`/`NEW` references on each `Var`.  A
    /// missing side of the command evaluates to NULL and must not force the
    /// provider to carry a row value or system column that PostgreSQL will not
    /// read.
    ///
    /// # Safety
    ///
    /// `returning` must be a live planner expression list for `rtindex`.
    pub(super) unsafe fn referenced_attributes(
        returning: *mut pg_sys::List,
        rtindex: pg_sys::Index,
        operation: ForeignModifyOperation,
    ) -> Result<Vec<pg_sys::AttrNumber>, ForeignModifyError> {
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        let _ = operation;

        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        let raw_attributes = {
            // pull_varattnos also descends into the planned SubPlan test and
            // argument expressions used by RETURNING.
            let mut attributes = ptr::null_mut();
            unsafe {
                pg_sys::pull_varattnos(
                    returning.cast::<pg_sys::Node>(),
                    rtindex,
                    &mut attributes,
                );
            }
            let mut raw_attributes = Vec::new();
            let mut bit = -1;
            loop {
                bit = unsafe { pg_sys::bms_next_member(attributes, bit) };
                if bit < 0 {
                    break;
                }
                raw_attributes.push(bit + pg_sys::FirstLowInvalidHeapAttributeNumber);
            }
            if !attributes.is_null() {
                unsafe { pg_sys::bms_free(attributes) };
            }
            raw_attributes
        };

        #[cfg(feature = "pg18")]
        let raw_attributes = {
            let mut state = ReturningAttributeWalker {
                rtindex,
                operation,
                attributes: Vec::new(),
            };
            unsafe {
                returning_attribute_walker(
                    returning.cast::<pg_sys::Node>(),
                    ptr::from_mut(&mut state).cast(),
                );
            }
            state.attributes.sort_unstable();
            state.attributes.dedup();
            state.attributes
        };

        raw_attributes
            .into_iter()
            .map(|attno| {
                pg_sys::AttrNumber::try_from(attno).map_err(|_| {
                    ForeignModifyError::framework(
                        "foreign modify RETURNING attribute exceeds PostgreSQL range",
                    )
                })
            })
            .collect()
    }

    #[inline]
    pub(crate) fn columns(&self) -> &[pg_sys::AttrNumber] {
        &self.columns
    }

    #[inline]
    pub(crate) const fn all_columns(&self) -> bool {
        self.all_columns
    }

    #[inline]
    pub(crate) fn contains(&self, attno: pg_sys::AttrNumber) -> bool {
        // Planner sources produce ascending columns and the wire codec
        // preserves that order.
        self.columns.binary_search(&attno).is_ok()
    }

    #[inline]
    pub(crate) fn requires_row(&self) -> bool {
        self.all_columns || !self.columns.is_empty()
    }

    /// Build the framework-owned name used to propagate one old column into a
    /// DELETE plan slot. This allocation is planner-only.
    pub(crate) fn returning_column_name(
        attribute: &pg_sys::FormData_pg_attribute,
    ) -> CString {
        RETURN_COLUMN_NAME.encode(attribute)
    }

    /// Resolve a framework-owned return-column target against the current
    /// leaf relation. The attribute name identifies the logical partition
    /// column; its leaf attno distinguishes Vars that PostgreSQL cannot place
    /// under one `RowIdentityVarInfo` name.
    ///
    /// # Safety
    ///
    /// `relation` must be live with the descriptor PostgreSQL created for it.
    pub(crate) unsafe fn returning_column_attno(
        relation: pg_sys::Relation,
        name: &CStr,
    ) -> Result<Option<pg_sys::AttrNumber>, ForeignModifyError> {
        // SAFETY: guaranteed by this method's contract.
        unsafe { RETURN_COLUMN_NAME.decode(relation, name) }.map_err(|_| {
            ForeignModifyError::framework(
                "foreign modify return column name is invalid",
            )
        })
    }

    /// Build a relation Var used by `AddForeignUpdateTargets` to propagate one
    /// DELETE RETURNING column into the ModifyTable plan slot.
    ///
    /// `attribute` must be a validated relation attribute, and `rtindex` must
    /// identify that relation in the live planner.
    pub(crate) fn returning_column_var(
        rtindex: pg_sys::Index,
        attribute: &pg_sys::FormData_pg_attribute,
    ) -> *mut pg_sys::Var {
        unsafe {
            pg_sys::makeVar(
                rtindex as c_int,
                attribute.attnum,
                attribute.atttypid,
                attribute.atttypmod,
                attribute.attcollation,
                0,
            )
        }
    }

    /// # Safety
    ///
    /// `relation` must be live with the descriptor PostgreSQL created for it.
    unsafe fn all_user_columns(
        relation: pg_sys::Relation,
    ) -> Result<Vec<pg_sys::AttrNumber>, ForeignModifyError> {
        let tuple_desc = unsafe { (*relation).rd_att };
        // SAFETY: guaranteed by this method's contract.
        let tuple_desc = unsafe { TupleDescHandle::from_raw(tuple_desc) };
        let mut columns = Vec::new();
        for (index, attribute) in tuple_desc.attributes().iter().enumerate() {
            if !attribute.attisdropped {
                columns.push(pg_sys::AttrNumber::try_from(index + 1).map_err(|_| {
                    ForeignModifyError::framework(
                        "foreign modify return column exceeds PostgreSQL attribute range",
                    )
                })?);
            }
        }
        Ok(columns)
    }
}

#[cfg(feature = "pg18")]
struct ReturningAttributeWalker {
    rtindex: pg_sys::Index,
    operation: ForeignModifyOperation,
    attributes: Vec<c_int>,
}

#[cfg(feature = "pg18")]
impl ReturningAttributeWalker {
    #[inline]
    const fn side_is_missing(&self, old: bool) -> bool {
        match self.operation {
            ForeignModifyOperation::Insert => old,
            ForeignModifyOperation::Update => false,
            ForeignModifyOperation::Delete => !old,
        }
    }

    #[inline]
    fn var_is_missing(&self, returning_type: pg_sys::VarReturningType::Type) -> bool {
        match returning_type {
            pg_sys::VarReturningType::VAR_RETURNING_OLD => self.side_is_missing(true),
            pg_sys::VarReturningType::VAR_RETURNING_NEW => {
                self.side_is_missing(false)
            }
            _ => false,
        }
    }
}

/// Collect current-level target Vars while respecting PG18 ReturningExpr
/// short-circuit semantics for a command's missing OLD or NEW side.
#[cfg(feature = "pg18")]
#[pg_guard]
unsafe extern "C-unwind" fn returning_attribute_walker(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let state = unsafe { &mut *context.cast::<ReturningAttributeWalker>() };
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_Var => {
            let var = node.cast::<pg_sys::Var>();
            if unsafe { (*var).varlevelsup } == 0
                && unsafe { (*var).varno } == state.rtindex as c_int
                && !state.var_is_missing(unsafe { (*var).varreturningtype })
            {
                state.attributes.push(unsafe { (*var).varattno as c_int });
            }
            false
        }
        pg_sys::NodeTag::T_ReturningExpr => {
            let returning = node.cast::<pg_sys::ReturningExpr>();
            if unsafe { (*returning).retlevelsup } == 0
                && state.side_is_missing(unsafe { (*returning).retold })
            {
                return false;
            }
            unsafe {
                returning_attribute_walker(
                    (*returning).retexpr.cast::<pg_sys::Node>(),
                    context,
                )
            }
        }
        _ => unsafe {
            pg_sys::expression_tree_walker(
                node,
                Some(returning_attribute_walker),
                context,
            )
        },
    }
}
