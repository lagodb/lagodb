//! Plan-time analysis of target-table values needed by DELETE output.

use core::ffi::c_int;
use core::ptr;
use std::ffi::CString;

use pgrx::pg_sys;

use super::error::ForeignModifyError;

const RETURN_COLUMN_PREFIX: &str = "__pg_lakebase_fdw_return_attr_";

/// Target-table user columns referenced by PostgreSQL's later DELETE output.
#[derive(Debug, Clone, Default)]
pub(crate) struct ForeignModifyReturnRequirements {
    columns: Box<[pg_sys::AttrNumber]>,
    all_columns: bool,
}

impl ForeignModifyReturnRequirements {
    /// # Safety
    ///
    /// `returning` must be a live planner targetlist for `rtindex`, and
    /// `relation` must remain open for the duration of this call.
    pub(crate) unsafe fn from_returning_list(
        returning: *mut pg_sys::List,
        relation: pg_sys::Relation,
        rtindex: pg_sys::Index,
    ) -> Result<Self, ForeignModifyError> {
        if relation.is_null() || unsafe { (*relation).rd_att.is_null() } {
            return Err(ForeignModifyError::framework(
                "foreign modify return analysis has no relation descriptor",
            ));
        }
        if returning.is_null() {
            return Ok(Self::default());
        }

        // PostgreSQL's planner utility handles Vars inside planned SubPlans,
        // including the testexpr and argument expressions.  Its bitmap uses
        // the same FirstLowInvalidHeapAttributeNumber offset as core planner
        // code, so system attributes and whole-row Vars are represented too.
        let mut attributes = ptr::null_mut();
        unsafe {
            pg_sys::pull_varattnos(
                returning.cast::<pg_sys::Node>(),
                rtindex,
                &mut attributes,
            );
        }

        let mut bitmap_attnos = Vec::new();
        if !attributes.is_null() {
            let mut bit = -1;
            loop {
                bit = unsafe { pg_sys::bms_next_member(attributes, bit) };
                if bit < 0 {
                    break;
                }
                bitmap_attnos.push(bit + pg_sys::FirstLowInvalidHeapAttributeNumber);
            }
            unsafe { pg_sys::bms_free(attributes) };
        }

        let mut all_columns = false;
        let mut columns = Vec::new();
        for raw_attno in bitmap_attnos {
            let attno = pg_sys::AttrNumber::try_from(raw_attno).map_err(|_| {
                ForeignModifyError::framework(
                    "foreign modify RETURNING attribute exceeds PostgreSQL range",
                )
            })?;
            if attno == 0 {
                all_columns = true;
            } else if attno > 0 {
                RelationAttributeMetadata::from_relation(relation, attno)?;
                columns.push(attno);
            }
        }

        if all_columns {
            columns = Self::all_user_columns(relation)?;
        }
        Ok(Self {
            columns: columns.into_boxed_slice(),
            all_columns,
        })
    }

    /// Extract the RETURNING requirements for one ModifyTable result relation.
    ///
    /// # Safety
    ///
    /// `plan` and `relation` must be live nodes for the same modify plan, and
    /// `subplan_index` must identify one entry in `plan->returningLists`.
    pub(crate) unsafe fn from_modify_plan(
        plan: *mut pg_sys::ModifyTable,
        relation: pg_sys::Relation,
        result_relation: pg_sys::Index,
        subplan_index: c_int,
    ) -> Result<Self, ForeignModifyError> {
        if plan.is_null() || subplan_index < 0 {
            return Err(ForeignModifyError::framework(
                "foreign modify return analysis has an incomplete modify plan",
            ));
        }
        let returning_lists = unsafe { (*plan).returningLists };
        let returning = if returning_lists.is_null() {
            ptr::null_mut()
        } else {
            let length = unsafe { pg_sys::list_length(returning_lists) };
            if length < 0 || length <= subplan_index {
                return Err(ForeignModifyError::framework(
                    "foreign modify RETURNING-list index is outside its plan list",
                ));
            }
            unsafe {
                pg_sys::list_nth(returning_lists, subplan_index) as *mut pg_sys::List
            }
        };
        unsafe { Self::from_returning_list(returning, relation, result_relation) }
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
        // bms_next_member enumerates ascending bits, and all_user_columns
        // produces ascending attribute numbers, so no runtime sort is needed.
        self.columns.binary_search(&attno).is_ok()
    }

    #[inline]
    pub(crate) fn requires_row(&self) -> bool {
        self.all_columns || !self.columns.is_empty()
    }

    /// Build the framework-owned name used to propagate one old column into a
    /// DELETE plan slot. This allocation is planner-only.
    pub(crate) fn returning_column_name(
        attno: pg_sys::AttrNumber,
    ) -> Result<CString, ForeignModifyError> {
        if attno <= 0 {
            return Err(ForeignModifyError::framework(
                "foreign modify return column must be positive",
            ));
        }
        CString::new(format!("{RETURN_COLUMN_PREFIX}{attno}")).map_err(|_| {
            ForeignModifyError::framework(
                "foreign modify return column name is invalid",
            )
        })
    }

    /// Build a relation Var used by `AddForeignUpdateTargets` to propagate one
    /// DELETE RETURNING column into the ModifyTable plan slot.
    ///
    /// # Safety
    ///
    /// `relation` must be open and `rtindex` must identify that relation in the
    /// live planner root.
    pub(crate) unsafe fn returning_column_var(
        relation: pg_sys::Relation,
        rtindex: pg_sys::Index,
        attno: pg_sys::AttrNumber,
    ) -> Result<*mut pg_sys::Var, ForeignModifyError> {
        let attribute = RelationAttributeMetadata::from_relation(relation, attno)?;
        let var = unsafe {
            pg_sys::makeVar(
                rtindex as c_int,
                attno,
                attribute.type_oid,
                attribute.type_mod,
                attribute.collation,
                0,
            )
        };
        if var.is_null() {
            return Err(ForeignModifyError::framework(
                "PostgreSQL returned NULL while creating a DELETE RETURNING Var",
            ));
        }
        Ok(var)
    }

    fn all_user_columns(
        relation: pg_sys::Relation,
    ) -> Result<Vec<pg_sys::AttrNumber>, ForeignModifyError> {
        let tuple_desc = unsafe { (*relation).rd_att };
        let natts = unsafe { (*tuple_desc).natts };
        if natts < 0
            || (natts > 0 && unsafe { (*tuple_desc).attrs.as_ptr().is_null() })
        {
            return Err(ForeignModifyError::framework(
                "foreign modify return analysis has an invalid relation descriptor",
            ));
        }
        let mut columns = Vec::new();
        for index in 0..natts as usize {
            let attribute = unsafe { &*(*tuple_desc).attrs.as_ptr().add(index) };
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

#[derive(Clone, Copy)]
pub(super) struct RelationAttributeMetadata {
    pub(super) relation_index: usize,
    pub(super) type_oid: pg_sys::Oid,
    pub(super) type_mod: i32,
    pub(super) collation: pg_sys::Oid,
}

impl RelationAttributeMetadata {
    pub(super) fn from_relation(
        relation: pg_sys::Relation,
        attno: pg_sys::AttrNumber,
    ) -> Result<Self, ForeignModifyError> {
        if relation.is_null() || unsafe { (*relation).rd_att.is_null() } {
            return Err(ForeignModifyError::framework(
                "foreign modify return column has no relation descriptor",
            ));
        }
        if attno <= 0 {
            return Err(ForeignModifyError::framework(
                "foreign modify return column must be positive",
            ));
        }
        let tuple_desc = unsafe { (*relation).rd_att };
        let natts = unsafe { (*tuple_desc).natts };
        let relation_index = usize::try_from(attno as i32 - 1).map_err(|_| {
            ForeignModifyError::framework(
                "foreign modify return column is out of range",
            )
        })?;
        if natts < 0
            || relation_index >= natts as usize
            || (natts > 0 && unsafe { (*tuple_desc).attrs.as_ptr().is_null() })
        {
            return Err(ForeignModifyError::framework(
                "foreign modify return column is outside the relation",
            ));
        }
        let attribute = unsafe { &*(*tuple_desc).attrs.as_ptr().add(relation_index) };
        if attribute.attisdropped {
            return Err(ForeignModifyError::framework(
                "foreign modify return column is dropped",
            ));
        }
        Ok(Self {
            relation_index,
            type_oid: attribute.atttypid,
            type_mod: attribute.atttypmod,
            collation: attribute.attcollation,
        })
    }
}
