//! Provider-facing planner contexts for foreign modify callbacks.

use core::ffi::c_int;
use core::ptr;

use pgrx::pg_sys;

use crate::handles::RelationHandle;

use super::super::row_identity::RowIdentityLayout;
use super::contract::ForeignModifyOperation;
use super::error::ForeignModifyError;
use super::return_requirements::ForeignModifyReturnRequirements;

/// Relation-scoped context used for capability discovery.
pub struct ForeignModifyRelationContext<'a> {
    relation: RelationHandle<'a>,
}

impl<'a> ForeignModifyRelationContext<'a> {
    pub(crate) unsafe fn from_raw(
        relation: pg_sys::Relation,
    ) -> Result<Self, ForeignModifyError> {
        if relation.is_null() {
            return Err(ForeignModifyError::framework(
                "foreign modify callback received a NULL relation",
            ));
        }
        Ok(Self {
            relation: unsafe { RelationHandle::from_raw(relation) },
        })
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'a> {
        &self.relation
    }
}

/// Planner context used to add foreign UPDATE/DELETE row identity targets.
pub struct ForeignUpdateTargetContext<'a> {
    root: *mut pg_sys::PlannerInfo,
    rtindex: pg_sys::Index,
    relation: RelationHandle<'a>,
    item_pointer_added: bool,
    attribute_identities: Vec<pg_sys::AttrNumber>,
    returning_columns: Vec<pg_sys::AttrNumber>,
    return_requirements: ForeignModifyReturnRequirements,
    operation: ForeignModifyOperation,
}

impl<'a> ForeignUpdateTargetContext<'a> {
    pub(crate) unsafe fn from_raw(
        root: *mut pg_sys::PlannerInfo,
        rtindex: pg_sys::Index,
        target_rte: *mut pg_sys::RangeTblEntry,
        relation: pg_sys::Relation,
    ) -> Result<Self, ForeignModifyError> {
        if root.is_null()
            || rtindex == 0
            || target_rte.is_null()
            || relation.is_null()
        {
            return Err(ForeignModifyError::framework(
                "AddForeignUpdateTargets received an incomplete planner context",
            ));
        }
        let parse = unsafe { (*root).parse };
        if parse.is_null() {
            return Err(ForeignModifyError::framework(
                "AddForeignUpdateTargets received no parse tree",
            ));
        }
        let operation =
            ForeignModifyOperation::from_pg(unsafe { (*parse).commandType })?;
        let return_requirements =
            if matches!(operation, ForeignModifyOperation::Delete) {
                unsafe {
                    ForeignModifyReturnRequirements::from_returning_list(
                        (*parse).returningList,
                        relation,
                        rtindex,
                    )
                }?
            } else {
                ForeignModifyReturnRequirements::default()
            };
        Ok(Self {
            root,
            rtindex,
            relation: unsafe { RelationHandle::from_raw(relation) },
            item_pointer_added: false,
            attribute_identities: Vec::new(),
            returning_columns: Vec::new(),
            return_requirements,
            operation,
        })
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'a> {
        &self.relation
    }

    #[inline]
    pub fn rtindex(&self) -> pg_sys::Index {
        self.rtindex
    }

    #[inline]
    pub fn operation(&self) -> ForeignModifyOperation {
        self.operation
    }

    /// Target-table user columns referenced by DELETE RETURNING.
    #[inline]
    pub fn returning_columns(&self) -> &[pg_sys::AttrNumber] {
        self.return_requirements.columns()
    }

    /// Whether DELETE RETURNING contains a whole-row reference.
    #[inline]
    pub fn returning_all_columns(&self) -> bool {
        self.return_requirements.all_columns()
    }

    /// Add the framework ItemPointer row identity. Repeated calls are
    /// idempotent for this planner callback.
    pub fn add_item_pointer_identity(&mut self) -> Result<(), ForeignModifyError> {
        if self.item_pointer_added {
            return Ok(());
        }
        let var = unsafe {
            pg_sys::makeVar(
                self.rtindex as c_int,
                pg_sys::SelfItemPointerAttributeNumber as pg_sys::AttrNumber,
                pg_sys::TIDOID,
                -1,
                pg_sys::InvalidOid,
                0,
            )
        };
        if var.is_null() {
            return Err(ForeignModifyError::framework(
                "PostgreSQL returned NULL while creating the ctid identity Var",
            ));
        }
        unsafe {
            pg_sys::add_row_identity_var(
                self.root,
                var,
                self.rtindex,
                RowIdentityLayout::item_pointer_identity_name().as_ptr(),
            );
        }
        self.item_pointer_added = true;
        Ok(())
    }

    /// Add one positive relation attribute as a provider-defined row identity.
    /// The framework gives it a stable internal name and decodes it from the
    /// plan slot for each UPDATE or DELETE row.
    pub fn add_attribute_identity(
        &mut self,
        attno: pg_sys::AttrNumber,
    ) -> Result<(), ForeignModifyError> {
        if attno <= 0 {
            return Err(ForeignModifyError::framework(
                "foreign row identity attributes must be positive",
            ));
        }
        if self.attribute_identities.contains(&attno) {
            return Ok(());
        }
        let tuple_desc = self.relation.tuple_desc();
        if tuple_desc.is_null() {
            return Err(ForeignModifyError::framework(
                "foreign relation has no tuple descriptor",
            ));
        }
        let index = usize::try_from(attno as i32 - 1).map_err(|_| {
            ForeignModifyError::framework(
                "foreign row identity attribute is out of range",
            )
        })?;
        let natts = unsafe { (*tuple_desc).natts };
        if natts < 0 || unsafe { (*tuple_desc).attrs.as_ptr().is_null() } {
            return Err(ForeignModifyError::framework(
                "foreign relation has an invalid tuple descriptor",
            ));
        }
        let natts = natts as usize;
        if index >= natts {
            return Err(ForeignModifyError::framework(
                "foreign row identity attribute is outside the relation",
            ));
        }
        let attr = unsafe { &*(*tuple_desc).attrs.as_ptr().add(index) };
        if attr.attisdropped {
            return Err(ForeignModifyError::framework(
                "foreign row identity cannot use a dropped attribute",
            ));
        }
        let var = unsafe {
            pg_sys::makeVar(
                self.rtindex as c_int,
                attno,
                attr.atttypid,
                attr.atttypmod,
                attr.attcollation,
                0,
            )
        };
        if var.is_null() {
            return Err(ForeignModifyError::framework(
                "PostgreSQL returned NULL while creating a row identity Var",
            ));
        }
        let name = RowIdentityLayout::attribute_identity_name(attno)?;
        unsafe {
            pg_sys::add_row_identity_var(self.root, var, self.rtindex, name.as_ptr());
        }
        self.attribute_identities.push(attno);
        Ok(())
    }

    /// Ask PostgreSQL to propagate one DELETE RETURNING old column into the
    /// ModifyTable plan slot. The column is kept in a separate return layout;
    /// it is not interpreted as a row identity by the executor.
    pub fn add_returning_column(
        &mut self,
        attno: pg_sys::AttrNumber,
    ) -> Result<(), ForeignModifyError> {
        if !matches!(self.operation, ForeignModifyOperation::Delete) {
            return Err(ForeignModifyError::unsupported(
                "foreign DELETE RETURNING columns can only be added for DELETE",
            ));
        }
        if !self.return_requirements.contains(attno) {
            return Err(ForeignModifyError::framework(
                "foreign DELETE RETURNING column was not requested by PostgreSQL",
            ));
        }
        if self.returning_columns.contains(&attno) {
            return Ok(());
        }
        let var = unsafe {
            ForeignModifyReturnRequirements::returning_column_var(
                self.relation.as_raw(),
                self.rtindex,
                attno,
            )
        }?;
        let name = ForeignModifyReturnRequirements::returning_column_name(attno)?;
        unsafe {
            pg_sys::add_row_identity_var(self.root, var, self.rtindex, name.as_ptr());
        }
        self.returning_columns.push(attno);
        Ok(())
    }
}

/// Planner context for provider modify private data.
pub struct ForeignModifyPlanContext<'a> {
    relation: RelationHandle<'a>,
    result_relation: pg_sys::Index,
    subplan_index: c_int,
    operation: ForeignModifyOperation,
    updated_columns: &'a [pg_sys::AttrNumber],
    returning: *mut pg_sys::List,
    return_requirements: ForeignModifyReturnRequirements,
    subplan_targetlist: *mut pg_sys::List,
}

impl<'a> ForeignModifyPlanContext<'a> {
    pub(crate) unsafe fn from_raw(
        root: *mut pg_sys::PlannerInfo,
        plan: *mut pg_sys::ModifyTable,
        relation: pg_sys::Relation,
        result_relation: pg_sys::Index,
        subplan_index: c_int,
        operation: ForeignModifyOperation,
        updated_columns: &'a [pg_sys::AttrNumber],
    ) -> Result<Self, ForeignModifyError> {
        if root.is_null() || plan.is_null() || relation.is_null() || subplan_index < 0
        {
            return Err(ForeignModifyError::framework(
                "PlanForeignModify received an incomplete planner context",
            ));
        }
        let returning_lists = unsafe { (*plan).returningLists };
        let returning = if returning_lists.is_null() {
            ptr::null_mut()
        } else if unsafe { pg_sys::list_length(returning_lists) } <= subplan_index {
            return Err(ForeignModifyError::framework(
                "PlanForeignModify returning-list index is outside its plan list",
            ));
        } else {
            (unsafe { pg_sys::list_nth(returning_lists, subplan_index) })
                as *mut pg_sys::List
        };
        let return_requirements =
            if matches!(operation, ForeignModifyOperation::Delete) {
                unsafe {
                    ForeignModifyReturnRequirements::from_returning_list(
                        returning,
                        relation,
                        result_relation,
                    )
                }?
            } else {
                ForeignModifyReturnRequirements::default()
            };
        let with_check_lists = unsafe { (*plan).withCheckOptionLists };
        if !with_check_lists.is_null()
            && unsafe { pg_sys::list_length(with_check_lists) } <= subplan_index
        {
            return Err(ForeignModifyError::framework(
                "PlanForeignModify WCO-list index is outside its plan list",
            ));
        }
        let subplan = unsafe { (*plan).plan.lefttree };
        if subplan.is_null() {
            return Err(ForeignModifyError::framework(
                "PlanForeignModify has no modify subplan",
            ));
        }
        let subplan_targetlist = unsafe { (*subplan).targetlist };
        if subplan_targetlist.is_null() {
            return Err(ForeignModifyError::framework(
                "PlanForeignModify subplan has no targetlist",
            ));
        }
        Ok(Self {
            relation: unsafe { RelationHandle::from_raw(relation) },
            result_relation,
            subplan_index,
            operation,
            updated_columns,
            returning,
            return_requirements,
            subplan_targetlist,
        })
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'a> {
        &self.relation
    }

    #[inline]
    pub fn operation(&self) -> ForeignModifyOperation {
        self.operation
    }

    #[inline]
    pub fn updated_columns(&self) -> &[pg_sys::AttrNumber] {
        self.updated_columns
    }

    /// Target-table user columns PostgreSQL may read from a DELETE result row.
    #[inline]
    pub fn returning_columns(&self) -> &[pg_sys::AttrNumber] {
        self.return_requirements.columns()
    }

    /// Whether DELETE RETURNING contains a whole-row target-table reference.
    #[inline]
    pub fn returning_all_columns(&self) -> bool {
        self.return_requirements.all_columns()
    }

    #[inline]
    pub fn result_relation(&self) -> pg_sys::Index {
        self.result_relation
    }

    #[inline]
    pub fn subplan_index(&self) -> c_int {
        self.subplan_index
    }

    pub(crate) fn returning_list(&self) -> *mut pg_sys::List {
        self.returning
    }

    pub(crate) fn subplan_targetlist(&self) -> *mut pg_sys::List {
        self.subplan_targetlist
    }
}
