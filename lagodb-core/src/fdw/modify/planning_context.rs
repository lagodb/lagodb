//! Provider-facing planner contexts for foreign modify callbacks.

use core::ffi::c_int;
use core::ptr;

use pgrx::pg_sys;

use crate::handles::RelationHandle;

use super::super::row_identity::ForeignRowIdentityKind;
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
    return_requirements: ForeignModifyReturnRequirements,
    operation: ForeignModifyOperation,
}

impl<'a> ForeignUpdateTargetContext<'a> {
    /// # Safety
    ///
    /// PostgreSQL must supply the live planner root, result-relation index and
    /// open target relation for one `AddForeignUpdateTargets` callback.
    pub(crate) unsafe fn from_raw(
        root: *mut pg_sys::PlannerInfo,
        rtindex: pg_sys::Index,
        relation: pg_sys::Relation,
    ) -> Result<Self, ForeignModifyError> {
        let parse = unsafe { (*root).parse };
        let operation =
            ForeignModifyOperation::from_pg(unsafe { (*parse).commandType })?;
        let return_requirements =
            if matches!(operation, ForeignModifyOperation::Delete) {
                // This early phase decides which physical values the provider
                // should propagate into the eventual modify subplan.
                let returning = unsafe {
                    Self::returning_list_for_relation(root, parse, rtindex)
                };
                unsafe {
                    ForeignModifyReturnRequirements::from_returning_list(
                        returning, relation, rtindex,
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
            return_requirements,
            operation,
        })
    }

    /// Translate the query-level RETURNING list to this leaf result relation.
    ///
    /// PostgreSQL performs the same multilevel appendrel adjustment while
    /// constructing each leaf's `ModifyTable.returningLists` entry. At this
    /// earlier callback, the parse list still refers to the named top-level
    /// target relation.
    unsafe fn returning_list_for_relation(
        root: *mut pg_sys::PlannerInfo,
        parse: *mut pg_sys::Query,
        rtindex: pg_sys::Index,
    ) -> *mut pg_sys::List {
        let returning = unsafe { (*parse).returningList };
        // Query.resultRelation is declared as int in PostgreSQL, while planner
        // relation indexes use Index. AddForeignUpdateTargets is invoked only
        // for a valid result relation, so this is the same conversion used by
        // the surrounding planner code.
        let top_result_relation = unsafe { (*parse).resultRelation as pg_sys::Index };
        if returning.is_null() || rtindex == top_result_relation {
            return returning;
        }

        // The leaf RelOptInfo is built only after AddForeignUpdateTargets
        // returns, but PostgreSQL has already installed every AppendRelInfo
        // in this result-relation chain.
        let mut appinfos = Vec::new();
        let mut child_relid = rtindex;
        while child_relid != top_result_relation {
            let appinfo =
                unsafe { *(*root).append_rel_array.add(child_relid as usize) };
            appinfos.push(appinfo);
            child_relid = unsafe { (*appinfo).parent_relid };
        }

        let mut adjusted = returning.cast::<pg_sys::Node>();
        for mut appinfo in appinfos.into_iter().rev() {
            adjusted = unsafe {
                pg_sys::adjust_appendrel_attrs(root, adjusted, 1, &mut appinfo)
            };
        }
        adjusted.cast::<pg_sys::List>()
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
        unsafe {
            pg_sys::add_row_identity_var(
                self.root,
                var,
                self.rtindex,
                ForeignRowIdentityKind::item_pointer_target_name().as_ptr(),
            );
        }
        self.item_pointer_added = true;
        Ok(())
    }

    /// Add one positive relation attribute as a provider-defined row identity.
    /// The framework gives it a stable internal name and decodes it from the
    /// plan slot for each UPDATE or DELETE row. The name preserves both the
    /// logical column name and this leaf relation's physical attno.
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
        let tuple_desc = self.relation.tuple_desc_handle();
        let index = usize::try_from(attno as i32 - 1).map_err(|_| {
            ForeignModifyError::framework(
                "foreign row identity attribute is out of range",
            )
        })?;
        let attrs = tuple_desc.attributes();
        if index >= attrs.len() {
            return Err(ForeignModifyError::framework(
                "foreign row identity attribute is outside the relation",
            ));
        }
        let attr = &attrs[index];
        if attr.attisdropped {
            return Err(ForeignModifyError::framework(
                "foreign row identity cannot use a dropped attribute",
            ));
        }
        let var = unsafe {
            pg_sys::makeVar(
                self.rtindex as c_int,
                attr.attnum,
                attr.atttypid,
                attr.atttypmod,
                attr.attcollation,
                0,
            )
        };
        let name = ForeignRowIdentityKind::attribute_target_name(attr);
        unsafe {
            pg_sys::add_row_identity_var(self.root, var, self.rtindex, name.as_ptr());
        }
        self.attribute_identities.push(attr.attnum);
        Ok(())
    }

    /// Register every target-table value required by DELETE RETURNING. This is
    /// framework-owned because providers do not interpret these as identities.
    pub(crate) fn register_returning_columns(&self) {
        let tuple_desc = self.relation.tuple_desc_handle();
        let attributes = tuple_desc.attributes();
        for &attno in self.return_requirements.columns() {
            // The requirements were built and validated from this descriptor
            // earlier in the same planner callback.
            let attribute = &attributes[attno as usize - 1];
            let var = ForeignModifyReturnRequirements::returning_column_var(
                self.rtindex,
                attribute,
            );
            let name =
                ForeignModifyReturnRequirements::returning_column_name(attribute);
            unsafe {
                pg_sys::add_row_identity_var(
                    self.root,
                    var,
                    self.rtindex,
                    name.as_ptr(),
                );
            }
        }
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
}

impl<'a> ForeignModifyPlanContext<'a> {
    pub(crate) unsafe fn from_raw(
        plan: *mut pg_sys::ModifyTable,
        relation: pg_sys::Relation,
        result_relation: pg_sys::Index,
        subplan_index: c_int,
        operation: ForeignModifyOperation,
        updated_columns: &'a [pg_sys::AttrNumber],
    ) -> Result<Self, ForeignModifyError> {
        if plan.is_null() || relation.is_null() || subplan_index < 0 {
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
        let with_check_lists = unsafe { (*plan).withCheckOptionLists };
        if !with_check_lists.is_null()
            && unsafe { pg_sys::list_length(with_check_lists) } <= subplan_index
        {
            return Err(ForeignModifyError::framework(
                "PlanForeignModify WCO-list index is outside its plan list",
            ));
        }
        let return_requirements =
            if matches!(operation, ForeignModifyOperation::Delete) {
                // returningLists is PostgreSQL's final per-result-relation
                // semantic requirement; the subplan targetlist is only its
                // physical transport layout.
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
        Ok(Self {
            relation: unsafe { RelationHandle::from_raw(relation) },
            result_relation,
            subplan_index,
            operation,
            updated_columns,
            returning,
            return_requirements,
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

    #[inline]
    pub(crate) fn return_requirements(&self) -> &ForeignModifyReturnRequirements {
        &self.return_requirements
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
}
