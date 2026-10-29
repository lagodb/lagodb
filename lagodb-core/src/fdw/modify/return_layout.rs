//! Plan-time layout for old values used by foreign DELETE return rows.

use core::ffi::CStr;

use pgrx::pg_sys;

use super::super::row_identity::ModifyPlanSlot;
use super::error::ForeignModifyError;
use super::return_requirements::ForeignModifyReturnRequirements;
use super::slot::ModifySlot;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ForeignModifyReturnColumn {
    pub(super) plan_index: usize,
    pub(super) relation_index: usize,
}

/// Cached mapping from DELETE plan-slot old values to relation attributes.
#[derive(Debug, Clone, Default)]
pub(crate) struct ForeignModifyReturnLayout {
    columns: Box<[ForeignModifyReturnColumn]>,
}

impl ForeignModifyReturnLayout {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    #[inline]
    pub(crate) fn has_plan_values(&self) -> bool {
        !self.columns.is_empty()
    }

    /// # Safety
    ///
    /// `targetlist` must be the live final ModifyTable subplan targetlist and
    /// `relation` must remain open for the duration of this call.
    pub(crate) unsafe fn from_targetlist(
        targetlist: *mut pg_sys::List,
        relation: pg_sys::Relation,
        requirements: &ForeignModifyReturnRequirements,
    ) -> Result<Self, ForeignModifyError> {
        let mut columns = Vec::new();
        let length = unsafe { pg_sys::list_length(targetlist) };
        for index in 0..length {
            let target_entry = unsafe { pg_sys::list_nth(targetlist, index) }
                as *mut pg_sys::TargetEntry;
            if !unsafe { (*target_entry).resjunk }
                || unsafe { (*target_entry).resname.is_null() }
            {
                continue;
            }
            let name = unsafe { CStr::from_ptr((*target_entry).resname) };
            let Some(relation_attno) = (unsafe {
                ForeignModifyReturnRequirements::returning_column_attno(
                    relation, name,
                )
            })?
            else {
                continue;
            };
            if !requirements.contains(relation_attno) {
                continue;
            }

            let plan_attno = unsafe { (*target_entry).resno };
            columns.push(ForeignModifyReturnColumn {
                plan_index: (plan_attno as usize) - 1,
                relation_index: (relation_attno as usize) - 1,
            });
        }

        Ok(Self {
            columns: columns.into_boxed_slice(),
        })
    }

    /// Borrow plan-slot values into the returned row before provider-specific
    /// DELETE logic runs. PostgreSQL materializes the returned projection
    /// before it reuses the plan slot for the next row.
    pub(crate) fn populate_from_plan_slot(
        &self,
        plan_slot: &ModifyPlanSlot<'_>,
        row: &mut ModifySlot<'_>,
    ) {
        for column in &self.columns {
            let (datum, is_null) = unsafe { plan_slot.datum_at(column.plan_index) };
            row.set_plan_datum(column.relation_index, datum, is_null);
        }
    }
}
