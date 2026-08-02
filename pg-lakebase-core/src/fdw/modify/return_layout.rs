//! Plan-time layout for old values used by foreign DELETE return rows.

use core::ffi::{CStr, c_int};
use std::str;

use pgrx::pg_sys::{self, INDEX_VAR, INNER_VAR, OUTER_VAR};

use super::super::row_identity::ModifyPlanSlot;
use super::error::ForeignModifyError;
use super::return_requirements::{
    ForeignModifyReturnRequirements, RelationAttributeMetadata,
};
use super::slot::ModifySlot;

const RETURN_COLUMN_PREFIX: &str = "__pg_lakebase_fdw_return_attr_";
const WHOLE_ROW_NAME: &CStr = c"wholerow";

#[derive(Debug, Clone, Copy)]
pub(crate) struct ForeignModifyReturnColumn {
    pub(super) plan_attno: pg_sys::AttrNumber,
    pub(super) plan_index: usize,
    pub(super) relation_attno: pg_sys::AttrNumber,
    pub(super) relation_index: usize,
    pub(super) type_oid: pg_sys::Oid,
    pub(super) type_mod: i32,
    pub(super) collation: pg_sys::Oid,
}

/// Cached mapping from DELETE plan-slot old values to relation attributes.
#[derive(Debug, Clone, Default)]
pub(crate) struct ForeignModifyReturnLayout {
    columns: Box<[ForeignModifyReturnColumn]>,
    whole_row_plan_index: Option<usize>,
    whole_row_is_full: bool,
    whole_row_columns: Box<[ForeignModifyReturnColumn]>,
}

impl ForeignModifyReturnLayout {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    #[inline]
    pub(crate) fn has_plan_values(&self) -> bool {
        !self.columns.is_empty() || !self.whole_row_columns.is_empty()
    }

    /// # Safety
    ///
    /// `targetlist` must be the live final ModifyTable subplan targetlist and
    /// `relation` must remain open for the duration of this call.
    pub(crate) unsafe fn from_targetlist(
        targetlist: *mut pg_sys::List,
        relation: pg_sys::Relation,
        rtindex: pg_sys::Index,
        requirements: &ForeignModifyReturnRequirements,
    ) -> Result<Self, ForeignModifyError> {
        if targetlist.is_null()
            || relation.is_null()
            || rtindex == 0
            || unsafe { (*relation).rd_att.is_null() }
        {
            return Err(ForeignModifyError::framework(
                "foreign modify return layout has incomplete target metadata",
            ));
        }

        let mut columns = Vec::new();
        let mut whole_row_plan_attno = None;
        let length = unsafe { pg_sys::list_length(targetlist) };
        if length < 0 {
            return Err(ForeignModifyError::framework(
                "foreign modify subplan targetlist has a negative length",
            ));
        }
        for index in 0..length {
            let target_entry = unsafe { pg_sys::list_nth(targetlist, index) }
                as *mut pg_sys::TargetEntry;
            if target_entry.is_null()
                || unsafe { (*target_entry).xpr.type_ }
                    != pg_sys::NodeTag::T_TargetEntry
            {
                return Err(ForeignModifyError::framework(
                    "foreign modify subplan targetlist has a malformed target entry",
                ));
            }
            if !unsafe { (*target_entry).resjunk }
                || unsafe { (*target_entry).resname.is_null() }
            {
                continue;
            }
            let name = unsafe { CStr::from_ptr((*target_entry).resname) };
            let plan_attno = unsafe { (*target_entry).resno };
            if plan_attno <= 0 {
                return Err(ForeignModifyError::framework(
                    "foreign modify return target has an invalid plan attribute",
                ));
            }
            let plan_index =
                usize::try_from(plan_attno as i32 - 1).map_err(|_| {
                    ForeignModifyError::framework(
                        "foreign modify return target has an invalid plan attribute",
                    )
                })?;
            if plan_index >= length as usize {
                return Err(ForeignModifyError::framework(
                    "foreign modify return target is outside its plan slot",
                ));
            }

            if name == WHOLE_ROW_NAME {
                if whole_row_plan_attno.replace(plan_index).is_some() {
                    return Err(ForeignModifyError::framework(
                        "foreign modify subplan has duplicate wholerow targets",
                    ));
                }
                let expr = unsafe { (*target_entry).expr };
                if expr.is_null()
                    || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var
                    || !Self::whole_row_var_matches_relation(
                        expr.cast::<pg_sys::Var>(),
                        rtindex,
                    )
                {
                    return Err(ForeignModifyError::framework(
                        "foreign modify wholerow target has a malformed expression",
                    ));
                }
                continue;
            }

            let bytes = name.to_bytes();
            if !bytes.starts_with(RETURN_COLUMN_PREFIX.as_bytes()) {
                continue;
            }
            let relation_attno = Self::parse_returning_attno(bytes)?;
            if !requirements.contains(relation_attno) {
                return Err(ForeignModifyError::framework(
                    "foreign modify return target is not required by DELETE RETURNING",
                ));
            }
            let attribute =
                RelationAttributeMetadata::from_relation(relation, relation_attno)?;
            let expr = unsafe { (*target_entry).expr };
            if expr.is_null() || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var {
                return Err(ForeignModifyError::framework(
                    "foreign modify return column has a malformed expression",
                ));
            }
            let var = expr.cast::<pg_sys::Var>();
            if !Self::attribute_var_matches_relation(var, rtindex, relation_attno)
                || unsafe { (*var).vartype } != attribute.type_oid
                || unsafe { (*var).vartypmod } != attribute.type_mod
                || unsafe { (*var).varcollid } != attribute.collation
            {
                return Err(ForeignModifyError::framework(
                    "foreign modify return column does not match its relation attribute",
                ));
            }
            if columns.iter().any(|column: &ForeignModifyReturnColumn| {
                column.plan_attno == plan_attno
                    || column.relation_attno == relation_attno
            }) {
                return Err(ForeignModifyError::framework(
                    "foreign modify subplan has duplicate return columns",
                ));
            }
            columns.push(ForeignModifyReturnColumn {
                plan_attno,
                plan_index,
                relation_attno,
                relation_index: attribute.relation_index,
                type_oid: attribute.type_oid,
                type_mod: attribute.type_mod,
                collation: attribute.collation,
            });
        }

        if let Some(whole_row_plan_index) = whole_row_plan_attno
            && columns
                .iter()
                .any(|column| column.plan_index == whole_row_plan_index)
        {
            return Err(ForeignModifyError::framework(
                "foreign modify subplan reuses a plan attribute for whole-row and scalar return values",
            ));
        }

        let whole_row_columns = if whole_row_plan_attno.is_some() {
            requirements
                .columns()
                .iter()
                .copied()
                .filter(|attno| {
                    !columns.iter().any(|column| column.relation_attno == *attno)
                })
                .map(|relation_attno| {
                    let attribute = RelationAttributeMetadata::from_relation(
                        relation,
                        relation_attno,
                    )?;
                    Ok(ForeignModifyReturnColumn {
                        plan_attno: 0,
                        plan_index: 0,
                        relation_attno,
                        relation_index: attribute.relation_index,
                        type_oid: attribute.type_oid,
                        type_mod: attribute.type_mod,
                        collation: attribute.collation,
                    })
                })
                .collect::<Result<Vec<_>, ForeignModifyError>>()?
                .into_boxed_slice()
        } else {
            Vec::new().into_boxed_slice()
        };
        Ok(Self {
            columns: columns.into_boxed_slice(),
            whole_row_plan_index: whole_row_plan_attno,
            whole_row_is_full: whole_row_plan_attno.is_some()
                && requirements.all_columns(),
            whole_row_columns,
        })
    }

    /// Validate the initialized executor descriptor once before row execution.
    ///
    /// # Safety
    ///
    /// `tuple_desc` must be the result descriptor of the live DELETE subplan.
    pub(crate) unsafe fn validate_tuple_desc(
        &self,
        tuple_desc: pg_sys::TupleDesc,
    ) -> Result<(), ForeignModifyError> {
        if tuple_desc.is_null() {
            return Err(ForeignModifyError::framework(
                "foreign modify return layout has no plan-slot descriptor",
            ));
        }
        let natts = unsafe { (*tuple_desc).natts };
        if natts < 0
            || (natts > 0 && unsafe { (*tuple_desc).attrs.as_ptr().is_null() })
        {
            return Err(ForeignModifyError::framework(
                "foreign modify return layout has an invalid plan-slot descriptor",
            ));
        }
        let max_plan_index = self
            .whole_row_plan_index
            .into_iter()
            .chain(self.columns.iter().map(|column| column.plan_index))
            .max();
        if let Some(plan_index) = max_plan_index
            && plan_index >= natts as usize
        {
            return Err(ForeignModifyError::framework(
                "foreign modify return target is outside its plan slot",
            ));
        }
        let attrs = unsafe { (*tuple_desc).attrs.as_ptr() };
        if let Some(plan_index) = self.whole_row_plan_index {
            let attribute = unsafe { &*attrs.add(plan_index) };
            if attribute.atttypid != pg_sys::RECORDOID
                || attribute.atttypmod != -1
                || attribute.attcollation != pg_sys::InvalidOid
            {
                return Err(ForeignModifyError::framework(
                    "foreign modify wholerow target has an invalid plan-slot type",
                ));
            }
        }
        for column in &self.columns {
            let attribute = unsafe { &*attrs.add(column.plan_index) };
            if attribute.atttypid != column.type_oid
                || attribute.atttypmod != column.type_mod
                || attribute.attcollation != column.collation
            {
                return Err(ForeignModifyError::framework(
                    "foreign modify return column has an invalid plan-slot type",
                ));
            }
        }
        Ok(())
    }

    /// Borrow plan-slot values into the returned row before provider-specific
    /// DELETE logic runs. PostgreSQL materializes the returned projection
    /// before it reuses the plan slot for the next row.
    pub(crate) fn populate_from_plan_slot(
        &self,
        plan_slot: &ModifyPlanSlot<'_>,
        row: &mut ModifySlot<'_>,
    ) {
        if let Some(plan_index) = self.whole_row_plan_index
            && !self.whole_row_columns.is_empty()
        {
            let (datum, _) = unsafe { plan_slot.datum_at(plan_index) };
            row.set_columns_from_composite(
                datum,
                &self.whole_row_columns,
                self.whole_row_is_full,
            );
        }
        for column in &self.columns {
            let (datum, is_null) = unsafe { plan_slot.datum_at(column.plan_index) };
            row.set_plan_datum(column.relation_index, datum, is_null);
        }
    }

    fn parse_returning_attno(
        name: &[u8],
    ) -> Result<pg_sys::AttrNumber, ForeignModifyError> {
        let digits = &name[RETURN_COLUMN_PREFIX.len()..];
        if digits.is_empty() {
            return Err(ForeignModifyError::framework(
                "foreign modify return column name has no attribute number",
            ));
        }
        let attno = str::from_utf8(digits)
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .and_then(|value| pg_sys::AttrNumber::try_from(value).ok())
            .filter(|attno| *attno > 0)
            .ok_or_else(|| {
                ForeignModifyError::framework(
                    "foreign modify return column name has an invalid attribute number",
                )
            })?;
        Ok(attno)
    }

    fn attribute_var_matches_relation(
        var: *mut pg_sys::Var,
        rtindex: pg_sys::Index,
        attno: pg_sys::AttrNumber,
    ) -> bool {
        unsafe {
            if (*var).varlevelsup != 0 {
                return false;
            }
            let direct = (*var).varno == rtindex as c_int && (*var).varattno == attno;
            let join_output = ((*var).varno == INNER_VAR
                || (*var).varno == OUTER_VAR)
                && (*var).varnosyn == rtindex
                && (*var).varattnosyn == attno;
            let projected_scan = (*var).varno == INDEX_VAR
                && (*var).varattno > 0
                && (*var).varnosyn == rtindex
                && (*var).varattnosyn == attno;
            direct || join_output || projected_scan
        }
    }

    fn whole_row_var_matches_relation(
        var: *mut pg_sys::Var,
        rtindex: pg_sys::Index,
    ) -> bool {
        unsafe {
            if (*var).varlevelsup != 0
                || (*var).vartype != pg_sys::RECORDOID
                || (*var).vartypmod != -1
                || (*var).varcollid != pg_sys::InvalidOid
            {
                return false;
            }
            let direct = (*var).varno == rtindex as c_int && (*var).varattno == 0;
            let join_output = ((*var).varno == INNER_VAR
                || (*var).varno == OUTER_VAR)
                && (*var).varnosyn == rtindex
                && (*var).varattnosyn == 0;
            let projected_scan = (*var).varno == INDEX_VAR
                && (*var).varattno > 0
                && (*var).varnosyn == rtindex
                && (*var).varattnosyn == 0;
            direct || join_output || projected_scan
        }
    }
}
