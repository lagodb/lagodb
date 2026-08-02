use pg_lakebase_core::fdw::{
    FdwModify, ForeignModifyBeginContext, ForeignModifyCapabilities,
    ForeignModifyError, ForeignModifyOperation, ForeignModifyOutcome,
    ForeignModifyPlanContext, ForeignModifyPlanSpec, ForeignModifyPrivate,
    ForeignModifyRelationContext, ForeignModifyState, ForeignPrivateReader,
    ForeignPrivateWriter, ForeignRowIdentity, ForeignUpdateTargetContext,
    ModifyPlanSlot, ModifySlot,
};
use pg_lakebase_core::tuple::Cell;
use pgrx::FromDatum;
use pgrx::pg_sys;

use super::fixture::{TestRow, TestStore, TestTrace, TraceEvent};
use super::provider::{FrameworkTestFdw, IdentityMode};

#[derive(Clone, Copy, Debug)]
pub struct ModifyPrivate {
    mode: IdentityMode,
}

impl ForeignModifyPrivate for ModifyPrivate {
    fn encode(
        &self,
        writer: &mut ForeignPrivateWriter,
    ) -> Result<(), ForeignModifyError> {
        writer.append_bool(matches!(self.mode, IdentityMode::ItemPointer));
        Ok(())
    }

    unsafe fn decode(
        reader: &mut ForeignPrivateReader<'_>,
    ) -> Result<Self, ForeignModifyError> {
        Ok(Self {
            mode: if reader.read_bool()? {
                IdentityMode::ItemPointer
            } else {
                IdentityMode::Attribute
            },
        })
    }
}

pub struct ModifyState {
    relation_oid: pg_sys::Oid,
    mode: IdentityMode,
    returned_item_pointer_required: bool,
}

impl FdwModify for FrameworkTestFdw {
    type ModifyPrivateData = ModifyPrivate;
    type ModifyState = ModifyState;

    fn capabilities(
        ctx: &ForeignModifyRelationContext<'_>,
    ) -> Result<ForeignModifyCapabilities, ForeignModifyError> {
        TestStore::ensure(ctx.relation().oid());
        Ok(ForeignModifyCapabilities::insert_update_delete())
    }

    fn add_update_targets(
        ctx: &mut ForeignUpdateTargetContext<'_>,
    ) -> Result<(), ForeignModifyError> {
        let mode = IdentityMode::for_relation(ctx.relation());
        match mode {
            IdentityMode::Attribute => ctx.add_attribute_identity(1)?,
            IdentityMode::ItemPointer => ctx.add_item_pointer_identity()?,
        }
        if matches!(ctx.operation(), ForeignModifyOperation::Delete) {
            let returning_columns = ctx.returning_columns().to_vec();
            for attno in returning_columns {
                ctx.add_returning_column(attno)?;
            }
        }
        Ok(())
    }

    fn plan_modify(
        ctx: &ForeignModifyPlanContext<'_>,
    ) -> Result<ForeignModifyPlanSpec<Self::ModifyPrivateData>, ForeignModifyError>
    {
        let mode = IdentityMode::for_relation(ctx.relation());
        let spec = ForeignModifyPlanSpec::new(ModifyPrivate { mode });
        Ok(if matches!(mode, IdentityMode::ItemPointer) {
            spec.with_returned_item_pointer()
        } else {
            spec
        })
    }

    fn begin_modify(
        ctx: ForeignModifyBeginContext<'_, Self::ModifyPrivateData>,
    ) -> Result<Self::ModifyState, ForeignModifyError> {
        if ctx.row_identity_count() != 1
            && matches!(
                ctx.operation(),
                ForeignModifyOperation::Update | ForeignModifyOperation::Delete
            )
        {
            return Err(ForeignModifyError::unsupported(
                "test FDW expected one row identity for UPDATE or DELETE",
            ));
        }
        TestStore::ensure(ctx.relation().oid());
        Ok(ModifyState {
            relation_oid: ctx.relation().oid(),
            mode: ctx.private_data().mode,
            returned_item_pointer_required: ctx.returned_item_pointer_required(),
        })
    }
}

impl ForeignModifyState for ModifyState {
    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        let row = row_from_slot(slot)?;
        let inserted = TestStore::insert(self.relation_oid, row)
            .map_err(ForeignModifyError::unsupported)?;
        if self.returned_item_pointer_required {
            slot.set_returned_item_pointer(&inserted.item_pointer());
        }
        TestTrace::record(TraceEvent::Modify {
            operation: "insert",
            identity: "none",
            id: inserted.id,
            returned_item_pointer: self.returned_item_pointer_required,
        });
        Ok(ForeignModifyOutcome::Applied)
    }

    fn update(
        &mut self,
        slot: &mut ModifySlot<'_>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        let identity = identity_from_plan(plan_slot, self.mode)?;
        let row = row_from_slot(slot)?;
        let updated = TestStore::update(self.relation_oid, identity.id, row)
            .ok_or_else(|| {
                ForeignModifyError::unsupported("test FDW could not update row")
            })?;
        if self.returned_item_pointer_required {
            slot.set_returned_item_pointer(&updated.item_pointer());
        }
        TestTrace::record(TraceEvent::Modify {
            operation: "update",
            identity: identity.kind,
            id: identity.id,
            returned_item_pointer: self.returned_item_pointer_required,
        });
        Ok(ForeignModifyOutcome::Applied)
    }

    fn delete(
        &mut self,
        returned_slot: Option<&mut ModifySlot<'_>>,
        plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ForeignModifyError> {
        let identity = identity_from_plan(plan_slot, self.mode)?;
        let deleted =
            TestStore::delete(self.relation_oid, identity.id).ok_or_else(|| {
                ForeignModifyError::unsupported("test FDW could not delete row")
            })?;
        if let Some(slot) = returned_slot {
            write_row_to_modify_slot(slot, &deleted)?;
            if self.returned_item_pointer_required {
                slot.set_returned_item_pointer(&deleted.item_pointer());
            }
        }
        TestTrace::record(TraceEvent::Modify {
            operation: "delete",
            identity: identity.kind,
            id: identity.id,
            returned_item_pointer: self.returned_item_pointer_required,
        });
        Ok(ForeignModifyOutcome::Applied)
    }

    fn finish(&mut self) -> Result<(), ForeignModifyError> {
        Ok(())
    }
}

fn row_from_slot(slot: &mut ModifySlot<'_>) -> Result<TestRow, ForeignModifyError> {
    Ok(TestRow {
        id: read_i32(slot, 1)?,
        sort_key: read_i32(slot, 2)?,
        payload: read_text(slot, 3)?,
    })
}

struct ResolvedIdentity {
    kind: &'static str,
    id: i32,
}

fn identity_from_plan(
    plan_slot: &ModifyPlanSlot<'_>,
    expected_mode: IdentityMode,
) -> Result<ResolvedIdentity, ForeignModifyError> {
    let identity = plan_slot.identity(0)?;
    match (expected_mode, identity) {
        (
            IdentityMode::Attribute,
            ForeignRowIdentity::Attribute { attno: 1, value },
        ) => {
            let id = unsafe { i32::from_datum(value.datum(), value.is_null()) }
                .ok_or_else(|| {
                    ForeignModifyError::unsupported("test FDW received a NULL id")
                })?;
            Ok(ResolvedIdentity {
                kind: "attribute",
                id,
            })
        }
        (
            IdentityMode::ItemPointer,
            ForeignRowIdentity::ItemPointer(item_pointer),
        ) => {
            let id = i32::from(item_pointer.offset()) - 1;
            Ok(ResolvedIdentity {
                kind: "item-pointer",
                id,
            })
        }
        (IdentityMode::Attribute, ForeignRowIdentity::Attribute { .. }) => {
            Err(ForeignModifyError::unsupported(
                "test FDW received an unexpected attribute identity",
            ))
        }
        (IdentityMode::Attribute, ForeignRowIdentity::ItemPointer(_)) => {
            Err(ForeignModifyError::unsupported(
                "test FDW received an item-pointer identity in attribute mode",
            ))
        }
        (IdentityMode::ItemPointer, ForeignRowIdentity::Attribute { .. }) => {
            Err(ForeignModifyError::unsupported(
                "test FDW received an attribute identity in item-pointer mode",
            ))
        }
    }
}

fn read_i32(
    slot: &mut ModifySlot<'_>,
    attno: pg_sys::AttrNumber,
) -> Result<i32, ForeignModifyError> {
    // SAFETY: `attno` is one of this test relation's catalog-defined,
    // non-dropped user attributes.
    let value = unsafe { slot.datum_by_attno(attno) };
    if value.is_null() {
        return Err(ForeignModifyError::unsupported(
            "test FDW received a NULL int4 column",
        ));
    }
    unsafe { i32::from_datum(value.datum(), false) }.ok_or_else(|| {
        ForeignModifyError::unsupported("test FDW received an invalid int4 datum")
    })
}

fn read_text(
    slot: &mut ModifySlot<'_>,
    attno: pg_sys::AttrNumber,
) -> Result<String, ForeignModifyError> {
    // SAFETY: `attno` is one of this test relation's catalog-defined,
    // non-dropped user attributes.
    let value = unsafe { slot.datum_by_attno(attno) };
    if value.is_null() {
        return Err(ForeignModifyError::unsupported(
            "test FDW received a NULL text column",
        ));
    }
    unsafe { String::from_datum(value.datum(), false) }.ok_or_else(|| {
        ForeignModifyError::unsupported("test FDW received an invalid text datum")
    })
}

fn write_row_to_modify_slot(
    slot: &mut ModifySlot<'_>,
    row: &TestRow,
) -> Result<(), ForeignModifyError> {
    // SAFETY: attributes 1..=3 are the test relation's catalog-defined,
    // non-dropped user columns.
    unsafe {
        slot.set_cell_by_attno(1, Some(Cell::I32(row.id)))?;
        slot.set_cell_by_attno(2, Some(Cell::I32(row.sort_key)))?;
        slot.set_cell_by_attno(3, Some(Cell::String(row.payload.clone())))?;
    }
    Ok(())
}
