//! Relation-layout and Iceberg field-id bindings shared by scans and predicates.
//!
//! PostgreSQL attribute names are resolved against an Iceberg schema once at
//! the catalog boundary. Ordered consumers use the compact bindings directly;
//! CustomScan additionally builds a direct `attno - 1` index for predicate
//! translation and retains it for the scan lifetime.

#[cfg(feature = "pg_test")]
mod pg_test;

use iceberg_lite::spec::Schema as IcebergSchema;
use pg_lakebase_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};

/// One live (non-dropped) PostgreSQL relation column.
#[derive(Debug)]
pub(crate) struct LiveColumn {
    /// One-based PostgreSQL attribute number.
    pub(crate) attno: pg_sys::AttrNumber,
    /// Column name used only to resolve the catalog-boundary Iceberg field id.
    pub(crate) name: String,
    /// PostgreSQL's declared nullability contract.
    pub(crate) required: bool,
}

impl LiveColumn {
    pub(crate) fn new(
        attno: pg_sys::AttrNumber,
        name: String,
        required: bool,
    ) -> Self {
        Self {
            attno,
            name,
            required,
        }
    }
}

/// Descriptor-derived relation layout shared by read and write paths.
#[derive(Debug)]
pub(crate) struct RelationShape {
    live_columns: Vec<LiveColumn>,
    slot_width: usize,
    attr_types: Vec<(pg_sys::Oid, i32)>,
}

impl RelationShape {
    pub(crate) fn from_relation(rel: &RelationHandle) -> IcebergResult<Self> {
        let live_columns = rel
            .live_columns()
            .iter()
            .map(|column| {
                let name = column.name().to_str().map_err(|_| {
                    IcebergError::SchemaBuildError(
                        "PostgreSQL column names must be valid UTF-8 for Iceberg"
                            .to_owned(),
                    )
                })?;
                Ok(LiveColumn::new(
                    column.attno(),
                    name.to_owned(),
                    column.is_not_null(),
                ))
            })
            .collect::<IcebergResult<Vec<_>>>()?;

        Ok(Self {
            live_columns,
            slot_width: rel.natts(),
            attr_types: rel.attr_types(),
        })
    }

    pub(crate) fn live_columns(&self) -> &[LiveColumn] {
        &self.live_columns
    }

    fn slot_width(&self) -> usize {
        self.slot_width
    }

    pub(crate) fn attr_types(&self) -> &[(pg_sys::Oid, i32)] {
        &self.attr_types
    }
}

/// One live relation field bound to an Iceberg field id.
#[derive(Debug)]
pub(crate) struct RelationFieldBinding {
    pub(crate) attno: pg_sys::AttrNumber,
    pub(crate) destination: usize,
    /// Iceberg field id; this is the execution identity.
    pub(crate) field_id: i32,
    /// Current name retained for bound-term display and diagnostics only.
    pub(crate) debug_name: String,
}

impl RelationFieldBinding {
    fn with_destination(&self, destination: usize) -> Self {
        Self {
            attno: self.attno,
            destination,
            field_id: self.field_id,
            debug_name: self.debug_name.clone(),
        }
    }
}

/// Compact ordered field bindings shared by read and write planning.
#[derive(Debug)]
pub(crate) struct RelationFieldMap {
    fields: Vec<RelationFieldBinding>,
    /// Width of the base relation's PostgreSQL attribute-number domain.
    relation_width: usize,
    slot_width: usize,
    attr_types: Vec<(pg_sys::Oid, i32)>,
}

impl RelationFieldMap {
    /// Resolve every live relation column against the current Iceberg schema.
    pub(crate) fn from_shape(
        schema: &IcebergSchema,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let mut fields = Vec::with_capacity(shape.live_columns().len());
        for col in shape.live_columns() {
            let field = schema
                .field_by_name(&col.name)
                .ok_or_else(|| IcebergError::ColumnNotFound(col.name.clone()))?;
            fields.push(RelationFieldBinding {
                attno: col.attno,
                destination: Self::attribute_offset(col.attno, shape.slot_width())?,
                field_id: field.id,
                debug_name: col.name.clone(),
            });
        }
        Ok(Self {
            fields,
            relation_width: shape.slot_width(),
            slot_width: shape.slot_width(),
            attr_types: shape.attr_types().to_vec(),
        })
    }

    /// Retain projected bindings in projection order and remap their tuple
    /// destinations. `projected` yields `(source attno, destination)` pairs.
    pub(crate) fn project(
        self,
        projected: impl ExactSizeIterator<Item = (pg_sys::AttrNumber, usize)>,
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let source_index = RelationFieldIndex::new(self);
        let mut fields = Vec::with_capacity(projected.len());
        for (attno, destination) in projected {
            if attno <= 0 {
                return Err(IcebergError::InvariantViolated(
                    "RelationFieldMap: projected source attno must be >= 1",
                ));
            }
            let binding = source_index.binding_for_attno(attno).ok_or_else(|| {
                IcebergError::ColumnNotFound(format!("attno {attno}"))
            })?;
            fields.push(binding.with_destination(Self::validate_destination(
                destination,
                slot_width,
            )?));
        }
        Ok(Self {
            fields,
            relation_width: source_index.relation_width(),
            slot_width,
            attr_types: attr_types.to_vec(),
        })
    }

    /// Add the direct user-attno lookup needed by CustomScan predicates.
    pub(crate) fn into_indexed(self) -> RelationFieldIndex {
        RelationFieldIndex::new(self)
    }

    pub(crate) fn bindings(&self) -> &[RelationFieldBinding] {
        &self.fields
    }

    pub(crate) fn slot_width(&self) -> usize {
        self.slot_width
    }

    pub(crate) fn attr_types(&self) -> &[(pg_sys::Oid, i32)] {
        &self.attr_types
    }

    pub(crate) fn field_ids(&self) -> Vec<i32> {
        self.fields.iter().map(|field| field.field_id).collect()
    }

    pub(crate) fn validate_destination(
        destination: usize,
        slot_width: usize,
    ) -> IcebergResult<usize> {
        if destination >= slot_width {
            return Err(IcebergError::InvariantViolated(
                "ColumnMapping: projected destination is outside the slot width",
            ));
        }
        Ok(destination)
    }

    fn attribute_offset(
        attno: pg_sys::AttrNumber,
        slot_width: usize,
    ) -> IcebergResult<usize> {
        if attno < 1 {
            return Err(IcebergError::InvariantViolated(
                "ColumnMapping: projected column attno must be >= 1",
            ));
        }
        let offset = (attno as usize) - 1;
        if offset >= slot_width {
            return Err(IcebergError::InvariantViolated(
                "ColumnMapping: computed dest is outside the slot width",
            ));
        }
        Ok(offset)
    }
}

/// Scan-lifetime direct lookup over one compact relation field map.
///
/// Only CustomScan constructs this index. TableAM, ANALYZE, and write paths
/// consume the ordered bindings while building their plans and then release
/// them.
#[derive(Debug)]
pub(crate) struct RelationFieldIndex {
    field_map: RelationFieldMap,
    /// Slot `attno - 1` contains the compact binding index, or
    /// `UNMAPPED_FIELD` for a dropped or unselected attribute.
    by_attno: Box<[usize]>,
}

impl RelationFieldIndex {
    const UNMAPPED_FIELD: usize = usize::MAX;

    fn new(field_map: RelationFieldMap) -> Self {
        let mut by_attno = vec![Self::UNMAPPED_FIELD; field_map.relation_width];
        for (index, binding) in field_map.fields.iter().enumerate() {
            // `from_shape` validates every attno against `relation_width`, and
            // `project` only copies bindings originating from such a map.
            let offset = (binding.attno as usize) - 1;
            by_attno[offset] = index;
        }
        Self {
            field_map,
            by_attno: by_attno.into_boxed_slice(),
        }
    }

    pub(crate) fn binding_for_attno(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> Option<&RelationFieldBinding> {
        if attno <= 0 {
            return None;
        }
        let index = *self.by_attno.get((attno as usize) - 1)?;
        (index != Self::UNMAPPED_FIELD).then(|| &self.field_map.fields[index])
    }

    fn relation_width(&self) -> usize {
        self.by_attno.len()
    }
}
