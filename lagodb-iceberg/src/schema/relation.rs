//! Relation-layout and Iceberg field-id bindings shared by scans and predicates.
//!
//! PostgreSQL attribute names are resolved against an Iceberg schema once at
//! the catalog boundary. Ordered consumers use the compact bindings directly;
//! local attributes use a direct `attno - 1` index; remote fields are resolved
//! only when a projection or predicate actually needs them.

use iceberg_lite::spec::Schema as IcebergSchema;
use lagodb_core::handles::RelationHandle;
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
pub(crate) struct RelationLayout {
    live_columns: Vec<LiveColumn>,
    by_attno: Box<[Option<usize>]>,
    slot_width: usize,
    attr_types: Vec<(pg_sys::Oid, i32)>,
}

impl RelationLayout {
    /// Construct a synthetic layout for backend binding tests.
    #[cfg(feature = "pg_test")]
    pub(crate) fn for_test(
        live_columns: Vec<LiveColumn>,
        slot_width: usize,
        attr_types: Vec<(pg_sys::Oid, i32)>,
    ) -> Self {
        Self::new(live_columns, slot_width, attr_types)
    }

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

        Ok(Self::new(live_columns, rel.natts(), rel.attr_types()))
    }

    fn new(
        live_columns: Vec<LiveColumn>,
        slot_width: usize,
        attr_types: Vec<(pg_sys::Oid, i32)>,
    ) -> Self {
        let mut by_attno = vec![None; slot_width];
        for (index, column) in live_columns.iter().enumerate() {
            by_attno[(column.attno - 1) as usize] = Some(index);
        }
        Self {
            live_columns,
            by_attno: by_attno.into_boxed_slice(),
            slot_width,
            attr_types,
        }
    }

    pub(crate) fn column(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> IcebergResult<&LiveColumn> {
        let offset = RelationFieldMap::attribute_offset(attno, self.slot_width)?;
        self.by_attno[offset]
            .map(|index| &self.live_columns[index])
            .ok_or_else(|| IcebergError::ColumnNotFound(format!("attno {attno}")))
    }

    pub(crate) fn live_columns(&self) -> &[LiveColumn] {
        &self.live_columns
    }

    pub(crate) fn slot_width(&self) -> usize {
        self.slot_width
    }

    pub(crate) fn attr_types(&self) -> &[(pg_sys::Oid, i32)] {
        &self.attr_types
    }

    /// Resolve a PostgreSQL attribute projection directly to Iceberg field
    /// ids without constructing slot destinations or datum-type bindings.
    pub(crate) fn project_field_ids(
        &self,
        schema: &IcebergSchema,
        projected: impl ExactSizeIterator<Item = pg_sys::AttrNumber>,
    ) -> IcebergResult<Box<[i32]>> {
        let mut field_ids = Vec::with_capacity(projected.len());
        for attno in projected {
            let column = self.column(attno)?;
            let field = schema
                .as_struct()
                .field_by_name(&column.name)
                .ok_or_else(|| IcebergError::ColumnNotFound(column.name.clone()))?;
            field_ids.push(field.id);
        }
        Ok(field_ids.into_boxed_slice())
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

/// Compact ordered field bindings shared by read and write planning.
#[derive(Debug)]
pub(crate) struct RelationFieldMap {
    fields: Vec<RelationFieldBinding>,
}

impl RelationFieldMap {
    /// Resolve every live relation column against the current Iceberg schema.
    pub(crate) fn bind(
        schema: &IcebergSchema,
        layout: &RelationLayout,
    ) -> IcebergResult<Self> {
        let mut fields = Vec::with_capacity(layout.live_columns().len());
        for col in layout.live_columns() {
            let field = schema
                .as_struct()
                .field_by_name(&col.name)
                .ok_or_else(|| IcebergError::ColumnNotFound(col.name.clone()))?;
            fields.push(RelationFieldBinding {
                attno: col.attno,
                destination: Self::attribute_offset(col.attno, layout.slot_width())?,
                field_id: field.id,
                debug_name: col.name.clone(),
            });
        }
        Ok(Self { fields })
    }

    /// Resolve only projected attributes, in storage order, to slot destinations.
    pub(crate) fn bind_projection(
        schema: &IcebergSchema,
        layout: &RelationLayout,
        projected: impl ExactSizeIterator<Item = (pg_sys::AttrNumber, usize)>,
    ) -> IcebergResult<Self> {
        let mut fields = Vec::with_capacity(projected.len());
        for (attno, destination) in projected {
            let column = layout.column(attno)?;
            let remote = schema
                .as_struct()
                .field_by_name(&column.name)
                .ok_or_else(|| IcebergError::ColumnNotFound(column.name.clone()))?;
            fields.push(RelationFieldBinding {
                attno,
                destination,
                field_id: remote.id,
                debug_name: column.name.clone(),
            });
        }
        Ok(Self { fields })
    }

    pub(crate) fn bindings(&self) -> &[RelationFieldBinding] {
        &self.fields
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
