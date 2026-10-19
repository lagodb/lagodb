//! Managed schema construction and field-id allocation.

use super::type_mapping::ManagedTypeMapping;
use crate::error::{IcebergError, IcebergResult};
use iceberg_lite::spec::{
    ListType, NestedField, NestedFieldRef, PrimitiveType, SCHEMA_NAME_DELIMITER,
    Schema, StructType, Type,
};
use lagodb_core::datum::PgType;
use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;
use std::sync::Arc;
use std::{ffi::CStr, slice};

/// Builder for an Iceberg [`Schema`] from a sequence of PostgreSQL columns.
///
/// `SchemaBuilder` is the sole owner of the field-id counter, so every field
/// id in the produced schema, including nested fields, comes from one counter.
/// Primitive admission is owned by `ManagedTypeMapping`; PG type inspection
/// is owned by core's `PgType`.
struct SchemaBuilder {
    next_field_id: i32,
    fields: Vec<NestedFieldRef>,
}

impl SchemaBuilder {
    fn new() -> Self {
        Self {
            next_field_id: 1,
            fields: Vec::new(),
        }
    }

    /// Append one PostgreSQL column.
    ///
    /// Allocates one field id for the column itself, plus any additional ids
    /// required by nested (list) types.
    fn add_field(
        &mut self,
        name: impl Into<String>,
        pg_type: PgType,
        required: bool,
    ) -> IcebergResult<&mut Self> {
        let name = name.into();
        if name.contains(SCHEMA_NAME_DELIMITER) {
            return Err(IcebergError::SchemaBuildError(format!(
                "Cannot add column with ambiguous name: {name}"
            )));
        }

        let field_id = self.allocate_field_id();
        let iceberg_type = self.convert(pg_type)?;

        let field = if required {
            NestedField::required(field_id, name, iceberg_type)
        } else {
            NestedField::optional(field_id, name, iceberg_type)
        };
        self.fields.push(Arc::new(field));
        Ok(self)
    }

    fn build(self) -> IcebergResult<Schema> {
        Schema::builder()
            .with_fields(self.fields)
            .build()
            .map_err(|e| IcebergError::SchemaBuildError(e.to_string()))
    }

    fn allocate_field_id(&mut self) -> i32 {
        let id = self.next_field_id;
        self.next_field_id += 1;
        id
    }

    /// Convert one [`PgType`], allocating field ids for any nested list
    /// elements as it goes.
    fn convert(&mut self, pg_type: PgType) -> IcebergResult<Type> {
        let mapping = ManagedTypeMapping::bind(pg_type)?;
        let base = mapping.pg_type();
        if base.is_composite() {
            let mut fields = Vec::new();
            for attr in base.composite_fields() {
                if attr.name.contains(SCHEMA_NAME_DELIMITER) {
                    return Err(IcebergError::SchemaBuildError(format!(
                        "Cannot add composite field with ambiguous name: {}",
                        attr.name
                    )));
                }
                let id = self.allocate_field_id();
                let ty = self.convert(attr.pg_type)?;
                fields.push(Arc::new(NestedField::optional(id, attr.name, ty)));
            }
            return Ok(Type::Struct(StructType::new(fields)));
        }
        if base.oid == pg_sys::INTERVALOID {
            let fields = ["months", "days", "microseconds"]
                .into_iter()
                .map(|name| {
                    Arc::new(NestedField::required(
                        self.allocate_field_id(),
                        name,
                        Type::Primitive(PrimitiveType::Long),
                    ))
                })
                .collect();
            return Ok(Type::Struct(StructType::new(fields)));
        }
        if let Some(element) = base.element() {
            return self.convert_array(element);
        }
        mapping.primitive_type()
    }

    fn convert_array(&mut self, element: PgType) -> IcebergResult<Type> {
        // Allocate the list-element id before recursing, matching the order
        // upstream Iceberg uses so the produced schema's id ordering is
        // deterministic regardless of the element type's internal shape.
        let element_id = self.allocate_field_id();
        let element_type = self.convert(element)?;

        // PostgreSQL arrays admit NULL elements unconditionally.
        let element_field =
            NestedField::list_element(element_id, element_type, false);
        Ok(Type::List(ListType::new(Arc::new(element_field))))
    }
}

/// Build an Iceberg [`Schema`] from the live (non-dropped) columns of a
/// PostgreSQL relation.
///
/// This entry point owns
/// the `TupleDesc` unsafe boundary so callers stay on safe Rust: the
/// relation handle keeps the descriptor alive, and the descriptor's `attrs`
/// array is laid out as a flat array of `natts` `FormData_pg_attribute`
/// records.
pub(crate) fn tuple_desc_to_schema(rel: &RelationHandle) -> IcebergResult<Schema> {
    // SAFETY: `rel` is a live RelationHandle, so `tuple_desc()` returns a
    // valid `TupleDesc`. PostgreSQL guarantees `attrs` is a contiguous array
    // of length `natts` for the duration of the descriptor's lifetime, which
    // is at least as long as `rel`.
    let attrs = unsafe {
        let tup_desc = rel.tuple_desc();
        let natts = (*tup_desc).natts as usize;
        slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts)
    };

    let mut builder = SchemaBuilder::new();
    for attr in attrs {
        if attr.attisdropped {
            continue;
        }
        // SAFETY: `attname.data` is a NUL-terminated `NameData` array owned
        // by the descriptor; valid for the borrow of `attrs`.
        let name = unsafe {
            CStr::from_ptr(attr.attname.data.as_ptr())
                .to_string_lossy()
                .into_owned()
        };
        let pg_type = PgType::new(attr.atttypid, attr.atttypmod);
        builder.add_field(name, pg_type, attr.attnotnull)?;
    }
    builder.build()
}

/// Convert one PostgreSQL column type for schema evolution.
///
/// The returned [`Type`] must not be inserted directly into an Iceberg schema:
/// nested list/map/struct field ids, if any, are placeholders allocated only to
/// build a well-formed type. `iceberg-lite`'s schema update planner rewrites
/// every id against the current table metadata when the column is prepared.
pub(crate) fn column_type_to_iceberg_type(
    _column_name: &str,
    type_oid: pg_sys::Oid,
    typmod: i32,
) -> IcebergResult<Type> {
    let pg_type = PgType::new(type_oid, typmod);
    SchemaBuilder::new().convert(pg_type)
}
