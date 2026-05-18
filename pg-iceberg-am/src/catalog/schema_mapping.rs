//! PostgreSQL to Iceberg type conversion.
//!
//! This module provides bidirectional type conversion between PostgreSQL types
//! and Iceberg types. The design follows Rust best practices:
//!
//! - **Direct mapping**: Type mappings are handled via pattern matching on OIDs.
//! - **Trait-based conversion**: Uses `ToIcebergType` trait.
//! - **Recursive handling**: Supports nested types (List, Map, Struct).
//!
//! # Architecture
//!
//! ```text
//! PostgreSQL Type -> PgType -> Iceberg Type
//! ```
//!
//! The `PgType` struct encapsulates PostgreSQL type information (OID + typemod),
//! and provides the bridge to Iceberg types.

use crate::error::{IcebergError, IcebergResult};
use iceberg_lite::spec::{
    ListType, NestedField, NestedFieldRef, PrimitiveType, Schema, Type,
};
use pg_lakebase_core::tuple::{NumericTypmod, numeric_precision_scale};
use pgrx::{PgBuiltInOids, PgOid, pg_sys};
use std::ffi::CStr;
use std::sync::Arc;

// ============================================================================
// Constants
// ============================================================================

/// Default precision for numeric types without explicit precision.
const DEFAULT_NUMERIC_PRECISION: u32 = 38;

/// Default scale for numeric types without explicit scale.
const DEFAULT_NUMERIC_SCALE: i32 = 18;

/// Maximum supported decimal precision in Iceberg.
const MAX_DECIMAL_PRECISION: u32 = 38;

// ============================================================================
// PgType: PostgreSQL Type Encapsulation
// ============================================================================

/// Encapsulates PostgreSQL type information.
///
/// This struct wraps a PostgreSQL type OID and type modifier, providing
/// a clean abstraction for type conversion operations. Similar to the
/// C implementation's `PGType` struct.
///
/// # Example
///
/// ```ignore
/// let pg_type = PgType::new(pg_sys::INT4OID, -1);
/// let iceberg_type = pg_type.to_iceberg_type()?;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgType {
    /// PostgreSQL type OID.
    pub oid: pg_sys::Oid,
    /// Type modifier (e.g., precision/scale for NUMERIC).
    pub type_mod: i32,
}

impl PgType {
    /// Creates a new `PgType` from OID and type modifier.
    #[inline]
    pub const fn new(oid: pg_sys::Oid, type_mod: i32) -> Self {
        Self { oid, type_mod }
    }

    /// Creates a new `PgType` from OID with default type modifier (-1).
    #[inline]
    pub const fn from_oid(oid: pg_sys::Oid) -> Self {
        Self::new(oid, -1)
    }

    /// Extracts precision and scale from NUMERIC type modifier.
    ///
    /// PostgreSQL stores NUMERIC precision and scale as:
    /// `type_mod = ((precision << 16) | scale) + VARHDRSZ`
    ///
    /// Returns `None` if type_mod is not set (-1).
    pub fn numeric_precision_scale(&self) -> Option<NumericTypmod> {
        numeric_precision_scale(self.type_mod)
    }

    /// Checks if this type is an array type.
    ///
    /// In PostgreSQL, array types have an element type OID that differs
    /// from InvalidOid.
    pub fn is_array(&self) -> bool {
        unsafe { pg_sys::get_element_type(self.oid) != pg_sys::InvalidOid }
    }

    /// Gets the element type OID for array types.
    ///
    /// Returns `None` if this is not an array type.
    pub fn element_type_oid(&self) -> Option<pg_sys::Oid> {
        let elem_oid = unsafe { pg_sys::get_element_type(self.oid) };
        if elem_oid != pg_sys::InvalidOid {
            Some(elem_oid)
        } else {
            None
        }
    }
}

/// Converts PostgreSQL NUMERIC type to Iceberg Decimal.
///
/// Extracts precision and scale from the type modifier. If not specified,
/// uses default values (38, 18) which is the maximum Iceberg supports.
fn convert_numeric_type(pg_type: &PgType) -> IcebergResult<Type> {
    let NumericTypmod { precision, scale } =
        pg_type.numeric_precision_scale().unwrap_or(NumericTypmod {
            precision: DEFAULT_NUMERIC_PRECISION,
            scale: DEFAULT_NUMERIC_SCALE,
        });

    // Validate precision is within Iceberg limits
    if precision > MAX_DECIMAL_PRECISION {
        return Err(IcebergError::UnsupportedColumnType(format!(
            "numeric({}, {}) precision exceeds maximum supported precision ({})",
            precision, scale, MAX_DECIMAL_PRECISION
        )));
    }

    if scale < 0 {
        return Err(IcebergError::UnsupportedColumnType(format!(
            "numeric({}, {}) negative scale is not supported by Iceberg decimal",
            precision, scale
        )));
    }

    let scale = scale as u32;

    Ok(Type::Primitive(PrimitiveType::Decimal { precision, scale }))
}

// ============================================================================
// Type Conversion Trait
// ============================================================================

/// Trait for converting PostgreSQL types to Iceberg types.
///
/// This trait provides the core type conversion functionality, supporting
/// both primitive types and complex nested types (arrays).
pub trait ToIcebergType {
    /// Converts to an Iceberg Type.
    fn to_iceberg_type(&self) -> IcebergResult<Type>;

    /// Converts to an Iceberg NestedField with the given field ID and name.
    fn to_iceberg_field(
        &self,
        field_id: i32,
        name: impl ToString,
        required: bool,
    ) -> IcebergResult<NestedField> {
        let iceberg_type = self.to_iceberg_type()?;
        let field = if required {
            NestedField::required(field_id, name, iceberg_type)
        } else {
            NestedField::optional(field_id, name, iceberg_type)
        };
        Ok(field)
    }
}

impl ToIcebergType for PgType {
    fn to_iceberg_type(&self) -> IcebergResult<Type> {
        // Use a temporary field ID counter for standalone type conversion.
        // For proper schema construction with globally unique IDs, use SchemaBuilder.
        let mut next_field_id = 1;
        convert_pg_type_with_field_ids(self, &mut next_field_id)
    }
}

// ============================================================================
// Internal Type Conversion with Field ID Allocation
// ============================================================================

/// Converts a PostgreSQL type to Iceberg type with proper field ID allocation.
///
/// This is the core conversion function used by both `ToIcebergType::to_iceberg_type()`
/// and `SchemaBuilder`. It handles primitive types and arrays,
/// allocating globally unique field IDs for nested structures.
///
/// Similar to the C implementation's `PostgresTypeToIcebergField` function which
/// uses `int *subFieldIndex` for field ID tracking.
fn convert_pg_type_with_field_ids(
    pg_type: &PgType,
    next_field_id: &mut i32,
) -> IcebergResult<Type> {
    // Check for array types first.
    if let Some(elem_oid) = pg_type.element_type_oid() {
        return convert_array_type_with_field_ids(
            elem_oid,
            pg_type.type_mod,
            next_field_id,
        );
    }

    // Try primitive conversion.
    // Known types with invalid parameters will return specific error messages.
    // Unknown types will return "PostgreSQL OID XXX".
    convert_primitive_type(pg_type)
}

/// Converts a PostgreSQL primitive type to Iceberg type.
fn convert_primitive_type(pg_type: &PgType) -> IcebergResult<Type> {
    let pg_oid = PgOid::from(pg_type.oid);

    match pg_oid {
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
            Ok(Type::Primitive(PrimitiveType::Boolean))
        }
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
            Ok(Type::Primitive(PrimitiveType::Int))
        }
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
            Ok(Type::Primitive(PrimitiveType::Int))
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
            Ok(Type::Primitive(PrimitiveType::Long))
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
            Ok(Type::Primitive(PrimitiveType::Float))
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
            Ok(Type::Primitive(PrimitiveType::Double))
        }
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
            Ok(Type::Primitive(PrimitiveType::Date))
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
            Ok(Type::Primitive(PrimitiveType::Time))
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
            Ok(Type::Primitive(PrimitiveType::Timestamp))
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
            Ok(Type::Primitive(PrimitiveType::Timestamptz))
        }
        // Iceberg does not have PostgreSQL json/jsonb types. We intentionally
        // map json to string and jsonb to binary as a pg-iceberg-am private
        // codec. jsonb binary values are PostgreSQL internal jsonb varlena
        // bytes written and read by this extension, not a portable Iceberg
        // JSON encoding. Revisit this mapping if Iceberg variant support is
        // added.
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
        | PgOid::BuiltIn(PgBuiltInOids::VARCHAROID)
        | PgOid::BuiltIn(PgBuiltInOids::BPCHAROID)
        | PgOid::BuiltIn(PgBuiltInOids::JSONOID)
        | PgOid::BuiltIn(PgBuiltInOids::NAMEOID) => {
            Ok(Type::Primitive(PrimitiveType::String))
        }
        PgOid::BuiltIn(PgBuiltInOids::BYTEAOID)
        | PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
            Ok(Type::Primitive(PrimitiveType::Binary))
        }
        PgOid::BuiltIn(PgBuiltInOids::UUIDOID) => {
            Ok(Type::Primitive(PrimitiveType::Uuid))
        }
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => convert_numeric_type(pg_type),
        _ => Err(IcebergError::UnsupportedColumnType(format!(
            "PostgreSQL OID {}",
            u32::from(pg_type.oid)
        ))),
    }
}

/// Allocates a new field ID and increments the counter.
#[inline]
fn allocate_field_id(next_field_id: &mut i32) -> i32 {
    let id = *next_field_id;
    *next_field_id += 1;
    id
}

/// Converts a PostgreSQL array type to Iceberg List type with field ID allocation.
fn convert_array_type_with_field_ids(
    element_oid: pg_sys::Oid,
    type_mod: i32,
    next_field_id: &mut i32,
) -> IcebergResult<Type> {
    // Allocate field ID for the list element first (like C implementation)
    let element_id = allocate_field_id(next_field_id);

    // Recursively convert element type
    let element_pg_type = PgType::new(element_oid, type_mod);
    let element_iceberg_type =
        convert_pg_type_with_field_ids(&element_pg_type, next_field_id)?;

    // Arrays in PostgreSQL allow NULL elements
    let element_field =
        NestedField::list_element(element_id, element_iceberg_type, false);

    Ok(Type::List(ListType::new(Arc::new(element_field))))
}

// ============================================================================
// Schema Builder
// ============================================================================

/// Builder for creating Iceberg schemas from PostgreSQL tuple descriptors.
///
/// This provides a higher-level API for converting entire table schemas,
/// handling field ID assignment and nested type processing.
///
/// Unlike standalone `to_iceberg_type()` calls which use temporary field ID
/// counters, `SchemaBuilder` maintains a persistent counter across all fields,
/// ensuring globally unique field IDs as required by the Iceberg specification.
pub struct SchemaBuilder {
    /// Next field ID to assign.
    next_field_id: i32,
    /// Collected fields.
    fields: Vec<NestedFieldRef>,
}

impl SchemaBuilder {
    /// Creates a new SchemaBuilder.
    pub fn new() -> Self {
        Self {
            next_field_id: 1,
            fields: Vec::new(),
        }
    }

    /// Adds a field from PostgreSQL attribute.
    ///
    /// This method allocates a globally unique field ID for the top-level field
    /// and any nested fields within arrays.
    pub fn add_field(
        &mut self,
        name: impl Into<String>,
        pg_type: PgType,
        required: bool,
    ) -> IcebergResult<&mut Self> {
        // Allocate field ID for this top-level field first
        let field_id = allocate_field_id(&mut self.next_field_id);

        // Convert the type, which may allocate additional IDs for nested types
        let iceberg_type =
            convert_pg_type_with_field_ids(&pg_type, &mut self.next_field_id)?;

        let name_string = name.into();

        let field = if required {
            NestedField::required(field_id, name_string, iceberg_type)
        } else {
            NestedField::optional(field_id, name_string, iceberg_type)
        };

        self.fields.push(Arc::new(field));
        Ok(self)
    }

    /// Builds the final Iceberg schema.
    pub fn build(self) -> IcebergResult<Schema> {
        Schema::builder()
            .with_fields(self.fields)
            .build()
            .map_err(|e| IcebergError::SchemaBuildError(e.to_string()))
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Public API Functions
// ============================================================================

/// Converts a PostgreSQL type OID to an Iceberg Type.
///
/// This is the main entry point for type conversion. It handles both
/// primitive types and complex types (arrays).
///
/// # Arguments
///
/// * `type_oid` - The PostgreSQL type OID
/// * `type_mod` - The type modifier (for types like numeric with precision/scale)
///
/// # Returns
///
/// The corresponding Iceberg Type, or an error if the type is not supported.
///
/// # Example
///
/// ```ignore
/// let iceberg_type = pg_type_to_iceberg_type(pg_sys::INT4OID, -1)?;
/// assert_eq!(iceberg_type, Type::Primitive(PrimitiveType::Int));
/// ```
pub fn pg_type_to_iceberg_type(
    type_oid: pg_sys::Oid,
    type_mod: i32,
) -> IcebergResult<Type> {
    let pg_type = PgType::new(type_oid, type_mod);
    pg_type.to_iceberg_type()
}

/// Converts a PostgreSQL TupleDesc to an Iceberg Schema.
///
/// This function iterates through all non-dropped columns in the tuple
/// descriptor and creates corresponding Iceberg fields with proper
/// field IDs.
///
/// # Safety
///
/// The caller must ensure that `tup_desc` is a valid pointer to a TupleDesc.
///
/// # Arguments
///
/// * `tup_desc` - A pointer to the PostgreSQL TupleDesc
///
/// # Returns
///
/// An Iceberg Schema with fields corresponding to the TupleDesc attributes.
pub unsafe fn tuple_desc_to_schema(
    tup_desc: pg_sys::TupleDesc,
) -> IcebergResult<Schema> {
    unsafe {
        let natts = (*tup_desc).natts as usize;
        let attrs = std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts);

        let mut builder = SchemaBuilder::new();

        for attr in attrs.iter() {
            // Skip dropped columns
            if attr.attisdropped {
                continue;
            }

            // Get column name
            let name_ptr = attr.attname.data.as_ptr();
            let name = CStr::from_ptr(name_ptr).to_string_lossy().to_string();

            // Get PostgreSQL type
            let pg_type = PgType::new(attr.atttypid, attr.atttypmod);

            // Add field to schema
            builder.add_field(name, pg_type, attr.attnotnull)?;
        }

        builder.build()
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Unit tests that don't require PostgreSQL runtime.
#[cfg(test)]
mod tests {
    use super::*;
    use pg_lakebase_core::tuple::numeric_typmod;

    #[test]
    fn test_pg_type_new() {
        let pg_type = PgType::new(pg_sys::Oid::from(23), 10);
        assert_eq!(pg_type.oid, pg_sys::Oid::from(23));
        assert_eq!(pg_type.type_mod, 10);
    }

    #[test]
    fn test_pg_type_from_oid() {
        let pg_type = PgType::from_oid(pg_sys::Oid::from(23));
        assert_eq!(pg_type.type_mod, -1);
    }

    #[test]
    fn test_numeric_precision_scale_extraction() {
        // numeric(10, 2)
        let type_mod = numeric_typmod(10, 2);
        let pg_type = PgType::new(
            pg_sys::Oid::from(PgBuiltInOids::NUMERICOID.value()),
            type_mod,
        );

        let typmod = pg_type.numeric_precision_scale().unwrap();
        assert_eq!(typmod.precision, 10);
        assert_eq!(typmod.scale, 2);
    }

    #[test]
    fn test_numeric_precision_scale_sign_extends_negative_scale() {
        // numeric(2, -3)
        let type_mod = numeric_typmod(2, -3);
        let pg_type = PgType::new(
            pg_sys::Oid::from(PgBuiltInOids::NUMERICOID.value()),
            type_mod,
        );

        let typmod = pg_type.numeric_precision_scale().unwrap();
        assert_eq!(typmod.precision, 2);
        assert_eq!(typmod.scale, -3);
    }

    #[test]
    fn test_numeric_precision_scale_none_when_no_typemod() {
        let pg_type =
            PgType::new(pg_sys::Oid::from(PgBuiltInOids::NUMERICOID.value()), -1);
        assert!(pg_type.numeric_precision_scale().is_none());
    }

    #[test]
    fn test_schema_builder_allocates_sequential_ids() {
        let builder = SchemaBuilder::new();
        assert_eq!(builder.next_field_id, 1);
    }
}
