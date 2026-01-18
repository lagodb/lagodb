//! PostgreSQL to Iceberg type conversion.
//!
//! This module provides bidirectional type conversion between PostgreSQL types
//! and Iceberg types. The design follows Rust best practices:
//!
//! - **Direct mapping**: Type mappings are handled via pattern matching on OIDs.
//! - **Trait-based conversion**: Uses `ToIcebergType` and `ToPgType` traits.
//! - **Recursive handling**: Supports nested types (List, Map, Struct).
//!
//! # Architecture
//!
//! ```text
//! PostgreSQL Type <-> PgType <-> Iceberg Type
//! ```
//!
//! The `PgType` struct encapsulates PostgreSQL type information (OID + typemod),
//! and provides the bridge to Iceberg types.

use crate::error::{IcebergError, IcebergResult};
use iceberg_lite::spec::{
    ListType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type,
};
use pg_lakehouse_core::pg_wrapper::PgWrapper;
use pgrx::{PgBuiltInOids, PgOid, pg_sys};
use std::ffi::CStr;
use std::sync::Arc;

// ============================================================================
// Constants
// ============================================================================

/// Default precision for numeric types without explicit precision.
const DEFAULT_NUMERIC_PRECISION: u32 = 38;

/// Default scale for numeric types without explicit scale.
const DEFAULT_NUMERIC_SCALE: u32 = 18;

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
    pub fn numeric_precision_scale(&self) -> Option<(u32, u32)> {
        PgWrapper::numeric_precision_scale(self.type_mod)
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

    /// Checks if this type is a composite (struct) type.
    pub fn is_composite(&self) -> bool {
        unsafe { pg_sys::get_typtype(self.oid) == b'c' as i8 }
    }
}

/// Converts PostgreSQL NUMERIC type to Iceberg Decimal.
///
/// Extracts precision and scale from the type modifier. If not specified,
/// uses default values (38, 18) which is the maximum Iceberg supports.
fn convert_numeric_type(pg_type: &PgType) -> IcebergResult<Type> {
    let (precision, scale) = pg_type
        .numeric_precision_scale()
        .unwrap_or((DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE));

    // Validate precision is within Iceberg limits
    if precision > MAX_DECIMAL_PRECISION {
        // For very large precision, fall back to string representation
        // (similar to C implementation's behavior)
        return Ok(Type::Primitive(PrimitiveType::String));
    }

    Ok(Type::Primitive(PrimitiveType::Decimal { precision, scale }))
}

// ============================================================================
// Type Conversion Trait
// ============================================================================

/// Trait for converting PostgreSQL types to Iceberg types.
///
/// This trait provides the core type conversion functionality, supporting
/// both primitive types and complex nested types (arrays, composites).
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
/// and `SchemaBuilder`. It handles primitive types, arrays, and composite types,
/// allocating globally unique field IDs for nested structures.
///
/// Similar to the C implementation's `PostgresTypeToIcebergField` function which
/// uses `int *subFieldIndex` for field ID tracking.
fn convert_pg_type_with_field_ids(
    pg_type: &PgType,
    next_field_id: &mut i32,
) -> IcebergResult<Type> {
    // Try primitive conversion first.
    // This handles most standard types and avoids catalog lookups for them,
    // which is essential for pure Rust unit tests without a running PostgreSQL backend.
    match convert_primitive_type(pg_type) {
        Ok(ty) => return Ok(ty),
        Err(e) => {
            // If it's not a primitive we recognize, continue to check for array/composite.
            // Only fall through if it's an UnsupportedColumnType error.
            if !matches!(e, IcebergError::UnsupportedColumnType(_)) {
                return Err(e);
            }
        }
    }

    // Check for array types
    if let Some(elem_oid) = pg_type.element_type_oid() {
        return convert_array_type_with_field_ids(
            elem_oid,
            pg_type.type_mod,
            next_field_id,
        );
    }

    // Check for composite types
    if pg_type.is_composite() {
        return convert_composite_type_with_field_ids(
            pg_type.oid,
            pg_type.type_mod,
            next_field_id,
        );
    }

    // Fall back to the original error if it's neither an array nor a composite
    Err(IcebergError::UnsupportedColumnType(format!(
        "PostgreSQL OID {}",
        u32::from(pg_type.oid)
    )))
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
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
        | PgOid::BuiltIn(PgBuiltInOids::VARCHAROID)
        | PgOid::BuiltIn(PgBuiltInOids::BPCHAROID)
        | PgOid::BuiltIn(PgBuiltInOids::NAMEOID)
        | PgOid::BuiltIn(PgBuiltInOids::JSONOID)
        | PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
            Ok(Type::Primitive(PrimitiveType::String))
        }
        PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => {
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

/// Converts a PostgreSQL composite type to Iceberg Struct type with field ID allocation.
fn convert_composite_type_with_field_ids(
    type_oid: pg_sys::Oid,
    type_mod: i32,
    next_field_id: &mut i32,
) -> IcebergResult<Type> {
    unsafe {
        let tuple_desc = pg_sys::lookup_rowtype_tupdesc(type_oid, type_mod);
        if tuple_desc.is_null() {
            return Err(IcebergError::UnsupportedColumnType(
                "Failed to lookup composite type".to_string(),
            ));
        }

        let natts = (*tuple_desc).natts as usize;
        let attrs = std::slice::from_raw_parts((*tuple_desc).attrs.as_ptr(), natts);

        let mut fields: Vec<NestedFieldRef> = Vec::with_capacity(natts);

        for attr in attrs.iter() {
            if attr.attisdropped {
                continue;
            }

            // Allocate field ID first (like C implementation)
            let field_id = allocate_field_id(next_field_id);

            let name_ptr = attr.attname.data.as_ptr();
            let name = CStr::from_ptr(name_ptr).to_string_lossy().to_string();

            // Recursively convert field type
            let field_pg_type = PgType::new(attr.atttypid, attr.atttypmod);
            let field_type =
                convert_pg_type_with_field_ids(&field_pg_type, next_field_id)?;

            let required = attr.attnotnull;

            let field = if required {
                NestedField::required(field_id, name, field_type)
            } else {
                NestedField::optional(field_id, name, field_type)
            };

            fields.push(Arc::new(field));
        }

        PgWrapper::release_tuple_desc(tuple_desc);

        Ok(Type::Struct(StructType::new(fields)))
    }
}

// ============================================================================
// Iceberg to PostgreSQL Type Conversion
// ============================================================================

/// Trait for converting Iceberg types back to PostgreSQL types.
///
/// This provides the reverse mapping, useful when reading Iceberg tables
/// and creating corresponding PostgreSQL types.
pub trait ToPgType {
    /// Converts to a PostgreSQL type OID.
    fn to_pg_oid(&self) -> IcebergResult<pg_sys::Oid>;

    /// Converts to a full PgType including type modifier.
    fn to_pg_type(&self) -> IcebergResult<PgType>;
}

impl ToPgType for PrimitiveType {
    fn to_pg_oid(&self) -> IcebergResult<pg_sys::Oid> {
        let oid = match self {
            PrimitiveType::Boolean => PgBuiltInOids::BOOLOID,
            PrimitiveType::Int => PgBuiltInOids::INT4OID,
            PrimitiveType::Long => PgBuiltInOids::INT8OID,
            PrimitiveType::Float => PgBuiltInOids::FLOAT4OID,
            PrimitiveType::Double => PgBuiltInOids::FLOAT8OID,
            PrimitiveType::Decimal { .. } => PgBuiltInOids::NUMERICOID,
            PrimitiveType::Date => PgBuiltInOids::DATEOID,
            PrimitiveType::Time => PgBuiltInOids::TIMEOID,
            PrimitiveType::Timestamp | PrimitiveType::TimestampNs => {
                PgBuiltInOids::TIMESTAMPOID
            }
            PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                PgBuiltInOids::TIMESTAMPTZOID
            }
            PrimitiveType::String => PgBuiltInOids::TEXTOID,
            PrimitiveType::Uuid => PgBuiltInOids::UUIDOID,
            PrimitiveType::Binary | PrimitiveType::Fixed(_) => {
                PgBuiltInOids::BYTEAOID
            }
        };
        Ok(pg_sys::Oid::from(oid.value()))
    }

    fn to_pg_type(&self) -> IcebergResult<PgType> {
        let oid = self.to_pg_oid()?;

        let type_mod = match self {
            PrimitiveType::Decimal { precision, scale } => {
                PgWrapper::numeric_typmod(*precision, *scale)
            }
            _ => -1,
        };

        Ok(PgType::new(oid, type_mod))
    }
}

impl ToPgType for Type {
    fn to_pg_oid(&self) -> IcebergResult<pg_sys::Oid> {
        match self {
            Type::Primitive(p) => p.to_pg_oid(),
            Type::List(list) => {
                // Get the array type OID for the element type
                let elem_oid = list.element_field.field_type.to_pg_oid()?;
                let array_oid = unsafe { pg_sys::get_array_type(elem_oid) };
                if array_oid == pg_sys::InvalidOid {
                    Err(IcebergError::UnsupportedColumnType(format!(
                        "No array type for element OID {}",
                        u32::from(elem_oid)
                    )))
                } else {
                    Ok(array_oid)
                }
            }
            Type::Struct(_) => {
                // Struct types require special handling - they need to be
                // created as composite types in PostgreSQL
                Err(IcebergError::NotImplemented(
                    "Struct type to PostgreSQL conversion requires type creation",
                ))
            }
            Type::Map(_) => {
                // Map types don't have a direct PostgreSQL equivalent
                Err(IcebergError::NotImplemented(
                    "Map type to PostgreSQL conversion is not supported",
                ))
            }
        }
    }

    fn to_pg_type(&self) -> IcebergResult<PgType> {
        match self {
            Type::Primitive(p) => p.to_pg_type(),
            Type::List(list) => {
                let oid = self.to_pg_oid()?;
                // For arrays, we might want to preserve element type's typemod
                let elem_pg_type = list.element_field.field_type.to_pg_type()?;
                Ok(PgType::new(oid, elem_pg_type.type_mod))
            }
            _ => {
                let oid = self.to_pg_oid()?;
                Ok(PgType::from_oid(oid))
            }
        }
    }
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
    /// and any nested fields within arrays or composite types.
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
/// primitive types and complex types (arrays, composites).
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

/// Converts an Iceberg Type to a PostgreSQL type OID.
///
/// This provides reverse mapping from Iceberg types back to PostgreSQL.
///
/// # Arguments
///
/// * `iceberg_type` - The Iceberg Type to convert
///
/// # Returns
///
/// The corresponding PostgreSQL type OID, or an error if not supported.
pub fn iceberg_type_to_pg_oid(iceberg_type: &Type) -> IcebergResult<pg_sys::Oid> {
    iceberg_type.to_pg_oid()
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
        let type_mod = PgWrapper::numeric_typmod(10, 2);
        let pg_type = PgType::new(
            pg_sys::Oid::from(PgBuiltInOids::NUMERICOID.value()),
            type_mod,
        );

        let (precision, scale) = pg_type.numeric_precision_scale().unwrap();
        assert_eq!(precision, 10);
        assert_eq!(scale, 2);
    }

    #[test]
    fn test_numeric_precision_scale_none_when_no_typemod() {
        let pg_type =
            PgType::new(pg_sys::Oid::from(PgBuiltInOids::NUMERICOID.value()), -1);
        assert!(pg_type.numeric_precision_scale().is_none());
    }

    // Tests for reverse conversion (Iceberg -> PostgreSQL)
    // These don't call pg_sys functions that require PostgreSQL runtime

    #[test]
    fn test_primitive_to_pg_oid_int() {
        let result = PrimitiveType::Int.to_pg_oid();
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            pg_sys::Oid::from(PgBuiltInOids::INT4OID.value())
        );
    }

    #[test]
    fn test_primitive_to_pg_oid_long() {
        let result = PrimitiveType::Long.to_pg_oid();
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            pg_sys::Oid::from(PgBuiltInOids::INT8OID.value())
        );
    }

    #[test]
    fn test_primitive_to_pg_oid_string() {
        let result = PrimitiveType::String.to_pg_oid();
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            pg_sys::Oid::from(PgBuiltInOids::TEXTOID.value())
        );
    }

    #[test]
    fn test_primitive_to_pg_type_decimal() {
        let decimal = PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        };
        let result = decimal.to_pg_type();
        assert!(result.is_ok());
        let pg_type = result.unwrap();
        assert_eq!(
            pg_type.oid,
            pg_sys::Oid::from(PgBuiltInOids::NUMERICOID.value())
        );

        // Verify type_mod encodes precision/scale correctly
        let (precision, scale) = pg_type.numeric_precision_scale().unwrap();
        assert_eq!(precision, 10);
        assert_eq!(scale, 2);
    }

    #[test]
    fn test_schema_builder_allocates_sequential_ids() {
        let builder = SchemaBuilder::new();
        assert_eq!(builder.next_field_id, 1);
    }
}
