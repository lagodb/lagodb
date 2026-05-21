//! PostgreSQL to Iceberg type conversion.
//!
//! # Architecture
//!
//! ```text
//! &RelationHandle ─► tuple_desc_to_schema ─► SchemaBuilder ─► Iceberg Schema
//!                                                  │
//!                                                  └─► PgType (per column)
//!                                                          │
//!                                                          └─► Iceberg Type
//! ```
//!
//! There is exactly one entry point: [`tuple_desc_to_schema`]. The module
//! itself, [`PgType`], and [`SchemaBuilder`] are crate-private so that the
//! "single field-id counter" invariant is enforced by the type system, not by
//! a comment: there is no public surface anyone could call to start a second,
//! divergent counter.
//!
//! [`SchemaBuilder`] owns field-id allocation and the recursion into nested
//! types. [`PgType`] is the thin `(oid, typmod)` wrapper that handles only
//! conversions independent of field ids (e.g. `to_decimal`).

use crate::error::{IcebergError, IcebergResult};
use iceberg_lite::spec::{
    ListType, NestedField, NestedFieldRef, PrimitiveType, Schema, Type,
};
use pg_lakebase_core::diag;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::tuple::{NumericTypmod, numeric_precision_scale};
use pgrx::{PgBuiltInOids, PgOid, pg_sys};
use std::ffi::CStr;
use std::sync::Arc;

// ============================================================================
// Constants
// ============================================================================

/// Default precision for `numeric` columns declared without `(p, s)`.
///
/// Picked to match the maximum precision Iceberg `decimal` supports so we can
/// accept the widest set of runtime values without changing the schema.
const DEFAULT_NUMERIC_PRECISION: u32 = 38;

/// Default scale for `numeric` columns declared without `(p, s)`. Together
/// with [`DEFAULT_NUMERIC_PRECISION`] this gives `decimal(38, 18)`, leaving
/// 20 integer digits of headroom — large enough for typical financial data
/// without giving up too much fractional resolution.
const DEFAULT_NUMERIC_SCALE: u32 = 18;

/// Maximum decimal precision Iceberg's spec permits.
const MAX_DECIMAL_PRECISION: u32 = 38;

// ============================================================================
// PgType
// ============================================================================

/// A PostgreSQL `(type oid, type modifier)` pair.
///
/// Conversions that depend only on this pair live as inherent methods here.
/// Anything that needs a field-id counter belongs on [`SchemaBuilder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PgType {
    oid: pg_sys::Oid,
    type_mod: i32,
}

impl PgType {
    #[inline]
    const fn new(oid: pg_sys::Oid, type_mod: i32) -> Self {
        Self { oid, type_mod }
    }

    #[inline]
    #[cfg(test)]
    const fn from_oid(oid: pg_sys::Oid) -> Self {
        Self::new(oid, -1)
    }

    /// Decode `numeric` precision/scale from `type_mod`.
    ///
    /// PostgreSQL packs them as `((precision << 16) | scale) + VARHDRSZ`.
    /// Returns `None` when the column has no modifier (`type_mod == -1`).
    #[inline]
    fn numeric_precision_scale(&self) -> Option<NumericTypmod> {
        numeric_precision_scale(self.type_mod)
    }

    /// Element OID for array types, or `None` if `self` is not an array.
    #[inline]
    fn element_type_oid(&self) -> Option<pg_sys::Oid> {
        // SAFETY: `get_element_type` only inspects `pg_type` syscache and
        // tolerates any OID; it returns `InvalidOid` for non-array types.
        let elem_oid = unsafe { pg_sys::get_element_type(self.oid) };
        (elem_oid != pg_sys::InvalidOid).then_some(elem_oid)
    }

    /// Convert this type to an Iceberg `Type` when no field-id allocation is
    /// required (i.e. primitives only). Returns
    /// [`IcebergError::UnsupportedColumnType`] for arrays — those need a
    /// [`SchemaBuilder`] because the list-element field needs a fresh id.
    fn to_primitive(&self) -> IcebergResult<Type> {
        let pg_oid = PgOid::from(self.oid);
        match pg_oid {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                Ok(Type::Primitive(PrimitiveType::Boolean))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2OID)
            | PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
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
            // Iceberg has no native PostgreSQL json/jsonb. We map json to
            // string and jsonb to binary as a pg-iceberg-am private codec:
            // jsonb bytes are PostgreSQL-internal varlena, not a portable
            // Iceberg JSON encoding. Revisit if Iceberg variant lands.
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
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => self.to_decimal(),
            _ => Err(IcebergError::UnsupportedColumnType(format!(
                "PostgreSQL OID {}",
                u32::from(self.oid)
            ))),
        }
    }

    /// Convert a PostgreSQL `numeric(p, s)` to an Iceberg `decimal`.
    ///
    /// Three cases:
    ///
    /// 1. **No modifier**: PostgreSQL accepts arbitrary-precision values.
    ///    We fall back to [`DEFAULT_NUMERIC_PRECISION`]/`SCALE`. The caller
    ///    is responsible for surfacing this fallback to the user (see
    ///    [`SchemaBuilder::add_field`]).
    /// 2. **`numeric(p, s)` with `s >= 0`**: maps 1:1 to `decimal(p, s)`.
    /// 3. **`numeric(p, -k)` with `k > 0`**: PG semantics is "round to the
    ///    nearest multiple of 10^k, total significant digits ≤ p". Mapping
    ///    to `decimal(p + k, 0)` is a strict superset (covers all values
    ///    PG can store), zero-loss in both directions: writes `floor(n * 1)
    ///    = n` since stored values are already 10^k-multiples; reads return
    ///    the same integer, which is a valid `numeric(p, -k)` value.
    ///
    /// Errors when the resulting precision exceeds Iceberg's 38-digit cap.
    fn to_decimal(&self) -> IcebergResult<Type> {
        let (precision, scale) = match self.numeric_precision_scale() {
            None => (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE),
            Some(NumericTypmod { precision, scale }) if scale >= 0 => {
                (precision, scale as u32)
            }
            Some(NumericTypmod { precision, scale }) => {
                // Negative scale: widen precision, set Iceberg scale to 0.
                // Reject up front when the widened precision overflows the
                // 38-digit cap so the error message references the *declared*
                // numeric(p, -k), not the post-widening shape.
                let widened = precision.checked_add(scale.unsigned_abs()).ok_or_else(
                    || {
                        IcebergError::UnsupportedColumnType(format!(
                            "numeric({precision}, {scale}) widened precision overflows u32",
                        ))
                    },
                )?;
                if widened > MAX_DECIMAL_PRECISION {
                    return Err(IcebergError::UnsupportedColumnType(format!(
                        "numeric({precision}, {scale}) maps to decimal({widened}, 0) \
                         which exceeds maximum supported precision ({MAX_DECIMAL_PRECISION})",
                    )));
                }
                (widened, 0)
            }
        };

        if precision > MAX_DECIMAL_PRECISION {
            return Err(IcebergError::UnsupportedColumnType(format!(
                "numeric({precision}, {scale}) precision exceeds maximum \
                 supported precision ({MAX_DECIMAL_PRECISION})",
            )));
        }

        Ok(Type::Primitive(PrimitiveType::Decimal { precision, scale }))
    }
}

// ============================================================================
// SchemaBuilder
// ============================================================================

/// Builder for an Iceberg [`Schema`] from a sequence of PostgreSQL columns.
///
/// `SchemaBuilder` is the sole owner of the field-id counter, so every field
/// id in the produced schema (top-level columns and list-element fields
/// alike) is allocated from a single monotonic source. That invariant is the
/// reason this module does not expose a "convert one column" entry point
/// alongside `add_field`: a standalone conversion would need its own counter,
/// and the two counters would silently disagree.
///
/// The type is crate-private and reachable only through
/// [`tuple_desc_to_schema`] — see the module docs for why.
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

        // WARNING (not NOTICE) on numeric-without-modifier defaulting: the
        // fallback silently changes what values the schema can hold, and
        // many production clients run with `client_min_messages = WARNING`
        // where a NOTICE would be invisible. Per-column on purpose: a
        // CREATE TABLE with five ambiguous numeric columns gets five lines,
        // one per column, telling the user exactly which columns to fix.
        // The PgType layer is intentionally column-name-agnostic, so the
        // user-facing message is assembled here where the column name is
        // in scope.
        if pg_type.oid == PgBuiltInOids::NUMERICOID.value()
            && pg_type.numeric_precision_scale().is_none()
        {
            diag::report_warning(&format!(
                "numeric column \"{name}\" has no precision/scale; defaulting to \
                 decimal({DEFAULT_NUMERIC_PRECISION}, {DEFAULT_NUMERIC_SCALE}). \
                 Use numeric(p, s) explicitly to avoid runtime overflow on values \
                 wider than {DEFAULT_NUMERIC_PRECISION} digits.",
            ));
        }

        let field_id = self.allocate_field_id();
        let iceberg_type = self.convert(&pg_type)?;

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
    fn convert(&mut self, pg_type: &PgType) -> IcebergResult<Type> {
        if let Some(elem_oid) = pg_type.element_type_oid() {
            return self.convert_array(elem_oid, pg_type.type_mod);
        }
        pg_type.to_primitive()
    }

    fn convert_array(
        &mut self,
        element_oid: pg_sys::Oid,
        type_mod: i32,
    ) -> IcebergResult<Type> {
        // Allocate the list-element id before recursing, matching the order
        // upstream Iceberg uses so the produced schema's id ordering is
        // deterministic regardless of the element type's internal shape.
        let element_id = self.allocate_field_id();
        let element_pg = PgType::new(element_oid, type_mod);
        let element_type = self.convert(&element_pg)?;

        // PostgreSQL arrays admit NULL elements unconditionally.
        let element_field =
            NestedField::list_element(element_id, element_type, false);
        Ok(Type::List(ListType::new(Arc::new(element_field))))
    }
}

// `Default` is intentionally not implemented for `SchemaBuilder`: the type is
// crate-private and only `tuple_desc_to_schema` constructs it, via `new()`.
// Adding `Default` would create an extra construction path with no caller.

// ============================================================================
// Entry point
// ============================================================================

/// Build an Iceberg [`Schema`] from the live (non-dropped) columns of a
/// PostgreSQL relation.
///
/// This is the only conversion entry point exposed by the module. It owns
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
        std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts)
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use pg_lakebase_core::tuple::numeric_typmod;

    #[test]
    fn pg_type_constructors_default_typmod_to_minus_one() {
        let t = PgType::from_oid(pg_sys::Oid::from(23));
        assert_eq!(t.type_mod, -1);
    }

    #[test]
    fn numeric_typmod_round_trips_positive_scale() {
        let pg = PgType::new(PgBuiltInOids::NUMERICOID.value(), numeric_typmod(10, 2));
        let typmod = pg.numeric_precision_scale().unwrap();
        assert_eq!((typmod.precision, typmod.scale), (10, 2));
    }

    #[test]
    fn numeric_typmod_sign_extends_negative_scale() {
        let pg = PgType::new(PgBuiltInOids::NUMERICOID.value(), numeric_typmod(2, -3));
        let typmod = pg.numeric_precision_scale().unwrap();
        assert_eq!((typmod.precision, typmod.scale), (2, -3));
    }

    #[test]
    fn numeric_typmod_is_none_without_modifier() {
        let pg = PgType::new(PgBuiltInOids::NUMERICOID.value(), -1);
        assert!(pg.numeric_precision_scale().is_none());
    }

    #[test]
    fn numeric_without_modifier_falls_back_to_default_decimal() {
        let pg = PgType::new(PgBuiltInOids::NUMERICOID.value(), -1);
        assert!(matches!(
            pg.to_decimal().unwrap(),
            Type::Primitive(PrimitiveType::Decimal {
                precision: DEFAULT_NUMERIC_PRECISION,
                scale: DEFAULT_NUMERIC_SCALE,
            })
        ));
    }

    #[test]
    fn numeric_negative_scale_widens_precision_and_zeroes_scale() {
        // numeric(2, -3) -> decimal(5, 0)
        let pg = PgType::new(PgBuiltInOids::NUMERICOID.value(), numeric_typmod(2, -3));
        match pg.to_decimal().unwrap() {
            Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
                assert_eq!((precision, scale), (5, 0));
            }
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn numeric_negative_scale_rejects_overflow_beyond_38() {
        // numeric(20, -20) would need decimal(40, 0), exceeding Iceberg's 38-digit cap.
        let pg =
            PgType::new(PgBuiltInOids::NUMERICOID.value(), numeric_typmod(20, -20));
        assert!(matches!(
            pg.to_decimal(),
            Err(IcebergError::UnsupportedColumnType(_))
        ));
    }

    #[test]
    fn schema_builder_allocates_sequential_ids() {
        let builder = SchemaBuilder::new();
        assert_eq!(builder.next_field_id, 1);
    }
}
