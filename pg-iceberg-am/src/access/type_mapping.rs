//! Iceberg type-system mapping for the access-method data path.
//!
//! This module owns the **position-agnostic** half of the conversion layer:
//! given an Iceberg `Type` / `Schema`, it produces the descriptors the data
//! path needs — the Arrow `DataType` / `Schema`, the target [`PgColumnType`],
//! and the resolved [`pg_arrow_conv::ColumnRule`] — plus the
//! [`ValidateSupported`] gate that rejects shapes the per-column dispatch
//! cannot materialize. Binding those per-field rules to slot/attno positions is
//! the job of [`column_mapping`](super::column_mapping).
//!
//! Everything here is expressed as **extension traits on the Iceberg types**
//! (`schema.to_arrow_schema()`, `field.resolve_rule()` /
//! `field.resolve_rule_for_column()`,
//! `ty.pg_column_type()`, `ty.validate_supported()`) rather than a chain of
//! similarly-named free functions, so call sites read as method calls on the
//! value being mapped.
//!
//! # Single source of truth for Iceberg → Arrow
//!
//! The Iceberg → Arrow primitive type table lives in `iceberg_lite::arrow`. We
//! deliberately do *not* maintain a parallel table here: in the past
//! pg-iceberg-am had its own mapping that disagreed with iceberg-lite (notably
//! mapping `timestamp_ns`/`timestamptz_ns` to microsecond Arrow types while the
//! Parquet writer's view of the table expected nanoseconds), which produced
//! silent schema-mismatch failures on the mutation write path. [`IcebergTypeExt`] /
//! [`IcebergSchemaExt`] are thin error-mapping wrappers over those converters
//! so callers stay on [`IcebergResult`].
//!
//! # Relationship to `catalog::schema_builder`
//!
//! [`IcebergTypeExt::pg_column_type`] is the format-level companion of
//! `catalog::schema_builder`'s PostgreSQL → Iceberg mapping (PG `uuid` →
//! Iceberg `Uuid` → PG `uuid`; PG `bytea` → Iceberg `Binary` → PG `bytea`;
//! PG `jsonb` → Iceberg `Binary` with an explicit JSONB-internal codec; PG
//! `text`/`varchar`/`bpchar`/`json`/`name` → Iceberg `String`). Iceberg `Binary`
//! cannot by itself distinguish `bytea` from `jsonb`, so live scan/write
//! planning must combine this coarse format type with the relation's actual
//! `(oid, typmod)` before resolving the rule. A round-trip test guards the
//! deliberately coarse correspondence against drift.

use arrow_schema::{DataType, Schema as ArrowSchema};
use iceberg_lite::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use pg_arrow_conv::{
    ArrowConversionError, ColumnRule, PgColumnType, resolve_column_rule,
};
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};

// ---------------------------------------------------------------------------
// Iceberg `Type` → Arrow / PG descriptors
// ---------------------------------------------------------------------------

/// Maps a single Iceberg [`Type`] to the conversion descriptors the data path
/// keys on: its Arrow `DataType` and its target [`PgColumnType`].
pub(crate) trait IcebergTypeExt {
    /// Arrow `DataType` for this Iceberg type.
    ///
    /// Thin adapter over [`iceberg_lite::arrow::type_to_arrow_type`] that
    /// re-routes its error into [`IcebergError`]. Used for nested-type builders
    /// (e.g. list element fields) and as the building block of
    /// [`IcebergFieldExt::resolve_rule`].
    fn arrow_type(&self) -> IcebergResult<DataType>;

    /// Target [`PgColumnType`] for this Iceberg type, or `None` for shapes that
    /// have no scalar PG column type (`Struct`/`Map`).
    ///
    /// `pg-arrow-conv` dispatches on the pair `(Arrow DataType, PgColumnType)`.
    /// The Arrow `DataType` already encodes decimal precision/scale, timestamp
    /// unit/tz, and fixed/binary width, so this is load-bearing for exactly one
    /// distinction the Arrow type alone cannot make: telling a `uuid` column
    /// apart from a fixed-width `bytea` column when both materialize as Arrow
    /// `FixedSizeBinary(16)`.
    ///
    /// `Struct`/`Map` return `None`; they are also rejected by the
    /// [`ValidateSupported`] gate that runs at the start of
    /// [`IcebergFieldExt::resolve_rule`], so the `None` arm is an honest "no
    /// target type" rather than a sentinel.
    fn pg_column_type(&self) -> Option<PgColumnType>;
}

impl IcebergTypeExt for Type {
    fn arrow_type(&self) -> IcebergResult<DataType> {
        Ok(iceberg_lite::arrow::type_to_arrow_type(self)?)
    }

    fn pg_column_type(&self) -> Option<PgColumnType> {
        let pg = match self {
            Type::Primitive(p) => match p {
                PrimitiveType::Boolean => PgColumnType::Bool,
                PrimitiveType::Int => PgColumnType::Int4,
                PrimitiveType::Long => PgColumnType::Int8,
                PrimitiveType::Float => PgColumnType::Float4,
                PrimitiveType::Double => PgColumnType::Float8,
                PrimitiveType::Decimal { .. } => PgColumnType::Numeric,
                PrimitiveType::Date => PgColumnType::Date,
                PrimitiveType::Time => PgColumnType::Time,
                PrimitiveType::Timestamp | PrimitiveType::TimestampNs => {
                    PgColumnType::Timestamp
                }
                PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                    PgColumnType::Timestamptz
                }
                PrimitiveType::String => PgColumnType::Text,
                // The load-bearing distinction: `Uuid` → `uuid`, `Fixed`/
                // `Binary` → `bytea` at the format-only boundary. A live
                // PostgreSQL JSONB target is classified separately by
                // `PgColumnType::from_pg_type` before rule resolution.
                PrimitiveType::Uuid => PgColumnType::Uuid,
                PrimitiveType::Fixed(_) | PrimitiveType::Binary => {
                    PgColumnType::Bytea
                }
            },
            // A list reproduces its element column's canonical PG element OID
            // (the inverse of `schema_builder`'s PG→Iceberg element mapping,
            // matching the scalar canonicalization above: Iceberg `Int`→`int4`,
            // `String`→`text`). The OID is what the read path's array datum
            // targets; only `bool`/`int`/`long`/`float`/`double`/`string`
            // elements are materializable (mirrors `resolve_list_element_rule`),
            // so any other element kind has no target and yields `None`.
            Type::List(list) => {
                let elem_oid = match list.element_field.field_type.as_ref() {
                    Type::Primitive(PrimitiveType::Boolean) => pg_sys::BOOLOID,
                    Type::Primitive(PrimitiveType::Int) => pg_sys::INT4OID,
                    Type::Primitive(PrimitiveType::Long) => pg_sys::INT8OID,
                    Type::Primitive(PrimitiveType::Float) => pg_sys::FLOAT4OID,
                    Type::Primitive(PrimitiveType::Double) => pg_sys::FLOAT8OID,
                    Type::Primitive(PrimitiveType::String) => pg_sys::TEXTOID,
                    _ => return None,
                };
                PgColumnType::Array(elem_oid)
            }
            Type::Struct(_) | Type::Map(_) => return None,
        };
        Some(pg)
    }
}

// ---------------------------------------------------------------------------
// Iceberg `NestedField` → ColumnRule
// ---------------------------------------------------------------------------

/// Resolves the `pg-arrow-conv` [`ColumnRule`] for one Iceberg field.
pub(crate) trait IcebergFieldExt {
    /// Resolve the [`ColumnRule`] for this field against an explicit target
    /// PostgreSQL type, keyed on the pair `(Arrow DataType, PgColumnType)`.
    ///
    /// This is the generic point where a format-only field's rule is resolved
    /// (and thereby validated). Live scan/write columns use
    /// [`Self::resolve_rule_for_column`] so a provider can add an explicit
    /// physical codec when the format type is coarser than the live PostgreSQL
    /// type. In either case, an unsupported or incompatible pair surfaces as a
    /// `ArrowConversionError`/[`IcebergError`] at session begin rather than mid-row.
    ///
    /// `pg` is the column's **real** target type — derived from the relation's
    /// `TupleDesc` (`PgColumnType::from_pg_type`) for a live column, so a desync
    /// between the stored Iceberg type and the PostgreSQL column type is caught
    /// here. (A dropped column lingering only in the Iceberg schema has no live
    /// PG column; the write path passes the Iceberg-derived type for those
    /// NULL-only columns via [`IcebergTypeExt::pg_column_type`].)
    fn resolve_rule(&self, pg: PgColumnType) -> IcebergResult<ColumnRule>;

    /// Resolve a rule for a live PostgreSQL column, allowing this provider to
    /// bind its private JSONB-in-Iceberg-Binary codec explicitly. The generic
    /// Arrow resolver intentionally does not infer that codec from `JSONBOID`.
    fn resolve_rule_for_column(
        &self,
        pg: PgColumnType,
        target_oid: pg_sys::Oid,
    ) -> IcebergResult<ColumnRule>;
}

impl IcebergFieldExt for NestedField {
    fn resolve_rule(&self, pg: PgColumnType) -> IcebergResult<ColumnRule> {
        // Shape gate, scoped to exactly this field. Rejecting unsupported
        // shapes (Struct, Map, nested/unsupported-element lists, oversized
        // `Fixed(len > i32::MAX)`) here — the single per-column
        // resolution+validation point — keeps the check on the columns a plan
        // actually maps. A whole-schema pass would instead reject a query like
        // `SELECT a FROM t` merely because some *unprojected* column `b` has an
        // unsupported shape. The `Fixed(len > i32::MAX)` arm in particular must
        // run before `arrow_type()`, whose `len as i32` would silently truncate
        // such a width into a valid-looking Arrow type and mis-match a rule.
        self.field_type.validate_supported()?;
        let arrow_dt = self.field_type.arrow_type()?;
        resolve_column_rule(&arrow_dt, pg).map_err(IcebergError::from)
    }

    fn resolve_rule_for_column(
        &self,
        pg: PgColumnType,
        target_oid: pg_sys::Oid,
    ) -> IcebergResult<ColumnRule> {
        self.field_type.validate_supported()?;
        let arrow_dt = self.field_type.arrow_type()?;

        if target_oid == pg_sys::JSONBOID {
            return match arrow_dt {
                DataType::Binary | DataType::LargeBinary => {
                    Ok(ColumnRule::PostgresJsonbVarlena)
                }
                _ => Err(IcebergError::from(
                    ArrowConversionError::IncompatibleColumnType(
                        format!("{arrow_dt:?}"),
                        "JSONB requires the provider's Binary JSONB codec"
                            .to_string(),
                    ),
                )),
            };
        }

        resolve_column_rule(&arrow_dt, pg).map_err(IcebergError::from)
    }
}

// ---------------------------------------------------------------------------
// Iceberg `Schema` → Arrow schema / per-column rules
// ---------------------------------------------------------------------------

/// Schema-level Iceberg → Arrow mapping.
pub(crate) trait IcebergSchemaExt {
    /// Convert this Iceberg schema to an Arrow schema.
    ///
    /// Thin adapter over [`iceberg_lite::arrow::schema_to_arrow_schema`]. The
    /// Arrow schema produced here is what the mutation write path's columnar slot
    /// buffer hands to the Parquet writer; making sure it is the same schema
    /// the Parquet writer derives internally is the point of going through the
    /// single iceberg-lite source of truth.
    fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema>;
}

impl IcebergSchemaExt for IcebergSchema {
    fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema> {
        Ok(iceberg_lite::arrow::schema_to_arrow_schema(self)?)
    }
}

// ---------------------------------------------------------------------------
// Supportability gate
// ---------------------------------------------------------------------------

/// Reject types that pg-iceberg-am's mutation/scan paths cannot handle.
///
/// Implemented for [`Type`] and [`PrimitiveType`] and invoked per field from
/// [`IcebergFieldExt::resolve_rule`] or
/// [`IcebergFieldExt::resolve_rule_for_column`] method, so the gate is scoped
/// to exactly the columns a plan maps rather than the whole stored schema.
///
/// A top-level column is accepted for any primitive type or for a single level
/// of `List` whose element type is one the format-neutral list dispatch in
/// `pg-arrow-conv` can materialize. Nested lists, `Struct`, `Map`, and lists of
/// unsupported element types are rejected at the boundary so a later "build
/// first batch" call doesn't surface a generic `UnsupportedColumnType` from
/// deep inside the per-row dispatch loop.
pub(crate) trait ValidateSupported {
    fn validate_supported(&self) -> IcebergResult<()>;
}

impl ValidateSupported for Type {
    fn validate_supported(&self) -> IcebergResult<()> {
        match self {
            Type::Primitive(p) => p.validate_supported(),
            Type::List(list) => match list.element_field.field_type.as_ref() {
                // Mirrors the element kinds the format-neutral list dispatch in
                // `pg-arrow-conv` can materialize (bool/int/long/float/double/
                // string). Anything else (including a nested list) is rejected.
                Type::Primitive(
                    p @ (PrimitiveType::Boolean
                    | PrimitiveType::Int
                    | PrimitiveType::Long
                    | PrimitiveType::Float
                    | PrimitiveType::Double
                    | PrimitiveType::String),
                ) => p.validate_supported(),
                other => Err(IcebergError::UnsupportedColumnType(format!(
                    "list element type {other:?} is not supported"
                ))),
            },
            Type::Struct(_) => Err(IcebergError::UnsupportedColumnType(
                "Struct type is not supported".to_string(),
            )),
            Type::Map(_) => Err(IcebergError::UnsupportedColumnType(
                "Map type is not supported".to_string(),
            )),
        }
    }
}

impl ValidateSupported for PrimitiveType {
    /// Reject primitive shapes pg-iceberg-am cannot encode into Arrow even
    /// though `iceberg_lite::arrow::type_to_arrow_type` is willing to map them.
    ///
    /// Currently the only such case is `Fixed(len)` with `len > i32::MAX`:
    /// iceberg-lite falls through to Arrow `LargeBinary` for that range, but
    /// our writer path uses `FixedSizeBinaryBuilder`, which is bounded by
    /// `i32`. Catching it here makes the invariant in the write path
    /// (`*len as i32` becomes a `try_from` once this validator has run)
    /// load-bearing.
    ///
    /// PG-side schema construction (`catalog::schema_builder`) maps PostgreSQL
    /// `uuid` to Iceberg `Uuid`, not `Fixed(16)`, and does not otherwise create
    /// `Fixed` columns. This only fires for externally-defined Iceberg tables
    /// imported into pg-iceberg-am.
    fn validate_supported(&self) -> IcebergResult<()> {
        match self {
            PrimitiveType::Fixed(len) if *len > i32::MAX as u64 => {
                Err(IcebergError::UnsupportedColumnType(format!(
                    "fixed[{len}] exceeds Arrow FixedSizeBinary i32 width limit"
                )))
            }
            _ => Ok(()),
        }
    }
}

// ============================================================================
// Tests — Iceberg → Arrow schema mapping (host; no PG backend)
//
// pg-arrow-conv is format-neutral and never sees Iceberg types, so it does not
// cover this layer. These pin the Iceberg → Arrow mapping pg-iceberg-am relies
// on (the module doc records a past regression where a parallel mapping table
// disagreed with the Parquet writer).
// ============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, TimeUnit};
    use iceberg_lite::spec::{
        NestedField, PrimitiveType, Schema as IcebergSchema, Type,
    };
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::IcebergSchemaExt;

    /// Build an Iceberg schema from the given fields.
    fn create_test_iceberg_schema(fields: Vec<NestedField>) -> IcebergSchema {
        IcebergSchema::builder()
            .with_fields(fields.into_iter().map(Arc::new))
            .build()
            .expect("Failed to build test schema")
    }

    #[test]
    fn iceberg_schema_to_arrow_schema_primitive_types() {
        let iceberg_schema = create_test_iceberg_schema(vec![
            NestedField::required(
                1,
                "bool_col",
                Type::Primitive(PrimitiveType::Boolean),
            ),
            NestedField::required(2, "int_col", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(
                3,
                "long_col",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                4,
                "float_col",
                Type::Primitive(PrimitiveType::Float),
            ),
            NestedField::optional(
                5,
                "double_col",
                Type::Primitive(PrimitiveType::Double),
            ),
            NestedField::required(
                6,
                "string_col",
                Type::Primitive(PrimitiveType::String),
            ),
        ]);

        let arrow_schema = iceberg_schema.to_arrow_schema().unwrap();

        assert_eq!(arrow_schema.fields().len(), 6);

        assert_eq!(arrow_schema.field(0).name(), "bool_col");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Boolean);
        assert!(!arrow_schema.field(0).is_nullable()); // required

        assert_eq!(arrow_schema.field(1).name(), "int_col");
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Int32);

        assert_eq!(arrow_schema.field(2).name(), "long_col");
        assert_eq!(arrow_schema.field(2).data_type(), &DataType::Int64);

        assert_eq!(arrow_schema.field(3).name(), "float_col");
        assert_eq!(arrow_schema.field(3).data_type(), &DataType::Float32);
        assert!(arrow_schema.field(3).is_nullable()); // optional

        assert_eq!(arrow_schema.field(4).name(), "double_col");
        assert_eq!(arrow_schema.field(4).data_type(), &DataType::Float64);

        assert_eq!(arrow_schema.field(5).name(), "string_col");
        assert_eq!(arrow_schema.field(5).data_type(), &DataType::Utf8);
    }

    #[test]
    fn iceberg_schema_to_arrow_schema_temporal_types() {
        let iceberg_schema = create_test_iceberg_schema(vec![
            NestedField::required(
                1,
                "date_col",
                Type::Primitive(PrimitiveType::Date),
            ),
            NestedField::required(
                2,
                "time_col",
                Type::Primitive(PrimitiveType::Time),
            ),
            NestedField::required(
                3,
                "timestamp_col",
                Type::Primitive(PrimitiveType::Timestamp),
            ),
            NestedField::required(
                4,
                "timestamptz_col",
                Type::Primitive(PrimitiveType::Timestamptz),
            ),
        ]);

        let arrow_schema = iceberg_schema.to_arrow_schema().unwrap();

        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Date32);
        assert_eq!(
            arrow_schema.field(1).data_type(),
            &DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            arrow_schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            arrow_schema.field(3).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );
    }

    #[test]
    fn iceberg_schema_to_arrow_schema_binary_types() {
        let iceberg_schema = create_test_iceberg_schema(vec![
            NestedField::required(
                1,
                "binary_col",
                Type::Primitive(PrimitiveType::Binary),
            ),
            NestedField::required(
                2,
                "fixed_col",
                Type::Primitive(PrimitiveType::Fixed(16)),
            ),
            NestedField::required(
                3,
                "uuid_col",
                Type::Primitive(PrimitiveType::Uuid),
            ),
        ]);

        let arrow_schema = iceberg_schema.to_arrow_schema().unwrap();

        // pg-iceberg-am delegates the Iceberg -> Arrow type table to
        // `iceberg_lite::arrow`, which maps Iceberg `Binary` to Arrow
        // `LargeBinary`. The read path accepts both `Binary` and `LargeBinary`
        // so external producers using the narrow variant are still readable.
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::LargeBinary);
        assert_eq!(
            arrow_schema.field(1).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            arrow_schema.field(2).data_type(),
            &DataType::FixedSizeBinary(16)
        );
    }

    #[test]
    fn iceberg_schema_to_arrow_schema_decimal() {
        let iceberg_schema = create_test_iceberg_schema(vec![NestedField::required(
            1,
            "decimal_col",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 10,
                scale: 2,
            }),
        )]);

        let arrow_schema = iceberg_schema.to_arrow_schema().unwrap();

        assert_eq!(
            arrow_schema.field(0).data_type(),
            &DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn iceberg_schema_to_arrow_schema_list() {
        let iceberg_schema = create_test_iceberg_schema(vec![NestedField::required(
            1,
            "list_col",
            Type::List(iceberg_lite::spec::ListType {
                element_field: NestedField::list_element(
                    2,
                    Type::Primitive(PrimitiveType::Int),
                    true,
                )
                .into(),
            }),
        )]);

        let arrow_schema = iceberg_schema.to_arrow_schema().unwrap();

        match arrow_schema.field(0).data_type() {
            DataType::List(element_field) => {
                assert_eq!(element_field.data_type(), &DataType::Int32);
            }
            _ => panic!("Expected List type"),
        }
    }

    #[test]
    fn iceberg_schema_to_arrow_schema_field_ids_in_metadata() {
        let iceberg_schema = create_test_iceberg_schema(vec![
            NestedField::required(42, "col1", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(99, "col2", Type::Primitive(PrimitiveType::String)),
        ]);

        let arrow_schema = iceberg_schema.to_arrow_schema().unwrap();

        let col1_meta = arrow_schema.field(0).metadata();
        assert_eq!(
            col1_meta.get(PARQUET_FIELD_ID_META_KEY).map(String::as_str),
            Some("42")
        );

        let col2_meta = arrow_schema.field(1).metadata();
        assert_eq!(
            col2_meta.get(PARQUET_FIELD_ID_META_KEY).map(String::as_str),
            Some("99")
        );
    }
}
