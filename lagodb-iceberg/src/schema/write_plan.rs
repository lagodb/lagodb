//! Relation-bound PostgreSQL write plans and nested JSONB write defaults.
//!
//! `PgWritePlan` covers every Iceberg output field in canonical schema order
//! and is consumed by the write layer's buffer. Type rules, source slot
//! positions, and nested JSONB defaults are resolved during construction,
//! never in the row loop.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{Field, Schema as ArrowSchema};
use iceberg_lite::spec::{
    Literal, NestedField, PrimitiveLiteral, Schema as IcebergSchema, Type,
};
use lagodb_arrow::{
    BoundDatumPlan, BoundWriteColumnPlan, NestedJsonbDefault, NestedJsonbDefaults,
    NestedJsonbPlan,
};
use lagodb_core::datum::PgType;
use pgrx::pg_sys;

use super::iceberg::IcebergSchemaExt;
use super::relation::{LiveColumn, RelationLayout};
use crate::error::{IcebergError, IcebergResult};

/// Output schema and source-bound column plans, without runtime builder state.
pub(crate) struct PgWritePlan {
    schema: Arc<ArrowSchema>,
    columns: Box<[BoundWriteColumnPlan]>,
}

impl PgWritePlan {
    /// Bind every Iceberg field in schema order before constructing the buffer.
    pub(crate) fn bind(
        schema: &IcebergSchema,
        layout: &RelationLayout,
    ) -> IcebergResult<Self> {
        let fields = schema.as_struct().fields();
        // Both managed and foreign writers require exact schema coverage.
        // A SELECT projection is not a write contract, and SQL defaults have
        // already been materialized into the source slot by PostgreSQL.
        let mut sources_by_field_id: HashMap<i32, &LiveColumn> =
            HashMap::with_capacity(layout.live_count());
        for col in layout.live_columns() {
            let field = schema.require_field_by_name(col.name())?;
            sources_by_field_id.insert(field.id, col);
        }
        if fields.len() != sources_by_field_id.len() {
            return Err(IcebergError::InvariantViolated(
                "Iceberg writes require an exact relation-to-storage field mapping",
            ));
        }
        let arrow_schema = Arc::new(schema.to_arrow_schema()?);
        let slot_width = layout.slot_width();
        let columns = fields
            .iter()
            .zip(arrow_schema.fields())
            .map(|(field, arrow_field)| {
                let col = sources_by_field_id.get(&field.id).ok_or(
                    IcebergError::InvariantViolated(
                        "Iceberg writes require a relation source for every storage field",
                    ),
                )?;
                let plan =
                    WriteField::new(field).plan(arrow_field.as_ref(), col.pg_type())?;
                Ok(BoundWriteColumnPlan::bind(
                    plan,
                    col.slot_index(),
                    slot_width,
                )?)
            })
            .collect::<IcebergResult<Box<[_]>>>()?;
        Ok(Self {
            schema: arrow_schema,
            columns,
        })
    }

    pub(crate) fn into_parts(
        self,
    ) -> (Arc<ArrowSchema>, Box<[BoundWriteColumnPlan]>) {
        (self.schema, self.columns)
    }
}

struct WriteField<'a>(&'a NestedField);

impl<'a> WriteField<'a> {
    const fn new(field: &'a NestedField) -> Self {
        Self(field)
    }

    /// Bind against the canonical Arrow field already produced by `schema.to_arrow_schema()`.
    fn plan(
        &self,
        arrow_field: &Field,
        pg_type: PgType,
    ) -> IcebergResult<BoundDatumPlan> {
        let source = self.0;
        if pg_type.oid == pg_sys::JSONBOID
            && Self::is_nested_jsonb(&source.field_type)
        {
            let nested = NestedJsonbPlan::from_field(arrow_field)?
                .with_write_defaults(self.defaults()?)?;
            return Ok(BoundDatumPlan::from_nested_jsonb(nested)?);
        }
        Ok(BoundDatumPlan::bind(arrow_field, pg_type)?)
    }

    fn is_nested_jsonb(field_type: &Type) -> bool {
        match field_type {
            Type::Struct(_) | Type::Map(_) => true,
            Type::List(list) => {
                !matches!(list.element_field.field_type.as_ref(), Type::Primitive(_))
            }
            Type::Primitive(_) | Type::Variant(_) => false,
        }
    }

    fn defaults(&self) -> IcebergResult<NestedJsonbDefaults> {
        let children = match self.0.field_type.as_ref() {
            Type::Struct(s) => s
                .fields()
                .iter()
                .map(|f| Self::new(f).defaults())
                .collect::<IcebergResult<Vec<_>>>()?,
            Type::List(l) => vec![Self::new(&l.element_field).defaults()?],
            Type::Map(m) => vec![
                Self::new(&m.key_field).defaults()?,
                Self::new(&m.value_field).defaults()?,
            ],
            _ => Vec::new(),
        };
        Ok(NestedJsonbDefaults {
            value: self
                .0
                .write_default
                .as_ref()
                .map(Self::literal)
                .transpose()?,
            children,
        })
    }

    fn literal(value: &Literal) -> IcebergResult<NestedJsonbDefault> {
        use NestedJsonbDefault as D;
        Ok(match value {
            Literal::Primitive(value) => match value {
                PrimitiveLiteral::Boolean(v) => D::Boolean(*v),
                PrimitiveLiteral::Int(v) => D::Int(*v),
                PrimitiveLiteral::Long(v) => D::Long(*v),
                PrimitiveLiteral::Float(v) => D::Float(v.0),
                PrimitiveLiteral::Double(v) => D::Double(v.0),
                PrimitiveLiteral::String(v) => D::String(v.as_str().into()),
                PrimitiveLiteral::Binary(v) => D::Binary(v.as_slice().into()),
                PrimitiveLiteral::Int128(v) => D::Decimal(*v),
                PrimitiveLiteral::UInt128(v) => D::Binary(v.to_be_bytes().into()),
                PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
                    return Err(IcebergError::UnsupportedColumnType(
                        "out-of-range sentinel is not a write default".into(),
                    ));
                }
            },
            Literal::Struct(v) => D::Struct(
                v.iter()
                    .map(|v| v.map(Self::literal).transpose())
                    .collect::<IcebergResult<_>>()?,
            ),
            Literal::List(v) => D::List(
                v.iter()
                    .map(|v| v.as_ref().map(Self::literal).transpose())
                    .collect::<IcebergResult<_>>()?,
            ),
            // Upstream exposes Map's owning iterator. This clone happens only
            // during schema binding, never when a row omits the field.
            Literal::Map(v) => D::Map(
                v.clone()
                    .into_iter()
                    .map(|(k, v)| {
                        Ok((
                            Self::literal(&k)?,
                            v.as_ref().map(Self::literal).transpose()?,
                        ))
                    })
                    .collect::<IcebergResult<_>>()?,
            ),
        })
    }
}
