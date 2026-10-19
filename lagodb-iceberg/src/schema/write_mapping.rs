//! Bind nested JSONB writes to the catalog schema, including write defaults.

use iceberg_lite::spec::{Literal, NestedField, PrimitiveLiteral, Type};
use lagodb_arrow::{
    BoundDatumPlan, NestedJsonbDefault, NestedJsonbDefaults, NestedJsonbPlan,
};
use lagodb_core::datum::PgType;
use pgrx::pg_sys;

use super::arrow_type::SemanticArrowType;
use crate::error::{IcebergError, IcebergResult};

pub(super) struct WriteField<'a>(&'a NestedField);

impl<'a> WriteField<'a> {
    pub(super) fn new(field: &'a NestedField) -> Self {
        Self(field)
    }

    pub(super) fn plan(&self, pg_type: PgType) -> IcebergResult<BoundDatumPlan> {
        let source = self.0;
        let oid = pg_type.oid;
        let nested_jsonb = matches!(
            source.field_type.as_ref(),
            Type::Struct(_) | Type::Map(_)
        ) || matches!(source.field_type.as_ref(), Type::List(list)
                if !matches!(list.element_field.field_type.as_ref(), Type::Primitive(_)));
        if oid != pg_sys::JSONBOID || !nested_jsonb {
            let semantic = SemanticArrowType::new(&source.field_type);
            semantic.validate_supported()?;
            return Ok(BoundDatumPlan::bind(
                &semantic.field(&source.name, !source.required)?,
                pg_type,
            )?);
        }
        SemanticArrowType::new(&source.field_type).validate_supported()?;
        let field = SemanticArrowType::new(&source.field_type)
            .field(&source.name, !source.required)?;
        let nested = NestedJsonbPlan::from_field(&field)?
            .with_write_defaults(self.defaults()?)?;
        Ok(BoundDatumPlan::from_nested_jsonb(nested)?)
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
