//! Schema-bound, recursive Arrow/JSONB conversion.
//!
//! Providers supply semantic Arrow fields (notably UUID versus fixed binary). No
//! table-format type or PostgreSQL attribute lookup enters the row loop.

mod defaults;
mod read;
mod scalar;
mod schema;
mod write;
mod write_map;
mod write_scalar;

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use lagodb_core::tuple::JsonbBuilder;

use crate::{ArrowConversionResult, CellRule};
use scalar::ScalarPlan;

pub use defaults::{NestedJsonbDefault, NestedJsonbDefaults};
pub(crate) use read::BoundNestedJsonb;
pub(crate) use write::NestedJsonbEncoder;

/// A format-neutral recursive plan for one JSONB value. Constructed once from
/// the provider's schema; array positions and concrete readers bind per batch.
#[derive(Debug)]
pub struct NestedJsonbPlan {
    node: NodePlan,
    field: FieldRef,
    default: Option<NestedJsonbDefault>,
}

#[derive(Debug)]
enum NodePlan {
    Scalar(ScalarPlan),
    Struct(Box<[FieldPlan]>),
    List(Box<NestedJsonbPlan>),
    Map {
        key: Box<NestedJsonbPlan>,
        value: Box<NestedJsonbPlan>,
        object: bool,
    },
}

#[derive(Debug)]
struct FieldPlan {
    name: Arc<str>,
    value: NestedJsonbPlan,
}

impl NestedJsonbPlan {
    /// Bind a leaf using the existing scalar conversion vocabulary. Private
    /// physical codecs and PostgreSQL array rules are not JSONB leaf semantics.
    fn scalar(field: &Field, rule: CellRule) -> ArrowConversionResult<Self> {
        Ok(Self {
            node: NodePlan::Scalar(ScalarPlan::new(rule)?),
            field: Arc::new(field.clone()),
            default: None,
        })
    }

    /// Bind struct fields in schema order. Names are checked once here, then
    /// borrowed by each row's JSONB builder without allocation or lookup.
    fn structure<N>(
        field: &Field,
        fields: impl IntoIterator<Item = (N, Self)>,
    ) -> ArrowConversionResult<Self>
    where
        N: Into<Arc<str>>,
    {
        let fields = fields
            .into_iter()
            .map(|(name, value)| {
                let name = name.into();
                JsonbBuilder::validate_string(&name)?;
                Ok(FieldPlan { name, value })
            })
            .collect::<ArrowConversionResult<Box<[_]>>>()?;
        Ok(Self {
            node: NodePlan::Struct(fields),
            field: Arc::new(field.clone()),
            default: None,
        })
    }

    /// Bind a list; nested lists and nullable elements retain their structure.
    fn list(field: &Field, element: Self) -> Self {
        Self {
            node: NodePlan::List(Box::new(element)),
            field: Arc::new(field.clone()),
            default: None,
        }
    }

    /// String-key maps become objects. All other key types use an array of
    /// {"key": ..., "value": ...} objects, preserving the key's original type.
    fn map(field: &Field, key: Self, value: Self) -> Self {
        let object = matches!(&key.node, NodePlan::Scalar(ScalarPlan::String));
        Self {
            field: Arc::new(field.clone()),
            default: None,
            node: NodePlan::Map {
                key: Box::new(key),
                value: Box::new(value),
                object,
            },
        }
    }

    /// Whether a physical array has the shape this plan was compiled for.
    /// Used by checked batch binding, never for each nested value. Trusted
    /// producers establish this contract without repeating the traversal.
    pub fn accepts(&self, data_type: &DataType) -> bool {
        match (&self.node, data_type) {
            (NodePlan::Scalar(plan), data_type) => plan.accepts(data_type),
            (NodePlan::Struct(plans), DataType::Struct(fields)) => {
                plans.len() == fields.len()
                    && plans.iter().zip(fields).all(|(plan, field)| {
                        plan.name.as_ref() == field.name()
                            && plan.value.accepts(field.data_type())
                    })
            }
            (NodePlan::List(plan), DataType::List(field)) => {
                plan.accepts(field.data_type())
            }
            (NodePlan::Map { key, value, .. }, DataType::Map(entries, _)) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return false;
                };
                fields.len() == 2
                    && key.accepts(fields[0].data_type())
                    && value.accepts(fields[1].data_type())
            }
            _ => false,
        }
    }

    pub(crate) fn is_container(&self) -> bool {
        !matches!(self.node, NodePlan::Scalar(_))
    }

    pub(crate) fn field(&self) -> &FieldRef {
        &self.field
    }
}
