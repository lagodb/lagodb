//! Format-neutral write defaults, supplied once by a schema-owning provider.

use super::scalar::ScalarPlan;
use super::{NestedJsonbPlan, NodePlan};
use crate::{ArrowConversionError, ArrowConversionResult};

/// The native value of a declared write default. Dates/times use their Arrow
/// integer units; decimal uses its unscaled coefficient. An empty struct
/// default populates its children from their individual write defaults.
#[derive(Debug, Clone)]
pub enum NestedJsonbDefault {
    Struct(Box<[Option<Self>]>),
    List(Box<[Option<Self>]>),
    Map(Box<[(Self, Option<Self>)]>),
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(Box<str>),
    Binary(Box<[u8]>),
    Decimal(i128),
}

/// Children follow schema order (struct fields, list element, map key/value).
/// An absent value denotes the schema's null/absent write default; nullable
/// fields accept it, required fields report an error when actually omitted.
#[derive(Debug, Default)]
pub struct NestedJsonbDefaults {
    pub value: Option<NestedJsonbDefault>,
    pub children: Vec<Self>,
}

impl NestedJsonbPlan {
    /// Attach a provider's write defaults without adding storage-format types
    /// or default lookup to row execution. Read plans do not need this step.
    pub fn with_write_defaults(
        mut self,
        defaults: NestedJsonbDefaults,
    ) -> ArrowConversionResult<Self> {
        self.attach_defaults(defaults)?;
        Ok(self)
    }

    fn attach_defaults(
        &mut self,
        defaults: NestedJsonbDefaults,
    ) -> ArrowConversionResult<()> {
        self.default = defaults.value;
        let mut children = defaults.children.into_iter();
        let missing = || {
            ArrowConversionError::InvariantViolated(
                "write defaults must follow the nested schema",
            )
        };
        match &mut self.node {
            NodePlan::Scalar(_) => {}
            NodePlan::Struct(fields) => {
                for field in fields {
                    field
                        .value
                        .attach_defaults(children.next().ok_or_else(missing)?)?;
                }
            }
            NodePlan::List(element) => {
                element.attach_defaults(children.next().ok_or_else(missing)?)?
            }
            NodePlan::Map { key, value, .. } => {
                key.attach_defaults(children.next().ok_or_else(missing)?)?;
                value.attach_defaults(children.next().ok_or_else(missing)?)?;
            }
        }
        if children.next().is_some() {
            return Err(missing());
        }
        if let Some(value) = &self.default {
            self.validate_default(value)?;
        }
        Ok(())
    }

    fn validate_default(
        &self,
        value: &NestedJsonbDefault,
    ) -> ArrowConversionResult<()> {
        use NestedJsonbDefault as D;
        let compatible = match (&self.node, value) {
            (NodePlan::Scalar(scalar), value) => matches!(
                (scalar, value),
                (ScalarPlan::Bool, D::Boolean(_))
                    | (ScalarPlan::I32 | ScalarPlan::Date, D::Int(_))
                    | (
                        ScalarPlan::I64
                            | ScalarPlan::Time
                            | ScalarPlan::Timestamp { .. },
                        D::Long(_)
                    )
                    | (ScalarPlan::F32, D::Float(_))
                    | (ScalarPlan::F64, D::Double(_))
                    | (ScalarPlan::String, D::String(_))
                    | (
                        ScalarPlan::Binary
                            | ScalarPlan::FixedBinary(_)
                            | ScalarPlan::Uuid,
                        D::Binary(_)
                    )
                    | (ScalarPlan::Decimal(_), D::Decimal(_))
            ),
            (NodePlan::Struct(fields), D::Struct(values)) => {
                if !values.is_empty() {
                    if fields.len() != values.len() {
                        return Err(Self::default_mismatch());
                    }
                    for (field, value) in fields.iter().zip(values) {
                        if let Some(value) = value {
                            field.value.validate_default(value)?;
                        }
                    }
                }
                true
            }
            (NodePlan::List(child), D::List(values)) => {
                for value in values.iter().flatten() {
                    child.validate_default(value)?;
                }
                true
            }
            (NodePlan::Map { key, value, .. }, D::Map(entries)) => {
                for (k, v) in entries {
                    key.validate_default(k)?;
                    if let Some(v) = v {
                        value.validate_default(v)?;
                    }
                }
                true
            }
            _ => false,
        };
        if compatible {
            Ok(())
        } else {
            Err(Self::default_mismatch())
        }
    }

    fn default_mismatch() -> ArrowConversionError {
        ArrowConversionError::IncompatibleColumnType(
            "nested JSONB write default".into(),
            "default shape does not match its field".into(),
        )
    }
}
