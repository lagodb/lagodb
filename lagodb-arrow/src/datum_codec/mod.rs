//! Recursive conversion bound to declared column types and Arrow storage fields.
//!
//! Providers choose the storage schema and admission policy. This module knows
//! neither Iceberg nor managed/foreign tables. Containers reuse scalar datum
//! codecs. LagoDB resolves declared types and I/O functions during binding,
//! and downcasts Arrow arrays per batch, not per element. PostgreSQL type I/O
//! can still perform syscache lookups per value (for example, enum_in).
//! Pass-by-reference Datums allocate in the caller's row memory context.

mod batch;
mod decoder;
mod encoder;
mod native;
mod native_list;
mod reader;
mod recursive_read;
mod state;
mod text;
mod write;

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use lagodb_core::datum::{
    ArrayDatumLayout, BoundColumnDatumCodec, ColumnDatumTarget, PgType,
};
use pgrx::pg_sys;

use crate::{
    ArrowConversionError, ArrowConversionResult, CellRule, DatumCodec,
    ListElementRule, NestedJsonbPlan, PgColumnType, resolve_cell_rule,
};

pub use batch::{BoundBatch, BoundColumn};
pub use decoder::{ArrowColumnDecoder, DecodedColumn};
pub(crate) use encoder::DatumEncoder;
pub(crate) use native::NativeEncoderPlan;
pub(crate) use state::DatumReadState;
pub(crate) use write::RecursiveDatumEncoder;

/// A column's declared type and storage representation, bound before row access.
/// Arrays and composites carry child plans; scalar adapters retain the type's
/// datum or text I/O codec. Providers choose which type/field pairs to admit.
#[derive(Debug)]
pub struct BoundDatumPlan {
    pg_type: PgType,
    base_type: PgType,
    field: FieldRef,
    node: NodePlan,
}

#[derive(Debug)]
enum NodePlan {
    Scalar {
        rule: CellRule,
        codec: DatumCodec,
    },
    TextIo {
        // None reads trusted PG output; Some selects semantic conversion.
        // Writers always use PG text output, independently of this codec.
        read_codec: Option<DatumCodec>,
    },
    Oid,
    NestedJsonb(Box<NestedJsonbPlan>),
    NativeList {
        rule: CellRule,
        layout: ArrayDatumLayout,
        codec: BoundColumnDatumCodec,
    },
    List {
        layout: ArrayDatumLayout,
        element: Box<BoundDatumPlan>,
    },
    Struct(Box<[(usize, BoundDatumPlan)]>),
    Interval,
}

/// Consumed while binding; row readers retain codecs, not this policy.
#[derive(Clone, Copy)]
enum JsonTextPolicy {
    // TODO: reuse a bound text-input buffer for checked JSON decoding. The
    // current core Cell::into_json_text_datum path creates a CString for each
    // non-NULL borrowed string, allocating and copying per JSON/JSONB leaf in
    // COPY rows, arrays and composites. Extend core's cached text-input codec
    // rather than adding an Arrow-local input implementation; retain NUL
    // rejection, PG parsing and COPY's row-error handling when reusing buffers.
    Parse,
    Prevalidated,
}

impl BoundDatumPlan {
    /// Bind a provider-approved PostgreSQL/Arrow pair. The field retains nested
    /// field IDs and logical types; the declared PG OID retains domain identity.
    /// Text destined for json/jsonb rejects embedded NUL and is parsed through
    /// PostgreSQL input, including array/composite leaves. Other producer
    /// contracts remain those of `DecodedColumn::bind`.
    pub fn bind(field: &Field, pg_type: PgType) -> ArrowConversionResult<Self> {
        Self::bind_with_json_text_policy(field, pg_type, JsonTextPolicy::Parse)
    }

    /// Bind the same conversion with already validated JSON text.
    ///
    /// # Safety
    /// Every non-NULL text value decoded into PG json/jsonb, including
    /// array/composite leaves, must already be valid JSON without embedded NUL.
    /// An Arrow string type or a target JSON OID does not establish this
    /// guarantee. Structural JSONB retains its own checked conversion; other
    /// TextIo types still require PostgreSQL's NUL-free output representation.
    pub unsafe fn bind_with_prevalidated_json_text(
        field: &Field,
        pg_type: PgType,
    ) -> ArrowConversionResult<Self> {
        Self::bind_with_json_text_policy(field, pg_type, JsonTextPolicy::Prevalidated)
    }

    fn bind_with_json_text_policy(
        field: &Field,
        pg_type: PgType,
        json_text_policy: JsonTextPolicy,
    ) -> ArrowConversionResult<Self> {
        let base = pg_type.base();
        let node = if pg_type.oid == pg_sys::JSONBOID
            && matches!(
                field.data_type(),
                DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
            ) {
            NodePlan::NestedJsonb(Box::new(NestedJsonbPlan::from_field(field)?))
        } else if let Some(element) = base.element() {
            let DataType::List(child) = field.data_type() else {
                return Err(Self::incompatible_type_error(field, pg_type));
            };
            // Select one list strategy for both readers and writers. Native
            // primitive lists dispatch before the element loop; other lists
            // recurse through their declared PostgreSQL element type.
            let native_rule = ListElementRule::for_arrow_type(child.data_type())
                .filter(|rule| rule.accepts_target_oid(element.oid))
                .map(|rule| CellRule::List {
                    element: rule,
                    field: Arc::clone(child),
                    elem_oid: element.oid,
                });
            let layout = ArrayDatumLayout::bind(element.oid);
            if let Some(rule) = native_rule {
                // Only the explicit prevalidated entry point may construct
                // JSON elements as text; the layout retains the JSON OID.
                let codec_oid = if element.oid == pg_sys::JSONOID
                    && matches!(json_text_policy, JsonTextPolicy::Prevalidated)
                {
                    pg_sys::TEXTOID
                } else {
                    element.oid
                };
                let codec = BoundColumnDatumCodec::bind(
                    ColumnDatumTarget::from_oid(codec_oid),
                )?;
                NodePlan::NativeList {
                    rule,
                    layout,
                    codec,
                }
            } else {
                NodePlan::List {
                    layout,
                    element: Box::new(Self::bind_with_json_text_policy(
                        child,
                        element,
                        json_text_policy,
                    )?),
                }
            }
        } else if base.is_composite() {
            let DataType::Struct(children) = field.data_type() else {
                return Err(Self::incompatible_type_error(field, pg_type));
            };
            let attributes = base.composite_fields();
            if children.len() != attributes.len() {
                return Err(Self::incompatible_type_error(field, pg_type));
            }
            // Providers establish the field correspondence and own schema
            // evolution. The codec binds physical attribute positions once.
            // Managed DDL guards must preserve this correspondence across
            // attribute add/drop/alter; equal live-field counts alone do not
            // preserve identity after a DROP followed by an ADD.
            let fields = children
                .iter()
                .zip(attributes)
                .map(|(child, attr)| {
                    Ok((
                        attr.index,
                        Self::bind_with_json_text_policy(
                            child,
                            attr.pg_type,
                            json_text_policy,
                        )?,
                    ))
                })
                .collect::<ArrowConversionResult<Box<[_]>>>()?;
            NodePlan::Struct(fields)
        } else if base.oid == pg_sys::INTERVALOID {
            let DataType::Struct(fields) = field.data_type() else {
                return Err(Self::incompatible_type_error(field, pg_type));
            };
            // Component nullability is part of the interval representation,
            // not a per-row constraint. Checked binding verifies it in `accepts_arrow_type`;
            // trusted binding inherits it from the producer's scan schema.
            // Both readers therefore need only the parent bitmap.
            if fields.len() != 3
                || fields.iter().zip(["months", "days", "microseconds"]).any(
                    |(f, name)| {
                        f.name() != name
                            || *f.data_type() != DataType::Int64
                            || f.is_nullable()
                    },
                )
            {
                return Err(Self::incompatible_type_error(field, pg_type));
            }
            NodePlan::Interval
        } else if base.is_object_id() && *field.data_type() == DataType::Int64 {
            NodePlan::Oid
        } else {
            match (PgColumnType::from_pg_type(base.oid), field.data_type()) {
                // The provider has already admitted this text representation.
                // Arrow does not own a second list of persistable PG types.
                (
                    None | Some(PgColumnType::Jsonb),
                    DataType::Utf8 | DataType::LargeUtf8,
                ) => {
                    let read_codec = if base.oid == pg_sys::JSONBOID
                        && matches!(json_text_policy, JsonTextPolicy::Parse)
                    {
                        Some(DatumCodec::standard(base.oid)?)
                    } else {
                        None
                    };
                    NodePlan::TextIo { read_codec }
                }
                (Some(target), _) => {
                    let rule = resolve_cell_rule(field.data_type(), target)?;
                    let codec = if base.oid == pg_sys::JSONOID
                        && matches!(json_text_policy, JsonTextPolicy::Prevalidated)
                    {
                        // SAFETY: only bind_with_prevalidated_json_text selects this
                        // policy, and recursion preserves its input contract.
                        unsafe { DatumCodec::prevalidated_json_text() }
                    } else {
                        DatumCodec::standard(base.oid)?
                    };
                    NodePlan::Scalar { rule, codec }
                }
                _ => return Err(Self::incompatible_type_error(field, pg_type)),
            }
        };
        Ok(Self {
            pg_type,
            base_type: base,
            field: Arc::new(field.clone()),
            node,
        })
    }

    fn incompatible_type_error(
        field: &Field,
        pg_type: PgType,
    ) -> ArrowConversionError {
        ArrowConversionError::IncompatibleColumnType(
            format!("{:?}", field.data_type()),
            format!(
                "PostgreSQL type {} with typmod {}",
                pg_type.oid, pg_type.typmod
            ),
        )
    }

    /// Bind a precompiled JSONB container plan, including provider-supplied
    /// write defaults. The nested plan remains independent of Datum planning.
    pub fn from_nested_jsonb(plan: NestedJsonbPlan) -> ArrowConversionResult<Self> {
        if !plan.is_container() {
            return Err(ArrowConversionError::UnsupportedColumnType(
                "a nested JSONB column must have a struct, list or map root".into(),
            ));
        }
        let pg_type = PgType::new(pg_sys::JSONBOID, -1);
        Ok(Self {
            pg_type,
            base_type: pg_type,
            field: Arc::clone(plan.field()),
            node: NodePlan::NestedJsonb(Box::new(plan)),
        })
    }

    pub fn pg_type_oid(&self) -> pg_sys::Oid {
        self.pg_type.oid
    }

    pub(crate) fn requires_datum_context(&self) -> bool {
        match &self.node {
            NodePlan::Oid => false,
            NodePlan::Scalar { .. } => unsafe {
                !pg_sys::get_typbyval(self.base_type.oid)
            },
            NodePlan::TextIo { .. }
            | NodePlan::NestedJsonb(_)
            | NodePlan::NativeList { .. }
            | NodePlan::List { .. }
            | NodePlan::Struct(_)
            | NodePlan::Interval => true,
        }
    }

    pub(crate) fn accepts_arrow_type(&self, data_type: &DataType) -> bool {
        match (&self.node, data_type) {
            (NodePlan::Scalar { rule, .. }, data_type) => rule.accepts(data_type),
            (NodePlan::NativeList { rule, .. }, data_type) => rule.accepts(data_type),
            (NodePlan::NestedJsonb(plan), data_type) => plan.accepts(data_type),
            (NodePlan::TextIo { .. }, DataType::Utf8 | DataType::LargeUtf8) => true,
            (NodePlan::Oid, DataType::Int64) => true,
            (NodePlan::List { element, .. }, DataType::List(field)) => {
                element.accepts_arrow_type(field.data_type())
            }
            (NodePlan::Struct(children), DataType::Struct(fields)) => {
                children.len() == fields.len()
                    && children.iter().zip(fields).all(|((_, child), field)| {
                        child.field.name() == field.name()
                            && child.accepts_arrow_type(field.data_type())
                    })
            }
            (NodePlan::Interval, DataType::Struct(fields)) => {
                fields.len() == 3
                    && fields.iter().zip(["months", "days", "microseconds"]).all(
                        |(field, name)| {
                            field.name() == name
                                && *field.data_type() == DataType::Int64
                                && !field.is_nullable()
                        },
                    )
            }
            _ => false,
        }
    }

    pub(crate) fn requires_utf8_server_encoding(&self) -> bool {
        match &self.node {
            NodePlan::TextIo { .. } => true,
            NodePlan::NestedJsonb(_) => true,
            NodePlan::NativeList { rule, .. } => rule.requires_utf8_server_encoding(),
            NodePlan::Scalar { rule, .. } => rule.requires_utf8_server_encoding(),
            NodePlan::List { element, .. } => element.requires_utf8_server_encoding(),
            NodePlan::Struct(fields) => fields
                .iter()
                .any(|(_, child)| child.requires_utf8_server_encoding()),
            NodePlan::Interval | NodePlan::Oid => false,
        }
    }
}
