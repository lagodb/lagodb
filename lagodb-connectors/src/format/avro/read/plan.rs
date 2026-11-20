//! Writer-schema read plans.
//!
//! A plan is compiled once per scan from the container's writer schema. Every
//! field gets a skip program that checks the record structure. Only fields
//! bound to an output column also get a decoder, so an unprojected string, enum,
//! or nested record is stepped over without allocation or value validation.
//! Structural checks still apply to skipped data: varints, lengths, union
//! branches, array and map blocks, and fixed sizes must fit the block.

use std::collections::HashMap;

use apache_avro::Schema;
use apache_avro::error::Details;
use apache_avro::schema::{
    DecimalSchema, FixedSchema, InnerDecimalSchema, Name, NamesRef, NamespaceRef,
    RecordSchema, ResolvedSchema, UuidSchema,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::decoder::{AvroCursor, AvroDatum, LeafDecode, avro_error};

type NodeId = usize;

/// One schema node of a skip program; nodes refer to each other by index so
/// recursive named types compile to a finite graph.
enum SkipNode {
    /// `null`, zero-size `fixed`, or a record of such fields.
    Empty,
    /// `boolean`, `float`, `double`, and every `fixed` form.
    Fixed(usize),
    /// `int`, `long`, `enum`, and the date/time logical types.
    Varint,
    /// Length-prefixed `bytes`, `string`, and their logical types.
    Sized,
    Record(Box<[NodeId]>),
    Array(NodeId),
    Map(NodeId),
    Union(Box<[NodeId]>),
    /// Back edge to an enclosing named type. Only these edges can nest
    /// without a schema-imposed bound, so only they check stack depth.
    Recursive(NodeId),
}

#[derive(Clone, Copy)]
struct UnionBranch {
    variants: usize,
    value: usize,
}

/// Decoder for a bound field: a scalar, or a union of that scalar and null.
struct FieldDecode {
    union: Option<UnionBranch>,
    leaf: LeafDecode,
}

impl FieldDecode {
    fn from_schema(schema: &Schema) -> Option<Self> {
        let Schema::Union(union) = schema else {
            return Some(Self {
                union: None,
                leaf: LeafDecode::from_schema(schema)?,
            });
        };
        let variants = union.variants();
        let mut values = variants
            .iter()
            .enumerate()
            .filter(|(_, variant)| !matches!(variant, Schema::Null));
        let (value, variant) = values.next()?;
        if values.next().is_some() {
            return None;
        }
        Some(Self {
            union: Some(UnionBranch {
                variants: variants.len(),
                value,
            }),
            leaf: LeafDecode::from_schema(variant)?,
        })
    }
}

struct FieldPlan {
    skip: NodeId,
    decode: Option<FieldDecode>,
}

pub(super) struct AvroReadPlan {
    schema: Schema,
    fields: Box<[FieldPlan]>,
    nodes: Box<[SkipNode]>,
}

impl AvroReadPlan {
    /// Compiles `schema`, decoding the writer fields at `bound` positions.
    pub(super) fn compile(
        schema: Schema,
        bound: impl IntoIterator<Item = usize>,
    ) -> Result<Self, ConnectorError> {
        let (fields, nodes) = {
            let Schema::Record(record) = &schema else {
                return Err(ConnectorError::invalid_object_schema(
                    FormatKind::Avro,
                    "the Avro container writer schema must be a record",
                ));
            };
            let mut decoded = vec![false; record.fields.len()];
            for source in bound {
                *decoded
                    .get_mut(source)
                    .expect("bound Avro fields come from this writer schema") = true;
            }
            let resolved = ResolvedSchema::try_from(&schema)?;
            let mut compiler = PlanCompiler {
                names: resolved.get_names(),
                nodes: Vec::new(),
                named: HashMap::new(),
            };
            let (_, skips) = compiler.record(record, None)?;
            let fields = record
                .fields
                .iter()
                .zip(skips.iter().copied())
                .zip(decoded)
                .map(|((field, skip), decoded)| {
                    let decode = decoded
                        .then(|| {
                            FieldDecode::from_schema(&field.schema).ok_or_else(|| {
                                ConnectorError::invalid_object_schema(
                                    FormatKind::Avro,
                                    "the Avro field type is not supported",
                                )
                            })
                        })
                        .transpose()?;
                    Ok(FieldPlan { skip, decode })
                })
                .collect::<Result<Box<[_]>, ConnectorError>>()?;
            (fields, compiler.finish())
        };
        Ok(Self {
            schema,
            fields,
            nodes,
        })
    }

    pub(super) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(super) fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Steps over one record, recording where each bound field starts.
    pub(super) fn scan_row(
        &self,
        cursor: &mut AvroCursor<'_>,
        starts: &mut [usize],
    ) -> Result<(), ConnectorError> {
        for (field, start) in self.fields.iter().zip(starts.iter_mut()) {
            if field.decode.is_some() {
                *start = cursor.position();
            }
            self.skip(field.skip, cursor)?;
        }
        Ok(())
    }

    /// Decodes a bound field whose structure [`Self::scan_row`] has checked.
    pub(super) fn decode_field<'a>(
        &'a self,
        field: usize,
        block: &'a [u8],
        start: usize,
    ) -> Result<AvroDatum<'a>, ConnectorError> {
        let decode = self.fields[field]
            .decode
            .as_ref()
            .expect("an Avro column reads only fields bound into its read plan");
        let mut cursor = AvroCursor::new(block, start);
        if let Some(union) = decode.union
            && cursor.read_union_index(union.variants)? != union.value
        {
            return Ok(AvroDatum::Null);
        }
        decode.leaf.decode(&mut cursor)
    }

    fn skip(
        &self,
        node: NodeId,
        cursor: &mut AvroCursor<'_>,
    ) -> Result<(), ConnectorError> {
        match &self.nodes[node] {
            SkipNode::Empty => Ok(()),
            SkipNode::Fixed(size) => cursor.advance(*size),
            SkipNode::Varint => cursor.skip_long(),
            SkipNode::Sized => cursor.read_sized().map(drop),
            SkipNode::Record(fields) => fields
                .iter()
                .try_for_each(|&field| self.skip(field, cursor)),
            SkipNode::Array(items) => {
                let empty = matches!(self.nodes[*items], SkipNode::Empty);
                Self::skip_blocks(cursor, empty, |cursor| self.skip(*items, cursor))
            }
            SkipNode::Map(values) => Self::skip_blocks(cursor, false, |cursor| {
                cursor.read_sized()?;
                self.skip(*values, cursor)
            }),
            SkipNode::Union(variants) => {
                let branch = cursor.read_union_index(variants.len())?;
                self.skip(variants[branch], cursor)
            }
            SkipNode::Recursive(target) => {
                // SAFETY: pgrx converts a stack-depth ERROR at this FFI call into
                // an unwind, as for the JSONB reader's container transitions.
                unsafe { pg_sys::check_stack_depth() };
                self.skip(*target, cursor)
            }
        }
    }

    /// Steps over the blocks of an array or map. A negative count carries the
    /// block's byte size, which Avro provides so readers can skip it whole.
    fn skip_blocks(
        cursor: &mut AvroCursor<'_>,
        empty_items: bool,
        mut skip_item: impl FnMut(&mut AvroCursor<'_>) -> Result<(), ConnectorError>,
    ) -> Result<(), ConnectorError> {
        loop {
            let count = cursor.read_long()?;
            if count == 0 {
                return Ok(());
            }
            if count < 0 {
                count
                    .checked_neg()
                    .ok_or_else(|| avro_error(Details::IntegerOverflow))?;
                let size = cursor.read_len()?;
                cursor.advance(size)?;
                continue;
            }
            if empty_items {
                continue;
            }
            // Every non-empty item takes at least one byte, so checking the
            // count first keeps a corrupt count from looping past the block.
            let count = count.unsigned_abs();
            if count > cursor.remaining() as u64 {
                return Err(avro_error(Details::ReadBlock));
            }
            for _ in 0..count {
                skip_item(cursor)?;
            }
        }
    }
}

struct PlanCompiler<'a> {
    names: &'a NamesRef<'a>,
    /// `None` marks a named record whose fields are still being compiled.
    nodes: Vec<Option<SkipNode>>,
    named: HashMap<Name, NodeId>,
}

impl<'a> PlanCompiler<'a> {
    fn compile(
        &mut self,
        schema: &'a Schema,
        namespace: NamespaceRef<'_>,
    ) -> Result<NodeId, ConnectorError> {
        let node = match schema {
            Schema::Null => SkipNode::Empty,
            Schema::Boolean => SkipNode::Fixed(1),
            Schema::Float => SkipNode::Fixed(4),
            Schema::Double => SkipNode::Fixed(8),
            Schema::Int
            | Schema::Long
            | Schema::Date
            | Schema::TimeMillis
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => SkipNode::Varint,
            Schema::Bytes
            | Schema::String
            | Schema::BigDecimal
            | Schema::Uuid(UuidSchema::String | UuidSchema::Bytes)
            | Schema::Decimal(DecimalSchema {
                inner: InnerDecimalSchema::Bytes,
                ..
            }) => SkipNode::Sized,
            Schema::Fixed(fixed)
            | Schema::Duration(fixed)
            | Schema::Uuid(UuidSchema::Fixed(fixed))
            | Schema::Decimal(DecimalSchema {
                inner: InnerDecimalSchema::Fixed(fixed),
                ..
            }) => return Ok(self.fixed(fixed, namespace)),
            Schema::Enum(enumeration) => {
                let name = enumeration.name.fully_qualified_name(namespace);
                return Ok(self.named(name.into_owned(), SkipNode::Varint));
            }
            Schema::Record(record) => {
                return self.record(record, namespace).map(|(id, _)| id);
            }
            Schema::Array(array) => {
                SkipNode::Array(self.compile(&array.items, namespace)?)
            }
            Schema::Map(map) => SkipNode::Map(self.compile(&map.types, namespace)?),
            Schema::Union(union) => SkipNode::Union(
                union
                    .variants()
                    .iter()
                    .map(|variant| self.compile(variant, namespace))
                    .collect::<Result<_, _>>()?,
            ),
            Schema::Ref { name } => return self.reference(name, namespace),
        };
        Ok(self.push(node))
    }

    /// A named `fixed`, including the fixed form of decimal, uuid, and
    /// duration.
    fn fixed(&mut self, fixed: &FixedSchema, namespace: NamespaceRef<'_>) -> NodeId {
        let node = if fixed.size == 0 {
            SkipNode::Empty
        } else {
            SkipNode::Fixed(fixed.size)
        };
        let name = fixed.name.fully_qualified_name(namespace).into_owned();
        self.named(name, node)
    }

    /// Compiles a record and returns its node with one node per field. The
    /// record is registered before its fields so they can refer back to it.
    fn record(
        &mut self,
        record: &'a RecordSchema,
        namespace: NamespaceRef<'_>,
    ) -> Result<(NodeId, Box<[NodeId]>), ConnectorError> {
        let name = record.name.fully_qualified_name(namespace).into_owned();
        let id = self.nodes.len();
        self.nodes.push(None);
        self.named.insert(name.clone(), id);
        let fields = record
            .fields
            .iter()
            .map(|field| self.compile(&field.schema, name.namespace()))
            .collect::<Result<Box<[_]>, _>>()?;
        let node = if fields
            .iter()
            .all(|&field| matches!(self.nodes[field], Some(SkipNode::Empty)))
        {
            SkipNode::Empty
        } else {
            SkipNode::Record(fields.clone())
        };
        self.nodes[id] = Some(node);
        Ok((id, fields))
    }

    /// Resolves a named reference the way apache-avro's decoder does.
    fn reference(
        &mut self,
        name: &Name,
        namespace: NamespaceRef<'_>,
    ) -> Result<NodeId, ConnectorError> {
        let name = name.fully_qualified_name(namespace).into_owned();
        if let Some(&id) = self.named.get(&name) {
            return Ok(if self.nodes[id].is_none() {
                self.push(SkipNode::Recursive(id))
            } else {
                id
            });
        }
        let schema = *self.names.get(&name).ok_or_else(|| {
            avro_error(Details::SchemaResolutionError(name.clone()))
        })?;
        self.compile(schema, name.namespace())
    }

    fn named(&mut self, name: Name, node: SkipNode) -> NodeId {
        let id = self.push(node);
        self.named.insert(name, id);
        id
    }

    fn push(&mut self, node: SkipNode) -> NodeId {
        self.nodes.push(Some(node));
        self.nodes.len() - 1
    }

    fn finish(self) -> Box<[SkipNode]> {
        self.nodes
            .into_iter()
            .map(|node| node.expect("every named record is filled after its fields"))
            .collect()
    }
}
