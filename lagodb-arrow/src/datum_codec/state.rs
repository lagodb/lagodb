//! Backend-local read state retained across Arrow batches and rescans.
//!
//! Every fallible conversion setup — target codecs, decimal codecs and
//! reusable datum buffers — happens here, once per decoder. Binding a batch to
//! this state only selects concrete arrays.

use std::{cell::RefCell, rc::Rc};

use lagodb_core::datum::{ArrayDatumBuffer, CompositeDatumCodec, PgTextIo};

use super::list_read::ListRead;
use super::reader::ScalarRead;
use super::{BoundDatumPlan, NodePlan};
use crate::error::ArrowConversionResult;
use crate::rule::CellRule;

pub(crate) enum DatumReadState {
    Scalar(ScalarRead),
    NestedJsonb,
    TextIo(Rc<PgTextIo>),
    List {
        buffer: Rc<RefCell<ArrayDatumBuffer>>,
        child: Box<Self>,
    },
    Struct {
        codec: Rc<RefCell<CompositeDatumCodec>>,
        children: Box<[Self]>,
    },
    Oid,
    Interval,
}

impl DatumReadState {
    pub(crate) fn new(plan: &BoundDatumPlan) -> ArrowConversionResult<Self> {
        Ok(match &plan.node {
            NodePlan::Scalar { rule, codec } => Self::Scalar(ScalarRead::bind(
                rule,
                *codec,
                Some(plan.base_type.typmod),
            )?),
            NodePlan::TextIo {
                read_codec: Some(codec),
            } => Self::Scalar(ScalarRead::bind(&CellRule::Utf8, *codec, None)?),
            NodePlan::TextIo { read_codec: None } => {
                Self::TextIo(Rc::new(PgTextIo::bind(plan.base_type)))
            }
            NodePlan::NestedJsonb(_) => Self::NestedJsonb,
            NodePlan::NativeList {
                element,
                layout,
                codec,
                ..
            } => Self::Scalar(ScalarRead::List(ListRead::new(
                *element, *codec, *layout,
            ))),
            NodePlan::List {
                layout, element, ..
            } => Self::List {
                buffer: Rc::new(RefCell::new(ArrayDatumBuffer::new(*layout))),
                child: Box::new(Self::new(element)?),
            },
            NodePlan::Struct(fields) => Self::Struct {
                codec: Rc::new(RefCell::new(CompositeDatumCodec::bind(
                    plan.base_type,
                ))),
                children: fields
                    .iter()
                    .map(|(_, child)| Self::new(child))
                    .collect::<ArrowConversionResult<_>>()?,
            },
            NodePlan::Oid => Self::Oid,
            NodePlan::Interval => Self::Interval,
        })
    }
}
