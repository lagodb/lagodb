//! Backend-local state retained across Arrow batches and rescans.

use std::{cell::RefCell, rc::Rc};

use lagodb_core::tuple::{ArrayDatumBuffer, CompositeDatumCodec, PgTextIo};

use super::{BoundDatumPlan, NodePlan};

pub(crate) enum DatumReadState {
    Scalar,
    NativeList(Rc<RefCell<ArrayDatumBuffer>>),
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
    pub(crate) fn new(plan: &BoundDatumPlan) -> Self {
        match &plan.node {
            NodePlan::Scalar { .. }
            | NodePlan::NestedJsonb(_)
            | NodePlan::TextIo {
                read_codec: Some(_),
            } => Self::Scalar,
            NodePlan::NativeList { layout, .. } => Self::NativeList(Rc::new(
                RefCell::new(ArrayDatumBuffer::new(*layout)),
            )),
            NodePlan::TextIo { read_codec: None } => {
                Self::TextIo(Rc::new(PgTextIo::bind(plan.base_type)))
            }
            NodePlan::List {
                layout, element, ..
            } => Self::List {
                buffer: Rc::new(RefCell::new(ArrayDatumBuffer::new(*layout))),
                child: Box::new(Self::new(element)),
            },
            NodePlan::Struct(fields) => Self::Struct {
                codec: Rc::new(RefCell::new(CompositeDatumCodec::bind(
                    plan.base_type,
                ))),
                children: fields.iter().map(|(_, child)| Self::new(child)).collect(),
            },
            NodePlan::Oid => Self::Oid,
            NodePlan::Interval => Self::Interval,
        }
    }
}
