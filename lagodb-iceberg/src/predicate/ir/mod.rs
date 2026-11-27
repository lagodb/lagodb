//! Persisted Iceberg predicate intermediate representation, wire codec, and
//! `EXPLAIN` formatting.

mod codec;
mod node;

use lagodb_core::expr::RuntimeValueId;
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};

use super::error::IcebergFilterError;

pub(super) use node::{PlannedColumn, PlannedNode, PlannedValueType};

/// Owned, `copyObject`-serializable Iceberg predicate plan.
#[derive(Debug)]
pub(crate) struct IcebergPlannedPredicate {
    schema_id: i32,
    root: PlannedNode,
}

impl IcebergPlannedPredicate {
    pub(in crate::predicate) const fn new(schema_id: i32, root: PlannedNode) -> Self {
        Self { schema_id, root }
    }

    #[inline]
    pub(in crate::predicate) const fn schema_id(&self) -> i32 {
        self.schema_id
    }

    #[inline]
    pub(in crate::predicate) const fn root(&self) -> &PlannedNode {
        &self.root
    }

    pub(crate) fn encode(&self, writer: &mut PlanDataWriter) {
        writer.append_i32(self.schema_id).append_nested(|root| {
            self.root.encode(root);
        });
    }

    pub(crate) fn decode(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self, IcebergFilterError> {
        let schema_id = reader.read_i32()?;
        let root = reader
            .read_nested(|nested| PlannedNode::decode(nested, binding_count))?;
        Ok(Self::new(schema_id, root))
    }

    /// Render the planned predicate for `EXPLAIN` using caller-formatted
    /// runtime value slots.
    pub(crate) fn explain_with<'a>(
        &self,
        mut value_text: impl FnMut(RuntimeValueId) -> &'a str,
    ) -> String {
        let mut out = String::new();
        self.root.write_explain(&mut out, &mut value_text);
        out
    }
}
