//! Parquet predicate pushdown shared by foreign scans and query offload.
//!
//! - FDW planning negotiates PostgreSQL fragments with the core
//!   `PredicatePlanner` walker (`planner`), persists a [`PlannedNode`] tree
//!   (`plan`) and binds it per execution (`bind`).
//! - Query offload negotiates runtime predicates against the bound schema
//!   (`runtime_predicate`).
//! - Both produce one file-independent bound tree (`bound`), which each file
//!   lowers to its physical representation (`lower`) before compiling an
//!   exact row filter (`execute`) and row-group pruning (`pruning`).

mod bind;
mod bound;
mod execute;
mod explain;
mod float;
mod integer;
mod lower;
mod operator;
mod plan;
mod planner;
mod policy;
mod pruning;
mod runtime_predicate;
mod value;

use lagodb_core::expr::{RuntimeValueBindings, RuntimeValueId};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};

use crate::error::ConnectorError;
use crate::format::FormatKind;

use self::bind::ParquetFilterBinder;
use self::plan::PlannedNode;

pub(crate) use self::bound::ParquetBoundPredicate;
pub(crate) use self::planner::ParquetFilterPlanner;
pub(crate) use self::pruning::ParquetFilePredicate;
pub(crate) use self::runtime_predicate::ParquetRuntimePlanner;

/// One planned FDW predicate.
pub(crate) struct ParquetPlannedPredicate {
    root: PlannedNode,
}

impl ParquetPlannedPredicate {
    const fn new(root: PlannedNode) -> Self {
        Self { root }
    }

    pub(crate) fn decode(
        kind: FormatKind,
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self, ConnectorError> {
        if kind != FormatKind::Parquet {
            return Err(ConnectorError::invalid_filter_plan(kind));
        }
        let root = reader
            .read_nested(|nested| PlannedNode::decode(nested, binding_count))?;
        Ok(Self { root })
    }

    pub(crate) fn encode(&self, writer: &mut PlanDataWriter) {
        writer.append_nested(|nested| self.root.encode(nested));
    }

    pub(crate) fn explain<'a>(
        &self,
        value: impl Fn(RuntimeValueId) -> &'a str,
    ) -> String {
        let mut output = String::new();
        self.root.write_explain(&mut output, &value);
        output
    }

    pub(crate) fn bind(
        &self,
        values: RuntimeValueBindings<'_>,
    ) -> Result<ParquetBoundPredicate, ConnectorError> {
        let root = ParquetFilterBinder::new(values).bind(&self.root, false)?;
        Ok(ParquetBoundPredicate::new(root))
    }
}
