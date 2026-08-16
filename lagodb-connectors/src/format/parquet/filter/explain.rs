//! Planning-time Parquet predicate descriptions for FDW EXPLAIN.

use std::ffi::{CStr, CString};

use pg_lakebase_core::fdw::ForeignFilterExplainValues;
use pgrx::pg_sys;

use super::{PlannedColumn, PlannedNode};

impl PlannedColumn {
    fn write_explain(&self, output: &mut String) {
        let name = CString::new(self.name.as_ref())
            .expect("PostgreSQL identifiers contain no NUL bytes");
        // The CString is live for the call, and PostgreSQL returns either its
        // input pointer or a planner-context copy.
        let quoted = unsafe { pg_sys::quote_identifier(name.as_ptr()) };
        let quoted = unsafe { CStr::from_ptr(quoted) }
            .to_str()
            .expect("Parquet filter planning requires UTF-8 column names");
        output.push_str(quoted);
    }
}

impl PlannedNode {
    pub(super) fn write_explain(
        &self,
        output: &mut String,
        values: ForeignFilterExplainValues<'_>,
    ) {
        output.push('(');
        match self {
            Self::Comparison {
                operator,
                column,
                value,
                ..
            } => {
                column.write_explain(output);
                output.push(' ');
                output.push_str(operator.sql());
                output.push(' ');
                output.push_str(values.value(*value));
            }
            Self::IsNull(column) => {
                column.write_explain(output);
                output.push_str(" IS NULL");
            }
            Self::IsNotNull(column) => {
                column.write_explain(output);
                output.push_str(" IS NOT NULL");
            }
            Self::And(children) => {
                Self::write_explain_children(output, values, children, " AND ");
            }
            Self::Or(children) => {
                Self::write_explain_children(output, values, children, " OR ");
            }
            Self::Not(child) => {
                output.push_str("NOT ");
                child.write_explain(output, values);
            }
        }
        output.push(')');
    }

    fn write_explain_children(
        output: &mut String,
        values: ForeignFilterExplainValues<'_>,
        children: &[Self],
        separator: &str,
    ) {
        for (index, child) in children.iter().enumerate() {
            if index > 0 {
                output.push_str(separator);
            }
            child.write_explain(output, values);
        }
    }
}
