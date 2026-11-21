//! Planning-time Parquet predicate descriptions.

use std::ffi::{CStr, CString};
use std::fmt::Write;

use lagodb_core::expr::RuntimeValueId;
use pgrx::pg_sys;

use super::plan::{PlannedColumn, PlannedNode};

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
    pub(super) fn write_explain<'a>(
        &self,
        output: &mut String,
        values: &impl Fn(RuntimeValueId) -> &'a str,
    ) {
        output.push('(');
        match self {
            Self::AlwaysTrue => output.push_str("true"),
            Self::AlwaysFalse => output.push_str("false"),
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
                output.push_str(values(*value));
            }
            Self::BooleanTest { kind, column } => {
                column.write_explain(output);
                write!(output, " IS {kind}")
                    .expect("writing to a String cannot fail");
            }
            Self::IsNull(column) => {
                column.write_explain(output);
                output.push_str(" IS NULL");
            }
            Self::IsNotNull(column) => {
                column.write_explain(output);
                output.push_str(" IS NOT NULL");
            }
            Self::IsNan(column) => {
                column.write_explain(output);
                output.push_str(" = 'NaN'");
            }
            Self::IsNotNan(column) => {
                column.write_explain(output);
                output.push_str(" <> 'NaN'");
            }
            Self::StartsWith { column, prefix } => {
                output.push_str("starts_with(");
                column.write_explain(output);
                output.push_str(", ");
                output.push_str(values(*prefix));
                output.push(')');
            }
            Self::InSet { column, set, .. } => {
                column.write_explain(output);
                output.push_str(" = ANY (");
                output.push_str(values(*set));
                output.push(')');
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

    fn write_explain_children<'a>(
        output: &mut String,
        values: &impl Fn(RuntimeValueId) -> &'a str,
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
