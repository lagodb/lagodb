//! PostgreSQL-format adapter for typed EXPLAIN trees.

use std::ffi::{CStr, CString};
use std::ptr;

use lagodb_query::plan::{PlanExplainNode, PlanExplainProperty, PlanExplainValue};
use pgrx::pg_sys;

use crate::query_host::error::QueryHostError;

const GROUP_PLAN: &CStr = c"Plan";
const GROUP_PLANS: &CStr = c"Plans";
const PROP_NODE_TYPE: &CStr = c"Node Type";
const PROP_RELATION_NAME: &CStr = c"Relation Name";
const PROP_SCHEMA: &CStr = c"Schema";
const PROP_ALIAS: &CStr = c"Alias";

pub(super) struct PgExplainTree<'a> {
    root: &'a PlanExplainNode,
}

impl<'a> PgExplainTree<'a> {
    pub(super) const fn new(root: &'a PlanExplainNode) -> Self {
        Self { root }
    }

    /// Emit the root as a real child plan of the surrounding Custom Scan.
    pub(super) unsafe fn emit_plan(
        &self,
        explain: *mut pg_sys::ExplainState,
    ) -> Result<(), QueryHostError> {
        if unsafe { (*explain).format } == pg_sys::ExplainFormat::EXPLAIN_FORMAT_TEXT
        {
            unsafe { Self::emit_text_node(self.root, explain, (*explain).indent) }
        } else {
            unsafe {
                pg_sys::ExplainOpenGroup(
                    GROUP_PLANS.as_ptr(),
                    GROUP_PLANS.as_ptr(),
                    false,
                    explain,
                );
                Self::emit_structured_node(self.root, explain)?;
                pg_sys::ExplainCloseGroup(
                    GROUP_PLANS.as_ptr(),
                    GROUP_PLANS.as_ptr(),
                    false,
                    explain,
                );
            }
            Ok(())
        }
    }

    /// Emit a secondary diagnostic tree under an explicit label.
    pub(super) unsafe fn emit_diagnostic(
        &self,
        label: &'static CStr,
        explain: *mut pg_sys::ExplainState,
    ) -> Result<(), QueryHostError> {
        if unsafe { (*explain).format } == pg_sys::ExplainFormat::EXPLAIN_FORMAT_TEXT
        {
            let heading = CString::new(format!("{}:\n", label.to_string_lossy()))
                .map_err(QueryHostError::invalid_plan)?;
            let base_indent = unsafe { (*explain).indent };
            unsafe {
                pg_sys::appendStringInfoSpaces((*explain).str_, base_indent * 2);
                pg_sys::appendStringInfoString((*explain).str_, heading.as_ptr());
                Self::emit_text_node(self.root, explain, base_indent + 1)
            }
        } else {
            unsafe {
                pg_sys::ExplainOpenGroup(
                    label.as_ptr(),
                    label.as_ptr(),
                    false,
                    explain,
                );
                Self::emit_structured_node(self.root, explain)?;
                pg_sys::ExplainCloseGroup(
                    label.as_ptr(),
                    label.as_ptr(),
                    false,
                    explain,
                );
            }
            Ok(())
        }
    }

    unsafe fn emit_text_node(
        node: &PlanExplainNode,
        explain: *mut pg_sys::ExplainState,
        depth: i32,
    ) -> Result<(), QueryHostError> {
        let node_name = unsafe { Self::text_node_name(node)? };
        let line = CString::new(format!("->  {}\n", node_name.to_string_lossy()))
            .map_err(QueryHostError::invalid_plan)?;
        unsafe {
            pg_sys::appendStringInfoSpaces((*explain).str_, depth * 2);
            pg_sys::appendStringInfoString((*explain).str_, line.as_ptr());
        }
        let saved_indent = unsafe { (*explain).indent };
        // PostgreSQL accounts for two indentation levels occupied by the
        // arrow and one by the node heading before emitting node properties.
        unsafe { (*explain).indent = depth + 3 };
        for property in node.properties() {
            unsafe { Self::emit_property(property, explain)? };
        }
        unsafe { (*explain).indent = saved_indent };
        for child in node.children() {
            unsafe { Self::emit_text_node(child, explain, depth + 3)? };
        }
        Ok(())
    }

    unsafe fn text_node_name(
        node: &PlanExplainNode,
    ) -> Result<CString, QueryHostError> {
        let mut name = node.node_type().to_owned();
        if let Some(relation) = node.relation() {
            name.push_str(" on ");
            if let Some(schema) = relation.schema() {
                unsafe { Self::append_quoted_identifier(&mut name, schema)? };
                name.push('.');
            }
            unsafe { Self::append_quoted_identifier(&mut name, relation.name())? };
            if relation.alias() != relation.name() {
                name.push(' ');
                unsafe {
                    Self::append_quoted_identifier(&mut name, relation.alias())?
                };
            }
        }
        CString::new(name).map_err(QueryHostError::invalid_plan)
    }

    unsafe fn append_quoted_identifier(
        target: &mut String,
        identifier: &str,
    ) -> Result<(), QueryHostError> {
        let identifier =
            CString::new(identifier).map_err(QueryHostError::invalid_plan)?;
        let quoted = unsafe { pg_sys::quote_identifier(identifier.as_ptr()) };
        let quoted = unsafe { CStr::from_ptr(quoted) }.to_string_lossy();
        target.push_str(&quoted);
        Ok(())
    }

    unsafe fn emit_structured_node(
        node: &PlanExplainNode,
        explain: *mut pg_sys::ExplainState,
    ) -> Result<(), QueryHostError> {
        unsafe {
            pg_sys::ExplainOpenGroup(GROUP_PLAN.as_ptr(), ptr::null(), true, explain);
            let node_type = CString::new(node.node_type())
                .map_err(QueryHostError::invalid_plan)?;
            pg_sys::ExplainPropertyText(
                PROP_NODE_TYPE.as_ptr(),
                node_type.as_ptr(),
                explain,
            );
            if let Some(relation) = node.relation() {
                let relation_name = CString::new(relation.name())
                    .map_err(QueryHostError::invalid_plan)?;
                pg_sys::ExplainPropertyText(
                    PROP_RELATION_NAME.as_ptr(),
                    relation_name.as_ptr(),
                    explain,
                );
                if let Some(schema) = relation.schema() {
                    let schema =
                        CString::new(schema).map_err(QueryHostError::invalid_plan)?;
                    pg_sys::ExplainPropertyText(
                        PROP_SCHEMA.as_ptr(),
                        schema.as_ptr(),
                        explain,
                    );
                }
                let alias = CString::new(relation.alias())
                    .map_err(QueryHostError::invalid_plan)?;
                pg_sys::ExplainPropertyText(
                    PROP_ALIAS.as_ptr(),
                    alias.as_ptr(),
                    explain,
                );
            }
            for property in node.properties() {
                Self::emit_property(property, explain)?;
            }
            if !node.children().is_empty() {
                pg_sys::ExplainOpenGroup(
                    GROUP_PLANS.as_ptr(),
                    GROUP_PLANS.as_ptr(),
                    false,
                    explain,
                );
                for child in node.children() {
                    Self::emit_structured_node(child, explain)?;
                }
                pg_sys::ExplainCloseGroup(
                    GROUP_PLANS.as_ptr(),
                    GROUP_PLANS.as_ptr(),
                    false,
                    explain,
                );
            }
            pg_sys::ExplainCloseGroup(
                GROUP_PLAN.as_ptr(),
                ptr::null(),
                true,
                explain,
            );
        }
        Ok(())
    }

    unsafe fn emit_property(
        property: &PlanExplainProperty,
        explain: *mut pg_sys::ExplainState,
    ) -> Result<(), QueryHostError> {
        let name =
            CString::new(property.name()).map_err(QueryHostError::invalid_plan)?;
        match property.value() {
            PlanExplainValue::Text(value) => {
                let value = CString::new(value.as_str())
                    .map_err(QueryHostError::invalid_plan)?;
                unsafe {
                    pg_sys::ExplainPropertyText(
                        name.as_ptr(),
                        value.as_ptr(),
                        explain,
                    )
                };
            }
            PlanExplainValue::Boolean(value) => unsafe {
                pg_sys::ExplainPropertyBool(name.as_ptr(), *value, explain)
            },
            PlanExplainValue::UInteger(value) => unsafe {
                pg_sys::ExplainPropertyUInteger(
                    name.as_ptr(),
                    ptr::null(),
                    *value,
                    explain,
                )
            },
            PlanExplainValue::Float {
                value,
                precision,
                unit,
            } => {
                let unit = unit
                    .as_deref()
                    .map(CString::new)
                    .transpose()
                    .map_err(QueryHostError::invalid_plan)?;
                unsafe {
                    pg_sys::ExplainPropertyFloat(
                        name.as_ptr(),
                        unit.as_ref().map_or(ptr::null(), |unit| unit.as_ptr()),
                        *value,
                        *precision,
                        explain,
                    )
                };
            }
        }
        Ok(())
    }
}
