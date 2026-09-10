//! Typed EXPLAIN tree shared by engine diagnostics and host-facing plans.

#[derive(Debug, Clone, PartialEq)]
pub enum PlanExplainValue {
    Text(String),
    Boolean(bool),
    UInteger(u64),
    Float {
        value: f64,
        precision: i32,
        unit: Option<String>,
    },
}

/// Optional PostgreSQL relation identity carried by a scan-like node.
///
/// The node type remains format-independent.  Text renderers may place this
/// identity in the node heading, while structured renderers expose its fields
/// as native plan properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanExplainRelation {
    name: String,
    alias: String,
    schema: Option<String>,
}

impl PlanExplainRelation {
    pub fn new(
        name: impl Into<String>,
        alias: impl Into<String>,
        schema: Option<impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            alias: alias.into(),
            schema: schema.map(Into::into),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanExplainProperty {
    name: String,
    value: PlanExplainValue,
}

impl PlanExplainProperty {
    fn new(name: impl Into<String>, value: PlanExplainValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn value(&self) -> &PlanExplainValue {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanExplainNode {
    node_type: String,
    relation: Option<PlanExplainRelation>,
    properties: Box<[PlanExplainProperty]>,
    children: Box<[Self]>,
}

impl PlanExplainNode {
    pub fn new(
        node_type: impl Into<String>,
        properties: Vec<PlanExplainProperty>,
        children: Vec<Self>,
    ) -> Self {
        Self {
            node_type: node_type.into(),
            relation: None,
            properties: properties.into_boxed_slice(),
            children: children.into_boxed_slice(),
        }
    }

    pub fn with_relation(mut self, relation: PlanExplainRelation) -> Self {
        self.relation = Some(relation);
        self
    }

    pub fn property(
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> PlanExplainProperty {
        PlanExplainProperty::new(name, PlanExplainValue::Text(value.into()))
    }

    pub fn boolean_property(
        name: impl Into<String>,
        value: bool,
    ) -> PlanExplainProperty {
        PlanExplainProperty::new(name, PlanExplainValue::Boolean(value))
    }

    pub fn uinteger_property(
        name: impl Into<String>,
        value: u64,
    ) -> PlanExplainProperty {
        PlanExplainProperty::new(name, PlanExplainValue::UInteger(value))
    }

    pub fn float_property(
        name: impl Into<String>,
        value: f64,
        precision: i32,
        unit: Option<&str>,
    ) -> PlanExplainProperty {
        PlanExplainProperty::new(
            name,
            PlanExplainValue::Float {
                value,
                precision,
                unit: unit.map(str::to_owned),
            },
        )
    }

    pub fn node_type(&self) -> &str {
        &self.node_type
    }

    pub const fn relation(&self) -> Option<&PlanExplainRelation> {
        self.relation.as_ref()
    }

    pub fn properties(&self) -> &[PlanExplainProperty] {
        &self.properties
    }

    pub fn children(&self) -> &[Self] {
        &self.children
    }

    /// Add root-local metadata to an engine-owned diagnostic tree.
    pub fn with_property(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let mut properties = Vec::from(self.properties);
        properties.push(PlanExplainProperty::new(
            name,
            PlanExplainValue::Text(value.into()),
        ));
        self.properties = properties.into_boxed_slice();
        self
    }

    /// Add a numeric root-local metric without degrading structured EXPLAIN
    /// output to a JSON/YAML string.
    pub fn with_uinteger_property(
        mut self,
        name: impl Into<String>,
        value: u64,
    ) -> Self {
        let mut properties = Vec::from(self.properties);
        properties.push(Self::uinteger_property(name, value));
        self.properties = properties.into_boxed_slice();
        self
    }
}
