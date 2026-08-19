// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::sync::Arc;

use typed_builder::TypedBuilder;

use crate::spec::{
    ListType, Literal, MapType, NestedField, NestedFieldRef, SCHEMA_NAME_DELIMITER,
    Schema, SchemaId, StructType, TableMetadata, Type,
};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

const DEFAULT_FIELD_ID: i32 = 0;

/// Declarative specification for adding a column in [`UpdateSchemaAction`].
#[derive(TypedBuilder)]
pub struct AddColumn {
    #[builder(default = None, setter(strip_option, into))]
    parent: Option<String>,
    #[builder(setter(into))]
    name: String,
    #[builder(default = false)]
    required: bool,
    field_type: Type,
    #[builder(default = None, setter(strip_option, into))]
    doc: Option<String>,
    #[builder(default = None, setter(strip_option))]
    initial_default: Option<Literal>,
    #[builder(default = None, setter(strip_option))]
    write_default: Option<Literal>,
}

impl AddColumn {
    /// Create a root-level optional column specification.
    pub fn optional(name: impl Into<String>, field_type: Type) -> Self {
        Self::builder()
            .name(name.into())
            .field_type(field_type)
            .required(false)
            .build()
    }

    /// Create a root-level required column specification.
    pub fn required(
        name: impl Into<String>,
        field_type: Type,
        initial_default: Literal,
    ) -> Self {
        Self::builder()
            .name(name.into())
            .field_type(field_type)
            .required(true)
            .initial_default(initial_default.clone())
            .write_default(initial_default)
            .build()
    }

    fn to_nested_field(&self) -> NestedFieldRef {
        let mut field = NestedField::new(
            DEFAULT_FIELD_ID,
            self.name.clone(),
            self.field_type.clone(),
            self.required,
        );

        field.doc = self.doc.clone();
        field.initial_default = self.initial_default.clone();
        field.write_default = self.write_default.clone();
        Arc::new(field)
    }
}

/// Schema evolution API modeled after the Java `SchemaUpdate` implementation.
pub struct UpdateSchemaAction {
    ops: Vec<SchemaUpdateIntent>,
}

enum SchemaUpdateIntent {
    Add(Box<AddColumn>),
    Delete { name: String },
    Rename { name: String, new_name: String },
    MakeOptional { name: String },
}

/// Schema update with all names resolved and all newly-added field ids fixed.
///
/// A prepared update represents one schema epoch. Schema/data action ordering
/// belongs to the transaction action log, not to this type.
#[derive(Debug, Clone)]
pub struct PreparedSchemaUpdate {
    base_schema_id: SchemaId,
    base_last_column_id: i32,
    ops: Vec<PreparedSchemaOp>,
    requires_last_field_id_match: bool,
}

#[derive(Debug, Clone)]
enum PreparedSchemaOp {
    Add {
        parent_id: Option<i32>,
        field: NestedFieldRef,
    },
    Delete {
        field_id: i32,
    },
    Rename {
        field_id: i32,
        new_name: String,
    },
    MakeOptional {
        field_id: i32,
    },
}

impl UpdateSchemaAction {
    pub(crate) fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Add a column to the table schema.
    pub fn add_column(mut self, add_column: AddColumn) -> Self {
        self.ops.push(SchemaUpdateIntent::Add(Box::new(add_column)));
        self
    }

    /// Record a column deletion by name.
    pub fn delete_column(mut self, name: impl Into<String>) -> Self {
        self.ops
            .push(SchemaUpdateIntent::Delete { name: name.into() });
        self
    }

    /// Record a column rename by name.
    pub fn rename_column(
        mut self,
        name: impl Into<String>,
        new_name: impl Into<String>,
    ) -> Self {
        self.ops.push(SchemaUpdateIntent::Rename {
            name: name.into(),
            new_name: new_name.into(),
        });
        self
    }

    /// Record a required-to-optional column change by name.
    pub fn make_column_optional(mut self, name: impl Into<String>) -> Self {
        self.ops
            .push(SchemaUpdateIntent::MakeOptional { name: name.into() });
        self
    }

    /// Resolve names and allocate field ids against the supplied table.
    pub fn prepare(&self, table: &Table) -> Result<PreparedSchemaUpdate> {
        SchemaUpdatePlanner::new(table).prepare(&self.ops)
    }
}

struct SchemaUpdatePlanner {
    current_schema: Schema,
    next_field_id: i32,
    base_schema_id: SchemaId,
    base_last_column_id: i32,
    ops: Vec<PreparedSchemaOp>,
    requires_last_field_id_match: bool,
    referenced_fields: ReferencedFields,
}

#[derive(Debug, Default)]
struct ReferencedFields {
    identifier: HashSet<i32>,
    partition_source: HashSet<i32>,
    sort_order_source: HashSet<i32>,
}

#[derive(Debug, Clone, Copy)]
enum FieldReferenceKind {
    Identifier,
    PartitionSource,
    SortOrderSource,
}

impl FieldReferenceKind {
    fn label(self) -> &'static str {
        match self {
            Self::Identifier => "identifier",
            Self::PartitionSource => "partition source",
            Self::SortOrderSource => "sort order source",
        }
    }
}

impl SchemaUpdatePlanner {
    fn new(table: &Table) -> Self {
        let metadata = table.metadata();
        Self {
            current_schema: (**metadata.current_schema()).clone(),
            next_field_id: metadata.last_column_id(),
            base_schema_id: metadata.current_schema().schema_id(),
            base_last_column_id: metadata.last_column_id(),
            ops: Vec::new(),
            requires_last_field_id_match: false,
            referenced_fields: ReferencedFields::from_metadata(metadata),
        }
    }

    fn prepare(
        mut self,
        intents: &[SchemaUpdateIntent],
    ) -> Result<PreparedSchemaUpdate> {
        for intent in intents {
            match intent {
                SchemaUpdateIntent::Add(add) => self.prepare_add(add)?,
                SchemaUpdateIntent::Delete { name } => self.prepare_delete(name)?,
                SchemaUpdateIntent::Rename { name, new_name } => {
                    self.prepare_rename(name, new_name)?;
                }
                SchemaUpdateIntent::MakeOptional { name } => {
                    self.prepare_make_optional(name)?;
                }
            }
        }

        Ok(PreparedSchemaUpdate {
            base_schema_id: self.base_schema_id,
            base_last_column_id: self.base_last_column_id,
            ops: self.ops,
            requires_last_field_id_match: self.requires_last_field_id_match,
        })
    }

    fn prepare_add(&mut self, add: &AddColumn) -> Result<()> {
        let pending_field = add.to_nested_field();

        if pending_field.name.contains(SCHEMA_NAME_DELIMITER) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "Cannot add column with ambiguous name: {}. Use `AddColumn::with_parent` to add a column to a nested struct.",
                    pending_field.name
                ),
            ));
        }

        if pending_field.required && pending_field.initial_default.is_none() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "Incompatible change: cannot add required column without an initial default: {}",
                    pending_field.name
                ),
            ));
        }

        let parent_id = match &add.parent {
            None => {
                if self
                    .current_schema
                    .as_struct()
                    .field_by_name(&pending_field.name)
                    .is_some()
                {
                    return Err(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!(
                            "Cannot add column, name already exists: {}",
                            pending_field.name
                        ),
                    ));
                }
                None
            }
            Some(parent_path) => {
                let (resolved_parent_id, parent_struct) =
                    self.resolve_parent_target(parent_path)?;

                if parent_struct
                    .fields()
                    .iter()
                    .any(|f| f.name == pending_field.name)
                {
                    return Err(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!(
                            "Cannot add column, name already exists in '{}': {}",
                            parent_path, pending_field.name
                        ),
                    ));
                }

                Some(resolved_parent_id)
            }
        };

        let field = self.assign_fresh_ids(&pending_field);
        let op = PreparedSchemaOp::Add { parent_id, field };
        self.push_op(op)?;
        self.requires_last_field_id_match = true;
        Ok(())
    }

    fn prepare_delete(&mut self, name: &str) -> Result<()> {
        let field = self.current_schema.field_by_name(name).ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot delete missing column: {name}"),
            )
        })?;

        self.ensure_field_tree_not_referenced(field, "delete", name)?;
        let field_id = field.id;

        self.push_op(PreparedSchemaOp::Delete { field_id })
    }

    fn prepare_rename(&mut self, name: &str, new_name: &str) -> Result<()> {
        if new_name.contains(SCHEMA_NAME_DELIMITER) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot rename column to ambiguous name: {new_name}"),
            ));
        }

        let field = self.current_schema.field_by_name(name).ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot rename missing column: {name}"),
            )
        })?;

        let field_id = field.id;

        if self.current_schema.field_by_name(new_name).is_some() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot rename column, name already exists: {new_name}"),
            ));
        }

        self.push_op(PreparedSchemaOp::Rename {
            field_id,
            new_name: new_name.to_owned(),
        })
    }

    fn prepare_make_optional(&mut self, name: &str) -> Result<()> {
        let field = self.current_schema.field_by_name(name).ok_or_else(|| {
            Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot update missing column: {name}"),
            )
        })?;

        if !field.required {
            return Ok(());
        }

        if self.referenced_fields.is_identifier(field.id) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot make identifier field optional: {name}"),
            ));
        }
        let field_id = field.id;

        self.push_op(PreparedSchemaOp::MakeOptional { field_id })
    }

    fn push_op(&mut self, op: PreparedSchemaOp) -> Result<()> {
        self.current_schema = SchemaRewriter::rewrite_schema(
            &self.current_schema,
            std::slice::from_ref(&op),
        )?;
        self.ops.push(op);
        Ok(())
    }

    fn assign_fresh_ids(&mut self, field: &NestedField) -> NestedFieldRef {
        self.next_field_id += 1;
        let new_id = self.next_field_id;
        let new_type = self.assign_fresh_ids_to_type(&field.field_type);

        Arc::new(NestedField {
            id: new_id,
            name: field.name.clone(),
            required: field.required,
            field_type: Box::new(new_type),
            doc: field.doc.clone(),
            initial_default: field.initial_default.clone(),
            write_default: field.write_default.clone(),
        })
    }

    fn assign_fresh_ids_to_type(&mut self, field_type: &Type) -> Type {
        match field_type {
            Type::Primitive(_) | Type::Variant(_) => field_type.clone(),
            Type::Struct(struct_type) => {
                let new_fields: Vec<NestedFieldRef> = struct_type
                    .fields()
                    .iter()
                    .map(|f| self.assign_fresh_ids(f))
                    .collect();
                Type::Struct(StructType::new(new_fields))
            }
            Type::List(list_type) => {
                let new_element = self.assign_fresh_ids(&list_type.element_field);
                Type::List(ListType {
                    element_field: new_element,
                })
            }
            Type::Map(map_type) => {
                let new_key = self.assign_fresh_ids(&map_type.key_field);
                let new_value = self.assign_fresh_ids(&map_type.value_field);
                Type::Map(MapType {
                    key_field: new_key,
                    value_field: new_value,
                })
            }
        }
    }

    fn resolve_parent_target<'a>(
        &'a self,
        parent: &str,
    ) -> Result<(i32, &'a StructType)> {
        self.current_schema
            .field_by_name(parent)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::PreconditionFailed,
                    format!("Cannot add column: parent '{parent}' not found"),
                )
            })
            .and_then(|parent_field| match parent_field.field_type.as_ref() {
                Type::Struct(s) => Ok((parent_field.id, s)),
                Type::Map(m) => match m.value_field.field_type.as_ref() {
                    Type::Struct(s) => Ok((m.value_field.id, s)),
                    _ => Err(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!(
                            "Cannot add column: map value of '{parent}' is not a struct"
                        ),
                    )),
                },
                Type::List(l) => match l.element_field.field_type.as_ref() {
                    Type::Struct(s) => Ok((l.element_field.id, s)),
                    _ => Err(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!(
                            "Cannot add column: list element of '{parent}' is not a struct"
                        ),
                    )),
                },
                _ => Err(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!(
                        "Cannot add column: parent '{parent}' is not a struct, map, or list"
                    ),
                )),
            })
    }

    fn ensure_field_not_referenced(
        &self,
        field_id: i32,
        operation: &str,
        name: &str,
    ) -> Result<()> {
        if let Some(kind) = self.referenced_fields.reference_kind(field_id) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot {operation} {} field: {name}", kind.label()),
            ));
        }
        Ok(())
    }

    fn ensure_field_tree_not_referenced(
        &self,
        field: &NestedFieldRef,
        operation: &str,
        name: &str,
    ) -> Result<()> {
        let mut field_ids = Vec::new();
        Self::collect_field_ids(field, &mut field_ids);
        for field_id in field_ids {
            self.ensure_field_not_referenced(field_id, operation, name)?;
        }
        Ok(())
    }

    fn collect_field_ids(field: &NestedFieldRef, output: &mut Vec<i32>) {
        output.push(field.id);
        Self::collect_type_field_ids(field.field_type.as_ref(), output);
    }

    fn collect_type_field_ids(field_type: &Type, output: &mut Vec<i32>) {
        match field_type {
            Type::Primitive(_) | Type::Variant(_) => {}
            Type::Struct(struct_type) => {
                for field in struct_type.fields() {
                    Self::collect_field_ids(field, output);
                }
            }
            Type::List(list_type) => {
                Self::collect_field_ids(&list_type.element_field, output);
            }
            Type::Map(map_type) => {
                Self::collect_field_ids(&map_type.key_field, output);
                Self::collect_field_ids(&map_type.value_field, output);
            }
        }
    }
}

impl ReferencedFields {
    fn from_metadata(metadata: &TableMetadata) -> Self {
        let identifier = metadata.current_schema().identifier_field_ids().collect();
        let partition_source = metadata
            .partition_specs_iter()
            .flat_map(|spec| spec.fields().iter().map(|field| field.source_id))
            .collect();
        let sort_order_source = metadata
            .sort_orders_iter()
            .flat_map(|order| order.fields.iter().map(|field| field.source_id))
            .collect();

        Self {
            identifier,
            partition_source,
            sort_order_source,
        }
    }

    fn is_identifier(&self, field_id: i32) -> bool {
        self.identifier.contains(&field_id)
    }

    fn reference_kind(&self, field_id: i32) -> Option<FieldReferenceKind> {
        if self.identifier.contains(&field_id) {
            Some(FieldReferenceKind::Identifier)
        } else if self.partition_source.contains(&field_id) {
            Some(FieldReferenceKind::PartitionSource)
        } else if self.sort_order_source.contains(&field_id) {
            Some(FieldReferenceKind::SortOrderSource)
        } else {
            None
        }
    }
}

struct SchemaRewriter<'a> {
    ops: &'a [PreparedSchemaOp],
}

impl<'a> SchemaRewriter<'a> {
    fn rewrite_schema(
        schema: &Schema,
        ops: &'a [PreparedSchemaOp],
    ) -> Result<Schema> {
        let mut rewriter = Self { ops };
        let fields = rewriter.rewrite_fields(schema.as_struct().fields(), None)?;
        Schema::builder()
            .with_schema_id(schema.schema_id())
            .with_fields(fields)
            .with_identifier_field_ids(schema.identifier_field_ids())
            .build()
    }

    fn rewrite_fields(
        &mut self,
        fields: &[NestedFieldRef],
        parent_id: Option<i32>,
    ) -> Result<Vec<NestedFieldRef>> {
        let mut current = fields.to_vec();
        for op in self.ops {
            current = self.apply_op_to_fields(&current, parent_id, op)?;
        }
        Ok(current)
    }

    fn apply_op_to_fields(
        &self,
        fields: &[NestedFieldRef],
        parent_id: Option<i32>,
        op: &PreparedSchemaOp,
    ) -> Result<Vec<NestedFieldRef>> {
        let mut rewritten = Vec::with_capacity(fields.len());
        let mut found = false;

        for field in fields {
            if matches!(op, PreparedSchemaOp::Delete { field_id } if *field_id == field.id)
            {
                found = true;
                continue;
            }

            let (next, changed) = self.apply_op_to_field(field, op)?;
            found |= changed;
            rewritten.push(next);
        }

        if let PreparedSchemaOp::Add {
            parent_id: target,
            field,
        } = op
            && *target == parent_id
        {
            rewritten.push(Arc::clone(field));
            found = true;
        }

        if !found {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "Cannot apply schema update; target field or parent not found: {op:?}"
                ),
            ));
        }

        Ok(rewritten)
    }

    fn apply_op_to_field(
        &self,
        field: &NestedFieldRef,
        op: &PreparedSchemaOp,
    ) -> Result<(NestedFieldRef, bool)> {
        let (rewritten_type, type_changed) =
            self.apply_op_to_type(field.field_type.as_ref(), Some(field.id), op)?;

        let mut next = if type_changed {
            NestedField {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
                field_type: Box::new(rewritten_type),
                doc: field.doc.clone(),
                initial_default: field.initial_default.clone(),
                write_default: field.write_default.clone(),
            }
        } else {
            field.as_ref().clone()
        };

        let mut changed = type_changed;
        match op {
            PreparedSchemaOp::Rename { field_id, new_name }
                if *field_id == field.id =>
            {
                next.name.clone_from(new_name);
                changed = true;
            }
            PreparedSchemaOp::MakeOptional { field_id } if *field_id == field.id => {
                next.required = false;
                changed = true;
            }
            _ => {}
        }

        Ok((Arc::new(next), changed))
    }

    fn apply_op_to_type(
        &self,
        field_type: &Type,
        parent_id: Option<i32>,
        op: &PreparedSchemaOp,
    ) -> Result<(Type, bool)> {
        match field_type {
            Type::Primitive(_) | Type::Variant(_) => Ok((field_type.clone(), false)),
            Type::Struct(struct_type) => {
                let rewritten =
                    self.apply_op_to_fields(struct_type.fields(), parent_id, op);
                match rewritten {
                    Ok(fields) => Ok((Type::Struct(StructType::new(fields)), true)),
                    Err(err)
                        if matches!(err.kind(), ErrorKind::PreconditionFailed) =>
                    {
                        Ok((field_type.clone(), false))
                    }
                    Err(err) => Err(err),
                }
            }
            Type::List(list_type) => {
                let (element, changed) =
                    self.apply_op_to_field(&list_type.element_field, op)?;
                if changed {
                    Ok((
                        Type::List(ListType {
                            element_field: element,
                        }),
                        true,
                    ))
                } else {
                    Ok((field_type.clone(), false))
                }
            }
            Type::Map(map_type) => {
                let (key, key_changed) =
                    self.apply_op_to_field(&map_type.key_field, op)?;
                let (value, value_changed) =
                    self.apply_op_to_field(&map_type.value_field, op)?;
                if key_changed || value_changed {
                    Ok((
                        Type::Map(MapType {
                            key_field: key,
                            value_field: value,
                        }),
                        true,
                    ))
                } else {
                    Ok((field_type.clone(), false))
                }
            }
        }
    }
}

impl PreparedSchemaUpdate {
    /// Returns true when this prepared update carries no schema operation.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Apply this update to a metadata copy for transaction-local visibility.
    pub fn apply_to_metadata(
        &self,
        metadata: &TableMetadata,
    ) -> Result<TableMetadata> {
        if self.is_empty() {
            return Ok(metadata.clone());
        }

        let mut commit = self.action_commit(metadata)?;
        let updates = commit.take_updates();
        let mut builder = metadata.clone().into_builder(None);
        for update in updates {
            builder = update.apply(builder)?;
        }
        Ok(builder.build()?.metadata)
    }

    /// Validate that this prepared update still targets the metadata base it
    /// was prepared against.
    pub fn validate_base_metadata(&self, metadata: &TableMetadata) -> Result<()> {
        self.check_base_metadata(metadata)
    }

    fn action_commit(&self, metadata: &TableMetadata) -> Result<ActionCommit> {
        if self.is_empty() {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }

        self.check_base_metadata(metadata)?;
        let requirements = self.requirements();

        let schema =
            SchemaRewriter::rewrite_schema(metadata.current_schema(), &self.ops)?;
        if let Some(existing_schema) = metadata
            .schemas_iter()
            .find(|existing| schema.is_same_schema(existing))
        {
            if metadata.current_schema_id() == existing_schema.schema_id() {
                return Ok(ActionCommit::new(Vec::new(), Vec::new()));
            }
            return Ok(ActionCommit::new(
                vec![TableUpdate::SetCurrentSchema {
                    schema_id: existing_schema.schema_id(),
                }],
                requirements,
            ));
        }
        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSchema { schema },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ],
            requirements,
        ))
    }

    fn requirements(&self) -> Vec<TableRequirement> {
        let mut requirements = vec![TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: self.base_schema_id,
        }];
        if self.requires_last_field_id_match {
            requirements.push(TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: self.base_last_column_id,
            });
        }
        requirements
    }

    fn check_base_metadata(&self, metadata: &TableMetadata) -> Result<()> {
        if metadata.current_schema_id() != self.base_schema_id {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "Cannot apply prepared schema update: current schema id changed from {} to {}",
                    self.base_schema_id,
                    metadata.current_schema_id()
                ),
            ));
        }

        if self.requires_last_field_id_match
            && metadata.last_column_id() != self.base_last_column_id
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!(
                    "Cannot apply prepared schema update: last assigned field id changed from {} to {}",
                    self.base_last_column_id,
                    metadata.last_column_id()
                ),
            ));
        }

        Ok(())
    }
}

impl TransactionAction for UpdateSchemaAction {
    fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        self.prepare(table)?.action_commit(table.metadata())
    }
}

impl TransactionAction for PreparedSchemaUpdate {
    fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        self.action_commit(table.metadata())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        NestedField, NullOrder, PartitionSpec, PrimitiveType, Schema, SortDirection,
        SortField, SortOrder, Transform, Type,
    };
    use crate::table::Table;
    use crate::transaction::{AddColumn, ApplyTransactionAction, Transaction};
    use crate::{Catalog, TableCreation, TableIdent};

    fn long_type() -> Type {
        Type::Primitive(PrimitiveType::Long)
    }

    fn string_type() -> Type {
        Type::Primitive(PrimitiveType::String)
    }

    fn make_table() -> Table {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", long_type()).into(),
                NestedField::required(2, "payload", string_type()).into(),
                NestedField::optional(3, "obsolete", long_type()).into(),
            ])
            .build()
            .unwrap();

        make_table_with(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
        )
    }

    fn make_table_with(
        schema: Schema,
        partition_spec: crate::spec::UnboundPartitionSpec,
        sort_order: SortOrder,
    ) -> Table {
        let catalog = new_memory_catalog();
        let table_ident =
            TableIdent::from_strs(["ns", "schema_update_test"]).unwrap();
        catalog
            .create_namespace(table_ident.namespace(), Default::default())
            .unwrap();

        let creation = TableCreation::builder()
            .name("schema_update_test".to_owned())
            .schema(schema)
            .partition_spec(partition_spec)
            .sort_order(sort_order)
            .build();
        catalog
            .create_table(table_ident.namespace(), creation)
            .unwrap()
    }

    fn make_partitioned_table() -> Table {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", long_type()).into(),
                NestedField::required(2, "payload", string_type()).into(),
                NestedField::optional(3, "obsolete", long_type()).into(),
            ])
            .build()
            .unwrap();
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("obsolete", "obsolete_part", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        make_table_with(
            schema,
            partition_spec.into_unbound(),
            SortOrder::unsorted_order(),
        )
    }

    fn make_sorted_table() -> Table {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", long_type()).into(),
                NestedField::required(2, "payload", string_type()).into(),
                NestedField::optional(3, "obsolete", long_type()).into(),
            ])
            .build()
            .unwrap();
        let sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_sort_field(
                SortField::builder()
                    .source_id(2)
                    .transform(Transform::Identity)
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .build(),
            )
            .build(&schema)
            .unwrap();

        make_table_with(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            sort_order,
        )
    }

    fn make_identifier_table() -> Table {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", long_type()).into(),
                NestedField::required(2, "payload", string_type()).into(),
            ])
            .with_identifier_field_ids(vec![1])
            .build()
            .unwrap();

        make_table_with(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
        )
    }

    #[test]
    fn prepares_add_column_with_fresh_field_id() {
        let table = make_table();
        let prepared = Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional("extra", long_type()))
            .prepare(&table)
            .unwrap();

        let metadata = prepared.apply_to_metadata(table.metadata()).unwrap();
        let field = metadata.current_schema().field_by_name("extra").unwrap();
        assert_eq!(field.id, 4);
        assert!(!field.required);
        assert_eq!(metadata.last_column_id(), 4);
    }

    #[test]
    fn prepares_drop_column_without_mutating_historical_schema() {
        let table = make_table();
        let base_schema_id = table.metadata().current_schema_id();
        let prepared = Transaction::new(&table)
            .update_schema()
            .delete_column("obsolete")
            .prepare(&table)
            .unwrap();

        let metadata = prepared.apply_to_metadata(table.metadata()).unwrap();
        assert!(
            metadata
                .current_schema()
                .field_by_name("obsolete")
                .is_none()
        );
        assert!(
            metadata
                .schema_by_id(base_schema_id)
                .unwrap()
                .field_by_name("obsolete")
                .is_some()
        );
    }

    #[test]
    fn drop_after_materialized_add_reuses_existing_schema_and_preserves_id_high_watermark()
     {
        let table = make_table();
        let base_schema_id = table.metadata().current_schema_id();
        let add = Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional("extra", long_type()))
            .prepare(&table)
            .unwrap();
        let metadata_after_add = add.apply_to_metadata(table.metadata()).unwrap();
        let add_schema_id = metadata_after_add.current_schema_id();
        assert_ne!(add_schema_id, base_schema_id);
        assert_eq!(metadata_after_add.last_column_id(), 4);

        let table_after_add =
            table.clone().with_metadata(Arc::new(metadata_after_add));
        let drop = Transaction::new(&table_after_add)
            .update_schema()
            .delete_column("extra")
            .prepare(&table_after_add)
            .unwrap();
        let metadata_after_drop =
            drop.apply_to_metadata(table_after_add.metadata()).unwrap();

        assert_eq!(metadata_after_drop.current_schema_id(), base_schema_id);
        assert_eq!(metadata_after_drop.last_column_id(), 4);
        assert!(
            metadata_after_drop
                .schema_by_id(add_schema_id)
                .unwrap()
                .field_by_name("extra")
                .is_some()
        );
    }

    #[test]
    fn transaction_commits_schema_epoch_that_returns_to_base_schema() {
        let catalog = new_memory_catalog();
        let table_ident =
            TableIdent::from_strs(["ns", "schema_epoch_commit_test"]).unwrap();
        catalog
            .create_namespace(table_ident.namespace(), Default::default())
            .unwrap();
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", long_type()).into(),
                NestedField::required(2, "payload", string_type()).into(),
            ])
            .build()
            .unwrap();
        let creation = TableCreation::builder()
            .name("schema_epoch_commit_test".to_owned())
            .schema(schema)
            .build();
        let table = catalog
            .create_table(table_ident.namespace(), creation)
            .unwrap();
        let base_schema_id = table.metadata().current_schema_id();

        let tx = Transaction::new(&table);
        let tx = tx
            .update_schema()
            .add_column(AddColumn::optional("extra", long_type()))
            .apply(tx)
            .unwrap();
        let tx = tx.update_schema().delete_column("extra").apply(tx).unwrap();
        let updated = tx.commit(&catalog).unwrap();

        assert_eq!(updated.metadata().current_schema_id(), base_schema_id);
        assert_eq!(updated.metadata().last_column_id(), 3);
        assert!(
            updated
                .metadata()
                .current_schema()
                .field_by_name("extra")
                .is_none()
        );
    }

    #[test]
    fn prepares_rename_column_preserving_field_id() {
        let table = make_table();
        let old_id = table
            .metadata()
            .current_schema()
            .field_by_name("payload")
            .unwrap()
            .id;
        let prepared = Transaction::new(&table)
            .update_schema()
            .rename_column("payload", "body")
            .prepare(&table)
            .unwrap();

        let metadata = prepared.apply_to_metadata(table.metadata()).unwrap();
        assert!(metadata.current_schema().field_by_name("payload").is_none());
        assert_eq!(
            metadata.current_schema().field_by_name("body").unwrap().id,
            old_id
        );
    }

    #[test]
    fn prepares_required_to_optional_change() {
        let table = make_table();
        let prepared = Transaction::new(&table)
            .update_schema()
            .make_column_optional("payload")
            .prepare(&table)
            .unwrap();

        let metadata = prepared.apply_to_metadata(table.metadata()).unwrap();
        let field = metadata.current_schema().field_by_name("payload").unwrap();
        assert!(!field.required);
    }

    #[test]
    fn rejects_deleting_partition_source_field() {
        let table = make_partitioned_table();
        let err = Transaction::new(&table)
            .update_schema()
            .delete_column("obsolete")
            .prepare(&table)
            .expect_err("partition source field deletion should be rejected");

        assert!(err.message().contains("partition source field"));
    }

    #[test]
    fn allows_renaming_partition_and_sort_source_fields() {
        let partitioned = make_partitioned_table();
        let partition_source_id = partitioned
            .metadata()
            .current_schema()
            .field_by_name("obsolete")
            .unwrap()
            .id;
        let prepared = Transaction::new(&partitioned)
            .update_schema()
            .rename_column("obsolete", "renamed_obsolete")
            .prepare(&partitioned)
            .unwrap();
        let metadata = prepared.apply_to_metadata(partitioned.metadata()).unwrap();
        assert_eq!(
            metadata
                .current_schema()
                .field_by_name("renamed_obsolete")
                .unwrap()
                .id,
            partition_source_id
        );
        assert!(
            metadata
                .partition_specs_iter()
                .flat_map(|spec| spec.fields().iter().map(|field| field.source_id))
                .any(|source_id| source_id == partition_source_id)
        );

        let table = make_sorted_table();
        let sort_source_id = table
            .metadata()
            .current_schema()
            .field_by_name("payload")
            .unwrap()
            .id;
        let prepared = Transaction::new(&table)
            .update_schema()
            .rename_column("payload", "body")
            .prepare(&table)
            .unwrap();
        let metadata = prepared.apply_to_metadata(table.metadata()).unwrap();
        assert_eq!(
            metadata.current_schema().field_by_name("body").unwrap().id,
            sort_source_id
        );
        assert!(
            metadata
                .sort_orders_iter()
                .flat_map(|order| order.fields.iter().map(|field| field.source_id))
                .any(|source_id| source_id == sort_source_id)
        );
    }

    #[test]
    fn rejects_identifier_field_changes() {
        let table = make_identifier_table();

        let delete_err = Transaction::new(&table)
            .update_schema()
            .delete_column("id")
            .prepare(&table)
            .expect_err("identifier field deletion should be rejected");
        assert!(delete_err.message().contains("identifier field"));

        let id_field_id = table
            .metadata()
            .current_schema()
            .field_by_name("id")
            .unwrap()
            .id;
        let rename = Transaction::new(&table)
            .update_schema()
            .rename_column("id", "new_id")
            .prepare(&table)
            .unwrap();
        let metadata = rename.apply_to_metadata(table.metadata()).unwrap();
        assert_eq!(
            metadata
                .current_schema()
                .field_by_name("new_id")
                .unwrap()
                .id,
            id_field_id
        );
        assert!(
            metadata
                .current_schema()
                .identifier_field_ids()
                .any(|id| id == id_field_id)
        );

        let optional_err = Transaction::new(&table)
            .update_schema()
            .make_column_optional("id")
            .prepare(&table)
            .expect_err("identifier field optionality change should be rejected");
        assert!(optional_err.message().contains("identifier field"));
    }

    #[test]
    fn prepared_update_rejects_schema_changed_base() {
        let table = make_table();
        let prepared = Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional("extra", long_type()))
            .prepare(&table)
            .unwrap();
        let concurrent = Transaction::new(&table)
            .update_schema()
            .rename_column("payload", "body")
            .prepare(&table)
            .unwrap();
        let changed_metadata =
            concurrent.apply_to_metadata(table.metadata()).unwrap();

        let err = prepared
            .validate_base_metadata(&changed_metadata)
            .expect_err("prepared update must reject changed schema base");

        assert!(err.message().contains(
            "Cannot apply prepared schema update: current schema id changed"
        ));
    }
}
