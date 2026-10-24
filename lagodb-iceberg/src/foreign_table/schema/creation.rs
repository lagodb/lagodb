//! Strict PostgreSQL-to-Iceberg schema construction for new catalog tables.

use std::sync::Arc;

use iceberg_lite::spec::{
    ListType, NestedField, NestedFieldRef, SCHEMA_NAME_DELIMITER, Schema, Type,
};
use lagodb_core::datum::PgType;
use lagodb_core::handles::RelationHandle;

use super::super::error::IcebergFdwError;
use super::super::type_mapping::{CreatedForeignType, PostgresForeignType};
use crate::error::IcebergError;

pub(in crate::foreign_table) struct ForeignTableCreation {
    next_field_id: i32,
    fields: Vec<NestedFieldRef>,
}

impl ForeignTableCreation {
    pub(in crate::foreign_table) fn from_relation(
        relation: &RelationHandle<'_>,
    ) -> Result<Schema, IcebergFdwError> {
        let mut creation = Self::new();
        for column in relation.live_columns() {
            let name =
                column
                    .name()
                    .to_str()
                    .map_err(|_| IcebergFdwError::InvalidUtf8 {
                        subject: "PostgreSQL column name",
                    })?;
            creation.add(
                name,
                PgType::new(column.type_oid(), column.type_mod()),
                column.is_not_null(),
            )?;
        }
        creation.build()
    }

    fn new() -> Self {
        Self {
            next_field_id: 1,
            fields: Vec::new(),
        }
    }

    fn add(
        &mut self,
        name: &str,
        pg_type: PgType,
        required: bool,
    ) -> Result<(), IcebergFdwError> {
        if name.contains(SCHEMA_NAME_DELIMITER) {
            return Err(IcebergError::SchemaBuildError(format!(
                "Cannot add column with ambiguous name: {name}"
            ))
            .into());
        }
        let field_id = self.allocate_field_id();
        let field_type = self.convert_type(name, pg_type)?;
        let field = if required {
            NestedField::required(field_id, name, field_type)
        } else {
            NestedField::optional(field_id, name, field_type)
        };
        self.fields.push(Arc::new(field));
        Ok(())
    }

    fn build(self) -> Result<Schema, IcebergFdwError> {
        Schema::builder()
            .with_fields(self.fields)
            .build()
            .map_err(IcebergFdwError::from)
    }

    fn allocate_field_id(&mut self) -> i32 {
        let id = self.next_field_id;
        self.next_field_id += 1;
        id
    }

    fn convert_type(
        &mut self,
        column: &str,
        pg_type: PgType,
    ) -> Result<Type, IcebergFdwError> {
        match PostgresForeignType::new(pg_type).to_iceberg(column)? {
            CreatedForeignType::Primitive(primitive) => {
                Ok(Type::Primitive(primitive))
            }
            CreatedForeignType::List(element_type) => {
                let element = NestedField::list_element(
                    self.allocate_field_id(),
                    Type::Primitive(element_type),
                    false,
                );
                Ok(Type::List(ListType::new(Arc::new(element))))
            }
        }
    }
}
