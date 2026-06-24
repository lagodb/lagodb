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

//! INT96 timestamp coercion for Parquet files.

use std::sync::Arc;

use arrow_schema::{
    DataType, Field, FieldRef, Fields, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::schema::{
    ArrowSchemaVisitor, DEFAULT_MAP_FIELD_NAME, visit_schema,
};
use crate::error::Result;
use crate::spec::{PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind};

/// Coerce Arrow schema types for INT96 columns to match the Iceberg table schema.
///
/// Arrow defaults INT96 to nanoseconds, which can overflow for dates outside
/// the i64 nanosecond range. Schema hints let the Parquet reader decode INT96
/// as microseconds for Iceberg `timestamp`/`timestamptz` fields, while keeping
/// nanoseconds for `timestamp_ns`/`timestamptz_ns`.
pub(crate) fn coerce_int96_timestamps(
    arrow_schema: &ArrowSchemaRef,
    iceberg_schema: &Schema,
) -> Option<Arc<ArrowSchema>> {
    let mut visitor = Int96CoercionVisitor::new(iceberg_schema);
    let coerced = visit_schema(arrow_schema, &mut visitor).ok()?;
    visitor.changed.then(|| Arc::new(coerced))
}

struct Int96CoercionVisitor<'a> {
    iceberg_schema: &'a Schema,
    field_stack: Vec<Field>,
    changed: bool,
}

impl<'a> Int96CoercionVisitor<'a> {
    fn new(iceberg_schema: &'a Schema) -> Self {
        Self {
            iceberg_schema,
            field_stack: Vec::new(),
            changed: false,
        }
    }

    fn target_unit(&self, field: &Field) -> Option<TimeUnit> {
        if !matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        ) {
            return None;
        }

        let target = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id_str| id_str.parse::<i32>().ok())
            .and_then(|field_id| self.iceberg_schema.field_by_id(field_id))
            .and_then(|field| match &*field.field_type {
                Type::Primitive(
                    PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
                ) => Some(TimeUnit::Microsecond),
                Type::Primitive(
                    PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
                ) => Some(TimeUnit::Nanosecond),
                _ => None,
            })
            .unwrap_or(TimeUnit::Microsecond);

        (target != TimeUnit::Nanosecond).then_some(target)
    }

    fn current_field(&self, context: &str) -> Result<&Field> {
        self.field_stack
            .last()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, context))
    }
}

impl ArrowSchemaVisitor for Int96CoercionVisitor<'_> {
    type T = Field;
    type U = ArrowSchema;

    fn before_field(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.as_ref().clone());
        Ok(())
    }

    fn after_field(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.as_ref().clone());
        Ok(())
    }

    fn after_list_element(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.as_ref().clone());
        Ok(())
    }

    fn after_map_key(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.as_ref().clone());
        Ok(())
    }

    fn after_map_value(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn schema(
        &mut self,
        schema: &ArrowSchema,
        values: Vec<Field>,
    ) -> Result<ArrowSchema> {
        Ok(ArrowSchema::new_with_metadata(
            values,
            schema.metadata().clone(),
        ))
    }

    fn r#struct(&mut self, _fields: &Fields, results: Vec<Field>) -> Result<Field> {
        let field_info = self.current_field("Field stack underflow in struct")?;
        Ok(Field::new(
            field_info.name(),
            DataType::Struct(Fields::from(results)),
            field_info.is_nullable(),
        )
        .with_metadata(field_info.metadata().clone()))
    }

    fn list(&mut self, list: &DataType, value: Field) -> Result<Field> {
        let field_info = self.current_field("Field stack underflow in list")?;
        let list_type = match list {
            DataType::List(_) => DataType::List(Arc::new(value)),
            DataType::LargeList(_) => DataType::LargeList(Arc::new(value)),
            DataType::FixedSizeList(_, size) => {
                DataType::FixedSizeList(Arc::new(value), *size)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Expected list type, got {list}"),
                ));
            }
        };
        Ok(
            Field::new(field_info.name(), list_type, field_info.is_nullable())
                .with_metadata(field_info.metadata().clone()),
        )
    }

    fn map(
        &mut self,
        map: &DataType,
        key_value: Field,
        value: Field,
    ) -> Result<Field> {
        let field_info = self.current_field("Field stack underflow in map")?;
        let sorted = match map {
            DataType::Map(_, sorted) => *sorted,
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Expected map type, got {map}"),
                ));
            }
        };
        let struct_field = Field::new(
            DEFAULT_MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![key_value, value])),
            false,
        );
        Ok(Field::new(
            field_info.name(),
            DataType::Map(Arc::new(struct_field), sorted),
            field_info.is_nullable(),
        )
        .with_metadata(field_info.metadata().clone()))
    }

    fn primitive(&mut self, data_type: &DataType) -> Result<Field> {
        let field_info = self
            .current_field("Field stack underflow in primitive")?
            .clone();

        if let Some(target_unit) = self.target_unit(&field_info) {
            let timezone = match field_info.data_type() {
                DataType::Timestamp(_, timezone) => timezone.clone(),
                _ => None,
            };
            self.changed = true;
            Ok(Field::new(
                field_info.name(),
                DataType::Timestamp(target_unit, timezone),
                field_info.is_nullable(),
            )
            .with_metadata(field_info.metadata().clone()))
        } else {
            Ok(Field::new(
                field_info.name(),
                data_type.clone(),
                field_info.is_nullable(),
            )
            .with_metadata(field_info.metadata().clone()))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::spec::{NestedField, PrimitiveType, Type};

    fn field_id_metadata(field_id: i32) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string())])
    }

    #[test]
    fn test_coerce_int96_timestamp_to_microseconds() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_metadata(1)),
        ]));
        let iceberg_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamp),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let coerced =
            coerce_int96_timestamps(&arrow_schema, &iceberg_schema).unwrap();

        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_keep_int96_timestamp_ns_as_nanoseconds() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_metadata(1)),
        ]));
        let iceberg_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "ts",
                    Type::Primitive(PrimitiveType::TimestampNs),
                )
                .into(),
            ])
            .build()
            .unwrap();

        assert!(coerce_int96_timestamps(&arrow_schema, &iceberg_schema).is_none());
    }
}
