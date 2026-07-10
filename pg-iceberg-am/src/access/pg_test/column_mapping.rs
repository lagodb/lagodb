//! Backend tests for [`super::column_mapping::ColumnMapping`].
//!
//! The assertions cover position arithmetic, but constructing a mapping calls
//! PostgreSQL type-resolution functions. The tests must therefore execute
//! inside PostgreSQL and be compiled only with the `pg_test` feature.

#[pgrx::pg_schema]
mod tests {
    use std::sync::Arc;

    use iceberg_lite::spec::{
        NestedField, PrimitiveType, Schema as IcebergSchema, Type,
    };
    use pgrx::pg_sys;

    use crate::access::column_mapping::{
        ColumnMapping, LiveColumn, RelationFieldMap, RelationShape,
    };
    use crate::access::projection::{ProjectedField, Projection};
    use crate::error::IcebergError;

    fn int_schema(names: &[&str]) -> IcebergSchema {
        let fields: Vec<_> = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                Arc::new(NestedField::required(
                    i32::try_from(index + 1).expect("test schema field id overflow"),
                    *name,
                    Type::Primitive(PrimitiveType::Int),
                ))
            })
            .collect();
        IcebergSchema::builder()
            .with_fields(fields)
            .build()
            .expect("failed to build test Iceberg schema")
    }

    fn live_columns(columns: &[(i16, &str)]) -> Vec<LiveColumn> {
        columns
            .iter()
            .map(|(attno, name)| LiveColumn::new(*attno, (*name).to_owned()))
            .collect()
    }

    fn int_attribute_types(count: usize) -> Vec<(pg_sys::Oid, i32)> {
        vec![(pg_sys::INT4OID, -1); count]
    }

    fn shape(columns: &[(i16, &str)], slot_width: usize) -> RelationShape {
        RelationShape::for_test(
            live_columns(columns),
            slot_width,
            int_attribute_types(slot_width),
        )
    }

    #[pgrx::pg_test(schema = "tests")]
    fn from_field_map_without_dropped_columns_is_identity() {
        let schema = int_schema(&["a", "b", "c"]);
        let field_map = RelationFieldMap::from_shape(
            &schema,
            &shape(&[(1, "a"), (2, "b"), (3, "c")], 3),
        )
        .unwrap();
        let plan = ColumnMapping::from_field_map(&schema, &field_map).unwrap();

        assert_eq!(plan.entries.len(), 3);
        for (index, entry) in plan.entries.iter().enumerate() {
            assert_eq!(entry.dest, index);
            assert_eq!(entry.src_col, index);
        }
    }

    #[pgrx::pg_test(schema = "tests")]
    fn from_field_map_with_dropped_column_leaves_gap() {
        let schema = int_schema(&["a", "b", "d"]);
        let field_map = RelationFieldMap::from_shape(
            &schema,
            &shape(&[(1, "a"), (2, "b"), (4, "d")], 4),
        )
        .unwrap();
        let plan = ColumnMapping::from_field_map(&schema, &field_map).unwrap();

        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.dest)
                .collect::<Vec<_>>(),
            vec![0, 1, 3]
        );
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.src_col)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[pgrx::pg_test(schema = "tests")]
    fn from_field_map_resolves_wider_iceberg_schema_by_field_id() {
        let schema = int_schema(&["a", "b", "c"]);
        let field_map =
            RelationFieldMap::from_shape(&schema, &shape(&[(1, "a"), (3, "c")], 3))
                .unwrap();
        let plan = ColumnMapping::from_field_map(&schema, &field_map).unwrap();

        assert_eq!(plan.entries.len(), 2);
        assert_eq!((plan.entries[0].src_col, plan.entries[0].dest), (0, 0));
        assert_eq!((plan.entries[1].src_col, plan.entries[1].dest), (1, 2));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn from_field_map_rejects_unresolved_name() {
        let schema = int_schema(&["a", "b"]);
        let result =
            RelationFieldMap::from_shape(&schema, &shape(&[(1, "a"), (2, "z")], 2));

        assert!(matches!(result, Err(IcebergError::ColumnNotFound(_))));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_decouples_source_order_from_destination() {
        let schema = int_schema(&["a", "b", "c", "d", "e"]);
        let full_map = RelationFieldMap::from_shape(
            &schema,
            &shape(&[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], 5),
        )
        .unwrap();
        let projection = Projection::new(vec![
            ProjectedField::new(2, 1),
            ProjectedField::new(5, 0),
        ]);
        let field_map = full_map
            .project(&projection, 2, &int_attribute_types(2))
            .unwrap();
        let plan = ColumnMapping::from_field_map(&schema, &field_map).unwrap();

        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.dest)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.src_col)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_with_dropped_column_uses_attribute_position() {
        let schema = int_schema(&["a", "b", "e"]);
        let full_map = RelationFieldMap::from_shape(
            &schema,
            &shape(&[(1, "a"), (2, "b"), (4, "e")], 4),
        )
        .unwrap();
        let projection = Projection::new(vec![
            ProjectedField::new(2, 0),
            ProjectedField::new(4, 1),
        ]);
        let field_map = full_map
            .project(&projection, 2, &int_attribute_types(2))
            .unwrap();
        let plan = ColumnMapping::from_field_map(&schema, &field_map).unwrap();

        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.dest)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_rejects_unresolved_name() {
        let schema = int_schema(&["a", "b"]);
        let full_map =
            RelationFieldMap::from_shape(&schema, &shape(&[(2, "b")], 2)).unwrap();
        let projection = Projection::new(vec![ProjectedField::new(1, 0)]);
        let result = full_map.project(&projection, 2, &int_attribute_types(2));

        assert!(matches!(result, Err(IcebergError::ColumnNotFound(_))));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_rejects_attribute_number_below_one() {
        let schema = int_schema(&["a", "b"]);
        let full_map =
            RelationFieldMap::from_shape(&schema, &shape(&[(1, "a")], 2)).unwrap();
        let projection = Projection::new(vec![ProjectedField::new(0, 0)]);
        let result = full_map.project(&projection, 2, &int_attribute_types(2));

        assert!(matches!(result, Err(IcebergError::InvariantViolated(_))));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_rejects_destination_out_of_range() {
        let schema = int_schema(&["a", "b"]);
        let full_map =
            RelationFieldMap::from_shape(&schema, &shape(&[(2, "b")], 2)).unwrap();
        let projection = Projection::new(vec![ProjectedField::new(2, 5)]);
        let result = full_map.project(&projection, 2, &int_attribute_types(2));

        assert!(matches!(result, Err(IcebergError::InvariantViolated(_))));
    }
}
