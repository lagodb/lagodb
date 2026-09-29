//! Backend tests for relation-bound read column plans.
//!
//! The assertions cover position arithmetic, but binding a column plan calls
//! PostgreSQL type-resolution functions. The tests must therefore execute
//! inside PostgreSQL and be compiled only with the `pg_test` feature.

#[pgrx::pg_schema]
mod tests {
    use std::sync::Arc;

    use iceberg_lite::spec::{
        NestedField, PrimitiveType, Schema as IcebergSchema, Type,
    };
    use pgrx::pg_sys;

    use crate::error::IcebergError;
    use crate::schema::column_plan::ReadColumnPlan;
    use crate::schema::projection::{ProjectedAttribute, SlotProjection};
    use crate::schema::relation::{LiveColumn, RelationFieldMap, RelationLayout};

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
            .map(|(attno, name)| LiveColumn::new(*attno, (*name).to_owned(), true))
            .collect()
    }

    fn int_attribute_types(count: usize) -> Vec<(pg_sys::Oid, i32)> {
        vec![(pg_sys::INT4OID, -1); count]
    }

    fn layout(columns: &[(i16, &str)], slot_width: usize) -> RelationLayout {
        RelationLayout::for_test(
            live_columns(columns),
            slot_width,
            int_attribute_types(slot_width),
        )
    }

    #[pgrx::pg_test(schema = "tests")]
    fn bind_without_dropped_columns_is_identity() {
        let schema = int_schema(&["a", "b", "c"]);
        let field_map = RelationFieldMap::bind(
            &schema,
            &layout(&[(1, "a"), (2, "b"), (3, "c")], 3),
        )
        .unwrap();
        let plan =
            ReadColumnPlan::bind(&schema, &field_map, 3, &int_attribute_types(3))
                .unwrap();

        assert_eq!(plan.columns.len(), 3);
        for (index, entry) in plan.columns.iter().enumerate() {
            assert_eq!(entry.destination, index);
            assert_eq!(entry.source_column, index);
        }
    }

    #[pgrx::pg_test(schema = "tests")]
    fn bind_with_dropped_column_leaves_gap() {
        let schema = int_schema(&["a", "b", "d"]);
        let field_map = RelationFieldMap::bind(
            &schema,
            &layout(&[(1, "a"), (2, "b"), (4, "d")], 4),
        )
        .unwrap();
        let plan =
            ReadColumnPlan::bind(&schema, &field_map, 4, &int_attribute_types(4))
                .unwrap();
        let field_index = field_map.into_index();

        assert_eq!(
            plan.columns
                .iter()
                .map(|entry| entry.destination)
                .collect::<Vec<_>>(),
            vec![0, 1, 3]
        );
        assert_eq!(
            plan.columns
                .iter()
                .map(|entry| entry.source_column)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );

        assert_eq!(field_index.binding_for_attno(1).unwrap().field_id, 1);
        assert_eq!(field_index.binding_for_attno(2).unwrap().field_id, 2);
        assert!(field_index.binding_for_attno(3).is_none());
        assert_eq!(field_index.binding_for_attno(4).unwrap().field_id, 3);
    }

    #[pgrx::pg_test(schema = "tests")]
    fn bind_resolves_wider_iceberg_schema_by_field_id() {
        let schema = int_schema(&["a", "b", "c"]);
        let field_map =
            RelationFieldMap::bind(&schema, &layout(&[(1, "a"), (3, "c")], 3))
                .unwrap();
        let plan =
            ReadColumnPlan::bind(&schema, &field_map, 3, &int_attribute_types(3))
                .unwrap();

        assert_eq!(plan.columns.len(), 2);
        assert_eq!(
            (plan.columns[0].source_column, plan.columns[0].destination),
            (0, 0)
        );
        assert_eq!(
            (plan.columns[1].source_column, plan.columns[1].destination),
            (1, 2)
        );
    }

    #[pgrx::pg_test(schema = "tests")]
    fn binding_rejects_unresolved_name() {
        let schema = int_schema(&["a", "b"]);
        let result =
            RelationFieldMap::bind(&schema, &layout(&[(1, "a"), (2, "z")], 2));

        assert!(matches!(result, Err(IcebergError::ColumnNotFound(_))));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_decouples_source_order_from_destination() {
        let schema = int_schema(&["a", "b", "c", "d", "e"]);
        let full_map = RelationFieldMap::bind(
            &schema,
            &layout(&[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], 5),
        )
        .unwrap();
        let projection = SlotProjection::from_outputs(vec![
            ProjectedAttribute::new(2, 1),
            ProjectedAttribute::new(5, 0),
        ]);
        let field_map = full_map
            .project(
                projection
                    .columns()
                    .iter()
                    .map(|field| (field.attno, field.destination)),
            )
            .unwrap();
        let plan =
            ReadColumnPlan::bind(&schema, &field_map, 2, &int_attribute_types(2))
                .unwrap();
        let field_index = field_map.into_index();

        assert!(field_index.binding_for_attno(1).is_none());
        assert_eq!(field_index.binding_for_attno(2).unwrap().field_id, 2);
        assert_eq!(field_index.binding_for_attno(5).unwrap().field_id, 5);

        assert_eq!(
            plan.columns
                .iter()
                .map(|entry| entry.destination)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );
        assert_eq!(
            plan.columns
                .iter()
                .map(|entry| entry.source_column)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_with_dropped_column_uses_attribute_position() {
        let schema = int_schema(&["a", "b", "e"]);
        let full_map = RelationFieldMap::bind(
            &schema,
            &layout(&[(1, "a"), (2, "b"), (4, "e")], 4),
        )
        .unwrap();
        let projection = SlotProjection::from_outputs(vec![
            ProjectedAttribute::new(2, 0),
            ProjectedAttribute::new(4, 1),
        ]);
        let field_map = full_map
            .project(
                projection
                    .columns()
                    .iter()
                    .map(|field| (field.attno, field.destination)),
            )
            .unwrap();
        let plan =
            ReadColumnPlan::bind(&schema, &field_map, 2, &int_attribute_types(2))
                .unwrap();

        assert_eq!(
            plan.columns
                .iter()
                .map(|entry| entry.destination)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_rejects_unmapped_attribute() {
        let schema = int_schema(&["a", "b"]);
        let full_map =
            RelationFieldMap::bind(&schema, &layout(&[(2, "b")], 2)).unwrap();
        let projection =
            SlotProjection::from_outputs(vec![ProjectedAttribute::new(1, 0)]);
        let result = full_map.project(
            projection
                .columns()
                .iter()
                .map(|field| (field.attno, field.destination)),
        );

        assert!(matches!(result, Err(IcebergError::ColumnNotFound(_))));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_rejects_attribute_number_below_one() {
        let schema = int_schema(&["a", "b"]);
        let full_map =
            RelationFieldMap::bind(&schema, &layout(&[(1, "a")], 2)).unwrap();
        let projection =
            SlotProjection::from_outputs(vec![ProjectedAttribute::new(0, 0)]);
        let result = full_map.project(
            projection
                .columns()
                .iter()
                .map(|field| (field.attno, field.destination)),
        );

        assert!(matches!(result, Err(IcebergError::InvariantViolated(_))));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn projection_rejects_destination_out_of_range() {
        let schema = int_schema(&["a", "b"]);
        let full_map =
            RelationFieldMap::bind(&schema, &layout(&[(2, "b")], 2)).unwrap();
        let projection =
            SlotProjection::from_outputs(vec![ProjectedAttribute::new(2, 5)]);
        let result = full_map
            .project(
                projection
                    .columns()
                    .iter()
                    .map(|field| (field.attno, field.destination)),
            )
            .and_then(|field_map| {
                ReadColumnPlan::bind(&schema, &field_map, 2, &int_attribute_types(2))
            });

        assert!(matches!(result, Err(IcebergError::InvariantViolated(_))));
    }
}
