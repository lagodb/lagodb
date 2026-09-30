//! PostgreSQL catalog definition to Iceberg table-definition lowering.

use iceberg_lite::spec::{PartitionSpec, Schema, Transform, UnboundPartitionSpec};
use lagodb_core::expr::pg::{PgConst, PgFuncExpr, PgVar};
use lagodb_core::handles::{
    PartitionKeyField, PartitionKeyHandle, PartitionStrategy, RelationHandle,
};
use lagodb_core::tuple::numeric_precision_scale;
use pgrx::{PgBuiltInOids, pg_sys};

use crate::error::{IcebergError, IcebergResult};

use super::schema_builder::tuple_desc_to_schema;

/// Fully validated metadata needed to create one managed Iceberg table.
pub(crate) struct ManagedTableDefinition {
    schema: Schema,
    partition_spec: UnboundPartitionSpec,
}

impl ManagedTableDefinition {
    pub(crate) fn build(rel: &RelationHandle<'_>) -> IcebergResult<Self> {
        let schema = tuple_desc_to_schema(rel)?;
        let partition_spec =
            if rel.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE {
                PgPartitionSpecAdapter::new(rel, &schema)?.build()?
            } else {
                UnboundPartitionSpec::default()
            };
        Ok(Self {
            schema,
            partition_spec,
        })
    }

    pub(crate) fn into_parts(self) -> (Schema, UnboundPartitionSpec) {
        (self.schema, self.partition_spec)
    }
}

struct PgPartitionSpecAdapter<'borrow, 'relation> {
    relation: &'borrow RelationHandle<'relation>,
    schema: &'borrow Schema,
    key: PartitionKeyHandle<'borrow>,
}

#[derive(Clone, Copy)]
struct LoweredPartitionField {
    source_attno: pg_sys::AttrNumber,
    transform: Transform,
    suffix: Option<&'static str>,
}

impl<'borrow, 'relation> PgPartitionSpecAdapter<'borrow, 'relation> {
    fn new(
        relation: &'borrow RelationHandle<'relation>,
        schema: &'borrow Schema,
    ) -> IcebergResult<Self> {
        let key = PartitionKeyHandle::for_relation(relation).ok_or(
            IcebergError::InvalidPartitionDefinition(
                "partitioned relation has no analyzed PostgreSQL partition key"
                    .to_owned(),
            ),
        )?;
        Ok(Self {
            relation,
            schema,
            key,
        })
    }

    fn build(self) -> IcebergResult<UnboundPartitionSpec> {
        if self.key.len() != 1 {
            return Err(IcebergError::UnsupportedPartitionDefinition(
                "managed Iceberg tables currently require exactly one PostgreSQL partition key"
                    .to_owned(),
            ));
        }

        let key_field =
            self.key
                .field(0)
                .ok_or(IcebergError::InvalidPartitionDefinition(
                    "PostgreSQL returned an empty partition key".to_owned(),
                ))?;
        let lowered = match self.key.strategy() {
            PartitionStrategy::List => self.lower_list(key_field)?,
            PartitionStrategy::Range => self.lower_range(key_field)?,
            PartitionStrategy::Hash => {
                return Err(IcebergError::UnsupportedPartitionDefinition(
                    "PARTITION BY HASH cannot define an Iceberg bucket transform because PostgreSQL's parent-table syntax has no bucket count"
                        .to_owned(),
                ));
            }
        };

        let source_name = self.column_name(lowered.source_attno)?;
        let partition_name = match lowered.suffix {
            Some(suffix) => format!("{source_name}_{suffix}"),
            None => source_name.clone(),
        };
        PartitionSpec::builder(self.schema.clone())
            .add_partition_field(&source_name, partition_name, lowered.transform)
            .and_then(|builder| builder.build())
            .map(PartitionSpec::into_unbound)
            .map_err(|error| {
                IcebergError::InvalidPartitionDefinition(error.to_string())
            })
    }

    fn lower_list(
        &self,
        field: PartitionKeyField<'borrow>,
    ) -> IcebergResult<LoweredPartitionField> {
        let attno = field.attno().ok_or_else(|| {
            IcebergError::UnsupportedPartitionDefinition(
                "PARTITION BY LIST expressions cannot be lowered to an Iceberg identity transform"
                    .to_owned(),
            )
        })?;
        Self::validate_identity_type(field)?;
        Ok(LoweredPartitionField {
            source_attno: attno,
            transform: Transform::Identity,
            suffix: None,
        })
    }

    fn lower_range(
        &self,
        field: PartitionKeyField<'borrow>,
    ) -> IcebergResult<LoweredPartitionField> {
        if let Some(attno) = field.attno() {
            if field.type_oid() != PgBuiltInOids::DATEOID.value() {
                return Err(IcebergError::UnsupportedPartitionDefinition(
                    "a direct PARTITION BY RANGE key is supported only for date; timestamp keys must use date_trunc('year'|'month'|'day'|'hour', column)"
                        .to_owned(),
                ));
            }
            return Ok(LoweredPartitionField {
                source_attno: attno,
                transform: Transform::Day,
                suffix: Some("day"),
            });
        }

        let expression = field.expression().ok_or_else(|| {
            IcebergError::InvalidPartitionDefinition(
                "expression partition key is missing its analyzed expression"
                    .to_owned(),
            )
        })?;
        let expression = expression.without_relabels();
        let function = PgFuncExpr::try_from_expr(expression).ok_or_else(|| {
            IcebergError::UnsupportedPartitionDefinition(
                "PARTITION BY RANGE expressions must be a supported built-in date_trunc call"
                    .to_owned(),
            )
        })?;
        self.lower_date_trunc(function)
    }

    fn lower_date_trunc(
        &self,
        function: PgFuncExpr<'borrow>,
    ) -> IcebergResult<LoweredPartitionField> {
        let function_oid = u32::from(function.function_oid());
        let (expected_type, has_timezone) = match function_oid {
            pg_sys::F_DATE_TRUNC_TEXT_TIMESTAMP => {
                (PgBuiltInOids::TIMESTAMPOID.value(), false)
            }
            pg_sys::F_DATE_TRUNC_TEXT_TIMESTAMPTZ_TEXT => {
                (PgBuiltInOids::TIMESTAMPTZOID.value(), true)
            }
            pg_sys::F_DATE_TRUNC_TEXT_TIMESTAMPTZ => {
                return Err(IcebergError::UnsupportedPartitionDefinition(
                    "date_trunc(text, timestamptz) is STABLE and timezone-dependent; use date_trunc(unit, column, 'UTC')"
                        .to_owned(),
                ));
            }
            _ => {
                return Err(IcebergError::UnsupportedPartitionDefinition(
                    "only PostgreSQL built-in date_trunc can define an Iceberg temporal partition transform"
                        .to_owned(),
                ));
            }
        };

        let expected_arity = if has_timezone { 3 } else { 2 };
        if function.arity() != expected_arity {
            return Err(IcebergError::InvalidPartitionDefinition(
                "built-in date_trunc has an unexpected argument count".to_owned(),
            ));
        }

        let unit = function
            .argument(0)
            .and_then(|argument| PgConst::try_from_expr(argument.without_relabels()))
            .and_then(PgConst::text_bytes)
            .ok_or_else(|| {
                IcebergError::UnsupportedPartitionDefinition(
                    "date_trunc partition unit must be a text constant".to_owned(),
                )
            })?;
        let (transform, suffix) = Self::temporal_transform(&unit)?;

        let value = function.argument(1).ok_or_else(|| {
            IcebergError::InvalidPartitionDefinition(
                "date_trunc partition expression has no source column".to_owned(),
            )
        })?;
        let var = PgVar::try_from_expr(value).ok_or_else(|| {
            IcebergError::UnsupportedPartitionDefinition(
                "date_trunc partition source must be a direct table column"
                    .to_owned(),
            )
        })?;
        if var.varlevelsup() != 0
            || var.varattno() <= 0
            || var.vartype() != expected_type
        {
            return Err(IcebergError::InvalidPartitionDefinition(
                "date_trunc partition source does not match its built-in overload"
                    .to_owned(),
            ));
        }

        if has_timezone {
            let timezone = function
                .argument(2)
                .and_then(|argument| {
                    PgConst::try_from_expr(argument.without_relabels())
                })
                .and_then(PgConst::text_bytes)
                .ok_or_else(|| {
                    IcebergError::UnsupportedPartitionDefinition(
                        "timestamptz partition timezone must be the text constant 'UTC'"
                            .to_owned(),
                    )
                })?;
            if !timezone.eq_ignore_ascii_case(b"UTC") {
                return Err(IcebergError::UnsupportedPartitionDefinition(
                    "Iceberg timestamptz partition transforms require the explicit timezone 'UTC'"
                        .to_owned(),
                ));
            }
        }

        Ok(LoweredPartitionField {
            source_attno: var.varattno(),
            transform,
            suffix: Some(suffix),
        })
    }

    fn temporal_transform(unit: &[u8]) -> IcebergResult<(Transform, &'static str)> {
        if unit.eq_ignore_ascii_case(b"year") {
            Ok((Transform::Year, "year"))
        } else if unit.eq_ignore_ascii_case(b"month") {
            Ok((Transform::Month, "month"))
        } else if unit.eq_ignore_ascii_case(b"day") {
            Ok((Transform::Day, "day"))
        } else if unit.eq_ignore_ascii_case(b"hour") {
            Ok((Transform::Hour, "hour"))
        } else {
            Err(IcebergError::UnsupportedPartitionDefinition(
                "date_trunc partition unit must be year, month, day, or hour"
                    .to_owned(),
            ))
        }
    }

    fn validate_identity_type(field: PartitionKeyField<'_>) -> IcebergResult<()> {
        let type_oid = field.type_oid();
        // UUID identity partitions are deliberately excluded until upstream
        // iceberg-rust and iceberg-lite agree on Iceberg's Avro fixed(16) UUID
        // representation for both manifest writing and reading. Their current
        // UUID schema is string-based while partition literals serialize as
        // bytes, so accepting this definition would defer failure until write.
        let supported = matches!(
            type_oid,
            pg_sys::BOOLOID
                | pg_sys::INT2OID
                | pg_sys::INT4OID
                | pg_sys::INT8OID
                | pg_sys::DATEOID
                | pg_sys::TIMEOID
                | pg_sys::TIMESTAMPOID
                | pg_sys::TIMESTAMPTZOID
                | pg_sys::TEXTOID
                | pg_sys::VARCHAROID
                | pg_sys::BYTEAOID
        ) || (type_oid == pg_sys::NUMERICOID
            && numeric_precision_scale(field.typmod()).is_some());

        if !supported {
            return Err(IcebergError::UnsupportedPartitionDefinition(format!(
                "PostgreSQL type OID {} is not supported for an Iceberg identity partition",
                u32::from(type_oid)
            )));
        }
        if matches!(type_oid, pg_sys::TEXTOID | pg_sys::VARCHAROID)
            && field.collation() != pg_sys::Oid::INVALID
            && !unsafe { pg_sys::get_collation_isdeterministic(field.collation()) }
        {
            return Err(IcebergError::UnsupportedPartitionDefinition(
                "text and varchar identity partitions require a deterministic collation"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn column_name(&self, attno: pg_sys::AttrNumber) -> IcebergResult<String> {
        self.relation
            .live_columns()
            .iter()
            .find(|column| column.attno() == attno)
            .map(|column| column.name().to_string_lossy().into_owned())
            .ok_or_else(|| {
                IcebergError::InvalidPartitionDefinition(format!(
                    "partition key attribute {attno} is not a live table column"
                ))
            })
    }
}
