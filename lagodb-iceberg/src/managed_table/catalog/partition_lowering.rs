//! PostgreSQL `PARTITION BY` to Iceberg partition-spec lowering.
//!
//! Each key entry becomes one partition field, in clause order, so a key with
//! several entries is one composite spec rather than a relation hierarchy. A
//! function entry carries its own transform under every strategy, which makes
//! `HASH (date_trunc('day', ts))` a day partition and not a hash of the day.
//! The strategy word only interprets a bare column:
//!
//! | Entry                    | `LIST`      | `RANGE`          | `HASH`                   |
//! |--------------------------|-------------|------------------|--------------------------|
//! | column                   | `identity`  | `day` for `date` | `bucket[32]`, sole entry |
//! | `date_trunc(...)`        | temporal    | temporal         | temporal                 |
//! | `iceberg.bucket(N, col)` | `bucket[N]` | `bucket[N]`      | `bucket[N]`              |
//!
//! Iceberg rejects redundant or same-named fields in its spec builder.

use std::ffi::CStr;

use iceberg_lite::Error as IcebergLiteError;
use iceberg_lite::spec::{PartitionSpec, Schema, Transform, UnboundPartitionSpec};
use lagodb_core::catalog::get_namespace_oid;
use lagodb_core::datum::PgType;
use lagodb_core::diag::PgReportError;
use lagodb_core::expr::pg::{PgConst, PgExprRef, PgFuncExpr, PgVar};
use lagodb_core::handles::{
    PartitionKey, PartitionKeyField, PartitionKeySource, PartitionStrategy,
    RelationHandle,
};
use pgrx::AnyElement;
use pgrx::prelude::*;

use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::constants::ICEBERG_SCHEMA;

use super::numeric_type::ManagedNumeric;

/// Bucket count of a bare `PARTITION BY HASH (column)`, whose syntax cannot name
/// one. `iceberg.bucket(N, column)` spells any other count.
const DEFAULT_HASH_BUCKETS: u32 = 32;

/// Identity of the `iceberg.bucket` SQL marker.
///
/// The OID is resolved on each use for the reason `IcebergAccessMethod::oid`
/// gives: a backend outlives `DROP EXTENSION`.
struct IcebergBucketFunction;

impl IcebergBucketFunction {
    const NAME: &'static CStr = c"bucket";
    const ARGUMENT_TYPES: [pg_sys::Oid; 2] = [pg_sys::INT4OID, pg_sys::ANYELEMENTOID];

    fn oid() -> IcebergResult<pg_sys::Oid> {
        let schema = get_namespace_oid(ICEBERG_SCHEMA, false)?;
        // SAFETY: buildoidvector copies the live array, and these keys are
        // PROCNAMEARGSNSP's (proname, proargtypes, pronamespace).
        unsafe {
            let arguments = pg_sys::buildoidvector(
                Self::ARGUMENT_TYPES.as_ptr(),
                Self::ARGUMENT_TYPES.len() as i32,
            );
            let oid = pg_sys::GetSysCacheOid(
                pg_sys::SysCacheIdentifier::PROCNAMEARGSNSP as i32,
                pg_sys::Anum_pg_proc_oid as pg_sys::AttrNumber,
                pg_sys::Datum::from(Self::NAME.as_ptr()),
                pg_sys::Datum::from(arguments as usize),
                pg_sys::Datum::from(schema),
                pg_sys::Datum::from(0),
            );
            pg_sys::pfree(arguments.cast());
            Ok(oid)
        }
    }

    /// The bucket-count and source arguments when `function` is the marker.
    fn operands<'a>(
        function: PgFuncExpr<'a>,
    ) -> IcebergResult<Option<(PgExprRef<'a>, PgExprRef<'a>)>> {
        Ok(if function.function_oid() == Self::oid()? {
            function.argument(0).zip(function.argument(1))
        } else {
            None
        })
    }
}

/// The table column a partition field reads. An expression entry's own type is
/// the transform's result, so the column's type comes from the column.
#[derive(Clone, Copy)]
struct PartitionSource {
    attno: pg_sys::AttrNumber,
    type_oid: pg_sys::Oid,
    typmod: i32,
    collation: pg_sys::Oid,
}

impl PartitionSource {
    fn from_column(attno: pg_sys::AttrNumber, field: &PartitionKeyField) -> Self {
        Self {
            attno,
            type_oid: field.type_oid(),
            typmod: field.typmod(),
            collation: field.collation(),
        }
    }

    fn from_argument(argument: PgExprRef<'_>) -> IcebergResult<Self> {
        let var = PgVar::try_from_expr(argument)
            .filter(|var| var.varattno() > 0)
            .ok_or_else(|| {
                IcebergError::unsupported_partition(
                    "a partition transform source must be a direct table column",
                )
            })?;
        Ok(Self {
            attno: var.varattno(),
            type_oid: var.vartype(),
            typmod: var.vartypmod(),
            collation: var.varcollid(),
        })
    }

    fn validate_identity(&self) -> IcebergResult<()> {
        // UUID identity partitions are deliberately excluded until upstream
        // iceberg-rust and iceberg-lite agree on Iceberg's Avro fixed(16) UUID
        // representation for both manifest writing and reading. Their current
        // UUID schema is string-based while partition literals serialize as
        // bytes, so accepting this definition would defer failure until write.
        self.validate_source("identity", pg_sys::BOOLOID)
    }

    fn validate_bucket(&self) -> IcebergResult<()> {
        // Iceberg defines no bucket hash for boolean or floating-point values.
        // A bucket partition value is an int, which avoids the UUID literal
        // problem described in `validate_identity`. The bucket count never
        // decides support, so messages omit the spec's `bucket[N]` spelling.
        self.validate_source("bucket", pg_sys::UUIDOID)
    }

    /// Check the types both transforms share plus `transform_only_type`. A
    /// date_trunc overload fixes its own source type, so it needs no check.
    fn validate_source(
        &self,
        name: &str,
        transform_only_type: pg_sys::Oid,
    ) -> IcebergResult<()> {
        let supported = match self.type_oid {
            pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::DATEOID
            | pg_sys::TIMEOID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID
            | pg_sys::TEXTOID
            | pg_sys::VARCHAROID
            | pg_sys::BYTEAOID => true,
            pg_sys::NUMERICOID => !ManagedNumeric::new(self.typmod).uses_double(),
            type_oid => type_oid == transform_only_type,
        };
        if !supported {
            let pg_type = PgType::new(self.type_oid, self.typmod).display_name();
            return Err(IcebergError::unsupported_partition(format!(
                "PostgreSQL type {pg_type} is not supported for an Iceberg {name} partition"
            )));
        }
        if matches!(self.type_oid, pg_sys::TEXTOID | pg_sys::VARCHAROID)
            && self.collation != pg_sys::Oid::INVALID
            && !unsafe { pg_sys::get_collation_isdeterministic(self.collation) }
        {
            return Err(IcebergError::unsupported_partition(format!(
                "text and varchar {name} partitions require a deterministic collation"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct LoweredPartitionField {
    source: PartitionSource,
    transform: Transform,
}

impl LoweredPartitionField {
    fn field_name(&self, source_name: &str) -> String {
        match self.transform {
            Transform::Identity => source_name.to_owned(),
            Transform::Bucket(count) => format!("{source_name}_bucket_{count}"),
            temporal => format!("{source_name}_{temporal}"),
        }
    }
}

pub(super) struct PgPartitionSpecAdapter<'borrow, 'relation> {
    relation: &'borrow RelationHandle<'relation>,
    schema: &'borrow Schema,
    key: &'borrow PartitionKey,
}

impl<'borrow, 'relation> PgPartitionSpecAdapter<'borrow, 'relation> {
    pub(super) fn new(
        relation: &'borrow RelationHandle<'relation>,
        schema: &'borrow Schema,
        key: &'borrow PartitionKey,
    ) -> Self {
        Self {
            relation,
            schema,
            key,
        }
    }

    pub(super) fn build(self) -> IcebergResult<UnboundPartitionSpec> {
        let attributes = self.relation.tuple_desc_handle().attributes();
        let mut builder = PartitionSpec::builder(self.schema.clone());
        for field in self.key.fields() {
            let lowered = self.lower_field(field)?;
            let source_name = Self::source_name(attributes, lowered.source.attno)?;
            builder = builder
                .add_partition_field(
                    &source_name,
                    lowered.field_name(&source_name),
                    lowered.transform,
                )
                .map_err(Self::invalid_spec)?;
        }
        builder
            .build()
            .map(PartitionSpec::into_unbound)
            .map_err(Self::invalid_spec)
    }

    /// A rejected spec is a bad definition, not the corrupted data `DataInvalid`
    /// means on `IcebergLiteError`. The spec builder's errors carry only a message.
    fn invalid_spec(error: IcebergLiteError) -> IcebergError {
        IcebergError::invalid_partition(error.message().to_owned())
    }

    /// PostgreSQL rejects generated columns for its own partitioned tables.
    /// Lowering leaves one direct column for both the name and expression forms.
    fn source_name(
        attributes: &[pg_sys::FormData_pg_attribute],
        attno: pg_sys::AttrNumber,
    ) -> IcebergResult<String> {
        let attribute = &attributes[(attno - 1) as usize];
        if attribute.attgenerated != 0 {
            return Err(IcebergError::invalid_partition(
                "cannot use generated column in partition key",
            ));
        }
        // SAFETY: `attname.data` is a NUL-terminated `NameData` array owned by
        // the descriptor; valid for the borrow of `attributes`.
        let name = unsafe { CStr::from_ptr(attribute.attname.data.as_ptr()) };
        Ok(name.to_string_lossy().into_owned())
    }

    fn lower_field(
        &self,
        field: &PartitionKeyField,
    ) -> IcebergResult<LoweredPartitionField> {
        match field.source() {
            PartitionKeySource::Column(attno) => {
                self.lower_column(PartitionSource::from_column(attno, field))
            }
            PartitionKeySource::Expression(expression) => {
                Self::lower_expression(expression)
            }
        }
    }

    fn lower_column(
        &self,
        source: PartitionSource,
    ) -> IcebergResult<LoweredPartitionField> {
        let transform = match self.key.strategy() {
            PartitionStrategy::List => {
                source.validate_identity()?;
                Transform::Identity
            }
            PartitionStrategy::Range => {
                if source.type_oid != pg_sys::DATEOID {
                    return Err(IcebergError::unsupported_partition(
                        "a direct PARTITION BY RANGE key is supported only for date; timestamp keys must use date_trunc('year'|'month'|'day'|'hour', column)",
                    ));
                }
                Transform::Day
            }
            PartitionStrategy::Hash => {
                // PostgreSQL combines several HASH columns into one value, and
                // Iceberg's bucket transform reads a single column.
                if self.key.fields().len() != 1 {
                    return Err(IcebergError::unsupported_partition(
                        "PARTITION BY HASH accepts a bare column only as its sole key; write iceberg.bucket(N, column) for each column",
                    ));
                }
                source.validate_bucket()?;
                Transform::Bucket(DEFAULT_HASH_BUCKETS)
            }
        };
        Ok(LoweredPartitionField { source, transform })
    }

    fn lower_expression(
        expression: PgExprRef<'_>,
    ) -> IcebergResult<LoweredPartitionField> {
        let function = PgFuncExpr::try_from_expr(expression.without_relabels())
            .ok_or_else(|| {
                IcebergError::unsupported_partition(
                    "a partition key expression must be an iceberg.bucket or built-in date_trunc call",
                )
            })?;
        match IcebergBucketFunction::operands(function)? {
            Some((count, value)) => Self::lower_bucket(count, value),
            None => Self::lower_date_trunc(function),
        }
    }

    fn lower_bucket(
        count: PgExprRef<'_>,
        value: PgExprRef<'_>,
    ) -> IcebergResult<LoweredPartitionField> {
        let count = PgConst::try_from_expr(count.without_relabels())
            .and_then(PgConst::int4_value)
            .and_then(|count| u32::try_from(count).ok())
            .filter(|count| *count > 0)
            .ok_or_else(|| {
                IcebergError::invalid_partition(
                    "iceberg.bucket requires a positive integer constant bucket count",
                )
            })?;
        let source = PartitionSource::from_argument(value)?;
        source.validate_bucket()?;
        Ok(LoweredPartitionField {
            source,
            transform: Transform::Bucket(count),
        })
    }

    fn lower_date_trunc(
        function: PgFuncExpr<'_>,
    ) -> IcebergResult<LoweredPartitionField> {
        let has_timezone = match u32::from(function.function_oid()) {
            pg_sys::F_DATE_TRUNC_TEXT_TIMESTAMP => false,
            pg_sys::F_DATE_TRUNC_TEXT_TIMESTAMPTZ_TEXT => true,
            pg_sys::F_DATE_TRUNC_TEXT_TIMESTAMPTZ => {
                return Err(IcebergError::unsupported_partition(
                    "date_trunc(text, timestamptz) is STABLE and timezone-dependent; use date_trunc(unit, column, 'UTC')",
                ));
            }
            _ => {
                return Err(IcebergError::unsupported_partition(
                    "a partition key expression must be an iceberg.bucket or built-in date_trunc call",
                ));
            }
        };

        let unit = function
            .argument(0)
            .and_then(|argument| PgConst::try_from_expr(argument.without_relabels()))
            .and_then(PgConst::text_bytes)
            .ok_or_else(|| {
                IcebergError::unsupported_partition(
                    "date_trunc partition unit must be a text constant",
                )
            })?;
        let transform = Self::temporal_transform(&unit)?;

        let value = function.argument(1).ok_or_else(|| {
            IcebergError::invalid_partition(
                "date_trunc partition expression has no source column",
            )
        })?;
        let source = PartitionSource::from_argument(value)?;

        if has_timezone {
            let timezone = function
                .argument(2)
                .and_then(|argument| {
                    PgConst::try_from_expr(argument.without_relabels())
                })
                .and_then(PgConst::text_bytes)
                .ok_or_else(|| {
                    IcebergError::unsupported_partition(
                        "timestamptz partition timezone must be the text constant 'UTC'",
                    )
                })?;
            if !timezone.eq_ignore_ascii_case(b"UTC") {
                return Err(IcebergError::unsupported_partition(
                    "Iceberg timestamptz partition transforms require the explicit timezone 'UTC'",
                ));
            }
        }

        Ok(LoweredPartitionField { source, transform })
    }

    fn temporal_transform(unit: &[u8]) -> IcebergResult<Transform> {
        if unit.eq_ignore_ascii_case(b"year") {
            Ok(Transform::Year)
        } else if unit.eq_ignore_ascii_case(b"month") {
            Ok(Transform::Month)
        } else if unit.eq_ignore_ascii_case(b"day") {
            Ok(Transform::Day)
        } else if unit.eq_ignore_ascii_case(b"hour") {
            Ok(Transform::Hour)
        } else {
            Err(IcebergError::unsupported_partition(
                "date_trunc partition unit must be year, month, day, or hour",
            ))
        }
    }
}

#[pg_schema]
mod iceberg {
    use super::*;

    /// Names a bucket transform in `PARTITION BY`. Lowering recognizes the call
    /// without running it. VOLATILE makes PostgreSQL reject it in a native
    /// partition key at CREATE; nullable arguments keep the function non-STRICT
    /// so a NULL can neither skip this error nor fold the call away.
    #[pg_extern(volatile)]
    fn bucket(num_buckets: Option<i32>, value: Option<AnyElement>) -> i32 {
        let _ = (num_buckets, value);
        PgReportError::from_domain_error(IcebergError::unsupported_partition(
            "iceberg.bucket only names a transform in PARTITION BY of a USING iceberg table and cannot be executed",
        ))
        .report()
    }
}
