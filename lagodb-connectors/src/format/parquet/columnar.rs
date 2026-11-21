//! Arrow projection of Parquet files onto a foreign table's declared schema.
//!
//! Query offload exposes one canonical Arrow schema per relation: the type
//! each declared PostgreSQL column is written as, with domains rejected
//! (`ParquetTypeMapping::declared_read_field`). The declared read contract
//! admits several physical encodings of each type, so a stream binds, from
//! its first file, the conversion of each projected root to its canonical
//! type: zone labels and nested field names, nullability or metadata are
//! rewritten without copying buffers, nanosecond timestamps floor to
//! microseconds as the datum read path does, and other string/binary
//! encodings are cast.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampMicrosecondType, TimestampNanosecondType};
use arrow_array::{ArrayRef, ListArray, RecordBatch, StructArray};
use arrow_cast::cast;
use arrow_schema::{
    ArrowError, DataType, FieldRef, Fields, Schema, SchemaRef, TimeUnit,
};
use lagodb_core::datum::PgType;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::source::ParquetBatchBinding;
use super::type_mapping::{ParquetReadMode, ParquetTypeMapping};

/// One declared column of a columnar projection.
pub(crate) struct DeclaredColumn<'a> {
    pub(crate) name: &'a str,
    pub(crate) pg_type: PgType,
}

/// Canonical Arrow projection bound from one file's schema.
pub(crate) struct ParquetColumnarPlan {
    roots: Box<[usize]>,
    /// Per output column: its position among the projected roots and the
    /// conversion to its canonical type.
    columns: Box<[(usize, ColumnConversion)]>,
    schema: SchemaRef,
}

impl ParquetColumnarPlan {
    /// The canonical Arrow schema of declared columns, or `None` when a
    /// declaration has no columnar representation.
    pub(crate) fn declared_schema<'a>(
        columns: impl IntoIterator<Item = DeclaredColumn<'a>>,
    ) -> Result<Option<SchemaRef>, ConnectorError> {
        let mut fields = Vec::new();
        for column in columns {
            let Some(field) =
                ParquetTypeMapping::declared_read_field(column.name, column.pg_type)?
            else {
                return Ok(None);
            };
            fields.push(field);
        }
        Ok(Some(Arc::new(Schema::new(fields))))
    }

    /// Bind `schema`, a projection of declared columns in output order whose
    /// fields come from [`Self::declared_schema`], to one file's schema.
    pub(crate) fn bind(
        file: &Schema,
        schema: &SchemaRef,
        targets: &[PgType],
    ) -> Result<Self, ConnectorError> {
        let mut pending = Vec::with_capacity(targets.len());
        for (field, target) in schema.fields().iter().zip(targets) {
            let source = file.index_of(field.name()).map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    format!(
                        "column {:?} is missing from the Parquet schema",
                        field.name()
                    ),
                )
            })?;
            let source_field = file.field(source);
            ParquetTypeMapping::check_read_field(
                source_field,
                *target,
                ParquetReadMode::Declared,
            )?;
            let conversion =
                ColumnConversion::plan(source_field.data_type(), field.data_type())
                    .ok_or_else(|| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        format!(
                            "column {:?}: Parquet type {} has no conversion to {}",
                            field.name(),
                            source_field.data_type(),
                            field.data_type(),
                        ),
                    )
                })?;
            pending.push((source, conversion));
        }
        let mut roots = pending
            .iter()
            .map(|(source, _)| *source)
            .collect::<Vec<_>>();
        roots.sort_unstable();
        roots.dedup();
        let columns = pending
            .into_iter()
            .map(|(source, conversion)| {
                let position = roots
                    .binary_search(&source)
                    .expect("projected Parquet source was retained");
                (position, conversion)
            })
            .collect();
        Ok(Self {
            roots: roots.into_boxed_slice(),
            columns,
            schema: Arc::clone(schema),
        })
    }
}

impl ParquetBatchBinding for ParquetColumnarPlan {
    type Batch = RecordBatch;

    fn roots(&self) -> &[usize] {
        &self.roots
    }

    unsafe fn bind_batch(
        &self,
        batch: RecordBatch,
    ) -> Result<RecordBatch, ConnectorError> {
        let rows = batch.num_rows();
        let columns = self
            .columns
            .iter()
            .map(|(position, conversion)| {
                conversion.convert(Arc::clone(batch.column(*position)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // SAFETY: every conversion yields exactly its output field's canonical
        // type (`ColumnConversion::plan`) and preserves the row count.
        Ok(unsafe {
            RecordBatch::new_unchecked(Arc::clone(&self.schema), columns, rows)
        })
    }
}

/// Conversion of one file column to its canonical type.
enum ColumnConversion {
    Identity,
    /// A zone-aware microsecond timestamp under the canonical zone label;
    /// the stored UTC instants are unchanged.
    Retimezone(Option<Arc<str>>),
    /// Floor to microseconds, as the datum read path does. Zone presence
    /// already matches the canonical type.
    NanosToMicros(Option<Arc<str>>),
    /// `LargeUtf8` to `Utf8`, or `Binary`/`FixedSizeBinary` to `LargeBinary`.
    Cast(DataType),
    List {
        element: Box<Self>,
        field: FieldRef,
    },
    Struct {
        children: Box<[Self]>,
        fields: Fields,
    },
}

impl ColumnConversion {
    fn plan(file: &DataType, canonical: &DataType) -> Option<Self> {
        if file == canonical {
            return Some(Self::Identity);
        }
        Some(match (file, canonical) {
            // Adding or dropping a zone would reinterpret the values; read
            // admission pairs zone-aware timestamps with timestamptz only.
            (
                DataType::Timestamp(TimeUnit::Microsecond, file_zone),
                DataType::Timestamp(TimeUnit::Microsecond, timezone),
            ) if file_zone.is_some() == timezone.is_some() => {
                Self::Retimezone(timezone.clone())
            }
            (
                DataType::Timestamp(TimeUnit::Nanosecond, file_zone),
                DataType::Timestamp(TimeUnit::Microsecond, timezone),
            ) if file_zone.is_some() == timezone.is_some() => {
                Self::NanosToMicros(timezone.clone())
            }
            (DataType::LargeUtf8, DataType::Utf8)
            | (
                DataType::Binary | DataType::FixedSizeBinary(_),
                DataType::LargeBinary,
            ) => Self::Cast(canonical.clone()),
            (DataType::List(file), DataType::List(field)) => Self::List {
                element: Box::new(Self::plan(file.data_type(), field.data_type())?),
                field: Arc::clone(field),
            },
            (DataType::Struct(file), DataType::Struct(fields))
                if file.len() == fields.len() =>
            {
                Self::Struct {
                    children: file
                        .iter()
                        .zip(fields.iter())
                        .map(|(file, field)| {
                            Self::plan(file.data_type(), field.data_type())
                        })
                        .collect::<Option<_>>()?,
                    fields: fields.clone(),
                }
            }
            _ => return None,
        })
    }

    fn convert(&self, array: ArrayRef) -> Result<ArrayRef, ArrowError> {
        Ok(match self {
            Self::Identity => array,
            Self::Retimezone(timezone) => Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .clone()
                    .with_timezone_opt(timezone.clone()),
            ),
            Self::NanosToMicros(timezone) => Arc::new(
                array
                    .as_primitive::<TimestampNanosecondType>()
                    .unary::<_, TimestampMicrosecondType>(|nanos| {
                        nanos.div_euclid(1_000)
                    })
                    .with_timezone_opt(timezone.clone()),
            ),
            Self::Cast(data_type) => cast(&array, data_type)?,
            Self::List { element, field } => {
                let (_, offsets, values, nulls) =
                    array.as_list::<i32>().clone().into_parts();
                Arc::new(ListArray::try_new(
                    Arc::clone(field),
                    offsets,
                    element.convert(values)?,
                    nulls,
                )?)
            }
            Self::Struct { children, fields } => {
                let (_, arrays, nulls) = array.as_struct().clone().into_parts();
                let arrays = children
                    .iter()
                    .zip(arrays)
                    .map(|(child, array)| child.convert(array))
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(StructArray::try_new(fields.clone(), arrays, nulls)?)
            }
        })
    }
}
