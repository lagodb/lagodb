//! Name-based Parquet projection and statement-owned Datum decoding.

use std::ffi::CStr;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use lagodb_arrow::{ArrowColumnDecoder, BoundBatch, DecodedColumn};
use lagodb_core::tuple::PgType;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::type_mapping::{ParquetReadMode, ParquetTypeMapping};

pub(crate) struct ParquetReadPlan {
    roots: Box<[usize]>,
    decoder: ArrowColumnDecoder,
}

impl ParquetReadPlan {
    /// Destinations come from the consumer's bound scan, sample or COPY layout.
    /// Copy mode requires the consumer to apply `CopyDatumCoercion` to each
    /// decoded column; composite member typmods must be satisfied by the schema.
    /// Declared mode instead requires file producers to preserve the foreign
    /// table's string lengths, temporal precision and valid, NUL-free JSON text,
    /// including recursive leaves. COPY validates JSON during decoding.
    pub(crate) fn bind<'a>(
        schema: &Schema,
        columns: impl IntoIterator<Item = (&'a CStr, PgType, usize)>,
        mode: ParquetReadMode,
    ) -> Result<Self, ConnectorError> {
        let mut pending = Vec::new();
        for (name, target, destination) in columns {
            let name = name.to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "PostgreSQL column names must be valid UTF-8 for Parquet",
                )
            })?;
            let source = schema.index_of(name).map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    format!("column {name:?} is missing from the Parquet schema"),
                )
            })?;
            let plan = ParquetTypeMapping::bind(schema.field(source), target, mode)?;
            pending.push((source, destination, plan));
        }
        let mut roots = pending
            .iter()
            .map(|(source, _, _)| *source)
            .collect::<Vec<_>>();
        roots.sort_unstable();
        roots.dedup();
        let columns = pending
            .into_iter()
            .map(|(source, destination, plan)| {
                let source = roots
                    .binary_search(&source)
                    .expect("projected Parquet source was retained");
                // SAFETY: destinations and declared types come from the consumer's
                // layout. Parquet admits no arbitrary TextIo types. Declared
                // json/jsonb text is producer-guaranteed valid and NUL-free;
                // COPY uses the validating codec, including recursive leaves.
                // String/temporal typmods follow the bound mode's producer
                // contract or COPY assignment, not a per-value decoder check.
                unsafe { DecodedColumn::bind(plan, source, destination) }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            roots: roots.into_boxed_slice(),
            decoder: ArrowColumnDecoder::new(columns),
        })
    }

    pub(super) fn roots(&self) -> &[usize] {
        &self.roots
    }

    /// # Safety
    /// The batch comes from this plan's projection of the bound file schema.
    pub(super) unsafe fn bind_batch(
        &self,
        batch: RecordBatch,
    ) -> Result<BoundBatch, ConnectorError> {
        Ok(unsafe { self.decoder.bind_datums_unchecked(batch) }?)
    }
}
