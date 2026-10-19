//! Read admission before binding Arrow's representation codecs.

use arrow_schema::{DataType, Field};
use lagodb_arrow::{BoundDatumPlan, PgColumnType};
use lagodb_core::datum::{PgType, numeric_typmod};
use pgrx::pg_sys;

use super::ParquetTypeMapping;
use crate::error::ConnectorError;
use crate::format::FormatKind;

/// A binding-time contract, never retained by a row reader.
#[derive(Clone, Copy)]
pub(crate) enum ParquetReadMode {
    /// Scans and ANALYZE restore values using the foreign table's established
    /// PG column types, whether explicitly supplied or inferred from Parquet.
    /// Inference maps strings to text; a json/jsonb target is explicitly declared.
    ///
    /// This mode intentionally adopts a producer contract: files must preserve
    /// declared string lengths, temporal precision and valid, NUL-free json/jsonb
    /// text, including recursive members. Neither a declaration nor schema
    /// admission validates existing file contents. Supplying violating data is
    /// outside this contract; reads do not promise to detect it. Do not add
    /// per-value validation solely because the file is external. COPY has a
    /// separate checked-input contract below.
    Declared,
    /// COPY validates JSON input and applies a top-level scalar/array typmod
    /// coercion after decoding.
    Copy,
    /// COPY cannot assign composite members; their typmods require schema proof.
    CopyMember,
}

impl ParquetTypeMapping {
    pub(in crate::format::parquet) fn bind(
        field: &Field,
        target: PgType,
        mode: ParquetReadMode,
    ) -> Result<BoundDatumPlan, ConnectorError> {
        Self::check_read_field(field, target, mode)?;
        let plan = match mode {
            ParquetReadMode::Declared => {
                // SAFETY: the foreign-table contract requires producers to
                // supply valid, NUL-free json/jsonb text, including array and
                // composite leaves. This guarantee is a prerequisite of the
                // Declared mode documented above, not a fact established by
                // check_read_field or by an Arrow string type. Keep validation
                // for COPY input; foreign scans intentionally trust producers.
                unsafe {
                    BoundDatumPlan::bind_with_prevalidated_json_text(field, target)
                }
            }
            ParquetReadMode::Copy | ParquetReadMode::CopyMember => {
                BoundDatumPlan::bind(field, target)
            }
        }?;
        Ok(plan)
    }

    fn check_read_field(
        field: &Field,
        target: PgType,
        mode: ParquetReadMode,
    ) -> Result<(), ConnectorError> {
        // Inspect the declaration before resolving its representation: neither
        // Arrow decoding nor COPY's typmod coercion checks domain constraints.
        if target.oid != target.base().oid {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                format!(
                    "column {:?}: domain read targets are not supported",
                    field.name(),
                ),
            ));
        }
        Self::check_array_type(target)?;
        if let Some(element) = target.element() {
            let DataType::List(child) = field.data_type() else {
                return Err(Self::incompatible(field, target));
            };
            // Arrays retain the enclosing contract. COPY's top-level array
            // coercion handles scalar elements, not members of composite elements.
            return Self::check_read_field(child, element, mode);
        }
        if target.is_composite() {
            let DataType::Struct(children) = field.data_type() else {
                return Err(Self::incompatible(field, target));
            };
            let attributes = Self::composite_fields(target)?;
            if children.len() != attributes.len() {
                return Err(Self::incompatible(field, target));
            }
            let member_mode = match mode {
                ParquetReadMode::Declared => ParquetReadMode::Declared,
                ParquetReadMode::Copy | ParquetReadMode::CopyMember => {
                    ParquetReadMode::CopyMember
                }
            };
            for (child, attribute) in children.iter().zip(attributes) {
                if child.name() != &attribute.name {
                    return Err(Self::incompatible(field, target));
                }
                Self::check_read_field(child, attribute.pg_type, member_mode)?;
            }
            return Ok(());
        }
        // Map, direct nested List and container-to-JSONB have no representation
        // in Parquet's declared-type contract.
        if field.data_type().is_nested() {
            return Err(Self::incompatible(field, target));
        }
        Self::canonical(field)?;
        if let Some(primitive) = Self::primitive_storage_type(target.oid)
            && field.data_type() != &primitive
        {
            return Err(Self::incompatible(field, target));
        }
        if PgColumnType::from_pg_type(target.oid).is_none() {
            return Err(Self::incompatible(field, target));
        }
        // Declared scans restore producer-constrained strings and timestamps;
        // COPY members cannot inherit that foreign-table contract. Decimal
        // precision/scale remains an explicit storage-schema requirement.
        let requires_schema_typmod = match mode {
            ParquetReadMode::Declared => !matches!(
                target.oid,
                pg_sys::VARCHAROID
                    | pg_sys::BPCHAROID
                    | pg_sys::TIMEOID
                    | pg_sys::TIMESTAMPOID
                    | pg_sys::TIMESTAMPTZOID
            ),
            ParquetReadMode::Copy => false,
            ParquetReadMode::CopyMember => true,
        };
        if requires_schema_typmod
            && target.typmod >= 0
            && !(target.oid == pg_sys::NUMERICOID
                && target.typmod == Self::decoded_typmod(field))
        {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                format!(
                    "column {:?}: declared typmod requires assignment not provided by Parquet decoding",
                    field.name(),
                ),
            ));
        }
        Ok(())
    }

    /// Typmod enforced by the decoder, before COPY's target assignment.
    pub(in crate::format::parquet) fn decoded_typmod(field: &Field) -> i32 {
        match field.data_type() {
            DataType::Decimal128(precision, scale) => {
                numeric_typmod(u32::from(*precision), i32::from(*scale))
            }
            DataType::List(element) => Self::decoded_typmod(element),
            _ => -1,
        }
    }
}
