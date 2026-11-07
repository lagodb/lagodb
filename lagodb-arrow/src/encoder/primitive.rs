//! Primitive Arrow builders: `bool` plus the fixed-width integer, float,
//! `date` and `time` columns.
//!
//! The bound `DatumEncoder` variant decodes the present PostgreSQL datum with
//! the matching `DatumGetX` representation and supplies the Arrow native
//! value, so this builder carries no source-type dispatch. `bool` keeps a
//! dedicated encoder because `BooleanBuilder` is not an
//! [`arrow_array::builder::PrimitiveBuilder`].

use std::mem::size_of;
use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, BooleanBuilder, PrimitiveBuilder};
use arrow_array::{ArrayRef, ArrowPrimitiveType};

use super::ColumnBuilder;
use crate::error::ArrowConversionResult;

/// A primitive Arrow column builder.
pub(crate) struct PrimitiveEncoder<T: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
}

impl<T: ArrowPrimitiveType> PrimitiveEncoder<T> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity),
        }
    }

    /// Append a present value, returning its estimated payload size.
    #[inline]
    pub(crate) fn append_value(&mut self, value: T::Native) -> usize {
        self.builder.append_value(value);
        size_of::<T::Native>()
    }
}

impl<T: ArrowPrimitiveType> ColumnBuilder for PrimitiveEncoder<T> {
    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}

pub(crate) struct BoolEncoder {
    builder: BooleanBuilder,
}

impl BoolEncoder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: BooleanBuilder::with_capacity(capacity),
        }
    }

    /// Append a present value, returning its estimated payload size.
    #[inline]
    pub(crate) fn append_value(&mut self, value: bool) -> usize {
        self.builder.append_value(value);
        size_of::<bool>()
    }
}

impl ColumnBuilder for BoolEncoder {
    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}
