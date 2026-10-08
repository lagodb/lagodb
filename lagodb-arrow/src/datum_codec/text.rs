//! Portable text storage through cached PostgreSQL type I/O.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, LargeStringBuilder, StringBuilder};
use arrow_schema::DataType;
use lagodb_core::tuple::{PgTextIo, PgType};
use pgrx::pg_sys;

/// Stores a datum's type-output text in an Arrow string array.
pub(super) struct TextIoEncoder {
    io: PgTextIo,
    builder: TextBuilder,
}

enum TextBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
}

impl TextIoEncoder {
    pub(super) fn new(ty: PgType, data_type: &DataType, capacity: usize) -> Self {
        let builder = match data_type {
            DataType::LargeUtf8 => TextBuilder::LargeUtf8(
                LargeStringBuilder::with_capacity(capacity, capacity * 16),
            ),
            _ => TextBuilder::Utf8(StringBuilder::with_capacity(
                capacity,
                capacity * 16,
            )),
        };
        Self {
            io: PgTextIo::bind(ty),
            builder,
        }
    }

    pub(super) unsafe fn append(&mut self, datum: pg_sys::Datum) -> usize {
        unsafe {
            self.io.with_output(datum, |text| {
                match &mut self.builder {
                    TextBuilder::Utf8(builder) => builder.append_value(text),
                    TextBuilder::LargeUtf8(builder) => builder.append_value(text),
                }
                text.len()
            })
        }
    }

    pub(super) fn append_null(&mut self) {
        match &mut self.builder {
            TextBuilder::Utf8(builder) => builder.append_null(),
            TextBuilder::LargeUtf8(builder) => builder.append_null(),
        }
    }

    pub(super) fn finish(&mut self) -> ArrayRef {
        match &mut self.builder {
            TextBuilder::Utf8(builder) => Arc::new(builder.finish()),
            TextBuilder::LargeUtf8(builder) => Arc::new(builder.finish()),
        }
    }

    pub(super) fn len(&self) -> usize {
        match &self.builder {
            TextBuilder::Utf8(builder) => builder.len(),
            TextBuilder::LargeUtf8(builder) => builder.len(),
        }
    }
}
