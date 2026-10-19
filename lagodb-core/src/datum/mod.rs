//! PostgreSQL value types and `Datum` conversion codecs.
//!
//! This module owns semantic scalar values, PostgreSQL type metadata, and the
//! codecs that translate those values to and from PostgreSQL `Datum`s. Tuple
//! structure and tuple-slot lifecycle adapters live in [`crate::tuple`].

mod array;
mod cell;
mod codec;
mod codec_context;
mod composite;
mod decimal128;
mod encoding;
mod error;
mod json;
pub mod jsonb;
mod numeric;
mod pg_type;
mod temporal;
mod text_io;
mod varlena;

pub use array::{ArrayDatumBuffer, ArrayDatumIter, ArrayDatumLayout};
pub use cell::{ByteaView, Cell, StringView};
pub use codec::{BoundColumnDatumCodec, ColumnDatumTarget};
pub use composite::CompositeDatumCodec;
pub use decimal128::Decimal128Semantics;
pub use encoding::Utf8ServerEncoding;
pub use error::{DatumConversionError, Utf8ServerEncodingError};
pub use json::{
    BoundJsonObjectEncoder, JsonDatumEncoder, JsonDatumKind, JsonText, JsonValueError,
};
pub use jsonb::{
    JsonbArray, JsonbBuilder, JsonbNumeric, JsonbObject, JsonbPosition, JsonbReader,
    JsonbRef, JsonbTemporalParser, JsonbValue,
};
pub use numeric::{
    Decimal128ComparisonValue, Decimal128NumericCodec, DecimalCodecError,
    NumericTypmod, PostgresNumericCodec, numeric_precision_scale, numeric_typmod,
};
pub use pg_type::{PgCompositeField, PgType};
pub use temporal::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
pub use text_io::PgTextIo;
pub use varlena::DetoastedVarlena;
