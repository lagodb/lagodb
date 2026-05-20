use arrow_array::{Array, ArrayRef};
use iceberg_lite::spec::Type;
use pg_lakebase_core::tuple::{Cell, Row};

use crate::error::IcebergResult;

/// Trait to extract a Cell from an Arrow Array based on the Iceberg type.
pub trait ArrowToCell {
    fn extract(
        &self,
        array: &dyn Array,
        row_idx: usize,
    ) -> IcebergResult<Option<Cell>>;
}

/// Trait to build an Arrow Array from Rows based on the Iceberg type.
pub trait RowsToArrow {
    fn build(&self, rows: &[Row], col_idx: usize) -> IcebergResult<ArrayRef>;
}

// Implement for the top-level Type enum to dispatch to specific implementations
impl ArrowToCell for Type {
    fn extract(
        &self,
        array: &dyn Array,
        row_idx: usize,
    ) -> IcebergResult<Option<Cell>> {
        match self {
            Type::Primitive(p) => p.extract(array, row_idx),
            Type::List(list) => {
                // Complex extraction logic implemented for ListType
                list.extract(array, row_idx)
            }
            Type::Struct(_) => {
                Err(crate::error::IcebergError::UnsupportedColumnType(
                    "Struct type is not supported".to_string(),
                ))
            }
            Type::Map(_) => Err(crate::error::IcebergError::UnsupportedColumnType(
                "Map type is not supported".to_string(),
            )),
        }
    }
}

impl RowsToArrow for Type {
    fn build(&self, rows: &[Row], col_idx: usize) -> IcebergResult<ArrayRef> {
        match self {
            Type::Primitive(p) => p.build(rows, col_idx),
            Type::List(list) => list.build(rows, col_idx),
            Type::Struct(_) => {
                Err(crate::error::IcebergError::UnsupportedColumnType(
                    "Struct type is not supported".to_string(),
                ))
            }
            Type::Map(_) => Err(crate::error::IcebergError::UnsupportedColumnType(
                "Map type is not supported".to_string(),
            )),
        }
    }
}
