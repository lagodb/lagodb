//! Relation-bound encoding shared by connector INSERT entry points.

use core::ffi::c_int;

use lagodb_core::handles::RelationHandle;
use lagodb_core::tuple::TupleSlotRow;

use crate::error::ConnectorError;
use crate::storage::ObjectOutput;

use super::FormatObject;

/// Bind format options and column encoders once for a live relation.
pub(crate) trait FormatWriter: FormatObject {
    fn begin(
        self: Box<Self>,
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError>;
}

/// The format owns encoding and buffering; the FDW adapter owns SQL operations.
pub(crate) trait FormatWriteState: 'static {
    fn batch_size(&self) -> c_int {
        1
    }

    /// The FDW adapter supplies slots from the relation used by `begin`.
    /// Format implementations bind column access once against that relation.
    fn write_row(&mut self, row: TupleSlotRow<'_>) -> Result<(), ConnectorError>;

    fn finish(&mut self) -> Result<(), ConnectorError>;
}
