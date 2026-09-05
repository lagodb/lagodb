use std::mem::size_of_val;

use arrow_array::{Array, ArrayRef, BinaryArray, Decimal128Array};
use arrow_buffer::i256;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::Accumulator;

use crate::plan::AggregateKind;

use super::output::NumericOutput;

const STATE_BYTES: usize = 40;

/// Fixed-width exact state for one bounded Decimal128 aggregate group.
#[derive(Debug)]
pub(super) struct NumericAccumulator {
    kind: AggregateKind,
    sum: i256,
    count: u64,
    scale: u32,
}

impl NumericAccumulator {
    pub(super) const fn new(kind: AggregateKind, scale: u32) -> Self {
        Self {
            kind,
            sum: i256::ZERO,
            count: 0,
            scale,
        }
    }

    fn update_values(&mut self, values: &Decimal128Array) -> Result<()> {
        let added = values.len() - values.null_count();
        self.count = self
            .count
            .checked_add(added as u64)
            .ok_or_else(|| Self::invalid_state("numeric aggregate count overflow"))?;

        if values.null_count() == 0 {
            for &value in values.values() {
                self.sum = self.sum.wrapping_add(i256::from_i128(value));
            }
        } else {
            for value in values.iter().flatten() {
                self.sum = self.sum.wrapping_add(i256::from_i128(value));
            }
        }
        Ok(())
    }

    fn encoded_state(&self) -> Vec<u8> {
        let mut state = Vec::with_capacity(STATE_BYTES);
        state.extend_from_slice(&self.count.to_be_bytes());
        state.extend_from_slice(&self.sum.to_be_bytes());
        state
    }

    fn merge_state(&mut self, state: &[u8]) -> Result<()> {
        let state: &[u8; STATE_BYTES] = state.try_into().map_err(|_| {
            Self::invalid_state("invalid numeric aggregate state length")
        })?;
        let count = u64::from_be_bytes(
            state[..8]
                .try_into()
                .expect("fixed-size numeric count state"),
        );
        let sum = i256::from_be_bytes(
            state[8..].try_into().expect("fixed-size numeric sum state"),
        );
        self.count = self
            .count
            .checked_add(count)
            .ok_or_else(|| Self::invalid_state("numeric aggregate count overflow"))?;
        self.sum = self.sum.wrapping_add(sum);
        Ok(())
    }

    fn invalid_state(message: &'static str) -> DataFusionError {
        DataFusionError::Internal(message.to_owned())
    }
}

impl Accumulator for NumericAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values] = values else {
            return Err(Self::invalid_state(
                "numeric aggregate requires one input array",
            ));
        };
        let values = values
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| {
                Self::invalid_state("numeric aggregate input is not Decimal128")
            })?;
        self.update_values(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            return Ok(ScalarValue::Binary(None));
        }
        NumericOutput::finalize(self.sum, self.count, self.scale, self.kind)
            .map(|value| ScalarValue::Binary(Some(value)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.encoded_state()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [states] = states else {
            return Err(Self::invalid_state(
                "numeric aggregate requires one state array",
            ));
        };
        let states =
            states
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    Self::invalid_state("numeric aggregate state is not Binary")
                })?;
        for state in states.iter().flatten() {
            self.merge_state(state)?;
        }
        Ok(())
    }
}
