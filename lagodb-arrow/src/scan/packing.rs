//! Bounded-lookback bin packing of weighted scan work items.
//!
//! Providers choose their own work items (file ranges, row-group spans) and
//! weights; this kernel only groups item identifiers into parallel work units.

use std::collections::VecDeque;
use std::num::{NonZeroU64, NonZeroUsize};

/// First-fit packing over the most recent `lookback` open bins. When a new
/// bin exceeds the lookback window, the heaviest open bin is emitted.
pub struct WeightedBinPacker {
    target_weight: u64,
    lookback: usize,
    open: VecDeque<WeightedBin>,
    closed: Vec<Box<[u32]>>,
}

#[derive(Default)]
struct WeightedBin {
    weight: u64,
    items: Vec<u32>,
}

impl WeightedBin {
    fn push(&mut self, item: u32, weight: u64) {
        self.weight = self.weight.saturating_add(weight);
        self.items.push(item);
    }
}

impl WeightedBinPacker {
    pub fn new(target_weight: NonZeroU64, lookback: NonZeroUsize) -> Self {
        Self {
            target_weight: target_weight.get(),
            lookback: lookback.get(),
            open: VecDeque::new(),
            closed: Vec::new(),
        }
    }

    pub fn push(&mut self, item: u32, weight: u64) {
        if let Some(bin) = self
            .open
            .iter_mut()
            .find(|bin| bin.weight.saturating_add(weight) <= self.target_weight)
        {
            bin.push(item, weight);
            return;
        }
        let mut bin = WeightedBin::default();
        bin.push(item, weight);
        self.open.push_back(bin);
        if self.open.len() > self.lookback {
            let heaviest = self
                .open
                .iter()
                .enumerate()
                .fold(None, |heaviest, (index, bin)| match heaviest {
                    Some((_, weight)) if weight >= bin.weight => heaviest,
                    _ => Some((index, bin.weight)),
                })
                .map(|(index, _)| index)
                .expect("a bin was added immediately above");
            let bin = self
                .open
                .remove(heaviest)
                .expect("heaviest bin index came from this deque");
            self.closed.push(bin.items.into_boxed_slice());
        }
    }

    /// All bins in emission order; empty when no item was pushed.
    pub fn finish(mut self) -> Vec<Box<[u32]>> {
        self.closed.extend(
            self.open
                .into_iter()
                .map(|bin| bin.items.into_boxed_slice()),
        );
        self.closed
    }
}
