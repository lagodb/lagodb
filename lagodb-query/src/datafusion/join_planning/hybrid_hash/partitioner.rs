//! Splits one join input into runs stored by unit.

use std::mem;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::{interleave_record_batch, take_record_batch};
use datafusion::common::Result;
use datafusion::common::hash_utils::{RandomState, create_hashes};
use datafusion::common::utils::memory::RecordBatchMemoryCounter;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::metrics::SpillMetrics;
use futures::StreamExt;

use super::probe_input::{ProbeInput, ProbeTail};
use super::runs::{BucketGroup, BucketSizes, BucketedInput};
use super::{BUCKET_BITS, BUCKETS};
use crate::datafusion::join_planning::adaptive::PartitionRun;
use crate::datafusion::join_planning::staged_build::StagedBuild;
use crate::datafusion::spill::{
    BucketRun, BucketRunFormat, BucketRunWriter, SPILL_CHUNK_BYTES,
};

/// Bucket hashes use their own seed, apart from DataFusion's repartition seed
/// (0) and its hash-join seed. A partition's rows already agree on the
/// repartition hash, and each group's hash table takes its slots from the
/// hash-join hash, so bucket bits taken from either would cluster.
const BUCKET_HASH: RandomState = RandomState::with_seed(0x9e37_79b9_7f4a_7c15);

/// Memory a row adds to a run besides its batch: its unit now and its place
/// in the unit order once the run is sealed.
const ROW_INDEX_BYTES: usize = size_of::<u16>() + size_of::<(u32, u32)>();

/// Where a partitioner files a row: the bucket one level of the bucket hash
/// gives it, stored under that bucket's unit. A build stores every bucket as
/// its own unit, because its groups come from the bucket sizes. A probe is
/// split once those groups are known and stores each group as one unit, as
/// PostgreSQL keeps one file per batch, so its segments are not cut finer
/// than a group.
pub(super) struct Routing {
    /// Hash bits below this level's bucket.
    shift: u32,
    units: Vec<u16>,
    unit_count: usize,
}

impl Routing {
    pub(super) fn buckets(level: u32) -> Self {
        Self {
            shift: level * BUCKET_BITS,
            units: (0..BUCKETS).map(|bucket| bucket as u16).collect(),
            unit_count: BUCKETS,
        }
    }

    pub(super) fn groups(level: u32, groups: &[BucketGroup]) -> Self {
        let mut units = vec![0; BUCKETS];
        for (unit, group) in groups.iter().enumerate() {
            units[group.buckets.clone()].fill(unit as u16);
        }
        Self {
            shift: level * BUCKET_BITS,
            units,
            unit_count: groups.len(),
        }
    }

    fn unit(&self, hash: u64) -> u16 {
        self.units[(hash >> self.shift) as usize & (BUCKETS - 1)]
    }
}

/// Batches kept in memory with each row's unit.
#[derive(Default)]
struct MemoryRun {
    batches: Vec<RecordBatch>,
    units: Vec<Vec<u16>>,
    rows: usize,
    /// Bytes of the batches taken, with their rows' index. `counter` has also
    /// seen a batch the run then turned away.
    bytes: usize,
    counter: RecordBatchMemoryCounter,
}

impl MemoryRun {
    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Bytes `batch` adds, not counting buffers the run already holds.
    fn bytes_of(&mut self, batch: &RecordBatch) -> usize {
        self.counter.count_batch(batch) + batch.num_rows() * ROW_INDEX_BYTES
    }

    /// Take `batch`, which adds `bytes` as `bytes_of` measured them.
    fn push(&mut self, batch: RecordBatch, units: Vec<u16>, bytes: usize) {
        self.rows += batch.num_rows();
        self.bytes += bytes;
        self.batches.push(batch);
        self.units.push(units);
    }

    /// Order the rows by their `unit_count` units with a counting sort.
    fn seal(self, unit_count: usize) -> SealedRun {
        let mut starts = vec![0; unit_count + 1];
        for &unit in self.units.iter().flatten() {
            starts[usize::from(unit) + 1] += 1;
        }
        for unit in 0..unit_count {
            starts[unit + 1] += starts[unit];
        }
        let mut next = starts[..unit_count].to_vec();
        let mut order = vec![(0, 0); self.rows];
        for (batch, units) in self.units.iter().enumerate() {
            for (row, &unit) in units.iter().enumerate() {
                let slot = &mut next[usize::from(unit)];
                order[*slot] = (batch as u32, row as u32);
                *slot += 1;
            }
        }
        // Every row carries its index, so the rounded-up share stays at least
        // that large.
        let row_bytes = (self.bytes.div_ceil(self.rows) - ROW_INDEX_BYTES).max(1);
        SealedRun {
            batches: self.batches,
            order,
            starts,
            window_rows: (SPILL_CHUNK_BYTES / row_bytes).max(1),
        }
    }
}

/// A memory run with its rows ordered by unit.
pub(super) struct SealedRun {
    batches: Vec<RecordBatch>,
    /// `(batch, row)` of every row, unit by unit.
    order: Vec<(u32, u32)>,
    /// Where each unit starts in `order`, with the end last.
    starts: Vec<usize>,
    /// Rows of one spill chunk at the run's average row width.
    window_rows: usize,
}

impl SealedRun {
    pub(super) fn units(&self) -> usize {
        self.starts.len() - 1
    }

    /// Rows of the units from `first`.
    pub(super) fn rows_from(&self, first: usize) -> usize {
        self.order.len() - self.starts[first]
    }

    /// Where the rows of `unit` are in the unit order.
    pub(super) fn unit_positions(&self, unit: usize) -> Range<usize> {
        self.starts[unit]..self.starts[unit + 1]
    }

    /// The rows at `positions` of the unit order that come from the batch of
    /// the first, and the position after them. A unit's rows keep their batch
    /// order, so they are taken from one batch at a time: a taken view array
    /// keeps the data buffers of its own batch, as that batch did, where an
    /// interleave would keep those of every batch the rows came from.
    pub(super) fn take_batch(
        &self,
        positions: Range<usize>,
    ) -> Result<(RecordBatch, usize)> {
        let rows = &self.order[positions.clone()];
        let (batch, _) = rows[0];
        let taken = rows
            .iter()
            .take_while(|&&(other, _)| other == batch)
            .count();
        let source = &self.batches[batch as usize];
        // Rows of one batch are distinct and ascending, so all of them are
        // the batch itself.
        let batch = if taken == source.num_rows() {
            source.clone()
        } else {
            let indices = UInt32Array::from_iter_values(
                rows[..taken].iter().map(|&(_, row)| row),
            );
            take_record_batch(source, &indices)?
        };
        Ok((batch, positions.start + taken))
    }

    /// Write the units from `first` to a new run, adding their sizes to
    /// `sizes`.
    pub(super) fn spill(
        &self,
        first: usize,
        format: &BucketRunFormat,
        sizes: &mut BucketSizes,
        spill: &SpillMetrics,
    ) -> Result<BucketRun> {
        let mut writer = BucketRunWriter::create(format)?;
        self.write(first, &mut writer, sizes)?;
        let disk = writer.finish(self.units())?;
        spill.spill_file_count.add(1);
        spill.spilled_rows.add(self.rows_from(first));
        spill.spilled_bytes.add(disk.bytes() as usize);
        Ok(disk)
    }

    /// Write the rows of the units from `first` to `writer` in unit order.
    /// Each window of `order` is copied out with one interleave and written as
    /// one slice per unit it holds. Besides the run, one window is copied at a time, with its
    /// encoding: about a chunk when row widths do not depend on the unit, and
    /// up to the run when one unit holds the run's wide rows. A window
    /// bounds rows, as DataFusion's own spill batches do. The copy stays
    /// outside the reservation, which no other consumer can consult while
    /// this synchronous write runs.
    fn write(
        &self,
        first: usize,
        writer: &mut BucketRunWriter<'_>,
        sizes: &mut BucketSizes,
    ) -> Result<()> {
        let batches = self.batches.iter().collect::<Vec<_>>();
        let skipped = self.starts[first];
        let order = &self.order[skipped..];
        let mut indices = Vec::with_capacity(self.window_rows.min(order.len()));
        let mut unit = first;
        for (window, rows) in order.chunks(self.window_rows).enumerate() {
            indices.clear();
            indices.extend(
                rows.iter()
                    .map(|&(batch, row)| (batch as usize, row as usize)),
            );
            let gathered = interleave_record_batch(&batches, &indices)?;
            let start = skipped + window * self.window_rows;
            let end = start + rows.len();
            let mut row = start;
            while row < end {
                while self.starts[unit + 1] <= row {
                    unit += 1;
                }
                let stop = self.starts[unit + 1].min(end);
                let slice = gathered.slice(row - start, stop - row);
                let bytes = writer.write(unit, &slice)?;
                sizes.add(unit, slice.num_rows(), bytes);
                row = stop;
            }
        }
        Ok(())
    }
}

/// Splits one input into runs through a memory run of at most `work_mem`.
pub(super) struct BucketPartitioner {
    keys: Vec<PhysicalExprRef>,
    routing: Routing,
    hashes: Vec<u64>,
    format: Arc<BucketRunFormat>,
    run: MemoryRun,
    runs: Vec<BucketRun>,
    sizes: BucketSizes,
    reservation: MemoryReservation,
    run_limit_bytes: usize,
    spill: SpillMetrics,
}

impl BucketPartitioner {
    pub(super) fn try_new(
        side: &str,
        keys: Vec<PhysicalExprRef>,
        input: &SendableRecordBatchStream,
        routing: Routing,
        run: &PartitionRun,
        spill: SpillMetrics,
    ) -> Result<Self> {
        let reservation =
            MemoryConsumer::new(format!("HybridHashJoin{side}[{}]", run.partition()))
                .with_can_spill(true)
                .register(run.context().memory_pool());
        Ok(Self {
            keys,
            hashes: Vec::new(),
            format: Arc::new(BucketRunFormat::try_new(input.schema())?),
            run: MemoryRun::default(),
            runs: Vec::new(),
            sizes: BucketSizes::new(routing.unit_count),
            routing,
            reservation,
            run_limit_bytes: run.policy().work_mem_bytes,
            spill,
        })
    }

    /// Split all of `input`.
    pub(super) async fn consume(
        mut self,
        mut input: SendableRecordBatchStream,
    ) -> Result<Self> {
        while let Some(batch) = input.next().await.transpose()? {
            let units = self.route(&batch)?;
            self.push(batch, units)?;
        }
        Ok(self)
    }

    /// Write the last memory run too. Every unit's size is then the exact
    /// bytes a reader loads, which a build's groups are cut by, and the
    /// input's `work_mem` is free while the groups join.
    pub(super) fn finish(mut self) -> Result<BucketedInput> {
        // TODO(hybrid-hash-memory-fast-path): a build's last memory run is
        // written and read back although it is already in memory, and it can
        // be the build's only run: a pool refusal overflows a build of any
        // size, and the table estimate counts a concatenated copy and a hash
        // index that a run does not. A build's groups are cut by exact on-disk
        // unit sizes, which rows in memory do not have: a batch's buffers are
        // shared by its buckets, and gathered view arrays keep the data buffers
        // of every batch their rows come from. So the first group's build is
        // read back, and only that group's probe rows join as they arrive. It
        // saves at most one work_mem of writes and reads per split, which pays
        // off only for a build not far above work_mem.
        if !self.run.is_empty() {
            self.spill()?;
        }
        Ok(BucketedInput::new(self.format, self.runs, self.sizes))
    }

    pub(super) fn finish_probe(mut self) -> Result<ProbeInput> {
        if !self.runs.is_empty() || self.run.is_empty() {
            return Ok(ProbeInput::Spilled(Arc::new(self.finish()?)));
        }
        let run = mem::take(&mut self.run).seal(self.routing.unit_count);
        Ok(ProbeInput::Memory(Arc::new(ProbeTail::new(
            self.format,
            run,
            self.reservation,
            self.spill,
        ))))
    }

    /// Take the rows of `batch`, filed under `units` as `route` gave them.
    pub(super) fn push(&mut self, batch: RecordBatch, units: Vec<u16>) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut bytes = self.run.bytes_of(&batch);
        if !self.admit(bytes)? {
            if !self.run.is_empty() {
                self.spill()?;
                bytes = self.run.bytes_of(&batch);
            }
            if !self.admit(bytes)? {
                // Alone over the bound: the batch becomes a run of its own.
                self.run.push(batch, units, bytes);
                return self.spill();
            }
        }
        self.run.push(batch, units, bytes);
        Ok(())
    }

    fn admit(&self, bytes: usize) -> Result<bool> {
        StagedBuild::try_resize_within(
            &self.reservation,
            self.reservation.size() + bytes,
            self.run_limit_bytes,
        )
    }

    /// The unit of every row of `batch`.
    pub(super) fn route(&mut self, batch: &RecordBatch) -> Result<Vec<u16>> {
        let rows = batch.num_rows();
        let keys = self
            .keys
            .iter()
            .map(|key| key.evaluate(batch)?.into_array(rows))
            .collect::<Result<Vec<_>>>()?;
        self.hashes.clear();
        self.hashes.resize(rows, 0);
        create_hashes(&keys, &BUCKET_HASH, &mut self.hashes)?;
        Ok(self
            .hashes
            .iter()
            .map(|&hash| self.routing.unit(hash))
            .collect())
    }

    /// Write the memory run to disk in unit order.
    fn spill(&mut self) -> Result<()> {
        let run = mem::take(&mut self.run).seal(self.routing.unit_count);
        let disk = run.spill(0, &self.format, &mut self.sizes, &self.spill)?;
        self.runs.push(disk);
        drop(run);
        self.reservation.free();
        Ok(())
    }
}
