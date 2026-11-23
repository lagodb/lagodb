//! Hybrid hash join for a partition whose build outgrows its `hash_mem` share.

mod partitioner;
mod probe_input;
mod runs;
mod split;

use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{Count, MetricBuilder, SpillMetrics};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt, stream};

use self::partitioner::{BucketPartitioner, Routing};
use self::probe_input::ProbeInput;
use self::runs::{BucketGroup, BucketedInput};
use self::split::ProbeSplit;
use super::adaptive::{PartitionRun, StagedInput};
use super::staged_build::StagedInputExec;
use crate::HashJoinFallback;

/// Hash bits one split takes.
const BUCKET_BITS: u32 = HashJoinFallback::HYBRID_HASH_BUCKET_BITS;
/// Buckets of one split, a power of two so a bucket is bits of a hash.
const BUCKETS: usize = 1 << BUCKET_BITS;
/// Whole splits a 64-bit bucket hash has bits for; the bits left over are
/// fewer than a split takes and stay unused.
const LEVELS: u32 = u64::BITS / BUCKET_BITS;

/// The joins of one partition.
pub(super) struct HybridHashJoin {
    run: PartitionRun,
    build_keys: Vec<PhysicalExprRef>,
    probe_keys: Vec<PhysicalExprRef>,
    batch_size: usize,
    spill: SpillMetrics,
    /// Buckets split again, as PostgreSQL's EXPLAIN shows batches grown past
    /// their original count.
    resplit_buckets: Count,
    sort_merge_fallback_groups: Count,
}

impl HybridHashJoin {
    /// Split both inputs, then join group after group. `build` starts with the
    /// batches the partition staged before it overflowed.
    pub(super) async fn execute(
        run: PartitionRun,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        let spill = SpillMetrics::new(run.metrics(), run.partition());
        let resplit_buckets = MetricBuilder::new(run.metrics())
            .counter("resplit_buckets", run.partition());
        let sort_merge_fallback_groups = MetricBuilder::new(run.metrics())
            .counter("sort_merge_fallback_groups", run.partition());
        let (build_keys, probe_keys): (Vec<_>, Vec<_>) = run
            .join()
            .on()
            .iter()
            .map(|(build, probe)| (Arc::clone(build), Arc::clone(probe)))
            .unzip();
        let batch_size = run.context().session_config().batch_size();
        let join = Arc::new(Self {
            run,
            build_keys,
            probe_keys,
            batch_size,
            spill,
            resplit_buckets,
            sort_merge_fallback_groups,
        });
        join.split(0, build, probe).await
    }

    /// Join `build` and `probe` split at `level` of the bucket hash.
    fn split(
        self: Arc<Self>,
        level: u32,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream>> {
        Box::pin(async move {
            let routing = Routing::buckets(level);
            let build = self
                .partitioner("Build", &self.build_keys, &build, routing)?
                .consume(build)
                .await?
                .finish()?;
            let build = Arc::new(build);
            let groups = build.groups(self.run.build_limit_bytes())?;
            let rows = build.rows(0..BUCKETS);
            if groups
                .iter()
                .any(|group| !group.fits && build.rows(group.buckets.clone()) == rows)
            {
                // The split made no progress, so the probe is not split either.
                return self
                    .sort_merge(build.stream(0..BUCKETS, self.batch_size), probe);
            }

            let routing = Routing::groups(level, &groups);
            let partitioner =
                self.partitioner("Probe", &self.probe_keys, &probe, routing)?;
            let first = &groups[0];
            let table = if first.fits {
                match self.run.stage(build.read(first.buckets.clone())).await? {
                    StagedInput::Fits(table) => Some(table),
                    // The pool refused it, so it waits on disk like the others.
                    StagedInput::Overflows(_) => None,
                }
            } else {
                None
            };
            let Some(table) = table else {
                let probe = partitioner.consume(probe).await?.finish_probe()?;
                return Ok(
                    Level::new(self, level, build, groups, probe).join_groups(0)
                );
            };

            let split = ProbeSplit::new(probe, partitioner, self.batch_size);
            let joined = self.hash(table, split.memory_input())?;
            let schema = joined.schema();
            let rest = async move {
                let probe = split.finish().await?;
                let level = Level::new(self, level, build, groups, probe);
                Ok::<_, DataFusionError>(level.join_groups(1))
            };
            let output = joined.chain(stream::once(rest).try_flatten());
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output))
                as SendableRecordBatchStream)
        })
    }

    fn partitioner(
        &self,
        side: &str,
        keys: &[PhysicalExprRef],
        input: &SendableRecordBatchStream,
        routing: Routing,
    ) -> Result<BucketPartitioner> {
        BucketPartitioner::try_new(
            side,
            keys.to_vec(),
            input,
            routing,
            &self.run,
            self.spill.clone(),
        )
    }

    /// The planned join over one group. `reset_state` drops the build future
    /// and the dynamic filter: the probe scan has already been read, and each
    /// group's bounds cover only its own keys. It keeps `fetch`, which the
    /// planned join never has: `JoinStrategyRule` wraps it before
    /// LimitPushdown runs, and the wrapper takes no fetch.
    fn hash(
        &self,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        self.run
            .join()
            .builder()
            .with_new_children(vec![
                Arc::new(StagedInputExec::single_partition(build))
                    as Arc<dyn ExecutionPlan>,
                Arc::new(StagedInputExec::single_partition(probe)),
            ])?
            .reset_state()
            .build()?
            .execute(0, Arc::clone(self.run.context()))
    }

    fn sort_merge(
        &self,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        self.sort_merge_fallback_groups.add(1);
        self.run.sort_merge(build, probe)
    }
}

/// Both inputs split at one level, joined group after group.
struct Level {
    join: Arc<HybridHashJoin>,
    level: u32,
    build: Arc<BucketedInput>,
    groups: Vec<BucketGroup>,
    /// Groups join one after another, and the probe may move to disk between
    /// them.
    probe: Mutex<ProbeInput>,
}

impl Level {
    fn new(
        join: Arc<HybridHashJoin>,
        level: u32,
        build: Arc<BucketedInput>,
        groups: Vec<BucketGroup>,
        probe: ProbeInput,
    ) -> Arc<Self> {
        Arc::new(Self {
            join,
            level,
            build,
            groups,
            probe: Mutex::new(probe),
        })
    }

    fn probe(&self) -> MutexGuard<'_, ProbeInput> {
        self.probe
            .lock()
            .expect("no code panics while holding a level's probe")
    }

    /// Join the groups from `first` on that hold rows. Each group's stream
    /// ends and releases its table before the next one stages its build.
    fn join_groups(self: Arc<Self>, first: usize) -> SendableRecordBatchStream {
        let schema = self.join.run.join().schema();
        let groups = {
            let probe = self.probe();
            (first..self.groups.len())
                .filter(|&group| {
                    self.build.rows(self.groups[group].buckets.clone())
                        + probe.rows(group)
                        > 0
                })
                .collect::<Vec<_>>()
        };
        let output = stream::iter(groups)
            .then(move |group| Arc::clone(&self).join_group(group))
            .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(schema, output))
    }

    async fn join_group(
        self: Arc<Self>,
        group: usize,
    ) -> Result<SendableRecordBatchStream> {
        let BucketGroup { buckets, fits } = &self.groups[group];
        let join = &self.join;
        if !fits {
            let build = self.build.stream(buckets.clone(), join.batch_size);
            let probe = self.spilled_probe(group)?;
            return if self.level + 1 < LEVELS {
                join.resplit_buckets.add(1);
                Arc::clone(join).split(self.level + 1, build, probe).await
            } else {
                join.sort_merge(build, probe)
            };
        }
        match join.run.stage(self.build.read(buckets.clone())).await? {
            StagedInput::Fits(build) => {
                let probe = self.probe().stream(group, join.batch_size);
                join.hash(build, probe)
            }
            StagedInput::Overflows(build) => {
                let probe = self.spilled_probe(group)?;
                join.sort_merge(build, probe)
            }
        }
    }

    /// The probe rows of `group` once the probe is on disk, for a group that
    /// splits again or sorts.
    fn spilled_probe(&self, group: usize) -> Result<SendableRecordBatchStream> {
        let mut probe = self.probe();
        probe.spill_from(group)?;
        Ok(probe.stream(group, self.join.batch_size))
    }
}
