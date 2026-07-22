//! Pure sampling algorithms for PostgreSQL ANALYZE over Iceberg rows.

use std::collections::HashSet;

use rand::Rng;

use crate::error::{IcebergError, IcebergResult};

pub(super) struct AnalyzePopulationSample {
    pub(super) files: Vec<AnalyzeFileSample>,
    pub(super) observation_count: u64,
}

pub(super) struct AnalyzeFileSample {
    pub(super) file_index: usize,
    pub(super) positions: Vec<SampledPosition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SampledPosition {
    pub(super) position: u64,
    pub(super) multiplicity: u64,
}

pub(super) struct AnalyzeSampler;

impl AnalyzeSampler {
    /// Floyd's algorithm: O(sample size) memory and random draws even when the
    /// physical population is much larger than platform `usize`.
    fn sample_ordinals<R: Rng + ?Sized>(
        population: u64,
        sample_size: u64,
        rng: &mut R,
    ) -> IcebergResult<Vec<u64>> {
        if sample_size > population {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE sample size exceeds physical row population",
            ));
        }
        let capacity = usize::try_from(sample_size).map_err(|_| {
            IcebergError::InvariantViolated(
                "ANALYZE sample size exceeds platform collection capacity",
            )
        })?;
        let mut selected = HashSet::with_capacity(capacity);
        for upper in population.saturating_sub(sample_size)..population {
            let candidate = rng.random_range(0..=upper);
            if !selected.insert(candidate) {
                selected.insert(upper);
            }
        }
        let mut ordinals = selected.into_iter().collect::<Vec<_>>();
        ordinals.sort_unstable();
        Ok(ordinals)
    }

    /// Select a locality-bounded file set whose probability is proportional to
    /// its manifest row count, then draw a fixed number of observations from
    /// that set. Every physical row has the same expected observation count.
    pub(super) fn sample_population<R: Rng + ?Sized>(
        record_counts: &[u64],
        physical_rows: u64,
        desired_candidates: u64,
        max_data_files: usize,
        rng: &mut R,
    ) -> IcebergResult<AnalyzePopulationSample> {
        if desired_candidates > physical_rows {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE desired sample exceeds physical row population",
            ));
        }
        if max_data_files == 0 {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE data-file limit must be positive",
            ));
        }
        let verified_rows = record_counts.iter().try_fold(0_u64, |sum, count| {
            sum.checked_add(*count)
                .ok_or(IcebergError::InvariantViolated(
                    "ANALYZE physical row population overflowed",
                ))
        })?;
        if verified_rows != physical_rows {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE file record counts do not match physical population",
            ));
        }
        if record_counts.is_empty() || physical_rows == 0 || desired_candidates == 0 {
            return Ok(AnalyzePopulationSample {
                files: Vec::new(),
                observation_count: 0,
            });
        }

        let sampled_file_count = record_counts.len().min(max_data_files);
        let total_file_count = u64::try_from(record_counts.len()).map_err(|_| {
            IcebergError::InvariantViolated(
                "ANALYZE file population exceeds unsigned long range",
            )
        })?;
        let anchor_ticket = rng.random_range(0..physical_rows);
        let mut cumulative_rows = 0_u64;
        let anchor_index = record_counts
            .iter()
            .position(|record_count| {
                cumulative_rows = cumulative_rows.saturating_add(*record_count);
                anchor_ticket < cumulative_rows
            })
            .ok_or(IcebergError::InvariantViolated(
                "ANALYZE weighted file sample did not select an anchor",
            ))?;

        let mut file_indices = Vec::with_capacity(sampled_file_count);
        file_indices.push(anchor_index);
        if sampled_file_count > 1 {
            let remaining_population = total_file_count.checked_sub(1).ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE file population underflowed",
                ),
            )?;
            let remaining_sample =
                u64::try_from(sampled_file_count - 1).map_err(|_| {
                    IcebergError::InvariantViolated(
                        "ANALYZE sampled file count exceeds unsigned long range",
                    )
                })?;
            for logical_index in
                Self::sample_ordinals(remaining_population, remaining_sample, rng)?
            {
                let logical_index = usize::try_from(logical_index).map_err(|_| {
                    IcebergError::InvariantViolated(
                        "ANALYZE sampled file index exceeds platform capacity",
                    )
                })?;
                file_indices.push(if logical_index < anchor_index {
                    logical_index
                } else {
                    logical_index + 1
                });
            }
        }
        file_indices.sort_unstable();

        let selected_rows = file_indices.iter().try_fold(0_u64, |sum, index| {
            sum.checked_add(record_counts[*index]).ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE selected row population overflowed",
                ),
            )
        })?;
        let observations =
            Self::sample_observations(selected_rows, desired_candidates, rng)?;
        let mut observations = observations.into_iter().peekable();
        let mut selected_offset = 0_u64;
        let mut files = Vec::with_capacity(file_indices.len());
        for file_index in file_indices {
            let file_end = selected_offset
                .checked_add(record_counts[file_index])
                .ok_or(IcebergError::InvariantViolated(
                    "ANALYZE selected file boundary overflowed",
                ))?;
            let mut positions = Vec::new();
            while let Some(observation) = observations.peek() {
                if observation.position >= file_end {
                    break;
                }
                let observation =
                    observations.next().ok_or(IcebergError::InvariantViolated(
                        "ANALYZE sampled observation disappeared",
                    ))?;
                positions.push(SampledPosition {
                    position: observation.position - selected_offset,
                    multiplicity: observation.multiplicity,
                });
            }
            if !positions.is_empty() {
                files.push(AnalyzeFileSample {
                    file_index,
                    positions,
                });
            }
            selected_offset = file_end;
        }
        if observations.next().is_some() {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE sampled observation lies outside selected files",
            ));
        }

        Ok(AnalyzePopulationSample {
            files,
            observation_count: desired_candidates,
        })
    }

    fn sample_observations<R: Rng + ?Sized>(
        population: u64,
        observation_count: u64,
        rng: &mut R,
    ) -> IcebergResult<Vec<SampledPosition>> {
        if observation_count <= population {
            return Ok(Self::sample_ordinals(population, observation_count, rng)?
                .into_iter()
                .map(|position| SampledPosition {
                    position,
                    multiplicity: 1,
                })
                .collect());
        }
        if population == 0 {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE cannot sample observations from an empty file set",
            ));
        }
        let capacity = usize::try_from(observation_count).map_err(|_| {
            IcebergError::InvariantViolated(
                "ANALYZE observation count exceeds platform collection capacity",
            )
        })?;
        let mut observations = Vec::with_capacity(capacity);
        for _ in 0..observation_count {
            observations.push(rng.random_range(0..population));
        }
        observations.sort_unstable();

        let mut sampled: Vec<SampledPosition> = Vec::with_capacity(
            observations
                .len()
                .min(usize::try_from(population).unwrap_or(usize::MAX)),
        );
        for position in observations {
            if let Some(last) = sampled.last_mut()
                && last.position == position
            {
                last.multiplicity = last.multiplicity.checked_add(1).ok_or(
                    IcebergError::InvariantViolated(
                        "ANALYZE observation multiplicity overflowed",
                    ),
                )?;
            } else {
                sampled.push(SampledPosition {
                    position,
                    multiplicity: 1,
                });
            }
        }
        Ok(sampled)
    }

    pub(super) fn desired_candidate_count(
        physical_rows: u64,
        virtual_blocks: u64,
        tickets: u64,
        target_rows: u64,
    ) -> IcebergResult<u64> {
        if tickets > virtual_blocks {
            return Err(IcebergError::InvariantViolated(
                "PostgreSQL ANALYZE selected more tickets than virtual blocks",
            ));
        }
        if tickets == 0 || physical_rows == 0 {
            return Ok(0);
        }
        if virtual_blocks == 0 {
            return Err(IcebergError::InvariantViolated(
                "non-empty ANALYZE sample has zero virtual blocks",
            ));
        }
        let requested_rows = if tickets < virtual_blocks {
            tickets
        } else {
            tickets.max(target_rows)
        };
        Ok(physical_rows.min(requested_rows))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn floyd_sampler_is_sorted_unique_and_bounded() {
        let mut rng = StdRng::seed_from_u64(42);
        let sample =
            AnalyzeSampler::sample_ordinals(10_000, 1_000, &mut rng).unwrap();

        assert_eq!(sample.len(), 1_000);
        assert!(sample.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(sample.iter().all(|ordinal| *ordinal < 10_000));
    }

    #[test]
    fn floyd_sampler_handles_empty_full_and_u64_populations() {
        let mut rng = StdRng::seed_from_u64(7);
        assert!(
            AnalyzeSampler::sample_ordinals(0, 0, &mut rng)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            AnalyzeSampler::sample_ordinals(5, 5, &mut rng).unwrap(),
            vec![0, 1, 2, 3, 4]
        );

        let sample = AnalyzeSampler::sample_ordinals(u64::MAX, 4, &mut rng).unwrap();
        assert_eq!(sample.len(), 4);
        assert!(sample.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(AnalyzeSampler::sample_ordinals(3, 4, &mut rng).is_err());
    }

    #[test]
    fn floyd_sampler_has_near_uniform_long_run_frequency() {
        const POPULATION: usize = 10;
        const SAMPLE_SIZE: u64 = 3;
        const RUNS: u64 = 10_000;
        let expected = RUNS * SAMPLE_SIZE / POPULATION as u64;
        let tolerance = expected / 10;
        let mut counts = [0_u64; POPULATION];

        for seed in 0..RUNS {
            let mut rng = StdRng::seed_from_u64(seed);
            for ordinal in AnalyzeSampler::sample_ordinals(
                POPULATION as u64,
                SAMPLE_SIZE,
                &mut rng,
            )
            .unwrap()
            {
                counts[ordinal as usize] += 1;
            }
        }

        assert!(
            counts
                .into_iter()
                .all(|count| count.abs_diff(expected) <= tolerance)
        );
    }

    #[test]
    fn locality_sample_has_fixed_observation_and_file_bounds() {
        let mut record_counts = vec![1; 99];
        record_counts.push(10_000_000);
        let physical_rows = record_counts.iter().sum();
        let mut rng = StdRng::seed_from_u64(9);
        let sample = AnalyzeSampler::sample_population(
            &record_counts,
            physical_rows,
            30_000,
            8,
            &mut rng,
        )
        .unwrap();

        assert_eq!(sample.observation_count, 30_000);
        assert!(sample.files.len() <= 8);
        let unique_positions = sample
            .files
            .iter()
            .map(|file| file.positions.len())
            .sum::<usize>();
        assert!(unique_positions <= 30_000);
        assert!(
            sample
                .files
                .windows(2)
                .all(|pair| pair[0].file_index < pair[1].file_index)
        );
        assert_eq!(
            sample
                .files
                .iter()
                .flat_map(|file| &file.positions)
                .map(|position| position.multiplicity)
                .sum::<u64>(),
            30_000
        );
    }

    #[test]
    fn uneven_files_produce_unbiased_fixed_size_observations() {
        const RUNS: u64 = 20_000;
        let record_counts = [10_u64, 100, 1_000];
        let physical_rows = record_counts.into_iter().sum();
        let mut observations = [0_u64; 3];

        for seed in 0..RUNS {
            let mut rng = StdRng::seed_from_u64(seed);
            let sample = AnalyzeSampler::sample_population(
                &record_counts,
                physical_rows,
                60,
                2,
                &mut rng,
            )
            .unwrap();
            assert_eq!(sample.observation_count, 60);
            for file in sample.files {
                observations[file.file_index] += file
                    .positions
                    .iter()
                    .map(|position| position.multiplicity)
                    .sum::<u64>();
            }
        }

        let total_observations = RUNS * 60;
        for (observed, population) in observations.into_iter().zip(record_counts) {
            let observed_fraction = observed as f64 / total_observations as f64;
            let population_fraction = population as f64 / physical_rows as f64;
            assert!((observed_fraction - population_fraction).abs() < 0.005);
        }
    }

    #[test]
    fn replacement_sampling_preserves_fixed_observation_count() {
        let sample = (0..10_000)
            .find_map(|seed| {
                let mut rng = StdRng::seed_from_u64(seed);
                let sample = AnalyzeSampler::sample_population(
                    &[1, 1, 1, 100],
                    103,
                    20,
                    1,
                    &mut rng,
                )
                .unwrap();
                (sample.files[0].positions[0].multiplicity > 1).then_some(sample)
            })
            .expect(
                "a small anchor file should be selected across deterministic seeds",
            );

        assert_eq!(sample.files.len(), 1);
        assert_eq!(sample.observation_count, 20);
        assert_eq!(sample.files[0].positions.len(), 1);
        assert_eq!(sample.files[0].positions[0].multiplicity, 20);
    }

    #[test]
    fn desired_candidates_are_bounded_by_postgresql_sample_target() {
        assert_eq!(
            AnalyzeSampler::desired_candidate_count(10_000, 1_000, 100, 30_000)
                .unwrap(),
            100
        );
        assert_eq!(
            AnalyzeSampler::desired_candidate_count(10_000_000, 1_000, 100, 30_000)
                .unwrap(),
            100
        );
        assert_eq!(
            AnalyzeSampler::desired_candidate_count(
                10_000_000,
                1_000,
                1_000,
                30_000,
            )
            .unwrap(),
            30_000
        );
        assert_eq!(
            AnalyzeSampler::desired_candidate_count(
                10_000_000, 1_000_000, 300_000, 30_000,
            )
            .unwrap(),
            300_000
        );
        assert!(
            AnalyzeSampler::desired_candidate_count(10, 100, 101, 30_000).is_err()
        );
    }

    #[test]
    fn locality_sampler_handles_empty_full_and_invalid_boundaries() {
        let mut rng = StdRng::seed_from_u64(23);
        let empty =
            AnalyzeSampler::sample_population(&[], 0, 0, 32, &mut rng).unwrap();
        assert!(empty.files.is_empty());
        assert_eq!(empty.observation_count, 0);

        let full =
            AnalyzeSampler::sample_population(&[5, 5], 10, 10, 32, &mut rng).unwrap();
        assert_eq!(full.observation_count, 10);
        assert_eq!(full.files.len(), 2);
        assert!(full.files.iter().all(|file| {
            file.positions.len() == 5
                && file
                    .positions
                    .iter()
                    .all(|position| position.multiplicity == 1)
        }));

        assert!(
            AnalyzeSampler::sample_population(&[10], 10, 1, 0, &mut rng).is_err()
        );
        assert!(AnalyzeSampler::sample_population(&[9], 10, 1, 1, &mut rng).is_err());
        assert!(
            AnalyzeSampler::sample_population(&[10], 10, 11, 1, &mut rng).is_err()
        );
    }

    #[test]
    fn ticket_boundaries_partition_every_candidate_once() {
        let boundaries = (1_u128..=7)
            .map(|ticket| u64::try_from(ticket * 23 / 7).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(boundaries.last(), Some(&23));
        assert!(boundaries.windows(2).all(|pair| pair[0] <= pair[1]));
    }
}
