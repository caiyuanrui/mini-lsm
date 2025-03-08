// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    // the number of tiers (levels)
    pub num_tiers: usize,
    // all levels except for last level size / last level size
    pub max_size_amplification_percent: usize,
    // (this tier - sum of all previous tiers) / sum of all previous tiers
    pub size_ratio: usize,
    // triggered by size ratio only when more than `min_merge_width` tiers are to be compacted
    pub min_merge_width: usize,
    // triggered by reducing sorted runs when the number of tiers > `max_merge_width`
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers || snapshot.levels.is_empty() {
            return None;
        }
        // space amplification ratio trigger
        let last_level_size = snapshot.levels.last().unwrap().1.len();
        let other_levels_size: usize = snapshot
            .levels
            .iter()
            .take(snapshot.levels.len() - 1)
            .map(|x| x.1.len())
            .sum();
        let space_amp_percent = other_levels_size as f64 * 100.0 / last_level_size as f64;
        if space_amp_percent >= self.options.max_size_amplification_percent as f64 {
            log::debug!(
                "tiered compaction triggered by space amplification ratio: {space_amp_percent}"
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        // size ratio trigger
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
            let next_level_size = snapshot.levels[id + 1].1.len();
            let current_size_ratio = next_level_size as f64 / size as f64;
            if current_size_ratio > 100.0 + self.options.size_ratio as f64
                && id + 1 >= self.options.min_merge_width
            {
                log::debug!("tiered compaction triggered by size ratio: {current_size_ratio}");
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.iter().take(id + 1).cloned().collect(),
                    bottom_tier_included: id + 1 == snapshot.levels.len(),
                });
            }
        }
        // reduce sorted runs
        let num_tiers_to_reduce = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        log::debug!("tiered compaction triggered by reducing sorted runs");
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_reduce)
                .cloned()
                .collect(),
            bottom_tier_included: snapshot.levels.len() == num_tiers_to_reduce,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        debug_assert!(snapshot.levels.len() >= task.tiers.len());

        let mut state = snapshot.clone();
        let mut files_to_remove = Vec::with_capacity(task.tiers.iter().map(|x| x.1.len()).sum());
        for (_, x) in &task.tiers {
            files_to_remove.extend(x);
        }

        // these are the tiers flushed after the task is spawned
        let new_tiers_end_at = (0..snapshot.levels.len())
            .find(|&i| snapshot.levels[i].0 == task.tiers[0].0)
            .unwrap();

        debug_assert_eq!(
            snapshot.levels[new_tiers_end_at..new_tiers_end_at + task.tiers.len()],
            task.tiers
        );
        debug_assert_eq!(
            new_tiers_end_at + task.tiers.len() == snapshot.levels.len(),
            task.bottom_tier_included
        );

        let mut new_tiers = Vec::with_capacity(snapshot.levels.len());
        new_tiers.extend_from_slice(&snapshot.levels[..new_tiers_end_at]);
        new_tiers.push((output[0], output.to_vec()));
        if !task.bottom_tier_included {
            new_tiers.extend_from_slice(&snapshot.levels[new_tiers_end_at + task.tiers.len()..]);
        }
        state.levels = new_tiers;
        (state, files_to_remove)
    }
}
