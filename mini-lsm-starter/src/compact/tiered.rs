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
        if snapshot.levels.is_empty() || snapshot.levels.last().unwrap().1.is_empty() {
            return None;
        }
        // space amplification ratio trigger
        let current_num_iters = snapshot.levels.len();
        let last_level_size = snapshot
            .levels
            .last()
            .map(|x| x.1.len())
            .unwrap_or_default();
        let other_levels_size: usize = snapshot
            .levels
            .iter()
            .take(current_num_iters - 1)
            .map(|x| x.1.len())
            .sum();
        debug_assert!(last_level_size != 0);
        let size_amplification_percent = other_levels_size * 100 / last_level_size;
        if size_amplification_percent >= self.options.max_size_amplification_percent {
            log::debug!(
                "compaction triggered by space amplification ratio: {size_amplification_percent}"
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        // size ratio trigger
        let mut all_previous_tiers_size = snapshot.levels[0].1.len();
        for (i, (_, tier)) in snapshot.levels.iter().enumerate().skip(1) {
            let value = tier.len() * 100 / all_previous_tiers_size;
            if value > (100 + self.options.size_ratio) {
                if i <= self.options.min_merge_width {
                    break;
                }
                let tiers: Vec<_> = snapshot.levels.iter().take(i).cloned().collect();
                log::debug!(
                    "compaction triggered by size ratio: {value} > {}",
                    100 + self.options.size_ratio
                );
                return Some(TieredCompactionTask {
                    bottom_tier_included: false,
                    tiers,
                });
            }
            all_previous_tiers_size += tier.len();
        }
        // reduce sorted runs
        if self
            .options
            .max_merge_width
            .is_some_and(|max_merge_width| current_num_iters > max_merge_width)
        {
            log::debug!("compaction triggered by reducing sorted runs");
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        None
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
