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

#![allow(unused)]

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        // selected ssts in upper level
        sst_ids: &[usize],
        // the level index of the lower level
        in_level: usize,
    ) -> Vec<usize> {
        let upper_first_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .unwrap();
        let upper_last_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .unwrap();
        let mut ssts_overlap = Vec::new();
        for &id in &snapshot.levels[in_level - 1].1 {
            let (lower_first_key, lower_last_key) = (
                snapshot.sstables[&id].first_key(),
                snapshot.sstables[&id].last_key(),
            );
            if !(upper_first_key > lower_last_key || upper_last_key < lower_first_key) {
                ssts_overlap.push(id);
            }
        }
        ssts_overlap
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        debug_assert!(self.options.max_levels > 0);
        debug_assert!(self.options.base_level_size_mb > 0);

        let mut target_level_sizes = vec![0; self.options.max_levels]; // exclude L0
        let mut real_level_sizes = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        for i in 0..self.options.max_levels {
            real_level_sizes.push(
                snapshot.levels[i]
                    .1
                    .iter()
                    .map(|id| snapshot.sstables[id].table_size())
                    .sum::<u64>() as usize,
            );
        }

        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        let bottome_level_size = real_level_sizes[self.options.max_levels - 1];
        target_level_sizes[self.options.max_levels - 1] =
            bottome_level_size.max(base_level_size_bytes);

        if bottome_level_size > base_level_size_bytes {
            for i in (0..self.options.max_levels - 1).rev() {
                let next_level_size = target_level_sizes[i + 1];
                let this_level_size = next_level_size / self.options.level_size_multiplier;
                if next_level_size >= base_level_size_bytes {
                    target_level_sizes[i] = this_level_size;
                    base_level = i + 1;
                }
            }
        }

        // always compact L0 with the other levels first
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {base_level}");
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut this_level = 0;
        let mut max_ratio = 1.0;
        let mut priority = Vec::new(); // used for debug
        for (i, ratio) in real_level_sizes
            .iter()
            .zip(target_level_sizes.iter())
            .take(self.options.max_levels - 1)
            .map(|(&current_size, &target_size)| current_size as f64 / target_size as f64)
            .enumerate()
        {
            if ratio > max_ratio {
                max_ratio = ratio;
                this_level = i + 1;
                priority.push((this_level, ratio));
            }
        }
        if max_ratio == 1.0 {
            return None;
        }

        // compact this_level with the lower level
        println!(
            "target level sizes: {:?}, real level sizes: {:?}, base level: {base_level}",
            target_level_sizes
                .iter()
                .map(|&s| format!("{:.3}MB", s as f64 / 1024.0 / 1024.0))
                .collect::<Vec<_>>(),
            real_level_sizes
                .iter()
                .map(|&s| format!("{:.3}MB", s as f64 / 1024.0 / 1024.0))
                .collect::<Vec<_>>(),
        );
        // select the oldest SST
        let selected_sst = *snapshot.levels[this_level - 1].1.iter().min().unwrap();
        println!(
            "compaction triggered by priority: {this_level} out of {priority:?},\
          select {selected_sst} for compaction"
        );

        Some(LeveledCompactionTask {
            upper_level: Some(this_level),
            upper_level_sst_ids: vec![selected_sst],
            lower_level: this_level + 1,
            lower_level_sst_ids: snapshot.levels[this_level].1.clone(),
            is_lower_level_bottom_level: this_level + 1 == self.options.max_levels,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut state = snapshot.clone();
        let files_to_remove = [
            task.upper_level_sst_ids.clone(),
            task.lower_level_sst_ids.clone(),
        ]
        .concat();

        match task.upper_level {
            Some(upper_level) => state.levels[upper_level - 1]
                .1
                .retain(|x| !task.upper_level_sst_ids.contains(x)),
            None => state
                .l0_sstables
                .retain(|x| !task.upper_level_sst_ids.contains(x)),
        }

        let lower_level_mut = &mut state.levels[task.lower_level - 1].1;
        lower_level_mut.retain(|x| !task.lower_level_sst_ids.contains(x));
        lower_level_mut.extend_from_slice(output);
        if !in_recovery {
            lower_level_mut.sort_unstable_by_key(|x| snapshot.sstables[x].first_key());
        }

        (state, files_to_remove)
    }
}
