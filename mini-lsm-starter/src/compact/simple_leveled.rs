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

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize, // lower_level / upper_level
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize, // >=L1
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if self.options.max_levels == 0 {
            return None;
        }

        // merge l0 with l1
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // println!(
            //     "compaction triggered at level 0 because L0 has {} SSTs >= {}",
            //     snapshot.l0_sstables.len(),
            //     self.options.level0_file_num_compaction_trigger
            // );
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: 1 == self.options.max_levels,
            });
        }

        // merge >=L1
        for i in 1..self.options.max_levels {
            let (lower_level, upper_level) = (i + 1, i);
            let lower_level_size = snapshot.levels[lower_level - 1].1.len();
            let upper_level_size = snapshot.levels[upper_level - 1].1.len();

            let size_ratio_percent = if upper_level_size == 0 {
                continue;
            } else {
                lower_level_size * 100 / upper_level_size
            };

            if size_ratio_percent < self.options.size_ratio_percent {
                // println!(
                //     "compaction triggered at level {} and {} with size ratio {}",
                //     i,
                //     lower_level,
                //     size_ratio_percent as f64 / 100.0
                // );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper_level),
                    upper_level_sst_ids: snapshot.levels[upper_level - 1].1.clone(),
                    lower_level,
                    lower_level_sst_ids: snapshot.levels[lower_level - 1].1.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    /// # Return
    /// (new state, deleted sst ids)
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        // list of SST ids
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();

        if let Some(upper) = task.upper_level {
            assert_eq!(
                snapshot.levels[upper - 1].1,
                task.upper_level_sst_ids,
                "sst mismatched"
            );
            new_state.levels[upper - 1]
                .1
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        } else {
            assert_eq!(
                snapshot.l0_sstables, task.upper_level_sst_ids,
                "sst mismatched"
            );
            new_state
                .l0_sstables
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        }

        // replace the lower level with the list of SST ids which is newly-built
        new_state.levels[task.lower_level - 1].1 = output.to_vec();

        let del_sst_ids = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .copied()
            .collect();

        (new_state, del_sst_ids)
    }
}
