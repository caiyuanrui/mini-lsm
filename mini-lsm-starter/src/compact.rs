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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    /// `compact` does the actual compaction job that merges some SST files and return a set of new SST files.
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let sstables_snapshot = { &self.state.read().sstables };
                let mut iters = Vec::new();
                for idx in l0_sstables.iter().chain(l1_sstables.iter()) {
                    // println!("chained idx: {idx}");
                    if let Some(table) = sstables_snapshot.get(idx).cloned() {
                        let iter = SsTableIterator::create_and_seek_to_first(table)?;
                        iters.push(Box::new(iter));
                    }
                }

                let mut iter = MergeIterator::create(iters);
                let mut builder = SsTableBuilder::new(self.options.block_size);
                let mut builders = Vec::new();

                while iter.is_valid() {
                    // please note that we ignored deleted key-value pair here
                    // because at present we only compact all ssts
                    // but the same assumption doesn't hold in the future
                    let (key, value) = (iter.key(), iter.value());

                    // println!(
                    //     "{} {}",
                    //     String::from_utf8_lossy(key.raw_ref()),
                    //     String::from_utf8_lossy(value)
                    // );

                    if !key.is_empty() && !value.is_empty() {
                        builder.add(key, value);
                    }

                    if builder.estimated_size() >= self.options.target_sst_size {
                        let builder = std::mem::replace(
                            &mut builder,
                            SsTableBuilder::new(self.options.block_size),
                        );
                        builders.push(builder);
                    }

                    iter.next()?;
                }

                // we cannot do this because the builder might not get freezed yet
                // which means the extimated size is zero
                // if builder.estimated_size() > 0 {
                //     builders.push(builder);
                // }
                if !builder.is_empty() {
                    builders.push(builder);
                }

                let mut sstables = Vec::new();
                for builder in builders {
                    let id = self.next_sst_id();
                    let block_cache = Some(self.block_cache.clone());
                    let path = self.path_of_sst(id);
                    let sst = Arc::new(builder.build(id, block_cache, path)?);
                    sstables.push(sst);
                }

                Ok(sstables)
            }
            _ => panic!("not support yet"),
        }
    }

    /// `force_full_compaction` is the compaction trigger that decides which files to compact and update the LSM state.
    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            assert_eq!(1, state.levels[0].0);
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let new_l1_sstables = self.compact(&task)?;

        // fn print_state(state: &LsmStorageState) {
        //     println!("l0_sstables: {:?}", state.l0_sstables);
        //     println!("levels {:?}", state.levels);
        //     println!(
        //         "sstable indices: {:?}",
        //         state.sstables.iter().map(|item| item.0).collect::<Vec<_>>()
        //     )
        // }

        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut new_state = state.as_ref().clone();
            // first, remove compacted l0_ssts' id
            new_state
                .l0_sstables
                .retain(|sst_id| !l0_sstables.contains(sst_id));

            // second, remove l0 and l1 <sst_id, table> pairs
            new_state.sstables.retain(|sst_id, sstable| {
                !l0_sstables.contains(sst_id) && !l1_sstables.contains(sst_id)
            });

            // third, update l1 ssts
            let mut l1_sst_ids = Vec::new();
            for sst in new_l1_sstables {
                l1_sst_ids.push(sst.sst_id());
                new_state.sstables.insert(sst.sst_id(), sst);
            }
            // we assume that the compaction runs in a single thread
            new_state.levels[0] = (1, l1_sst_ids);

            *state = Arc::new(new_state);
        }

        // finally, remove tale files
        for &sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
            let path = self.path_of_sst(sst_id);
            if let Err(err) = std::fs::remove_file(path) {
                if err.kind() == std::io::ErrorKind::NotFound {
                    // eprintln!("sst file to be removed not found");
                } else {
                    bail!("failed to remove sst file: {:?}", err);
                }
            }
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let num_memtable = { self.state.read().imm_memtables.len() + 1 };
        if num_memtable > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
