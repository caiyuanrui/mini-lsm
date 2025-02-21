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

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
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

    /// Takes the state snapshot, compaction task and new sstables' indices,
    /// then returns the updated state and the sstables' indices to be removed.\
    /// Note that only `l0_sstables` and `levels` are updated (they are indices).
    /// You still need to synchronize the other state fields if they might have changed.
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

fn get_first_last_key_from_iter_for_debug(
    mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
) -> (String, String) {
    let mut first_key = Vec::new();
    let mut last_key = Vec::new();
    while iter.is_valid() {
        last_key = iter.key().raw_ref().to_vec();
        if first_key.is_empty() {
            first_key = iter.key().raw_ref().to_vec();
        }
        iter.next().unwrap();
    }
    (
        String::from_utf8(first_key).unwrap(),
        String::from_utf8(last_key).unwrap(),
    )
}

fn print_levels_for_debug(snapshot: &LsmStorageState) {
    let mut levels: Vec<(usize, Vec<(String, String)>)> = Vec::new();
    let mut key_ranges: Vec<(String, String)> = Vec::new();
    for sst_id in &snapshot.l0_sstables {
        let sst = &snapshot.sstables[sst_id];
        let key = String::from_utf8(sst.first_key().raw_ref().to_vec()).unwrap();
        let value = String::from_utf8(sst.last_key().raw_ref().to_vec()).unwrap();
        key_ranges.push((key, value));
    }
    levels.push((0, key_ranges));

    for (level, sst_ids) in &snapshot.levels {
        let mut key_ranges: Vec<(String, String)> = Vec::new();
        for sst_id in sst_ids {
            let sst = &snapshot.sstables[sst_id];
            let key = String::from_utf8(sst.first_key().raw_ref().to_vec()).unwrap();
            let value = String::from_utf8(sst.last_key().raw_ref().to_vec()).unwrap();
            key_ranges.push((key, value));
        }
        levels.push((*level, key_ranges));
    }

    for (level, key_ranges) in levels {
        log::debug!("========== L{level} ==========");
        for (k1, k2) in key_ranges {
            log::debug!("{k1} {k2}");
        }
    }
}

impl LsmStorageInner {
    /// compact the sstables produced by the iterator and return a sorted run.
    fn compact_with_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut sstables = Vec::new();
        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder_inner = builder.as_mut().unwrap();
            let (key, value) = (iter.key(), iter.value());
            if !compact_to_bottom_level || !value.is_empty() {
                builder_inner.add(key, value);
            }
            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let id = self.next_sst_id();
                sstables.push(Arc::new(builder.take().unwrap().build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?));
            }
            iter.next()?;
        }
        if let Some(builder) = builder {
            let id = self.next_sst_id();
            sstables.push(Arc::new(builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?));
        }
        Ok(sstables)
    }

    /// `compact` does the actual compaction job that merges some SST files and return a set of new SST files.
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let snapshot = { self.state.read().clone() };
                let l0_iter = {
                    let mut iters = Vec::new();
                    for id in l0_sstables {
                        let table = snapshot.sstables.get(id).unwrap().clone();
                        iters.push(Box::new(SsTableIterator::create_and_seek_to_first(table)?));
                    }
                    MergeIterator::create(iters)
                };
                let l1_iter = SstConcatIterator::create_and_seek_to_first(
                    l1_sstables
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect(),
                )?;
                let iter = TwoMergeIterator::create(l0_iter, l1_iter)?;
                let sstables = self.compact_with_iter(iter, task.compact_to_bottom_level())?;
                Ok(sstables)
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => {
                let state = self.state.read();

                match upper_level {
                    // merge >=L1
                    Some(upper_level) => {
                        let upper_iter = SstConcatIterator::create_and_seek_to_first(
                            upper_level_sst_ids
                                .iter()
                                .map(|id| state.sstables.get(id).unwrap().clone())
                                .collect(),
                        )?;
                        let lower_iter = SstConcatIterator::create_and_seek_to_first(
                            lower_level_sst_ids
                                .iter()
                                .map(|id| state.sstables.get(id).unwrap().clone())
                                .collect(),
                        )?;
                        let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_with_iter(iter, task.compact_to_bottom_level())
                    }
                    // merge l0 with l1
                    None => {
                        let upper_iter = {
                            let mut iters = Vec::new();
                            for id in upper_level_sst_ids {
                                let table = state.sstables.get(id).unwrap().clone();
                                let iter = SsTableIterator::create_and_seek_to_first(table)?;
                                iters.push(Box::new(iter));
                            }
                            MergeIterator::create(iters)
                        };
                        let lower_iter = SstConcatIterator::create_and_seek_to_first(
                            lower_level_sst_ids
                                .iter()
                                .map(|id| state.sstables.get(id).unwrap().clone())
                                .collect(),
                        )?;

                        let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_with_iter(iter, task.compact_to_bottom_level())
                    }
                }
            }
            _ => panic!("not support yet"),
        }
    }

    /// `force_full_compaction` is the compaction trigger that decides which files to compact and update the LSM state.
    pub fn force_full_compaction(&self) -> Result<()> {
        // snapshot 中 L0 和 L1 的索引会被更新，sstables 的索引表也会被更新
        // 但是在此期间，L0 仍然可能有新的数据写入，并且内存中的数据也会被更新如 memtables、imm_memtables
        // 因此不可以直接用 snapshot 替换 state，而是只替换部分字段
        let state_lock = self.state_lock.lock();
        let mut snapshot = { self.state.read().as_ref().clone() };

        let (l0_sstables, l1_sstables) =
            (snapshot.l0_sstables.clone(), snapshot.levels[0].1.clone());
        let sstables_to_remove = [l0_sstables.clone(), l1_sstables.clone()].concat();

        debug_assert_eq!(1, snapshot.levels[0].0);

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.to_vec(),
            l1_sstables: l1_sstables.to_vec(),
        };
        let new_sstables = self.compact(&task)?;

        // remove indices
        snapshot.levels[0].1 = new_sstables.iter().map(|table| table.sst_id()).collect();
        snapshot
            .l0_sstables
            .retain(|id| !sstables_to_remove.contains(id));
        // remove sstables
        for id in &sstables_to_remove {
            let result = snapshot.sstables.remove(id);
            debug_assert!(result.is_some(), "{id}");
        }
        // insert sstables (l0 + l1 -> l1)
        for table in new_sstables {
            let result = snapshot.sstables.insert(table.sst_id(), table);
            debug_assert!(result.is_none(), "{}", result.unwrap().sst_id());
        }

        let mut state = self.state.write();
        let mut new_state = state.as_ref().clone();
        new_state.sstables = snapshot.sstables;
        new_state.l0_sstables = snapshot.l0_sstables;
        new_state.levels = snapshot.levels;
        *state = Arc::new(new_state);

        drop(state);
        drop(state_lock);

        for id in sstables_to_remove {
            std::fs::remove_file(self.path_of_sst(id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = { self.state.read().clone() };
        let task = match self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        {
            Some(task) => task,
            None => return Ok(()),
        };

        let sstables = self.compact(&task)?;
        let output: Vec<usize> = sstables.iter().map(|x| x.sst_id()).collect();

        let ssts_to_remove = {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            let mut new_sst_ids = Vec::new();
            for sst_to_add in &sstables {
                new_sst_ids.push(sst_to_add.sst_id());
                let result = snapshot
                    .sstables
                    .insert(sst_to_add.sst_id(), sst_to_add.clone());
                assert!(result.is_none());
            }
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);
            let mut ssts_to_remove = Vec::new();
            // can we adjust the order to avoid the resize of the hashmap?
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some(), "cannot remove {file_to_remove}.sst");
                ssts_to_remove.push(result.unwrap());
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            ssts_to_remove
        };

        log::debug!(
            "compaction finished: {} files removed, {} files added, output={:?}",
            ssts_to_remove.len(),
            output.len(),
            output
        );
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }

        Ok(())
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
                            log::error!("compaction failed: {}", e);
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
                        log::debug!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
