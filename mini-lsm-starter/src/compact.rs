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

// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

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
use crate::lsm_storage::{CompactionFilter, LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
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
        match self {
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction => true,
            Self::Tiered(_) => false,
        }
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
    /// compact the sstables produced by the iterator and return a sorted run.
    /// Note that, the `compact_to_bottom_level` is ignored
    /// and we allow one SST exceeds the size limit to contain all the same keys in one SST.
    /// (week3 day2)
    fn compact_with_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut sstables = Vec::new();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut last_key: Vec<u8> = Vec::new();
        let watermark = self.mvcc().watermark();
        let mut first_key_below_watermark = false;
        let compaction_filter = self.compaction_filters.lock().clone();

        'outer: while iter.is_valid() {
            let is_first_key = last_key != iter.key().key_ref();
            if is_first_key {
                first_key_below_watermark = true;
            }

            if builder.estimated_size() >= self.options.target_sst_size && is_first_key {
                let id = self.next_sst_id();
                sstables.push(Arc::new(builder.build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?));
                builder = SsTableBuilder::new(self.options.block_size);
            }

            if iter.key().ts() > watermark {
                first_key_below_watermark = true;
                builder.add(iter.key(), iter.value());
                last_key.clear();
                last_key.extend(iter.key().key_ref());
            } else if first_key_below_watermark {
                first_key_below_watermark = false;
                for filter in compaction_filter.iter() {
                    match filter {
                        CompactionFilter::Prefix(needle) => {
                            if iter.key().key_ref().starts_with(needle) {
                                iter.next()?;
                                continue 'outer;
                            }
                        }
                    }
                }
                if !iter.value().is_empty() || !compact_to_bottom_level {
                    builder.add(iter.key(), iter.value());
                }
                last_key.clear();
                last_key.extend(iter.key().key_ref());
            }

            iter.next()?;
        }
        if !builder.is_empty() {
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
    /// We do not hold state lock before the `compact` function executed. Handle the state carefully if the new ssts are flushed.
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
                lower_level: _,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => {
                let state = self.state.read();

                match upper_level {
                    // merge >=L1
                    Some(_upper_level) => {
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
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included: _,
            }) => {
                let snapshot = { self.state.read().clone() };
                let iter = {
                    let mut iters = Vec::new();
                    for (_, files_to_merge) in tiers {
                        iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                            files_to_merge
                                .iter()
                                .map(|x| snapshot.sstables[x].clone())
                                .collect(),
                        )?));
                    }
                    MergeIterator::create(iters)
                };
                self.compact_with_iter(iter, task.compact_to_bottom_level())
            }
        }
    }

    /// `force_full_compaction` is the compaction trigger that decides which files to compact and update the LSM state.
    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = { self.state.read().as_ref().clone() };

        let (l0_sstables, l1_sstables) =
            (snapshot.l0_sstables.clone(), snapshot.levels[0].1.clone());
        let sstables_to_remove = [l0_sstables.clone(), l1_sstables.clone()].concat();

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let new_sstables = self.compact(&task)?;
        let ids: Vec<usize> = new_sstables.iter().map(|x| x.sst_id()).collect();

        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            // remove indices
            state.levels[0].1 = ids.clone();
            state
                .l0_sstables
                .retain(|id| !sstables_to_remove.contains(id));
            // remove sstables
            for id in &sstables_to_remove {
                let result = state.sstables.remove(id);
                debug_assert!(result.is_some(), "{id}");
            }
            // insert sstables (l0 + l1 -> l1)
            for table in new_sstables {
                let result = state.sstables.insert(table.sst_id(), table);
                debug_assert!(result.is_none(), "{}", result.unwrap().sst_id());
            }

            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&state_lock, ManifestRecord::Compaction(task, ids))?;
        }

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
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            for sst_to_add in &sstables {
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
            drop(state);

            self.sync_dir()?;
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&state_lock, ManifestRecord::Compaction(task, output))?;

            ssts_to_remove
        };

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
