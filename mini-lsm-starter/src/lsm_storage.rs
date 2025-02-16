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

use std::collections::HashMap;
use std::ops::{Bound, Not, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    // To protect the creation of the new memtable and allow other readers to operate concurrently
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        unimplemented!()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state_snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        if let Some(value) = state_snapshot.memtable.get(key) {
            return Ok(value.is_empty().not().then_some(value));
        }

        for imm_memtable in &state_snapshot.imm_memtables {
            if let Some(value) = imm_memtable.get(key) {
                return Ok(value.is_empty().not().then_some(value));
            }
        }

        let mut iters = Vec::new();
        for idx in &state_snapshot.l0_sstables {
            if let Some(sst_table) = state_snapshot.sstables.get(idx).map(Arc::clone) {
                let iter = SsTableIterator::create_and_seek_to_first(sst_table)?;
                if iter.is_valid() {
                    iters.push(Box::new(iter));
                }
            }
        }

        for idx in &state_snapshot.l0_sstables {
            let sst_table = match state_snapshot.sstables.get(idx) {
                Some(sst_table) => sst_table.clone(),
                None => continue,
            };
            let iter =
                SsTableIterator::create_and_seek_to_key(sst_table, KeySlice::from_slice(key))?;
            if iter.is_valid() && iter.key().raw_ref() == key {
                if iter.value() == b"" {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // put things into the memtable, and drop the read lock on LSM state
        {
            let state = self.state.read();
            state.memtable.put(key, value)?;
        }

        if { self.state.read().memtable.approximate_size() } > self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            if { self.state.read().memtable.approximate_size() } > self.options.target_sst_size {
                // gonna require write lock on `state`
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, b"")
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // Protected by _state_lock_observer
        // `create_with_wal` involves I/O operations, which might takes 1 millisecond.
        // In this way, other threads can modify the `state` concurrently when we are creating a new memtable.
        let next_id = self.next_sst_id();
        let next_memtable = Arc::new(MemTable::create(next_id));

        {
            // Protected by write lockguard, only one thread can access `state`
            let mut lock = self.state.write();
            // This is a brand-new state, but every field still points to the original address
            let mut state_cloned = lock.as_ref().clone();

            // 1. Replace the current memtable with the new one
            let old_memtable = std::mem::replace(&mut state_cloned.memtable, next_memtable);
            // 2. Insert the current memtable into the head of the immu_memtables
            state_cloned.imm_memtables.insert(0, old_memtable);
            // 3. Update the self's state with the new one
            *lock = Arc::new(state_cloned);
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        let mut snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };

        // 1. select a memtable to flush
        let memtable_to_flush = match snapshot.imm_memtables.pop() {
            Some(value) => value,
            None => return Ok(()),
        };

        // 2. create a sst file corresponding to a memtable
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut builder)?;

        let path = self.path_of_sst(memtable_to_flush.id());
        let sst = builder.build(self.next_sst_id(), Some(self.block_cache.clone()), path)?;

        // 3. remove the metable from the imm memtable lists and add the SST file to l0 SSTS
        // please note that the l0_sstables stores from the latest to the earliest
        snapshot.l0_sstables.insert(0, sst.sst_id());
        snapshot.sstables.insert(sst.sst_id(), Arc::new(sst));

        let mut state = self.state.write();
        *state = Arc::new(snapshot);

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mut mt_iters = Vec::with_capacity(state.imm_memtables.len() + 1);
        mt_iters.push(Box::new(state.memtable.scan(lower, upper)));
        mt_iters.extend(
            state
                .imm_memtables
                .iter()
                .map(|mt| Box::new(mt.scan(lower, upper))),
        );

        let key_slice = match lower {
            Bound::Included(bytes) => KeySlice::from_slice(bytes),
            Bound::Excluded(bytes) => KeySlice::from_slice(bytes),
            Bound::Unbounded => KeySlice::default(),
        };

        let mut sst_iters = Vec::with_capacity(state.l0_sstables.len());
        for idx in &state.l0_sstables {
            let sst_table = match state.sstables.get(idx) {
                // sst_table is bounded and inclusive
                Some(sst_table)
                    if has_overlap(
                        (
                            sst_table.first_key().raw_ref(),
                            sst_table.last_key().raw_ref(),
                        ),
                        (lower, upper),
                    ) =>
                {
                    sst_table.clone()
                }
                _ => continue,
            };
            let sst_iter = {
                let mut sst_iter = SsTableIterator::create_and_seek_to_key(sst_table, key_slice)?;
                if let Bound::Excluded(key) = lower {
                    if key == key_slice.raw_ref() {
                        sst_iter.next()?;
                    }
                }
                if !sst_iter.is_valid() {
                    continue;
                }
                sst_iter
            };
            sst_iters.push(Box::new(sst_iter));
        }

        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                MergeIterator::create(mt_iters),
                MergeIterator::create(sst_iters),
            )?,
            upper,
        )?))
    }

    pub fn scan_range(&self, range: impl RangeBounds<[u8]>) -> Result<FusedIterator<LsmIterator>> {
        self.scan(range.start_bound(), range.end_bound())
    }
}

// r1 is bounded and inclusive
fn has_overlap<Idx: PartialOrd>(r1: (Idx, Idx), r2: (Bound<Idx>, Bound<Idx>)) -> bool {
    let (m, n) = r1;
    match r2 {
        // r1 :   |---| [m, n]
        // r2: ..--------.. (-inf, +inf)
        (Bound::Unbounded, Bound::Unbounded) => true,
        // r1:    |-----|
        // r2: ..----| (-inf, y)
        (Bound::Unbounded, Bound::Included(y)) => y >= m,
        // r1:    |-----|
        // r2: ..----o
        (Bound::Unbounded, Bound::Excluded(y)) => y > m,
        // r1:  |-----|
        // r2:    |-------..
        (Bound::Included(x), Bound::Unbounded) => x <= n,
        // r1:  |-----|
        // r2:    o-------..
        (Bound::Excluded(x), Bound::Unbounded) => x < n,
        // r1:   |----|
        // r2: |----|
        (Bound::Included(x), Bound::Included(y)) => !(y < m || x > n),
        // r1: |----|
        // r2: o---|
        (Bound::Excluded(x), Bound::Included(y)) => !(y < m || x >= n),
        // r1: |----|
        // r2: |---o
        (Bound::Included(x), Bound::Excluded(y)) => !(y <= m || x > n),
        // r1: |----|
        // r2: o---o
        (Bound::Excluded(x), Bound::Excluded(y)) => !(y <= m || x >= n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;

    #[test]
    fn test_has_overlap_deepseek() {
        // 测试用例 1: r1 和 r2 都是无界的
        assert!(has_overlap((1, 5), (Bound::Unbounded, Bound::Unbounded)));

        // 测试用例 2: r2 是无界的下界，有界的上界（包含）
        assert!(has_overlap((1, 5), (Bound::Unbounded, Bound::Included(3))));
        assert!(!has_overlap((1, 5), (Bound::Unbounded, Bound::Included(0))));

        // 测试用例 3: r2 是无界的下界，有界的上界（排除）
        assert!(has_overlap((1, 5), (Bound::Unbounded, Bound::Excluded(3))));
        assert!(!has_overlap((1, 5), (Bound::Unbounded, Bound::Excluded(1))));

        // 测试用例 4: r2 是有界的下界（包含），无界的上界
        assert!(has_overlap((1, 5), (Bound::Included(3), Bound::Unbounded)));
        assert!(!has_overlap((1, 5), (Bound::Included(6), Bound::Unbounded)));

        // 测试用例 5: r2 是有界的下界（排除），无界的上界
        assert!(has_overlap((1, 5), (Bound::Excluded(3), Bound::Unbounded)));
        assert!(!has_overlap((1, 5), (Bound::Excluded(5), Bound::Unbounded)));

        // 测试用例 6: r2 是有界的下界（包含）和上界（包含）
        assert!(has_overlap(
            (1, 5),
            (Bound::Included(3), Bound::Included(4))
        ));
        assert!(!has_overlap(
            (1, 5),
            (Bound::Included(6), Bound::Included(7))
        ));

        // 测试用例 7: r2 是有界的下界（排除）和上界（包含）
        assert!(has_overlap(
            (1, 5),
            (Bound::Excluded(0), Bound::Included(3))
        ));
        assert!(!has_overlap(
            (1, 5),
            (Bound::Excluded(5), Bound::Included(6))
        ));

        // 测试用例 8: r2 是有界的下界（包含）和上界（排除）
        assert!(has_overlap(
            (1, 5),
            (Bound::Included(3), Bound::Excluded(6))
        ));
        assert!(!has_overlap(
            (1, 5),
            (Bound::Included(6), Bound::Excluded(7))
        ));

        // 测试用例 9: r2 是有界的下界（排除）和上界（排除）
        assert!(has_overlap(
            (1, 5),
            (Bound::Excluded(0), Bound::Excluded(6))
        ));
        assert!(!has_overlap(
            (1, 5),
            (Bound::Excluded(5), Bound::Excluded(6))
        ));

        // 测试用例 10: r1 和 r2 完全不重叠
        assert!(!has_overlap(
            (1, 5),
            (Bound::Included(6), Bound::Included(7))
        ));
        assert!(!has_overlap(
            (1, 5),
            (Bound::Excluded(5), Bound::Excluded(6))
        ));

        // 测试用例 11: r1 和 r2 完全重叠
        assert!(has_overlap(
            (1, 5),
            (Bound::Included(1), Bound::Included(5))
        ));
        assert!(has_overlap(
            (1, 5),
            (Bound::Excluded(0), Bound::Excluded(6))
        ));

        // 测试用例 12: r1 和 r2 部分重叠
        assert!(has_overlap(
            (1, 5),
            (Bound::Included(3), Bound::Included(7))
        ));
        assert!(has_overlap(
            (1, 5),
            (Bound::Excluded(0), Bound::Excluded(3))
        ));
    }

    #[test]
    fn test_has_overlap_chatgpt() {
        use std::ops::Bound::*;

        // Overlapping cases
        assert!(has_overlap((5, 10), (Unbounded, Unbounded))); // Unbounded range always overlaps
        assert!(has_overlap((5, 10), (Unbounded, Included(8)))); // Partially overlaps
        assert!(has_overlap((5, 10), (Included(7), Unbounded))); // Partially overlaps
        assert!(has_overlap((5, 10), (Included(6), Included(9)))); // Fully inside
        assert!(has_overlap((5, 10), (Included(10), Included(15)))); // Overlaps at boundary
        assert!(has_overlap((5, 10), (Excluded(4), Included(6)))); // Overlaps
        assert!(has_overlap((5, 10), (Excluded(9), Included(12)))); // Overlaps at 10

        // Non-overlapping cases
        assert!(!has_overlap((5, 10), (Included(11), Included(15)))); // Fully after
        assert!(!has_overlap((5, 10), (Included(1), Included(4)))); // Fully before
        assert!(!has_overlap((5, 10), (Excluded(10), Included(15)))); // Touching but not overlapping
        assert!(!has_overlap((5, 10), (Excluded(4), Excluded(5)))); // Touching but not overlapping

        // Edge cases
        assert!(has_overlap((5, 10), (Included(5), Included(5)))); // Single element overlap
        assert!(!has_overlap((5, 10), (Excluded(10), Excluded(11)))); // Touching at exclusive boundary
        assert!(has_overlap((5, 10), (Included(10), Excluded(11)))); // Touching at inclusive boundary
    }
}
