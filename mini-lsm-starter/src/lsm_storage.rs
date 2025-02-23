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

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::{Bound, Not, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, OnceLock};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};
use rayon::iter::{ParallelBridge, ParallelIterator};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

static LOGGER: OnceLock<()> = OnceLock::new();

fn init_logger() {
    LOGGER.get_or_init(|| {
        env_logger::builder()
            .format_file(true)
            .format_line_number(true)
            .init();
    });
}

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
        self.inner.sync_dir()?;
        self.flush_notifier.send(())?;
        self.compaction_notifier.send(())?;

        if let Some(handle) = self.flush_thread.lock().take() {
            handle
                .join()
                .map_err(|e| anyhow!("flush thread panics: {e:?}"))?;
        }
        if let Some(handle) = self.compaction_thread.lock().take() {
            handle
                .join()
                .map_err(|e| anyhow!("compaction thread panics: {e:?}"))?;
        }

        if self.inner.options.enable_wal {
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            // don't record the empty memtable's id in the manifest
            self.inner
                .freeze_memtable(Arc::new(MemTable::create(self.inner.next_sst_id())))?;
        }

        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        init_logger();
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

    pub fn scan_range(&self, range: impl RangeBounds<[u8]>) -> Result<FusedIterator<LsmIterator>> {
        self.scan(range.start_bound(), range.end_bound())
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
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1024));
        let mut next_sst_id = 1;

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

        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB directory")?;
        }
        let manifest_path = path.join("MANIFEST");
        let manifest;
        if !manifest_path.exists() {
            manifest = Manifest::create(manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(manifest_path)?;
            manifest = m;

            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable doesn't exist");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            // tiered compaction stragety
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(sst_id) => {
                        next_sst_id = next_sst_id.max(sst_id);
                        memtables.insert(sst_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, sst_ids) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }
            // recover SSTs
            let sst_cnt = AtomicUsize::new(0);
            let results: Result<Vec<_>> = state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|x| x.1.iter()))
                .par_bridge()
                .map(|&sst_id| {
                    let sst = Arc::new(SsTable::open(
                        sst_id,
                        Some(block_cache.clone()),
                        FileObject::open(&Self::path_of_sst_static(path, sst_id))?,
                    )?);
                    sst_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok::<_, anyhow::Error>((sst_id, sst))
                })
                .collect();
            for (sst_id, sst) in results? {
                state.sstables.insert(sst_id, sst);
            }
            println!(
                "{} SSTs opened",
                sst_cnt.load(std::sync::atomic::Ordering::Relaxed)
            );

            next_sst_id += 1;

            // sort each level for leveled compaction
            if let CompactionController::Leveled(_) = compaction_controller {
                for (_, sst_ids) in state.levels.iter_mut() {
                    sst_ids.sort_unstable_by_key(|x| state.sstables[x].first_key());
                }
            }
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: Arc::new(options),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()?;
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let key_slice = KeySlice::from_slice(key);
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        if let Some(value) = snapshot.memtable.get(key) {
            return Ok(value.is_empty().not().then_some(value));
        }

        for imm_memtable in &snapshot.imm_memtables {
            if let Some(value) = imm_memtable.get(key) {
                return Ok(value.is_empty().not().then_some(value));
            }
        }

        for idx in &snapshot.l0_sstables {
            let sst_table = match snapshot.sstables.get(idx) {
                Some(sst_table) if sst_table.may_contain_key(key) => sst_table.clone(),
                _ => continue,
            };
            let iter = SsTableIterator::create_and_seek_to_key(
                sst_table.clone(),
                KeySlice::from_slice(key),
            )?;
            if iter.is_valid() && iter.key().raw_ref() == key {
                if iter.value() == b"" {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        for (k, level) in snapshot.levels.iter() {
            let sstables: Vec<_> = level
                .iter()
                .map(|id| snapshot.sstables[id].clone())
                .collect();
            let concat_iter = SstConcatIterator::create_and_seek_to_key(sstables, key_slice)?;
            if concat_iter.is_valid() && concat_iter.key().raw_ref() == key {
                if concat_iter.value() == b"" {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(concat_iter.value())));
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
        assert!(!key.is_empty(), "key cannot be empty");
        {
            let state = self.state.read();
            state.memtable.put(key, value)?;
        }

        if { self.state.read().memtable.approximate_size() } >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            if { self.state.read().memtable.approximate_size() } >= self.options.target_sst_size {
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
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };
        self.freeze_memtable(memtable)?;
        self.manifest.as_ref().unwrap().add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;
        self.sync_dir()?;
        Ok(())
    }

    fn freeze_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        *guard = Arc::new(snapshot);
        old_memtable.sync_wal()?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let memtable_to_flush = {
            let state = self.state.read();
            match state.imm_memtables.last() {
                Some(value) => value.clone(),
                None => return Ok(()),
            }
        };

        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut builder)?;
        let sst_id = memtable_to_flush.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        let log_first_key = sst.first_key().raw_ref().to_vec();
        let log_last_key = sst.last_key().raw_ref().to_vec();

        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();

        snapshot.imm_memtables.retain(|x| x.id() != sst_id);
        snapshot.sstables.insert(sst_id, sst);
        if self.compaction_controller.flush_to_l0() {
            snapshot.l0_sstables.insert(0, sst_id);
        } else {
            snapshot.levels.insert(0, (sst_id, vec![sst_id]));
        }

        *guard = Arc::new(snapshot);
        drop(guard);

        self.sync_dir()?;
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&state_lock, ManifestRecord::Flush(sst_id))?;

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
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mut mt_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        mt_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        mt_iters.extend(
            snapshot
                .imm_memtables
                .iter()
                .map(|mt| Box::new(mt.scan(lower, upper))),
        );

        let key_slice = match lower {
            Bound::Included(bytes) => KeySlice::from_slice(bytes),
            Bound::Excluded(bytes) => KeySlice::from_slice(bytes),
            Bound::Unbounded => KeySlice::default(),
        };

        let mut l0_sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for idx in &snapshot.l0_sstables {
            let sst_table = match snapshot.sstables.get(idx) {
                // sst_table is bounded and inclusive
                Some(sst_table)
                    if range_overlap(
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
            l0_sst_iters.push(Box::new(sst_iter));
        }

        let mut level_iters = Vec::new();
        for (k, level) in snapshot.levels.iter() {
            level_iters.push(Box::new(SstConcatIterator::create_and_seek_to_key(
                level
                    .iter()
                    .map(|id| snapshot.sstables[id].clone())
                    .collect(),
                key_slice,
            )?));
        }

        let a = MergeIterator::create(mt_iters);
        let b = MergeIterator::create(l0_sst_iters);
        let mem_and_l0_iter = TwoMergeIterator::create(a, b)?;
        let l1_iter = MergeIterator::create(level_iters);
        let inner = TwoMergeIterator::create(mem_and_l0_iter, l1_iter)?;

        Ok(FusedIterator::new(LsmIterator::new(inner, upper)?))
    }

    pub fn scan_range(&self, range: impl RangeBounds<[u8]>) -> Result<FusedIterator<LsmIterator>> {
        self.scan(range.start_bound(), range.end_bound())
    }
}

// r1 is bounded and inclusive
fn range_overlap<Idx: PartialOrd>(r1: (Idx, Idx), r2: (Bound<Idx>, Bound<Idx>)) -> bool {
    let (a, b) = r1;
    let (c, d) = r2;
    let left_check = match c {
        Bound::Included(c) => b < c,  // [c, ...
        Bound::Excluded(c) => b <= c, // (c, ...
        Bound::Unbounded => false,    // (-∞, ...
    };
    let right_check = match d {
        Bound::Included(d) => a > d,  // ..., d]
        Bound::Excluded(d) => a >= d, // ..., d)
        Bound::Unbounded => false,    // ..., +∞)
    };
    !(left_check || right_check)
}

#[cfg(test)]
mod my_tests {
    use super::*;
    use std::ops::Bound;

    #[test]
    fn test_range_overlap_deepseek() {
        // 测试用例 1: r1 和 r2 都是无界的
        assert!(range_overlap((1, 5), (Bound::Unbounded, Bound::Unbounded)));

        // 测试用例 2: r2 是无界的下界，有界的上界（包含）
        assert!(range_overlap(
            (1, 5),
            (Bound::Unbounded, Bound::Included(3))
        ));
        assert!(!range_overlap(
            (1, 5),
            (Bound::Unbounded, Bound::Included(0))
        ));

        // 测试用例 3: r2 是无界的下界，有界的上界（排除）
        assert!(range_overlap(
            (1, 5),
            (Bound::Unbounded, Bound::Excluded(3))
        ));
        assert!(!range_overlap(
            (1, 5),
            (Bound::Unbounded, Bound::Excluded(1))
        ));

        // 测试用例 4: r2 是有界的下界（包含），无界的上界
        assert!(range_overlap(
            (1, 5),
            (Bound::Included(3), Bound::Unbounded)
        ));
        assert!(!range_overlap(
            (1, 5),
            (Bound::Included(6), Bound::Unbounded)
        ));

        // 测试用例 5: r2 是有界的下界（排除），无界的上界
        assert!(range_overlap(
            (1, 5),
            (Bound::Excluded(3), Bound::Unbounded)
        ));
        assert!(!range_overlap(
            (1, 5),
            (Bound::Excluded(5), Bound::Unbounded)
        ));

        // 测试用例 6: r2 是有界的下界（包含）和上界（包含）
        assert!(range_overlap(
            (1, 5),
            (Bound::Included(3), Bound::Included(4))
        ));
        assert!(!range_overlap(
            (1, 5),
            (Bound::Included(6), Bound::Included(7))
        ));

        // 测试用例 7: r2 是有界的下界（排除）和上界（包含）
        assert!(range_overlap(
            (1, 5),
            (Bound::Excluded(0), Bound::Included(3))
        ));
        assert!(!range_overlap(
            (1, 5),
            (Bound::Excluded(5), Bound::Included(6))
        ));

        // 测试用例 8: r2 是有界的下界（包含）和上界（排除）
        assert!(range_overlap(
            (1, 5),
            (Bound::Included(3), Bound::Excluded(6))
        ));
        assert!(!range_overlap(
            (1, 5),
            (Bound::Included(6), Bound::Excluded(7))
        ));

        // 测试用例 9: r2 是有界的下界（排除）和上界（排除）
        assert!(range_overlap(
            (1, 5),
            (Bound::Excluded(0), Bound::Excluded(6))
        ));
        assert!(!range_overlap(
            (1, 5),
            (Bound::Excluded(5), Bound::Excluded(6))
        ));

        // 测试用例 10: r1 和 r2 完全不重叠
        assert!(!range_overlap(
            (1, 5),
            (Bound::Included(6), Bound::Included(7))
        ));
        assert!(!range_overlap(
            (1, 5),
            (Bound::Excluded(5), Bound::Excluded(6))
        ));

        // 测试用例 11: r1 和 r2 完全重叠
        assert!(range_overlap(
            (1, 5),
            (Bound::Included(1), Bound::Included(5))
        ));
        assert!(range_overlap(
            (1, 5),
            (Bound::Excluded(0), Bound::Excluded(6))
        ));

        // 测试用例 12: r1 和 r2 部分重叠
        assert!(range_overlap(
            (1, 5),
            (Bound::Included(3), Bound::Included(7))
        ));
        assert!(range_overlap(
            (1, 5),
            (Bound::Excluded(0), Bound::Excluded(3))
        ));
    }

    #[test]
    fn test_range_overlap_chatgpt() {
        use std::ops::Bound::*;

        // Overlapping cases
        assert!(range_overlap((5, 10), (Unbounded, Unbounded))); // Unbounded range always overlaps
        assert!(range_overlap((5, 10), (Unbounded, Included(8)))); // Partially overlaps
        assert!(range_overlap((5, 10), (Included(7), Unbounded))); // Partially overlaps
        assert!(range_overlap((5, 10), (Included(6), Included(9)))); // Fully inside
        assert!(range_overlap((5, 10), (Included(10), Included(15)))); // Overlaps at boundary
        assert!(range_overlap((5, 10), (Excluded(4), Included(6)))); // Overlaps
        assert!(range_overlap((5, 10), (Excluded(9), Included(12)))); // Overlaps at 10

        // Non-overlapping cases
        assert!(!range_overlap((5, 10), (Included(11), Included(15)))); // Fully after
        assert!(!range_overlap((5, 10), (Included(1), Included(4)))); // Fully before
        assert!(!range_overlap((5, 10), (Excluded(10), Included(15)))); // Touching but not overlapping
        assert!(!range_overlap((5, 10), (Excluded(4), Excluded(5)))); // Touching but not overlapping

        // Edge cases
        assert!(range_overlap((5, 10), (Included(5), Included(5)))); // Single element overlap
        assert!(!range_overlap((5, 10), (Excluded(10), Excluded(11)))); // Touching at exclusive boundary
        assert!(range_overlap((5, 10), (Included(10), Excluded(11)))); // Touching at inclusive boundary
    }
}
