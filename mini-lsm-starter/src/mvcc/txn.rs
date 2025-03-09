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

use std::{
    collections::BTreeSet,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::{map::Entry, SkipMap};
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound,
};

use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(WriteSet, ReadSet)>>,
}

#[derive(Debug, Default)]
pub struct ReadSet {
    point_reads: BTreeSet<Bytes>,
    range_reads: Vec<(Bound<Bytes>, Bound<Bytes>)>,
}

#[derive(Debug, Default)]
pub struct WriteSet {
    point_writes: BTreeSet<Bytes>,
}

impl ReadSet {
    fn add_point(&mut self, key: Bytes) {
        self.point_reads.insert(key);
    }

    fn add_range(&mut self, lower: Bound<Bytes>, upper: Bound<Bytes>) {
        self.range_reads.push((lower, upper));
    }

    fn is_key_overlap(&self, key: &Bytes) -> bool {
        if self.point_reads.contains(key) {
            return true;
        }
        for (lower, upper) in &self.range_reads {
            let lower_ok = match lower {
                Bound::Unbounded => true,
                Bound::Included(lower) => lower <= key,
                Bound::Excluded(lower) => lower < key,
            };
            let upper_ok = match upper {
                Bound::Unbounded => true,
                Bound::Included(upper) => upper >= key,
                Bound::Excluded(upper) => upper > key,
            };
            if lower_ok && upper_ok {
                return true;
            }
        }
        false
    }
}

impl WriteSet {
    fn add_point(&mut self, key: Bytes) {
        self.point_writes.insert(key);
    }
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        assert!(
            !self.committed.load(Ordering::Acquire),
            "cannot operate on committed txn"
        );
        if let Some(ref key_hashes) = self.key_hashes {
            let read_set = &mut key_hashes.lock().1;
            read_set.add_point(Bytes::copy_from_slice(key));
        }
        if let Some(entry) = self.local_storage.get(key) {
            let value = entry.value();
            return Ok((!value.is_empty()).then_some(value.clone()));
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        assert!(
            !self.committed.load(Ordering::Acquire),
            "cannot operate on committed txn"
        );
        if let Some(ref key_hashes) = self.key_hashes {
            let read_set = &mut key_hashes.lock().1;
            read_set.add_range(map_bound(lower), map_bound(upper));
        }
        let mut local_iter = TxnLocalIterator::new(
            self.local_storage.clone(),
            |map| map.range((map_bound(lower), map_bound(upper))),
            Default::default(),
        );
        local_iter.next().unwrap();
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        assert!(
            !self.committed.load(Ordering::Acquire),
            "cannot operate on committed txn"
        );
        if let Some(ref key_hashes) = self.key_hashes {
            let write_set = &mut key_hashes.lock().0;
            write_set.add_point(Bytes::copy_from_slice(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        assert!(
            !self.committed.load(Ordering::Acquire),
            "cannot operate on committed txn"
        );
        if let Some(ref key_hashes) = self.key_hashes {
            let write_set = &mut key_hashes.lock().0;
            write_set.add_point(Bytes::copy_from_slice(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        let _commit_lock = self.inner.mvcc().commit_lock.lock();

        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .expect("cannot operate on committed txn");

        if self.inner.options.serializable && !self.check_serializable() {
            bail!("serialization failed");
        }

        let batch: Vec<_> = self
            .local_storage
            .iter()
            .map(|entry| {
                let (key, value) = (entry.key(), entry.value());
                if value.is_empty() {
                    WriteBatchRecord::Del(key.clone())
                } else {
                    WriteBatchRecord::Put(key.clone(), value.clone())
                }
            })
            .collect();
        let commit_ts: u64 = self.inner.write_batch_inner(&batch[..])?;

        if let Some(ref key_hashes) = self.key_hashes {
            let mut committed_txns = self.inner.mvcc().committed_txns.lock();
            let mut key_hashes = key_hashes.lock();
            let write_set = &mut key_hashes.0;
            let old_data = committed_txns.insert(
                commit_ts,
                CommittedTxnData::new(
                    std::mem::take(&mut write_set.point_writes),
                    self.read_ts,
                    commit_ts,
                ),
            );
            debug_assert!(old_data.is_none());

            let watermark = self.inner.mvcc().watermark();
            while let Some(entry) = committed_txns.first_entry() {
                if *entry.key() < watermark {
                    entry.remove_entry();
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    fn check_serializable(&self) -> bool {
        let (write_set, read_set) = &*self.key_hashes.as_ref().unwrap().lock();
        log::debug!("write_set: {:?}, read_set: {:?}", write_set, read_set);
        log::debug!(
            "committed_txns: {:?}",
            self.inner.mvcc().committed_txns.lock()
        );
        write_set.point_writes.is_empty()
            || self
                .inner
                .mvcc()
                .committed_txns
                .lock()
                .range(self.read_ts + 1..)
                .all(|(_, txn)| txn.keys.iter().all(|key| !read_set.is_key_overlap(key)))
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut ts = self.inner.mvcc().ts.lock();
        ts.1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_default()
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|user| {
            *user.item = Self::entry_to_item(user.iter.next());
        });
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter };
        iter.skip_deletes()?;
        if iter.is_valid() {
            iter.add_to_read_set(iter.key());
        }
        Ok(iter)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.next()?;
        }
        Ok(())
    }

    fn add_to_read_set(&self, key: &[u8]) {
        if let Some(ref key_hashes) = self.txn.key_hashes {
            let read_set = &mut key_hashes.lock().1;
            read_set.add_point(Bytes::copy_from_slice(key));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deletes()?;
        if self.is_valid() {
            self.add_to_read_set(self.key());
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}

#[cfg(test)]
mod my_tests {
    use tempfile::tempdir;

    use crate::{
        compact::CompactionOptions,
        lsm_storage::{LsmStorageOptions, MiniLsm},
    };

    use super::*;

    #[test]
    fn test_serializable_6_scan_serializable() {
        let get_len = |mut iter: TxnIterator| {
            let mut len = 0;
            while iter.is_valid() {
                len += 1;
                iter.next().unwrap();
            }
            len
        };
        let dir = tempdir().unwrap();
        let mut options =
            LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
        options.serializable = true;
        let storage = MiniLsm::open(&dir, options.clone()).unwrap();
        storage.put(b"key1", b"1").unwrap();
        storage.put(b"key2", b"2").unwrap();
        let txn1 = storage.new_txn().unwrap();
        let txn2 = storage.new_txn().unwrap();
        let len = get_len(txn1.scan(Bound::Unbounded, Bound::Unbounded).unwrap());
        txn1.put(b"key_len", format!("{len}").as_bytes());
        let len = get_len(txn2.scan(Bound::Unbounded, Bound::Unbounded).unwrap());
        txn2.put(b"key_len", format!("{len}").as_bytes());
        txn1.commit().unwrap();
        assert!(txn2.commit().is_err());
        assert_eq!(storage.get(b"key_len").unwrap(), Some(Bytes::from("2")));
    }
}
