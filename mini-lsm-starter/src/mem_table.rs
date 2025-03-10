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

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    pub map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn map_bound_plus_ts(bound: Bound<&[u8]>, ts: u64) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, ts)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, ts)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn map_key_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            wal: Some(Wal::create(path)?),
            ..Self::create(id)
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());
        let wal = Wal::recover(path, &map)?;
        let size = wal.size() as usize;
        Ok(Self {
            id,
            map,
            wal: Some(wal),
            approximate_size: Arc::new(AtomicUsize::new(size)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            map_bound_plus_ts(lower, TS_DEFAULT),
            map_bound_plus_ts(upper, TS_DEFAULT),
        )
    }

    pub fn for_debugging_print(&self) {
        println!(
            "id:{}, approximate_size: {}",
            self.id,
            self.approximate_size
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        for kv in self.map.iter() {
            let (k, v) = (kv.key(), kv.value());
            println!(
                "key: {:x}, val: {:x}",
                Bytes::copy_from_slice(k.key_ref()),
                v
            );
        }
    }

    /// Get a value by key.
    /// This interface should not be used in week 3
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        self.map
            .get(&KeyBytes::from_bytes_with_ts(
                Bytes::from_static(unsafe {
                    std::mem::transmute::<&[u8], &'static [u8]>(key.key_ref())
                }),
                key.ts(),
            ))
            .map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        if let Some(wal) = self.wal.as_ref() {
            wal.put_batch(data)?;
        }
        let mut size = 0;
        for (key, value) in data {
            self.map.insert(
                key.to_key_vec().into_key_bytes(),
                Bytes::copy_from_slice(value),
            );
            size += key.raw_len() + value.len();
        }
        self.approximate_size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iter = MemTableIterator::new(
            self.map.clone(),
            |map| map.range((map_key_bound(lower), map_key_bound(upper))),
            (KeyBytes::new(), Bytes::new()),
        );
        iter.next().unwrap();
        iter
    }

    // pub fn scan_range<'a>(&self, range: impl RangeBounds<KeySlice<'a>>) -> MemTableIterator {
    //     self.scan(range.start_bound().cloned(), range.end_bound().cloned())
    // }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key().as_key_slice(), entry.value());
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_default()
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|fields| *fields.item = entry);
        Ok(())
    }
}

#[cfg(test)]
mod my_tests {
    use crate::key::{TS_RANGE_BEGIN, TS_RANGE_END};

    use super::*;

    #[test]
    fn test_memtable_iter() {
        let table = MemTable::create(0);
        table.put(KeySlice::from_slice(b"1", 0), b"233").unwrap();
        table.put(KeySlice::from_slice(b"2", 1), b"2333").unwrap();
        table.put(KeySlice::from_slice(b"3", 2), b"23333").unwrap();
        table.put(KeySlice::from_slice(b"1", 3), b"").unwrap();
        let mut iter = table.scan(
            Bound::Included(KeySlice::from_slice(b"1", TS_RANGE_BEGIN)),
            Bound::Included(KeySlice::from_slice(b"1", TS_RANGE_END)),
        );
        while iter.is_valid() {
            println!(
                "{}/{} {}",
                String::from_utf8_lossy(iter.key().key_ref()),
                iter.key().ts(),
                String::from_utf8_lossy(iter.value()),
            );
            iter.next().unwrap();
        }
    }
}
