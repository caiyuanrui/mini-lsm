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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    #[cfg(debug_assertions)]
    #[track_caller]
    fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        for sst in sstables {
            assert!(sst.first_key() <= sst.last_key(),);
        }
        for win in sstables.windows(2) {
            assert!(win[0].last_key() < win[1].first_key(),)
        }
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        let mut iter = Self::create(sstables)?;
        iter.seek_to_first()?;
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        let mut iter = Self::create(sstables)?;
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    fn create(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        Ok(Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        })
    }

    fn seek_to_first(&mut self) -> Result<()> {
        if self.sstables.is_empty() {
            self.current = None;
            return Ok(());
        }
        let table = self.sstables[0].clone();
        self.current = Some(SsTableIterator::create_and_seek_to_first(table)?);
        self.next_sst_idx = 1;
        Ok(())
    }

    fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let idx = self
            .sstables
            .binary_search_by(|sst| {
                if sst.first_key().as_key_slice() > key {
                    std::cmp::Ordering::Greater
                } else if sst.last_key().as_key_slice() < key {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .unwrap_or_else(std::convert::identity);

        if idx >= self.sstables.len() {
            self.current = None;
            return Ok(());
        }

        self.next_sst_idx = idx + 1;
        let table = self.sstables[idx].clone();
        self.current = Some(SsTableIterator::create_and_seek_to_key(table, key)?);
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map_or(KeySlice::default(), |iter| iter.key())
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().map_or(&[], |iter| iter.value())
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|iter| iter.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(current_iter) = self.current.as_mut() {
            current_iter.next()?;

            while !current_iter.is_valid() && self.next_sst_idx < self.sstables.len() {
                let table = self.sstables[self.next_sst_idx].clone();
                *current_iter = SsTableIterator::create_and_seek_to_first(table)?;
                self.next_sst_idx += 1;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod my_tests {

    use tempfile::{tempdir, TempDir};

    use crate::{key::KeyVec, table::SsTableBuilder};

    use super::*;

    fn key_of(idx: usize) -> KeyVec {
        KeyVec::for_testing_from_vec_no_ts(format!("key_{:06}", idx).into_bytes())
    }

    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:0104}", idx).into_bytes()
    }

    fn generate_sst() -> (TempDir, SsTable) {
        let mut builder = SsTableBuilder::new(128);
        for idx in 0..5000 {
            let key = key_of(idx);
            let value = value_of(idx);
            builder.add(key.as_key_slice(), &value[..]);
        }
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        (dir, builder.build_for_test(path).unwrap())
    }

    #[test]
    fn test_concat_iterator() {
        let mut temp_dirs = Vec::new();
        let mut sstables = Vec::new();
        let mut expected = Vec::new();
        for i in 0..10 {
            let mut builder = SsTableBuilder::new(128);
            for idx in i * 5000..(i + 1) * 5000 {
                let key = key_of(idx);
                let value = value_of(idx);
                builder.add(key.as_key_slice(), &value[..]);
                expected.push((key, value));
            }
            let dir = tempdir().unwrap();
            let path = dir.path().join("1.sst");
            temp_dirs.push(dir);
            sstables.push(Arc::new(builder.build_for_test(path).unwrap()));
        }

        let mut iter = SstConcatIterator::create_and_seek_to_first(sstables).unwrap();

        for (expected_key, expected_value) in expected {
            assert!(
                iter.is_valid(),
                "{} {}",
                String::from_utf8(expected_key.raw_ref().to_vec()).unwrap(),
                String::from_utf8(expected_value.to_vec()).unwrap()
            );
            assert_eq!(expected_key.as_key_slice(), iter.key());
            assert_eq!(expected_value.as_slice(), iter.value());
            iter.next().unwrap();
        }
        assert!(!iter.is_valid());
    }
}
