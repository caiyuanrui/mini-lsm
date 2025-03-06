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

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: Default::default(),
                current: None,
            };
        }
        let mut iters = iters
            .into_iter()
            .enumerate()
            .filter_map(|(i, iter)| iter.is_valid().then_some(HeapWrapper(i, iter)))
            .collect::<BinaryHeap<_>>();
        let current = iters.pop();
        Self { iters, current }
    }
}

impl<I> StorageIterator for MergeIterator<I>
where
    I: 'static,
    I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().map(|c| c.1.key()).unwrap()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().map(|c| c.1.value()).unwrap()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|c| c.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        // current must be valid, if we have tranverse all elements, current will be assigned None
        let current = match self.current.as_mut() {
            Some(current) => current,
            None => return Ok(()),
        };
        let current_key = current.1.key();

        // advance all iters until no key is identical to the current key
        while let Some(mut pm) = self.iters.peek_mut() {
            if pm.1.key() != current_key {
                break;
            }

            // advance peek iter, if it returns err, remove it from iters
            if let e @ Err(_) = pm.1.next() {
                PeekMut::pop(pm);
                return e;
            }
            // if peek iter is not valid, we need to evict it from the binary heap
            if !pm.1.is_valid() {
                PeekMut::pop(pm);
            }
        }

        // then we advance current iter
        // note that, there is no identical keys in one iterator
        current.1.next()?;

        if !current.1.is_valid() {
            self.current = self.iters.pop();
        } else if let Some(mut pm) = self.iters.peek_mut() {
            if current < &mut pm {
                std::mem::swap(current, &mut pm);
            }
        }

        Ok(())
    }

    /// the sum of `num_active_iterators` of all children iterators
    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|hw| hw.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|hw| hw.1.num_active_iterators())
                .unwrap_or(0)
    }
}

#[cfg(test)]
mod my_tests {
    use std::collections::BTreeMap;

    use proptest::{collection::btree_map, prelude::*, proptest};

    use crate::iterators::mock_iterator::{key_of, to_string, value_of, MockIterator};

    use super::*;

    proptest! {
      #[test]
      fn test_merge_iterator(data in prop::collection::vec(btree_map(any::<u32>(), any::<u32>(), 1..1000), 2..10)) {
        let mut runs = Vec::new();
        for sorted_run in data {
          let mut data: Vec<_> = sorted_run.into_iter().map(|(k, v)| (key_of(k), value_of(v))).collect();
          data.sort_unstable();
          runs.push(data);
        }

        let iters: Vec<_> = runs.iter().map(|run| {
          Box::new(MockIterator::new(run.clone()))
        }).collect();

        let mut merge_iter = MergeIterator::create(iters);

        let mut expected_data = BTreeMap::new();
        for run in runs.iter() {
            for (key, value) in run.iter() {
              expected_data.entry(key.clone()).or_insert(value.clone());
            }
        }
        for (key, value) in expected_data {
          assert!(merge_iter.is_valid());
          assert_eq!(merge_iter.key().key_ref(), key.as_ref(), "Key Mismatch: {} {}", to_string(&key), to_string(merge_iter.key().key_ref()));
          assert_eq!(merge_iter.value(), value.as_ref(), "Value Mismatch: {} {}", to_string(&value), to_string(merge_iter.value()));
          merge_iter.next().unwrap();
        }
        assert!(!merge_iter.is_valid());
      }
    }
}
