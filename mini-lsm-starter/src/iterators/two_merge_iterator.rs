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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
}

impl<A, B> TwoMergeIterator<A, B>
where
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b };
        iter.skip_b()?;
        Ok(iter)
    }

    fn choose_a(&self) -> bool {
        if !self.a.is_valid() {
            return false;
        }
        if !self.b.is_valid() {
            return true;
        }
        self.a.key() < self.b.key()
    }

    fn skip_b(&mut self) -> Result<()> {
        while self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() {
            self.b.next()?;
        }
        Ok(())
    }
}

impl<A, B> StorageIterator for TwoMergeIterator<A, B>
where
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.choose_a() {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.choose_a() {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.choose_a() {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.choose_a() {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.skip_b()?;
        Ok(())
    }

    /// the sum of `num_active_iterators` of all children iterators
    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}

#[cfg(test)]
mod my_tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;
    use crate::iterators::mock_iterator::*;

    proptest! {
        #[test]
        fn test_two_merge_iterator_with_random_data(
            data1 in prop::collection::btree_map(any::<u32>(), any::<u32>(), 1..1000),
            data2 in prop::collection::btree_map(any::<u32>(), any::<u32>(), 1..1000),
        ) {
            let mut data1: Vec<_> = data1.into_iter()
                .map(|(k, v)| (key_of(k), value_of(v)))
                .collect();
            let mut data2: Vec<_> = data2.into_iter()
                .map(|(k, v)| (key_of(k), value_of(v)))
                .collect();
            data1.sort_unstable();
            data2.sort_unstable();

            let mut expect_data = BTreeMap::new();
            expect_data.extend(data2.iter().cloned());
            expect_data.extend(data1.iter().cloned());

            let a = MockIterator::new(data1);
            let b = MockIterator::new(data2);
            let mut iter = TwoMergeIterator::create(a, b).unwrap();

            for (key, value) in expect_data {
                assert!(iter.is_valid());
                assert_eq!(key.as_ref(), iter.key().key_ref(), "Key Mismatch: {} {}", to_string(&key), to_string(iter.key().key_ref()));
                assert_eq!(value.as_ref(), iter.value(), "Value Mismatch: {} {}", to_string(&value), to_string(iter.value()));
                iter.next().unwrap();
            }
            assert!(!iter.is_valid());
        }
    }
}
