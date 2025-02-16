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
        Ok(Self { a, b })
    }
}

impl<A, B> StorageIterator for TwoMergeIterator<A, B>
where
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if !self.a.is_valid() {
            return self.b.key();
        }
        if !self.b.is_valid() {
            return self.a.key();
        }
        if self.a.key() <= self.b.key() {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if !self.a.is_valid() {
            return self.b.value();
        }
        if !self.b.is_valid() {
            return self.a.value();
        }
        if self.a.key() <= self.b.key() {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if !self.a.is_valid() {
            self.b.next()?;
            return Ok(());
        }
        if !self.b.is_valid() {
            self.a.next()?;
            return Ok(());
        }

        debug_assert!(self.a.is_valid() && self.b.is_valid());

        // Why I have to declare the scope explicitly
        // If I don't do this, Rust's borrow checker will yell at me
        let cmp = { self.a.key().cmp(&self.b.key()) };

        match cmp {
            std::cmp::Ordering::Less => self.a.next()?,
            std::cmp::Ordering::Greater => self.b.next()?,
            std::cmp::Ordering::Equal => {
                self.a.next()?;
                self.b.next()?;
            }
        }

        Ok(())
    }

    /// the sum of `num_active_iterators` of all children iterators
    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
