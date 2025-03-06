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

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end: Bound<Bytes>,
    prev_key: Vec<u8>,
    is_valid: bool,
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(inner: LsmIteratorInner, end: Bound<Bytes>, read_ts: u64) -> Result<Self> {
        let mut iter = Self {
            is_valid: inner.is_valid(),
            inner,
            end,
            read_ts,
            prev_key: Vec::new(),
        };
        iter.move_to_key()?;
        Ok(iter)
    }

    /// advance the inner iterator unless
    /// 1. inner iterator is invalid
    /// 2. exceed the end bound
    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.end {
            Bound::Unbounded => {}
            Bound::Included(ref key) => self.is_valid = self.inner.key().key_ref() <= key.as_ref(),
            Bound::Excluded(ref key) => self.is_valid = self.inner.key().key_ref() < key.as_ref(),
        }
        Ok(())
    }

    fn move_to_key(&mut self) -> Result<()> {
        loop {
            // skip identical keys with older version
            while self.is_valid && self.inner.key().key_ref() == self.prev_key {
                self.next_inner()?;
            }
            if !self.is_valid {
                break;
            }
            self.prev_key.clear();
            self.prev_key.extend(self.inner.key().key_ref());
            // 1/3 1/2 1/1 2/4 2/2 read_ts = 2
            //      |       |
            // last_key  prev_key
            // skip identical keys with newer timestamp
            while self.is_valid
                && self.inner.key().key_ref() == self.prev_key
                && self.inner.key().ts() > self.read_ts
            {
                self.next_inner()?;
            }
            if !self.is_valid {
                break;
            }
            // oops! we skip all the keys and stop at the next different key
            if self.inner.key().key_ref() != self.prev_key {
                continue;
            }
            // skip deleted keys
            if !self.inner.value().is_empty() {
                break;
            }
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_key()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow::anyhow!("the iterator has fused"));
        }
        if !self.iter.is_valid() {
            return Ok(());
        }
        if self.iter.next().is_err() {
            self.has_errored = true;
            return Err(anyhow::anyhow!("the iterator has fused"));
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
