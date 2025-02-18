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
// type LsmIteratorInner = MergeIterator<MemTableIterator>;
// type LsmIteratorInner =
//     TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(inner: LsmIteratorInner, end: Bound<&[u8]>) -> Result<Self> {
        let end = match end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
            Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        };
        let mut iter = Self { inner, end };
        iter.skip_delete_keys()?;
        Ok(iter)
    }

    /// If the current value is empty, skip to the next iter with different key.
    /// Otherwise, do nothing.
    fn skip_delete_keys(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.next()?;
        }
        Ok(())
    }

    fn is_valid(&self) -> bool {
        if !self.inner.is_valid() {
            return false;
        }

        match self.end {
            Bound::Included(ref x) => self.inner.key().raw_ref() <= x,
            Bound::Excluded(ref x) => self.inner.key().raw_ref() < x,
            Bound::Unbounded => true,
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.skip_delete_keys()?;

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
