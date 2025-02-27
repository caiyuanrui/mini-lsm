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

use std::{io::Cursor, sync::Arc};

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
#[derive(Debug, Default)]
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: usize::MAX,
            first_key: KeyVec::new(),
        };
        // load the first key immediately in case I forget to load it later...
        iter.load_first_key();
        iter
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut ret = Self::new(block);
        ret.seek_to_first();
        ret
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut ret = Self::new(block);
        ret.seek_to_key(key);
        ret
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_index(0);
    }

    /// Seeks to the last key in the block.
    pub fn seek_to_last(&mut self) {
        self.seek_to_index(self.block.offsets.len() - 1);
    }

    fn load_first_key(&mut self) {
        // key_overlap_len (u16) | key_overlap (0) | rest_key_len (u16) | rest_key (rest_key_len) | timestamp (u64)
        let mut cursor = Cursor::new(&self.block.data);
        let key_overlap_len = cursor.get_u16();
        assert_eq!(key_overlap_len, 0);
        let key_len = cursor.get_u16();
        let key = cursor.copy_to_bytes(key_len as usize);
        let ts = cursor.get_u64();
        // Bytes -> Vec<u8> is a shallow copy
        self.first_key = KeyVec::from_vec_with_ts(key.into(), ts);
    }

    /// Will mark the iterator as invalid if `index` is out of range `[0, num_of_elements)`.
    pub fn seek_to_index(&mut self, index: usize) {
        let num_of_elements = self.block.offsets.len();
        if index >= num_of_elements {
            self.key.clear();
            return;
        }

        let offset = self.block.offsets[index] as usize;
        let mut cursor = Cursor::new(&self.block.data[offset..]);

        let overlap_key_len = cursor.get_u16();
        let overlap_key = &self.first_key.key_ref()[..overlap_key_len as usize];
        let rest_key_len = cursor.get_u16();
        let mut key = Vec::with_capacity(overlap_key_len as usize + rest_key_len as usize);
        key.extend_from_slice(overlap_key);
        cursor.copy_to_slice(&mut key[overlap_key_len as usize..]);
        let ts = cursor.get_u64();
        let key = KeyVec::from_vec_with_ts(key, ts);

        let value_len = cursor.get_u16();
        let value_offset = offset + cursor.position() as usize;
        self.idx = index;
        self.key = key;
        self.value_range = (value_offset, value_offset + value_len as usize);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_index(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let (mut lower, mut upper) = (0, self.block.offsets.len());

        while lower < upper {
            let index = (lower + upper) / 2;
            self.seek_to_index(index);
            if self.key() >= key {
                upper = index;
            } else {
                lower = index + 1;
            }
        }

        if lower == self.block.offsets.len() {
            self.key.clear();
            return;
        }

        self.seek_to_index(lower);
    }
}
