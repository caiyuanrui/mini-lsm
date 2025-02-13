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

use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::{
    builder::{KEY_LEN_SIZE, VAL_LEN_SIZE},
    Block,
};

/// Iterates on a block.
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
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
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
        let first_key_start = KEY_LEN_SIZE;
        let first_key_len = self.block.data[..first_key_start].as_ref().get_u16() as usize;
        let first_key_end = first_key_start + first_key_len;
        let first_key = KeyVec::from_vec(self.block.data[first_key_start..first_key_end].to_vec());
        let first_value_start = first_key_end + VAL_LEN_SIZE;
        let first_value_len = self.block.data[first_key_end..first_value_start]
            .as_ref()
            .get_u16() as usize;
        let value_range = (first_value_start, first_value_start + first_value_len);

        self.key = first_key.clone();
        self.first_key = first_key;
        self.value_range = value_range;
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }

        println!(
            "num of elements: {}, idx: {}",
            self.block.offsets.len(),
            self.idx
        );

        if self.block.offsets.len() <= self.idx + 1 {
            self.key.clear();
            return;
        }

        let next_key_start = KEY_LEN_SIZE + self.value_range.1;
        let next_key_len = self.block.data[self.value_range.1..next_key_start]
            .as_ref()
            .get_u16() as usize;
        let next_key_end = next_key_start + next_key_len;
        let next_key = KeyVec::from_vec(self.block.data[next_key_start..next_key_end].to_vec());
        let next_value_start = next_key_end + VAL_LEN_SIZE;
        let next_value_len = self.block.data[next_key_end..next_value_start]
            .as_ref()
            .get_u16() as usize;
        let next_value_range = (next_value_start, next_value_start + next_value_len);

        self.idx += 1;
        self.key = next_key;
        self.value_range = next_value_range;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.is_valid() && self.key.as_key_slice() < key {
            self.next();
        }
    }
}
