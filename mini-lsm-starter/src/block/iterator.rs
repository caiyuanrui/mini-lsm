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
        self.seek_to_index(0);
        self.first_key = self.key().to_key_vec();
    }

    /// Seeks to the last key in the block.
    pub fn seek_to_last(&mut self) {
        let num_of_elements = self.block.offsets.len();
        self.seek_to_index(num_of_elements - 1);
    }

    /// Will mark the iterator as invalid if `index` is out of range `[0, num_of_elements)`.
    pub fn seek_to_index(&mut self, index: usize) {
        let num_of_elements = self.block.offsets.len();
        if index >= num_of_elements {
            self.key.clear();
            return;
        }

        let offset = self.block.offsets[index] as usize;
        let key_begin = offset + KEY_LEN_SIZE;
        let key_len = self.block.data[offset..key_begin].as_ref().get_u16() as usize;
        let key_end = key_begin + key_len;
        let key = KeyVec::from_vec(self.block.data[key_begin..key_end].to_vec());
        let value_begin = key_end + VAL_LEN_SIZE;
        let value_len = self.block.data[key_end..value_begin].as_ref().get_u16() as usize;
        let value_end = value_begin + value_len;

        self.idx = index;
        self.key = key;
        self.value_range = (value_begin, value_end);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }
        self.seek_to_index(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let (mut lower, mut upper) = (0, self.block.offsets.len());
        while lower < upper {
            let mid = (lower + upper) / 2;
            let offset = self.block.offsets[mid] as usize;
            let key_len = self.block.data[offset..offset + KEY_LEN_SIZE]
                .as_ref()
                .get_u16() as usize;
            let mid_key =
                self.block.data[offset + KEY_LEN_SIZE..offset + KEY_LEN_SIZE + key_len].as_ref();
            if mid_key < key.into_inner() {
                lower = mid + 1;
            } else {
                upper = mid;
            }
        }

        if lower >= self.block.offsets.len() {
            self.key.clear();
            return;
        }

        let offset = self.block.offsets[lower] as usize;

        let key_len = self.block.data[offset..offset + 2].as_ref().get_u16() as usize;
        let key_end = offset + 2 + key_len;
        let key = &self.block.data[offset + 2..key_end];
        let value_len = self.block.data[key_end..key_end + 2].as_ref().get_u16() as usize;
        let value_begin = key_end + 2;

        self.idx = offset;
        self.key = KeyVec::from_vec(key.to_vec());
        self.value_range = (value_begin, value_begin + value_len);
    }
}
