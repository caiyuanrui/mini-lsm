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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

// After prefix encoding, the key's layout becomes like this
// | key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len) | timestamp (u64) | value_len (u16) | value (value_len) |

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::with_capacity(block_size),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    pub fn key_overlap_len(&self, key: &[u8]) -> usize {
        self.first_key
            .key_ref()
            .iter()
            .zip(key.iter())
            .take_while(|(b1, b2)| b1 == b2)
            .count()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// Data is stored in big-endian byte order.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        const KEY_OVERLAP_LEN_SIZE: usize = size_of::<u16>();
        const REST_KEY_LEN_SIZE: usize = size_of::<u16>();
        const TIMESTAMP_SIZE: usize = size_of::<u64>();
        const VALUE_LEN_SIZE: usize = size_of::<u16>();
        const OFFSET_SIZE: usize = size_of::<u16>();

        let key_overlap_len = self.key_overlap_len(key.key_ref());
        let rest_key_len = key.key_len() - key_overlap_len;
        let estimated_size = KEY_OVERLAP_LEN_SIZE
            + REST_KEY_LEN_SIZE
            + rest_key_len
            + TIMESTAMP_SIZE
            + VALUE_LEN_SIZE
            + value.len()
            + OFFSET_SIZE;

        if self.is_empty() {
            assert_eq!(key_overlap_len, 0);
            self.first_key = key.to_key_vec();
        } else if self.current_size() + estimated_size > self.block_size {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        // | key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len) | timestamp (u64) |
        self.data.put_u16(key_overlap_len as u16);
        self.data.put_u16(rest_key_len as u16);
        self.data.put_slice(&key.key_ref()[key_overlap_len..]);
        self.data.put_u64(key.ts());
        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    /// Data Section + Offset Section + Extra
    fn current_size(&self) -> usize {
        self.data.len() + self.offsets.len() * size_of::<u16>() + size_of::<u16>()
    }
}
