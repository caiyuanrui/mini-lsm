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

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut sst_iter = Self {
            table,
            blk_iter: Default::default(),
            blk_idx: Default::default(),
        };
        sst_iter.seek_to_first()?;
        Ok(sst_iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_iter = BlockIterator::create_and_seek_to_first(self.table.read_block(0)?);
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut sst_iter = Self {
            table,
            blk_iter: BlockIterator::default(),
            blk_idx: 0,
        };
        sst_iter.seek_to_key(key)?;
        Ok(sst_iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        // find the block whose first_key is equal or greater than key
        // 1. first_key == key: happend to be what we want
        // 2. first_key > key: the right pivot might be in the previous block
        let blk_idx = self.table.find_block_idx(key);
        println!("blk_idx: {blk_idx}");

        // if there is not such block whose first key-value pair is >= `key`
        // search this key in the last block
        if blk_idx == self.table.block_meta.len() {
            let block = self.table.read_block(blk_idx - 1)?;
            self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
            self.blk_idx = blk_idx - 1;
            return Ok(());
        }

        let block_meta = &self.table.block_meta[blk_idx];
        if blk_idx == 0 || block_meta.first_key.as_key_slice() == key {
            let block = self.table.read_block(blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
            self.blk_idx = blk_idx;
        } else if block_meta.first_key.as_key_slice() > key {
            // attempt to search this key in the previous block
            let block = self.table.read_block(blk_idx - 1)?;
            let blk_iter = BlockIterator::create_and_seek_to_key(block, key);
            if blk_iter.is_valid() {
                self.blk_iter = blk_iter;
                self.blk_idx = blk_idx - 1;
                return Ok(());
            }
            // now we have first_key > key and prev_block.last_key < key
            let block = self.table.read_block(blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
            self.blk_idx = blk_idx;
        } else {
            unreachable!()
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() && self.blk_idx + 1 < self.table.block_meta.len() {
            self.blk_idx += 1;
            self.blk_iter =
                BlockIterator::create_and_seek_to_first(self.table.read_block(self.blk_idx)?);
        }
        Ok(())
    }
}
