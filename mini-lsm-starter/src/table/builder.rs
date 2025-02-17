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

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::BufMut;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::{BlockBuilder, BlockIterator},
    key::KeySlice,
    lsm_storage::BlockCache,
};

const SST_SIZE: usize = 512 * 1024 * 1024;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::with_capacity(SST_SIZE),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));

        while !self.builder.add(key, value) {
            self.freeze_block_builder();
        }
    }

    /// Build block_builder and append the generated block and metadata to the vector.
    /// Will do nothing if current builder is empty
    fn freeze_block_builder(&mut self) {
        if self.builder.is_empty() {
            return;
        }

        let block_builder =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = Arc::new(block_builder.build());

        let mut iter = BlockIterator::create_and_seek_to_first(block.clone());
        let first_key = iter.key().to_key_vec().into_key_bytes();
        iter.seek_to_last();
        let last_key = iter.key().to_key_vec().into_key_bytes();

        let meta = BlockMeta {
            offset: self.data.len(),
            first_key,
            last_key,
        };

        self.data.put_slice(block.encode().as_ref());
        self.meta.push(meta);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.builder.is_empty()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.freeze_block_builder();

        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();

        let mut bytes = Vec::new();
        // extend from blocks data
        bytes.extend_from_slice(self.data.as_ref());
        // extend from metadata
        BlockMeta::encode_block_meta(self.meta.as_ref(), &mut bytes);
        // put extra metablock offset
        bytes.put_u32(self.data.len() as u32);

        let bloom_filter_offset = bytes.len() as u32;
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom_filter = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
        bloom_filter.encode(bytes.as_mut());
        bytes.put_u32(bloom_filter_offset);

        // create a file and write bytes into it
        let file = FileObject::create(path.as_ref(), bytes)
            .map_err(|e| anyhow!("failed to create file {}: {:?}", path.as_ref().display(), e))?;

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: self.data.len(),
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
