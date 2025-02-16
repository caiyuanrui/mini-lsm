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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::io::{Read, Seek};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for block_meta in block_meta {
            buf.put_u32(block_meta.offset as u32);
            buf.put_u16(block_meta.first_key.len() as u16);
            buf.put_slice(block_meta.first_key.raw_ref());
            buf.put_u16(block_meta.last_key.len() as u16);
            buf.put_slice(block_meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16();
            let first_key = KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                buf.chunk()[..first_key_len as usize].as_ref(),
            ));
            buf.advance(first_key_len as usize);
            let last_key_len = buf.get_u16();
            let last_key = KeyBytes::from_bytes(bytes::Bytes::copy_from_slice(
                buf.chunk()[..last_key_len as usize].as_ref(),
            ));
            buf.advance(last_key_len as usize);

            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let mut file_handler = file.0.ok_or(anyhow::anyhow!("file object is none"))?;
        let file_size = file.1;

        // read sst into memory
        let bytes = {
            let mut bytes = vec![0u8; file_size as usize];
            file_handler.read_exact(&mut bytes)?;
            bytes
        };

        const BLOOM_OFFSET_SIZE: usize = size_of::<u32>();
        const BLOCK_META_OFFSET_SIZE: usize = size_of::<u32>();

        let bloom_offset =
            (&bytes[file_size as usize - BLOCK_META_OFFSET_SIZE..]).get_u32() as usize;
        let bloom = Bloom::decode(&bytes[bloom_offset..file_size as usize - BLOOM_OFFSET_SIZE])?;

        let block_meta_offset = bytes[bloom_offset - BLOCK_META_OFFSET_SIZE..bloom_offset]
            .as_ref()
            .get_u32() as usize;
        let block_meta = BlockMeta::decode_block_meta(
            &bytes[block_meta_offset..bloom_offset - BLOCK_META_OFFSET_SIZE],
        );

        let first_key = block_meta
            .first()
            .map(|m| m.first_key.clone())
            .unwrap_or_default();
        let last_key = block_meta
            .last()
            .map(|m| m.last_key.clone())
            .unwrap_or_default();

        file_handler.rewind()?;

        Ok(Self {
            file: FileObject(Some(file_handler), file_size),
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0u64,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let file = match self.file.0 {
            Some(ref f) => f,
            None => return Err(anyhow::anyhow!("file doesn't exist")),
        };

        if block_idx >= self.block_meta.len() {
            return Err(anyhow::anyhow!(
                "block_idx is out of range: {block_idx} >= {}",
                self.block_meta.len()
            ));
        }

        let offset = self.block_meta[block_idx].offset;
        let length = self
            .block_meta
            .get(block_idx + 1)
            .map(|m| m.offset)
            .unwrap_or(self.block_meta_offset)
            - offset;
        let mut buf = vec![0; length];
        file.read_exact_at(&mut buf, offset as u64)?;

        Ok(Arc::new(Block::decode(buf.as_ref())))
    }

    /// Read a block from disk, with block cache. (Day 4)
    /// Blocks are cached by cache key (sst_id, block_id)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match self.block_cache {
            Some(ref cache) => cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|err| anyhow::anyhow!(err)),
            None => self.read_block(block_idx),
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .binary_search_by_key(&key, |meta| meta.first_key.as_key_slice())
            .unwrap_or_else(std::convert::identity)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn may_contain_key(&self, key: &[u8]) -> bool {
        match self.bloom.as_ref() {
            None => true,
            Some(bloom) => bloom.may_contain(farmhash::fingerprint32(key)),
        }
    }
}

#[cfg(test)]
mod my_tests {
    use super::*;

    #[test]
    fn test_block_meta_encoding_and_decoding() {
        let block_meta = vec![
            BlockMeta {
                offset: 42,
                first_key: KeyBytes::from_bytes(bytes::Bytes::from_static(b"hello")),
                last_key: KeyBytes::from_bytes(bytes::Bytes::from_static(b"23333")),
            },
            BlockMeta {
                offset: 69,
                first_key: KeyBytes::from_bytes(bytes::Bytes::from_static(&[1; u16::MAX as usize])),
                last_key: KeyBytes::from_bytes(bytes::Bytes::from_static(&[2; u16::MAX as usize])),
            },
        ];

        let mut buf = Vec::new();
        BlockMeta::encode_block_meta(block_meta.as_ref(), &mut buf);
        let decoded = BlockMeta::decode_block_meta(buf.as_ref());

        assert_eq!(block_meta, decoded);
    }
}
