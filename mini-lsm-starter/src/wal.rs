// REMOVE THIS LINE after fully implementing this functionality
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

use std::io::{BufWriter, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

// | batch_size (u32) | key_len (u16) | key | ts (u64) | value_len (u16) | value | ... | checksum (u32) |
pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let display_path = format!("{}", path.as_ref().display());
        File::create_new(path)
            .map(BufWriter::new)
            .map(Mutex::new)
            .map(Arc::new)
            .map(|file| Self { file })
            .with_context(|| format!("failed to create wal file {display_path}"))
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open wal file {}", path.as_ref().display()))?;
        let mut buf = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut buf)?;

        let mut buf = buf.as_slice();
        while buf.has_remaining() {
            let batch_size = buf.get_u32() as usize;
            if buf.len() < batch_size + size_of::<u32>() {
                bail!("incomplete WAL")
            }
            let mut batch = &buf[..batch_size];
            buf.advance(batch_size);
            let checksum = buf.get_u32();
            if crc32fast::hash(batch) != checksum {
                bail!("checksum mismatched")
            }

            while batch.has_remaining() {
                let key_len = batch.get_u16() as usize;
                let key = batch.copy_to_bytes(key_len);
                let ts = batch.get_u64();
                let value_len = batch.get_u16() as usize;
                let value = batch.copy_to_bytes(value_len);
                skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
            }
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Note that data is persisted only when `sync` is called.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let batch_size: usize = data
            .iter()
            .map(|&(k, v)| 2 * size_of::<u16>() + size_of::<u64>() + k.key_len() + v.len())
            .sum();
        let total_size = batch_size + size_of::<u32>() * 2;
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_u32(batch_size as u32);
        for (key, value) in data {
            buf.put_u16(key.key_len() as u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value.len() as u16);
            buf.put_slice(value);
        }
        let checksum = crc32fast::hash(&buf[size_of::<u32>()..]);
        buf.put_u32(checksum);
        let encoded = buf.freeze();
        self.file.lock().write_all(&encoded)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?; // flush all data
        file.get_mut().sync_all()?; // sync content and metadata
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.file.lock().get_ref().metadata().unwrap().len()
    }
}
