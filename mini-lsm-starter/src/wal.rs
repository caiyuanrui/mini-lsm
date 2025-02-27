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

use std::io::{BufWriter, Cursor, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

// | key_len (exclude ts len) (u16) | key | ts (u64) | value_len (u16) | value | checksum (u32) |
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
        let mut cursor = Cursor::new(buf.as_slice());
        while cursor.has_remaining() {
            let offset = cursor.position() as usize;
            let key_len = cursor.get_u16();
            let key = cursor.copy_to_bytes(key_len as usize);
            let ts = cursor.get_u64();
            let value_len = cursor.get_u16();
            let value = cursor.copy_to_bytes(value_len as usize);
            let hash = crc32fast::hash(&buf[offset..cursor.position() as usize]);
            let checksum = cursor.get_u32();
            if checksum != hash {
                bail!("wal checksum mismatch")
            }
            skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Note that data is persisted only when `sync` is called.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        // | key_len | key | ts (u64) | value_len | value | checksum |
        let mut buf = BytesMut::with_capacity(key.raw_len() + value.len());
        buf.put_u16(key.key_len() as u16);
        buf.put_slice(key.key_ref());
        buf.put_u64(key.ts());
        buf.put_u16(value.len() as u16);
        buf.put_slice(value);
        buf.put_u32(crc32fast::hash(&buf));
        let mut file = self.file.lock();
        file.write_all(&buf.freeze())?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
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
