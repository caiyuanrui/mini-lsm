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
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open wal file {}", path.as_ref().display()))?;
        let mut buf = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut buf)?;
        let mut cursor = Cursor::new(buf);
        while cursor.has_remaining() {
            let key_len = cursor.get_u32();
            let key = cursor.copy_to_bytes(key_len as usize);
            let value_len = cursor.get_u32();
            let value = cursor.copy_to_bytes(value_len as usize);
            let checksum = cursor.get_u32();
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&key_len.to_be_bytes());
            hasher.update(&key);
            hasher.update(&value_len.to_be_bytes());
            hasher.update(&value);
            if checksum != hasher.finalize() {
                bail!("wal checksum mismatch")
            }
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Note that data is persisted only when `sync` is called.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // | key_len | key | value_len | value | checksum |
        let mut file = self.file.lock();
        let (key_len, value_len) = (key.len() as u32, value.len() as u32);
        file.write_all(&key_len.to_be_bytes())?;
        file.write_all(key)?;
        file.write_all(&value_len.to_be_bytes())?;
        file.write_all(value)?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&key_len.to_be_bytes());
        hasher.update(key);
        hasher.update(&value_len.to_be_bytes());
        hasher.update(value);
        file.write_all(&hasher.finalize().to_be_bytes())?;
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
