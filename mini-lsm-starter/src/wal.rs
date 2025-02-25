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

use std::io::{BufReader, BufWriter, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        File::create_new(path)
            .map(BufWriter::new)
            .map(Mutex::new)
            .map(Arc::new)
            .map(|file| Self { file })
            .context("failed to create wal file")
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .context("failed to open wal file")?;
        let mut reader = BufReader::new(&file);
        let (mut bytes_read, file_len) = (0, file.metadata()?.len());
        while bytes_read < file_len {
            let mut key_len = [0; size_of::<u32>()];
            reader.read_exact(&mut key_len)?;
            let key_len = u32::from_be_bytes(key_len);
            let mut key = BytesMut::zeroed(key_len as usize);
            reader.read_exact(&mut key)?;
            let mut value_len = [0; size_of::<u32>()];
            reader.read_exact(&mut value_len)?;
            let value_len = u32::from_be_bytes(value_len);
            let mut value = BytesMut::zeroed(value_len as usize);
            reader.read_exact(&mut value)?;
            skiplist.insert(key.freeze(), value.freeze());
            bytes_read += 2 * size_of::<u32>() as u64 + key_len as u64 + value_len as u64;
        }
        assert_eq!(
            bytes_read, file_len,
            "don't manipulate lsm engine when wal recovery is running on"
        );
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Note that data is persisted only when `sync` is called.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // | key_len | key | value_len | value |
        let mut file = self.file.lock();
        let (key_len, value_len) = (key.len() as u32, value.len() as u32);
        file.write_all(&key_len.to_be_bytes())?;
        file.write_all(key)?;
        file.write_all(&value_len.to_be_bytes())?;
        file.write_all(value)?;
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
