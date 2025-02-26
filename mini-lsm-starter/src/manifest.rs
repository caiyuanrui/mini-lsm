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

use std::fs::OpenOptions;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create manifest")?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover manifest file")?;
        let mut records = Vec::new();
        let mut buf = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut buf)?;
        let mut cursor = Cursor::new(buf);
        while cursor.has_remaining() {
            let len = cursor.get_u32();
            let json_bytes = cursor.copy_to_bytes(len as usize);
            let checksum = cursor.get_u32();
            if crc32fast::hash(&json_bytes) != checksum {
                bail!("manifest checksum mismatch")
            }
            records.push(serde_json::from_slice(&json_bytes)?);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let json = serde_json::to_vec(&record)?;
        let mut buf = Vec::with_capacity(json.len() + 2 * size_of::<u32>());
        buf.put_u32(json.len() as u32);
        buf.put_slice(&json);
        buf.put_u32(crc32fast::hash(&json));

        let mut file = self.file.lock();
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
}
