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
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Context, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
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
        let f = std::fs::File::open(path.as_ref())?;
        let reader = std::io::BufReader::new(f);
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<ManifestRecord>();
        let mut records = Vec::new();
        for value in stream {
            records.push(value?);
        }
        let file = std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .map(|file| Arc::new(Mutex::new(file)))?;
        Ok((Self { file }, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let buf = serde_json::to_vec(&record)?;
        let mut file = self.file.lock();
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
}
