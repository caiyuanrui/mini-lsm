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

pub mod txn;
pub mod watermark;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{atomic::AtomicBool, Arc},
};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::lsm_storage::LsmStorageInner;

use self::{txn::Transaction, watermark::Watermark};

#[derive(Debug)]
pub(crate) struct CommittedTxnData {
    pub(crate) keys: BTreeSet<Bytes>,
    // pub(crate) key_hashes: HashSet<u32>,
    #[allow(dead_code)]
    pub(crate) read_ts: u64,
    #[allow(dead_code)]
    pub(crate) commit_ts: u64,
}

impl CommittedTxnData {
    pub(crate) const fn new(
        keys: BTreeSet<Bytes>,
        // key_hashes: HashSet<u32>,
        read_ts: u64,
        commit_ts: u64,
    ) -> Self {
        Self {
            keys,
            // key_hashes,
            read_ts,
            commit_ts,
        }
    }
}

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().0 = ts;
    }

    /// All ts (strictly) below this ts can be garbage collected.
    pub fn watermark(&self) -> u64 {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }

    pub fn new_txn(&self, inner: Arc<LsmStorageInner>, serializable: bool) -> Arc<Transaction> {
        let mut ts = self.ts.lock();
        let read_ts = ts.0;
        ts.1.add_reader(read_ts);
        Arc::new(Transaction {
            read_ts,
            inner,
            local_storage: Default::default(),
            committed: Arc::new(AtomicBool::new(false)),
            key_hashes: if serializable {
                Some(Default::default())
            } else {
                None
            },
        })
    }
}
