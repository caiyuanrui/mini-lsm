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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use builder::OFFSET_SIZE;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Debug, Default)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        // Data Section
        bytes.put_slice(&self.data);
        // Offset Section
        for &offset in &self.offsets {
            bytes.put_u16(offset);
        }
        // Number Of Elements
        bytes.put_u16(self.offsets.len() as u16);
        bytes.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let extra_start = data.len() - size_of::<u16>();
        let num_of_elements = (&data[extra_start..]).get_u16();
        let offset_start = extra_start - OFFSET_SIZE * num_of_elements as usize;
        Self {
            data: data[..offset_start].to_vec(),
            offsets: data[offset_start..extra_start]
                .chunks(OFFSET_SIZE)
                .map(|mut chunk| chunk.get_u16())
                .collect(),
        }
    }
}
