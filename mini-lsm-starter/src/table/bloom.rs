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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Implements a bloom filter
// |------------------------|
// | filter ([u8]) | k (u8) |
// |------------------------|
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        // | varlen | u8 | u32 |
        let filter_length = buf.len() - size_of::<u8>() - size_of::<u32>();
        let filter = &buf[..filter_length];
        let k = buf[filter_length];
        let checksum = buf[filter_length + size_of::<u8>()..].as_ref().get_u32();
        if crc32fast::hash(filter) != checksum {
            bail!("checksum dismatch")
        }
        Ok(Self {
            filter: Bytes::copy_from_slice(filter),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend(&self.filter);
        buf.put_u8(self.k);
        buf.put_u32(crc32fast::hash(&self.filter));
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.clamp(1, 30);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        // TODO: build the bloom filter

        // 使用单个 hash 值生成多个 hash 索引
        // 因为使用 k 个不同的 hash 函数太慢了，通常改用 double hashing
        // Hi(x) = (H1(x) + i * H2(x)) mod m
        // H1(x) 是 x 的初始 hash 值，H2(x) 是扰动值
        for key in keys {
            let mut h = *key;
            let delta = h.rotate_left(15);
            for _ in 0..k {
                let idx = h as usize % nbits;
                filter.set_bit(idx, true);
                h = h.wrapping_add(delta);
            }
        }

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = h.rotate_left(15);

            // TODO: probe the bloom filter
            for _ in 0..self.k {
                let idx = h as usize % nbits;
                if !self.filter.get_bit(idx) {
                    return false;
                }
                h = h.wrapping_add(delta);
            }

            true
        }
    }
}

#[cfg(test)]
mod my_tests {
    use super::*;

    fn key(id: usize) -> String {
        format!("key_{:05}", id)
    }

    #[test]
    fn bloom_insert() {
        let size = 10000;
        let mut key_hashes = Vec::with_capacity(size);

        for id in 0..size {
            key_hashes.push(farmhash::fingerprint32(key(id).as_bytes()));
        }

        let bits_per_key = Bloom::bloom_bits_per_key(size, 0.01);
        let bloom = Bloom::build_from_key_hashes(&key_hashes, bits_per_key);

        for id in 0..size {
            assert!(bloom.may_contain(farmhash::fingerprint32(key(id).as_bytes())));
        }

        let mut false_posotive_cnt = 0;
        let attempts = 1_000_000;
        for id in (size..).take(attempts) {
            if bloom.may_contain(farmhash::fingerprint32(key(id).as_bytes())) {
                false_posotive_cnt += 1;
            }
        }

        assert!((false_posotive_cnt as f64 / attempts as f64) < 0.015);
    }
}
