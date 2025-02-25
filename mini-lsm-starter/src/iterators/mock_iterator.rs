#![cfg(test)]
#![allow(unused)]

use anyhow::{bail, Result};
use bytes::Bytes;

use crate::key::KeySlice;

use super::StorageIterator;

#[derive(Clone)]
pub(super) struct MockIterator {
    pub(super) data: Vec<(Bytes, Bytes)>,
    pub(super) error_when: Option<usize>,
    pub(super) index: usize,
}

impl MockIterator {
    pub(super) fn new(data: Vec<(Bytes, Bytes)>) -> Self {
        Self {
            data,
            index: 0,
            error_when: None,
        }
    }

    pub(super) fn new_with_error(data: Vec<(Bytes, Bytes)>, error_when: usize) -> Self {
        Self {
            data,
            index: 0,
            error_when: Some(error_when),
        }
    }
}

impl StorageIterator for MockIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn next(&mut self) -> Result<()> {
        if self.index < self.data.len() {
            self.index += 1;
        }
        if let Some(error_when) = self.error_when {
            if self.index == error_when {
                bail!("fake error!");
            }
        }
        Ok(())
    }

    fn key(&self) -> KeySlice {
        if let Some(error_when) = self.error_when {
            if self.index >= error_when {
                panic!("invalid access after next returns an error!");
            }
        }
        KeySlice::for_testing_from_slice_no_ts(self.data[self.index].0.as_ref())
    }

    fn value(&self) -> &[u8] {
        if let Some(error_when) = self.error_when {
            if self.index >= error_when {
                panic!("invalid access after next returns an error!");
            }
        }
        self.data[self.index].1.as_ref()
    }

    fn is_valid(&self) -> bool {
        if let Some(error_when) = self.error_when {
            if self.index >= error_when {
                panic!("invalid access after next returns an error!");
            }
        }
        self.index < self.data.len()
    }
}

pub(crate) fn key_of(x: u32) -> Bytes {
    let len = x.to_string().len();
    let key = format!("key_{:0len$}", x);
    Bytes::copy_from_slice(key.as_bytes())
}

pub(crate) fn value_of(x: u32) -> Bytes {
    let len = x.to_string().len();
    let value = format!("value_{:0len$}", x);
    Bytes::copy_from_slice(value.as_bytes())
}

pub(crate) fn to_string(bytes: impl AsRef<[u8]>) -> String {
    unsafe { String::from_utf8_unchecked(bytes.as_ref().to_vec()) }
}
