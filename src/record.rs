use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};
use vlseqid::id::BigID;

use crate::{serialization::BinCode, timestamp::Timestamp, topic::TOPIC_KEY_PREFIX};

pub type Record = Vec<u8>;

impl BinCode for Record {}

#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct SeqRecord {
    pub key: BigID,
    pub value: Record,
}

impl Debug for SeqRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeqRecord")
            .field("key", &self.key.to_string())
            .field("value", &self.value)
            .finish()
    }
}

impl Display for SeqRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeqRecord")
            .field("key", &self.key.to_string())
            .field("value", &self.value)
            .finish()
    }
}

impl From<Option<(&[u8], &[u8])>> for SeqRecord {
    fn from(value: Option<(&[u8], &[u8])>) -> Self {
        if let Some((key, value)) = value {
            let kk = std::str::from_utf8(key).unwrap();
            let key = BigID::from(kk);
            let value = value.to_vec();

            Self { key, value }
        } else {
            Self {
                key: BigID::default(),
                value: vec![],
            }
        }
    }
}

impl SeqRecord {
    pub fn new(key: BigID, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    pub fn size(&self) -> usize {
        self.value.len()
    }

    pub fn is_valid(&self) -> bool {
        self.key.to_string().starts_with(TOPIC_KEY_PREFIX)
    }
}

pub struct KVRecord {
    pub key: String,
    pub value: Box<[u8]>,

    pub timestamp: Timestamp,
}

impl KVRecord {
    pub fn key(&mut self) -> &[u8] {
        let value = self.key.as_bytes();
        value
    }

    pub fn value(&mut self) -> &[u8] {
        let value = self.value.as_ref();
        value
    }
}
