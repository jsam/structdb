use std::fmt::{Debug, Display};

use byte_counter::counter::ByteCounter;
use serde::{Deserialize, Serialize};

use crate::{serialization::BinCode, topic::TOPIC_KEY_PREFIX};

pub type Record = Vec<u8>;

impl BinCode for Record {}

#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct SeqRecord {
    pub key: ByteCounter,
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
            let key_str = String::from_utf8_lossy(key).to_string();
            let key = ByteCounter::from(&key_str);
            let value = value.to_vec();

            Self { key, value }
        } else {
            Self {
                key: ByteCounter::default(),
                value: vec![],
            }
        }
    }
}

impl SeqRecord {
    pub fn new(key: ByteCounter, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    pub fn size(&self) -> usize {
        self.value.len()
    }

    pub fn is_valid(&self) -> bool {
        self.key.to_string().starts_with(TOPIC_KEY_PREFIX)
    }
}
